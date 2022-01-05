use std::marker::PhantomData;

use log::{info, trace};

use crate::state_machine::StateMachine;
use crate::{error::CriticalError, persistent_log::Log};

use crate::config::{ConsensusConfig, ConsensusOptions};
//use crate::entry::{ConsensusConfig, Entry, EntryData};
use crate::error::Error;
use crate::handler::Handler;
use crate::raft::CurrentState;
use crate::state_impl::StateImpl;
use crate::{debug_where, message::*};
use crate::{LogIndex, ServerId, Term};

use crate::follower::Follower;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct State<M, H, S>
where
    M: StateMachine,
    H: Handler,
{
    // The ID of this consensus instance.
    pub(crate) id: ServerId,

    // The IDs of peers in the consensus group.
    pub(crate) config: ConsensusConfig,

    // The client state machine to which client commands are applied.
    pub(crate) state_machine: M,

    // Index of the latest entry known to be committed.
    pub(crate) commit_index: LogIndex,

    // The minimal index at which entries can be appended. This bit of state
    // allows avoiding overwriting of possibly committed parts of the log
    // when messages arrive out of order.
    // It is set when follower becomes leader and otherwise left untouched.
    // See see ktoso/akka-raft#66.
    pub(crate) min_index: LogIndex,

    // Index of the latest entry applied to the state machine.
    //    pub(crate) last_applied: LogIndex,

    // State-specific data
    pub(crate) state_data: S,

    pub(crate) options: ConsensusOptions,

    pub(crate) _h: PhantomData<H>,
}

/// Common functions for any state
impl<M, H, S> State<M, H, S>
where
    M: StateMachine,
    H: Handler,
{
    /// Returns the current term.
    pub(crate) fn current_term(&self) -> Result<Term, Error> {
        Ok(self
            .log()
            .current_term()
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?)
    }

    /// Increases the current term and resets the voted_for value.
    pub(crate) fn inc_current_term(&mut self) -> Result<(), Error> {
        let current = self
            .log()
            .current_term()
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
        self.log_mut()
            .set_voted_for(None)
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;
        self.log_mut()
            .set_current_term(current + 1)
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))
    }

    /// Helper for returning the term of the latest applied log entry.
    /// Hints are for cases when values were already read from log.
    #[inline]
    pub(crate) fn latest_log_term(
        &self,
        latest_index_hint: Option<LogIndex>,
    ) -> Result<Term, Error> {
        let latest_index = if let Some(index) = latest_index_hint {
            index
        } else {
            self.log()
                .latest_index()
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?
        };
        if latest_index == LogIndex(0) {
            return Ok(Term(0));
        }

        self.log()
            .term_of(latest_index)
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))
    }

    /// Helper for returning the index of the latest applied log entry.
    #[inline]
    pub(crate) fn latest_log_index(&self) -> Result<LogIndex, Error> {
        Ok(self
            .log()
            .latest_index()
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?)
    }

    /// Helper for self.state_machine.log()
    #[inline]
    pub(crate) fn log(&self) -> &M::Log {
        self.state_machine.log()
    }

    /// Helper for self.state_machine.log_mut()
    #[inline]
    pub(crate) fn log_mut(&mut self) -> &mut M::Log {
        self.state_machine.log_mut()
    }
}

// State change helpers
impl<M, H, S> State<M, H, S>
where
    M: StateMachine,
    H: Handler,
    Self: StateImpl<M, H>,
{
    pub(crate) fn into_follower(
        self,
        handler: &mut H,
        from: ConsensusState,
        leader_term: Term,
    ) -> Result<State<M, H, Follower>, Error> {
        trace!("id={} transitioning to follower", self.id);
        handler.state_changed(from, ConsensusState::Follower);

        let mut follower_state = State {
            id: self.id,
            config: self.config,
            state_machine: self.state_machine,
            commit_index: self.commit_index,
            min_index: self.min_index,
            _h: PhantomData,
            // this function is called when migrating from other states, meaning
            // node already was a candidate or a leader, meaning it definitely
            // can vote
            state_data: Follower::new(true),
            options: self.options,
        };

        // Apply all changes via the new state
        follower_state
            .log_mut()
            .set_current_term(leader_term)
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;

        follower_state.config.clear_heartbeats(handler);
        handler.set_timeout(Timeout::Election);

        Ok(follower_state)
    }

    // At some point all nodes behave in the same way regardless of their state
    pub(crate) fn common_request_vote_request(
        mut self,
        handler: &mut H,
        from: ServerId,
        request: RequestVoteRequest,
        from_state: ConsensusState,
    ) -> Result<(RequestVoteResponse, CurrentState<M, H>), Error> {
        let candidate_term = request.term;
        let candidate_log_term = request.last_log_term;
        let candidate_log_index = request.last_log_index;
        trace!(
            "RequestVoteRequest from id: {}, term: {}, latest_log_term: \
             {}, latest_log_index: {}, self: {:?}",
            &from,
            candidate_term,
            candidate_log_term,
            candidate_log_index,
            &from_state
        );

        let current_term = self.current_term()?;
        let (new_local_term, change_to_follower) = if candidate_term > current_term {
            info!(
                "id={} received RequestVoteRequest with newer term from Consensus {{ id: {}, term: {} }}; \
                 transitioning to Follower", self.id,
                 from, candidate_term
            );

            (candidate_term, true)
        } else {
            (current_term, false)
        };

        let latest_log_index = self.latest_log_index()?;
        let message = if candidate_term < current_term {
            RequestVoteResponse::StaleTerm(new_local_term)
        } else if candidate_log_term < self.latest_log_term(Some(latest_log_index))?
            || candidate_log_index < latest_log_index
        {
            RequestVoteResponse::InconsistentLog(new_local_term)
        } else {
            // candidate_term == current_term
            match self
                .log()
                .voted_for()
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?
            {
                None => {
                    self.log_mut().set_voted_for(Some(from)).map_err(|e| {
                        Error::Critical(CriticalError::PersistentLogWrite(Box::new(e)))
                    })?;
                    trace!("granted vote to {}", from);
                    RequestVoteResponse::Granted(new_local_term)
                }
                Some(voted_for) => {
                    trace!("found already voted for {}", voted_for);
                    if voted_for == from {
                        RequestVoteResponse::Granted(new_local_term)
                    } else {
                        RequestVoteResponse::AlreadyVoted(new_local_term)
                    }
                }
            }
        };

        if change_to_follower {
            let new_state = self.into_follower(handler, from_state, candidate_term)?;
            Ok((message, new_state.into()))
        } else {
            Ok((message, self.into()))
        }
    }

    pub(crate) fn common_client_ping_request(
        &self,
        kind: ConsensusState,
    ) -> Result<PingResponse, Error> {
        Ok(PingResponse {
            term: self.current_term()?,
            index: self.latest_log_index()?,
            state: kind,
        })
    }

    // checks snapshot only if forced,
    pub(crate) fn common_check_compaction(&mut self, force: bool) -> Result<bool, Error> {
        let info = self
            .state_machine
            .snapshot_info()
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

        if let Some(info) = info {
            if info.index != self.commit_index || self.commit_index != LogIndex(0) {
                trace!("not taking snapshot because log index did not change or commit index is not known yet");
                return Ok(false);
            }

            if self.commit_index < info.index {
                // snapshot is from later index, meaning it is corrupted
                return Err(Error::Critical(CriticalError::SnapshotCorrupted));
            } else if self.commit_index == info.index {
                // taking snapshot is not required
                return Ok(false);
            } else {
                if !force {
                    return Ok(false);
                }
            }
        }

        // we should get here if:
        // * commit_index > info.index
        // * snapshot_info is None
        // both meaning there is a point to make the snapshot
        let commit_term = self
            .log()
            .term_of(self.commit_index)
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
        self.state_machine
            .take_snapshot(self.commit_index, commit_term)
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;
        let cut_until = self.commit_index;
        self.log_mut()
            .discard_until(cut_until)
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;
        return Ok(true);
    }
}

#[cfg(test)]
mod test {
    /*
           use std::collections::HashSet;
           use std::collections::VecDeque;

           use super::*;
           use crate::persistent_log::mem::MemLog;
           use crate::state_machine::null::NullStateMachine;

           use crate::handler::CollectHandler;
           use pretty_env_logger;

           #[derive(Debug)]
           struct TestHandler(CollectHandler);

           impl Handler for TestHandler {
        /// Saves peer message to a vector
        fn send_peer_message(&mut self, id: ServerId, message: PeerMessage) {
        self.0.send_peer_message(id, message)
        }

        /// Saves client message to a vector
        fn send_client_response(&mut self, id: ClientId, message: ClientResponse) {
        self.0.send_client_response(id, message)
        }

        /// Collects timeouts uniquely
        fn set_timeout(&mut self, timeout: ConsensusTimeout) {
        self.0.set_timeout(timeout)
        }

        fn clear_timeout(&mut self, timeout: ConsensusTimeout) {
        self.0.clear_timeout(timeout)
        }

        fn state_changed(&mut self, old: CurrentState, new: CurrentState) {
        match (&old, &new) {
        (&CurrentState::Leader, &CurrentState::Candidate) => {
        panic!("Bad state transition: leader to candidate")
        }
        // TODO: this test is ok for single node
        // (&ConsensusState::Follower, &ConsensusState::Leader) => {
        //warn!("Bad state transition: follower to leader (ok for solitary transition)")
        //}
        (old, new) => trace!("state transition {:?} -> {:?}", old, new),
        }
        self.0.state_changed(old, new);
        }

        fn done(&mut self) {
        trace!("apply done")
        }
        }
        impl TestHandler {
        fn clear(&mut self) {
        self.0.clear()
        }
        }

        type TestPeer = HandledConsensus<MemLog, NullStateMachine, TestHandler>;

        #[derive(Debug)]
        struct TestCluster {
        pub peers: HashMap<ServerId, TestPeer>,
        }

        impl TestCluster {
        fn new(size: usize) -> Self {
        pretty_env_logger::try_init().unwrap_or_else(|_| ());
        let ids: Vec<ServerId> = (0..size).map(|i| (i as u64).into()).collect();
        let mut peers = HashMap::with_capacity(size);
        for i in 0..size {
        let mut ids = ids.clone();
        ids.remove(i); // remove self
        let id = ServerId(i as u64);
        let store = MemLog::new();
        let handler = TestHandler(CollectHandler::new());
        let mut consensus =
            HandledConsensus::new(id, ids, store, NullStateMachine, handler).unwrap();
        consensus.init();
        peers.insert(id, consensus);
    }
    Self { peers }
    }

    // Applies the actions to the consensus peers (recursively applying any resulting
    // actions) and returns any client messages and set timeouts.
    fn apply_peer_messages(
        &mut self,
    ) -> (
    HashMap<ServerId, HashSet<ConsensusTimeout>>,
    HashMap<ClientId, Vec<ClientResponse>>,
    ) {
        trace!("apply peer messages");
        let mut queue: VecDeque<(ServerId, ServerId, PeerMessage)> = VecDeque::new();
        let mut timeouts: HashMap<ServerId, HashSet<ConsensusTimeout>> = HashMap::new();
        let mut client_messages: HashMap<ClientId, Vec<ClientResponse>> = HashMap::new();
        for (peer, mut consensus) in self.peers.iter_mut() {
            for (to, messages) in consensus.handler.0.peer_messages.drain() {
                for message in messages.into_iter() {
                    queue.push_back((peer.clone(), to, message));
                }
            }

            let mut entry = timeouts.entry(peer.clone()).or_insert(HashSet::new());

            for timeout in consensus.handler.0.timeouts.clone() {
                if let ConsensusTimeout::Election = timeout {
                    entry.insert(timeout);
                }
            }

            client_messages.extend(consensus.handler.0.client_messages.clone());
            consensus.handler.clear();
        }
        trace!("Initial queue: {:?}", queue);
        while let Some((from, to, message)) = queue.pop_front() {
            let mut peer_consensus = self.peers.get_mut(&to).unwrap();
            peer_consensus.apply_peer_message(from, message).unwrap();
            for (to, messages) in peer_consensus.handler.0.peer_messages.drain() {
                for message in messages.into_iter() {
                    queue.push_back((peer_consensus.inner.id.clone(), to, message));
                }
            }

            trace!("Queue: {:?}", queue);
            let mut entry = timeouts
                .entry(peer_consensus.inner.id.clone())
                .or_insert(HashSet::new());
            for timeout in peer_consensus.handler.0.timeouts.clone() {
                if let ConsensusTimeout::Election = timeout {
                    entry.insert(timeout);
                }
            }

            client_messages.extend(peer_consensus.handler.0.client_messages.clone());
            peer_consensus.handler.clear();
        }
        (timeouts, client_messages)
    }

    fn into_peers(self) -> HashMap<ServerId, TestPeer> {
        self.peers
    }

    // Elect `leader` as the leader of a cluster with the provided followers.
    // The leader and the followers must be in the same term.
    fn elect_leader(&mut self, leader: ServerId) {
        {
            let leader_peer = self.peers.get_mut(&leader).unwrap();
            leader_peer
                .apply_timeout(ConsensusTimeout::Election)
                .unwrap();
        }
        //let client_messages = apply_actions(leader, actions, peers);
        //let client_messages = self.apply_peer_messages();
        self.apply_peer_messages();
        // TODO client messages
        // assert!(client_messages.is_empty());
        assert!(self.peers[&leader].is_leader());
    }
    }

    // Tests the majority function.
    #[test]
    fn test_majority() {
        let peers = TestCluster::new(1).peers;
        let majority = peers.values().next().unwrap().majority();
        assert_eq!(1, majority);

        let peers = TestCluster::new(2).peers;
        let majority = peers.values().next().unwrap().majority();
        assert_eq!(2, majority);

        let peers = TestCluster::new(3).peers;
        let majority = peers.values().next().unwrap().majority();
        assert_eq!(2, majority);
        let peers = TestCluster::new(4).peers;
        let majority = peers.values().next().unwrap().majority();
        assert_eq!(3, majority);
    }

    // Tests that a consensus state machine with no peers will transitition immediately to the
    // leader state upon the first election timeout.
    #[test]
    fn test_solitary_consensus_transition_to_leader() {
        let (_, mut peer) = TestCluster::new(1).into_peers().into_iter().next().unwrap();
        assert!(peer.is_follower());

        peer.apply_timeout(ConsensusTimeout::Election).unwrap();
        assert!(peer.is_leader());
        assert!(peer.handler.0.peer_messages.is_empty());
        assert!(peer.handler.0.client_messages.is_empty());
        // make sure all timeouts are clear
        for to in peer.handler.0.timeouts {
            assert!(peer.handler.0.clear_timeouts.iter().any(|&t| t == to))
        }
    }

    /// A simple election test over multiple group sizes.
    #[test]
    fn test_election() {
        for group_size in 1..10 {
            trace!("Group size: {}", group_size);
            let mut cluster = TestCluster::new(group_size);
            let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
            let leader = &peer_ids[0];
            cluster.elect_leader(leader.clone());
            assert!(cluster.peers[leader].is_leader());
            for follower in peer_ids.iter().skip(1) {
                assert!(cluster.peers[follower].is_follower());
            }
        }
    }

    /// Test the new node mechanism
    /// Ensure the catching up node cathes up correctly and the cluster is able to
    /// make it follower after catching up is done
    #[test]
    fn test_new_node() {
        // create a typical 3-node cluster first
        let mut cluster = TestCluster::new(3);
        let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
        let leader = &peer_ids[0];
        cluster.elect_leader(leader.clone());
        assert!(cluster.peers[leader].is_leader());
        for follower in peer_ids.iter().skip(1) {
            assert!(cluster.peers[follower].is_follower());
        }

        let mut leader = cluster.peers[leader];
        //pub fn apply_add_server_message<H: ConsensusHandler>(
        //&mut self,
        //handler: &mut H,
        //request: &AddServerRequest,
        //) -> Result<ServerCommandResponse, Error> {

        leader.apply_add_server_message();
    }

    /// Tests the Raft heartbeating mechanism. The leader receives a heartbeat
    /// timeout, and in response sends an AppendEntries message to the follower.
    /// The follower in turn resets its election timout, and replies to the
    /// leader.
    #[test]
    fn test_heartbeat() {
        let mut cluster = TestCluster::new(2);
        let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
        let leader_id = &peer_ids[0];
        let follower_id = &peer_ids[1];
        cluster.elect_leader(leader_id.clone());

        let peer_message = {
            // Leader pings with a heartbeat timeout
            let leader = cluster.peers.get_mut(&leader_id).unwrap();
            leader
                .apply_timeout(ConsensusTimeout::Heartbeat(follower_id.clone()))
                .unwrap();

            let (to, peer_messages) = leader.handler.0.peer_messages.iter().next().unwrap();
            assert_eq!(*to, follower_id.clone());
            peer_messages[0].clone()
        };

        // Follower responds
        let follower_response = {
            let follower = cluster.peers.get_mut(&follower_id).unwrap();

            // Ensure follower has set it's election timeout
            follower
                .apply_peer_message(leader_id.clone(), peer_message)
                .unwrap();
            assert_eq!(follower.handler.0.timeouts[0], ConsensusTimeout::Election);

            let (to, peer_messages) = follower.handler.0.peer_messages.iter().next().unwrap();
            assert_eq!(*to, leader_id.clone());
            peer_messages[0].clone()
        };

        // Leader applies and sends back a heartbeat to establish leadership.
        let leader = cluster.peers.get_mut(&leader_id).unwrap();
        leader
            .apply_peer_message(follower_id.clone(), follower_response)
            .unwrap();
        let heartbeat_timeout = leader.handler.0.timeouts.pop().unwrap();
        assert_eq!(
            heartbeat_timeout,
            ConsensusTimeout::Heartbeat(follower_id.clone())
        );
    }

    /// Emulates a slow heartbeat message in a two-node cluster.
    ///
    /// The initial leader (Consensus 0) sends a heartbeat, but before it is received by the follower
    /// (Consensus 1), Consensus 1's election timeout fires. Consensus 1 transitions to candidate state
    /// and attempts to send a RequestVote to Consensus 0. When the partition is fixed, the
    /// RequestVote should prompt Consensus 0 to step down. Consensus 1 should send a stale term
    /// message in response to the heartbeat from Consensus 0.
    #[test]
    fn test_slow_heartbeat() {
        let mut cluster = TestCluster::new(2);
        let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
        let peer_0 = &peer_ids[0];
        let peer_1 = &peer_ids[1];
        cluster.elect_leader(peer_0.clone());

        cluster
            .peers
            .get_mut(peer_0)
            .unwrap()
            .apply_timeout(ConsensusTimeout::Heartbeat(*peer_1))
            .unwrap();
        assert!(cluster.peers[peer_0].is_leader());

        cluster
            .peers
            .get_mut(peer_1)
            .unwrap()
            .apply_timeout(ConsensusTimeout::Election)
            .unwrap();
        assert!(cluster.peers[peer_1].is_candidate());
        cluster.apply_peer_messages();
        // Apply candidate messages.
        assert!(cluster.peers[peer_0].is_follower());
        assert!(cluster.peers[peer_1].is_leader());

        // Apply stale heartbeat.
        assert!(cluster.peers[peer_0].is_follower());
        assert!(cluster.peers[peer_1].is_leader());
    }

    /// Tests that a client proposal is correctly replicated to peers, and the client is notified
    /// of the success.
    #[test]
    fn test_proposal() {
        // Test various sizes.
        for size in 1..7 {
            trace!("testing size {} cluster", size);
            let mut cluster = TestCluster::new(size);
            let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
            let leader = &peer_ids[0];
            cluster.elect_leader(leader.clone());

            assert!(cluster.peers[leader].is_leader());

            let value = b"foo".to_vec();
            let proposal = ClientRequest::Proposal(value.clone());
            let client = ClientId::new();

            cluster
                .peers
                .get_mut(&leader)
                .unwrap()
                .apply_client_message(client, proposal)
                .unwrap();
            let (_, client_messages) = cluster.apply_peer_messages();
            assert_eq!(1, client_messages.len());

            for (_, peer) in cluster.peers {
                let mut entry = Vec::new();
                let term = peer.inner.log.entry(LogIndex(1), Some(&mut entry)).unwrap();
                assert_eq!(Term(1), term);
                assert_eq!(value, entry);
            }
        }
    }

    #[test]
    // Verify that out-of-order appends don't lead to the log tail being
    // dropped. See https://github.com/ktoso/akka-raft/issues/66; it's
    // not actually something that can happen in practice with TCP, but
    // wise to avoid it altogether.
    fn test_append_reorder() {
        let mut cluster = TestCluster::new(2);
        let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
        let follower = cluster.peers.get_mut(&ServerId(0)).unwrap();

        let value = b"foo".to_vec();
        let entries = vec![
            Entry::new(Term(1), value.clone()),
            Entry::new(Term(1), value.clone()),
        ];
        let msg1 = PeerMessage::AppendEntriesRequest(AppendEntriesRequest {
            term: Term(1),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            leader_commit: LogIndex(0),
            entries: entries.clone(),
        });

        let mut unordered = entries.clone();
        unordered.pop();
        let msg2 = PeerMessage::AppendEntriesRequest(AppendEntriesRequest {
            term: Term(1),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            leader_commit: LogIndex(0),
            entries: unordered,
        });

        follower.apply_peer_message(peer_ids[1], msg1).unwrap();
        follower.apply_peer_message(peer_ids[1], msg2).unwrap();

        let mut entry1 = Vec::new();
        let term1 = follower
            .inner
            .log
            .entry(LogIndex(1), Some(&mut entry1))
            .unwrap();
        let mut entry2 = Vec::new();
        let term2 = follower
            .inner
            .log
            .entry(LogIndex(2), Some(&mut entry2))
            .unwrap();
        assert_eq!((Term(1), &value), (term1, &entry1));
        assert_eq!((Term(1), &value), (term2, &entry2));
    }
    */
}
