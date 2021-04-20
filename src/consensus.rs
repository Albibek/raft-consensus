use log::{debug, info, trace};
use std::cmp;
use std::collections::HashMap;

use crate::error::Error;
use crate::handler::ConsensusHandler;
use crate::message::*;
use crate::state::{
    apply_client_message, apply_config_change_message, apply_peer_message, apply_timeout,
    ConsensusState, StateHandler,
};
use crate::{ClientId, ConsensusConfig, Entry, EntryData, LogIndex, Peer, ServerId, Term};

use crate::candidate::CandidateState;
//use crate::catching_up::CatchingUpState;
use crate::follower::FollowerState;
use crate::leader::LeaderState;
use crate::persistent_log::Log;
use crate::state_machine::StateMachine;

pub struct Consensus<L, M> {
    state: ConsensusState<L, M>,
}

macro_rules! all_states {
    ($self:ident, $state:pat, $e:expr) => {
        match $self.state {
            ConsensusState::Leader($state) => $e,
            ConsensusState::Follower($state) => $e,
            ConsensusState::Candidate($state) => $e,
        };
    };
}

impl<L, M> Consensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    /// Triggered when confgiuration change message is received
    fn apply_config_change_message<H: ConsensusHandler>(
        &mut self,
        handler: &mut H,
        request: &AddServerRequest,
    ) -> Result<ServerCommandResponse, Error> {
        let response = all_states!(
            self,
            st,
            apply_config_change_message(&mut st, handler, request)
        );
        response
    }
}

/// An instance of a Raft state machine. The Consensus controls a client state machine, to which it
/// applies entries in a globally consistent order.
///
/// Each event incoming from outside, like timer or a consensus packet, should be passed to
/// corresponsing finction in consensus along with the handler implementation. Then handler functions
/// will be called when corresponsing events happen
#[derive(Debug, Clone)]
pub struct State<L, M, S> {
    // The ID of this consensus instance.
    pub(crate) id: ServerId,

    // The IDs of peers in the consensus group.
    pub(crate) peers: Vec<Peer>,

    // The persistent log.
    pub(crate) log: L,

    // The client state machine to which client commands are applied.
    pub(crate) state_machine: M,

    // Index of the latest entry known to be committed.
    pub(crate) commit_index: LogIndex,

    // The minimal index at which entries can be appended. This bit of state
    // allows avoiding overwriting of possibly committed parts of the log
    // when messages arrive out of order. It is reset on set_leader() and
    // otherwise left untouched.
    // See see ktoso/akka-raft#66.
    pub(crate) min_index: LogIndex,

    // Index of the latest entry applied to the state machine.
    pub(crate) last_applied: LogIndex,

    // The current state of the `Consensus` (`Leader`, `Candidate`, or `Follower`).
    pub(crate) state: S,
}

/// Common functions for any state
impl<L, M, S> State<L, M, S>
where
    L: Log,
    M: StateMachine,
{
    /// Applies all committed but unapplied log entries to the state machine. Returns the set of
    /// return values from the commits applied.
    /// if results are not required, returns an empty map
    pub(crate) fn apply_commits(&mut self, results_required: bool) -> HashMap<LogIndex, Vec<u8>> {
        let mut results = HashMap::new();
        let config_changed = false;
        while self.last_applied < self.commit_index {
            let mut entry = Entry::default();
            // Unwrap is justified here since we know there is an entry in the log.
            self.log.entry(self.last_applied + 1, &mut entry).unwrap();

            match entry.data {
                EntryData::Heartbeat => {
                    // nothing to commit here
                }
                EntryData::Client(data) => {
                    let result = self.state_machine.apply(&data, results_required);
                    self.last_applied = self.last_applied + 1;
                    if results_required {
                        results.insert(self.last_applied, result);
                    }
                }
                EntryData::Config(config) => {
                    self.log.set_latest_config_index(self.last_applied);
                    // according to raft paper, nodes do not need to
                    // wait for entries to be committed by majority
                    // and can apply configuration right away as it goes to the log
                    if let &EntryData::Config(ConsensusConfig { peers }) = &entry.data {
                        self.peers = peers.clone();
                    }
                }
            }
        }

        results
    }

    /// Get the cluster quorum majority size.
    pub(crate) fn majority(&self) -> Result<usize, Error> {
        let peers = self.peers.len();
        let cluster_members = peers.checked_add(1).ok_or(Error::BadClusterSize)?;
        Ok((cluster_members >> 1) + 1)
    }

    /// Returns the current term.
    pub(crate) fn current_term(&self) -> Result<Term, Error> {
        Ok(self
            .log
            .current_term()
            .map_err(|e| Error::PersistentLog(Box::new(e)))?)
    }

    /// Returns the term of the latest applied log entry.
    pub(crate) fn latest_log_term(&self) -> Result<Term, Error> {
        Ok(self
            .log
            .latest_log_term()
            .map_err(|e| Error::PersistentLog(Box::new(e)))?)
    }

    /// Returns the index of the latest applied log entry.
    pub(crate) fn latest_log_index(&self) -> Result<LogIndex, Error> {
        Ok(self
            .log
            .latest_log_index()
            .map_err(|e| Error::PersistentLog(Box::new(e)))?)
    }

    pub(crate) fn with_log_mut<T, F>(&mut self, mut f: F) -> Result<T, Error>
    where
        F: FnMut(&mut L) -> Result<T, L::Error>,
    {
        f(&mut self.log).map_err(|e| Error::PersistentLog(Box::new(e)))
    }

    pub(crate) fn with_log<T, F>(&self, f: F) -> Result<T, Error>
    where
        F: Fn(&L) -> Result<T, L::Error>,
    {
        f(&self.log).map_err(|e| Error::PersistentLog(Box::new(e)))
    }
}

// State change helpers
impl<L, M, S> State<L, M, S>
where
    L: Log,
    M: StateMachine,
{
    pub(crate) fn to_follower<H: ConsensusHandler>(
        &mut self,
        handler: &mut H,
        from: ConsensusStateKind,
        leader_term: Term,
    ) -> Result<ConsensusState<L, M>, Error> {
        let follower_state = State {
            id: self.id,
            peers: self.peers.clone(),
            log: self.log,
            state_machine: self.state_machine,
            commit_index: self.commit_index,
            min_index: self.min_index,
            last_applied: self.last_applied,
            state: FollowerState::new(),
        };
        // recognize the new leader, return to follower state, and apply the entries
        self.with_log_mut(|log| log.set_current_term(leader_term))?;
        handler.state_changed(from, &ConsensusStateKind::Follower);

        for &peer in &self.peers {
            handler.clear_timeout(ConsensusTimeout::Heartbeat(peer.id));
        }

        handler.set_timeout(ConsensusTimeout::Election);
        Ok(ConsensusState::Follower(follower_state))
    }

    pub(crate) fn reset_uncommitted_config<H: ConsensusHandler>(
        &mut self,
        handler: &mut H,
    ) -> Result<(), Error> {
        todo!("set current config to last committed config");
        handler.ensure_connected(&self.peers);
    }

    // All nodes behave in the same way regardless of their current state
    // we receive this message when some node got timeout reaching leader and considers leader
    // dead
    pub(crate) fn common_request_vote_request<H>(
        &mut self,
        handler: &mut H,
        from: ServerId,
        request: RequestVoteRequest,
        from_state: ConsensusStateKind,
    ) -> Result<(Option<RequestVoteResponse>, Option<ConsensusState<L, M>>), Error>
    where
        H: ConsensusHandler,
    {
        // for correct manual leadership change, node should ignore or delay vote requests
        // coming within election timeout unless there is special flag set signalling
        // the leadership was given away voluntarily
        if !request.is_voluntary_step_down && !self.is_within_election() {
            return Ok((None, None));
        }

        let candidate_term = request.term;
        let candidate_log_term = request.last_log_term;
        let candidate_log_index = request.last_log_index;
        let latest_config_index = self.with_log(|log| log.latest_config_index())?;
        debug!(
            "RequestVoteRequest from Consensus {{ id: {}, term: {}, latest_log_term: \
             {}, latest_log_index: {} }}",
            &from, candidate_term, candidate_log_term, candidate_log_index
        );

        let current_term = self.current_term()?;
        let (new_local_term, new_state) = if candidate_term > current_term {
            info!(
                "received RequestVoteRequest from Consensus {{ id: {}, term: {} }} \
                 with newer term; transitioning to Follower",
                from, candidate_term
            );

            self.reset_uncommitted_config(handler)?;
            (
                candidate_term,
                Some(self.to_follower(handler, from_state, candidate_term)?),
            )
        } else {
            (current_term, None)
        };

        let message = if candidate_term < current_term {
            RequestVoteResponse::StaleTerm(new_local_term)
        } else if candidate_log_term < self.latest_log_term()?
            || candidate_log_index < self.latest_log_index()?
            || candidate_log_index < latest_config_index
        {
            RequestVoteResponse::InconsistentLog(new_local_term)
        } else {
            match self.with_log(|log| log.voted_for())? {
                None => {
                    self.with_log_mut(|log| log.set_voted_for(candidate))?;
                    RequestVoteResponse::Granted(new_local_term)
                }
                Some(voted_for) => {
                    if voted_for == candidate {
                        RequestVoteResponse::Granted(new_local_term)
                    } else {
                        RequestVoteResponse::AlreadyVoted(new_local_term)
                    }
                }
            }
        };
        Ok((message, new_state))
    }

    pub(crate) fn common_client_ping_request(&self) -> Result<PingResponse, Error> {
        Ok(PingResponse {
            term: self.current_term()?,
            index: self.latest_log_index()?,
            state: self.state.kind(),
        })
    }
}

//impl<L, M> State<L, M, ()>
//where
//L: Log,
//M: StateMachine,
//{

///// Applies a peer request vote request to the consensus state machine.
//pub(crate) fn request_vote_request<H: ConsensusHandler>(
//&mut self,
//handler: &mut H,
//candidate: ServerId,
//request: &RequestVoteRequest,
//) -> Result<RequestVoteResponse, Error> {
//// FIXME:
//// if candidate node's index is less than last committed configuration change index,
//// we should not vote for this node
////
//// FIXME:
//// there is also leader change, meaning all uncommitted configuration changes
//// must be revert (QUESTION: does this repend on candidate's log index or not?)
//// FIXME: when voting, node should not count itself as majority
//self.reset_catching_up(handler)?;
//let candidate_term = request.term;
//let candidate_log_term = request.last_log_term;
//let candidate_log_index = request.last_log_index;
//debug!(
//"RequestVoteRequest from Consensus {{ id: {}, term: {}, latest_log_term: \
//{}, latest_log_index: {} }}",
//&candidate, candidate_term, candidate_log_term, candidate_log_index
//);
//let local_term = self.current_term();

//let new_local_term = if candidate_term > local_term {
//info!(
//"received RequestVoteRequest from Consensus {{ id: {}, term: {} }} \
//with newer term; transitioning to Follower",
//candidate, candidate_term
//);
//self.transition_to_follower(handler, candidate_term, candidate)?;
//candidate_term
//} else {
//local_term
//};

//let message = if candidate_term < local_term {
//RequestVoteResponse::StaleTerm(new_local_term)
//} else if candidate_log_term < self.latest_log_term()
//|| candidate_log_index < self.latest_log_index()
//{
//RequestVoteResponse::InconsistentLog(new_local_term)
//} else {
//match self.with_log(|log| log.voted_for())? {
//None => {
//self.with_log_mut(|log| log.set_voted_for(candidate))?;
//RequestVoteResponse::Granted(new_local_term)
//}
//Some(voted_for) => {
//if voted_for == candidate {
//RequestVoteResponse::Granted(new_local_term)
//} else {
//RequestVoteResponse::AlreadyVoted(new_local_term)
//}
//}
//}
//};
//Ok(message)
//}

///// Applies a request vote response to the consensus state machine.
//pub(crate) fn request_vote_response<H: ConsensusHandler>(
//&mut self,
//handler: &mut H,
//from: ServerId,
//response: &RequestVoteResponse,
//) -> Result<(), Error> {
//debug!("RequestVoteResponse from peer {}", from);

//let response = response;
//let local_term = self.current_term();
//let voter_term = response.voter_term();
//let majority = self.majority();
//if local_term < voter_term {
//// Responder has a higher term number. The election is compromised; abandon it and
//// revert to follower state with the updated term number. Any further responses we
//// receive from this election term will be ignored because the term will be outdated.

//// The responder is not necessarily the leader, but it is somewhat likely, so we will
//// use it as the leader hint.
//info!(
//"received RequestVoteResponse from Consensus {{ id: {}, term: {} }} \
//with newer term; transitioning to Follower",
//from, voter_term
//);
//self.transition_to_follower(handler, voter_term, from)
//} else if local_term > voter_term {
//// Ignore this message; it came from a previous election cycle.
//Ok(())
//} else {
//// local_term == voter_term
//if let ConsensusState::Candidate(ref mut state) = self.state {
//// A vote was received!
//if let RequestVoteResponse::Granted(_) = response {
//state.record_vote(from);
//if state.count_votes() >= majority {
//info!(
//"election for term {} won; transitioning to Leader",
//local_term
//);
//self.transition_to_leader(handler)
//} else {
//Ok(())
//}
//} else {
//Ok(())
//}
//} else {
//// received response with local_term = voter_term, but state is not candidate
//// It's ok because some votes can come after we became follower or leader
//Ok(())
//}
//}
//}
//}

/// Cluster membership change processing
impl<L, M> Consensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    /// Handles initiating of adding a new server to consensus
    pub fn apply_add_server_message<H: ConsensusHandler>(
        &mut self,
        handler: &mut H,
        request: &AddServerRequest,
    ) -> Result<ServerCommandResponse, Error> {
        // TODO: we can send this message from the node that just have started
        // the leader could recognize the node as the one existing in consensus
        // or totally new one and behave correspondingly
        // but we always expect this message from any node making the connection
        todo!();
        // FIXME: when removed by configuration change leader must not count itself in a majority
        debug!("got requests for adding new server {:?}", request);
        match &mut self.state {
            ConsensusState::CatchingUp(_) => Err(Error::UnexpectedMessage),
            ConsensusState::Candidate(_) => Ok(ServerCommandResponse::UnknownLeader),
            ConsensusState::Follower(ref state) => state
                .leader
                .map_or(Ok(ServerCommandResponse::UnknownLeader), |leader| {
                    Ok(ServerCommandResponse::NotLeader(leader))
                }),
            ConsensusState::Leader(ref mut state) => {
                // there was a BUG found in Raft dissertation saying we should only start
                // the catch-up procedure after the majority is done with the full commit procedure
                // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J
                //
                // The point is that we should check if leader has a committed entry in the
                // current term, meaning the majority knows about it

                let term = self.current_term();
                let committed_term = self.with_log(|log| log.term(self.commit_index))?;

                if committed_term != term {
                    return Ok(ServerCommandResponse::LeaderJustChanged);
                }

                // we also may have a config change already pending
                if self.config_change.is_some() {
                    return Ok(ServerCommandResponse::AlreadyPending);
                }

                // After all checks done, we must initiate the connection with the peer we want to
                // catch up
                //
                // We fall into 2 cases here:
                // * connect from peer followed by AddServer: someone started a new node, and after
                // that have sent the AddServer command. In this case we'll have a call to peer_connected in the past,
                // but will ignore it because we did not know anything about the peer yet
                // * AddServer followed by connect: someone first called the AddServer command and
                // only then started the node
                //
                // Due to lack of knowlege about peer connection status, we want handler to resolve
                // this inconsistency via speciall callback: new_server
                //
                // By convention, the handler should make sure the peer can receive messages and
                // come back to us calling peer_connected method after having peer online

                // let handler know about a peer and check it's validity
                if handler.new_server(request.id, &request.info).is_err() {
                    return Ok(ServerCommandResponse::BadPeer);
                }

                // start the configuration change procedure and wait for peer to connect
                state.start_config_change_unchecked(request.id);

                // start counting rounds for a new peer right away to make sure slow connections
                // are handled by timeout
                handler.set_timeout(ConsensusTimeout::Election);
                handler.done();
                Ok(ServerCommandResponse::Success)
            }
        }
    }
}

/*
/// Client messages processing
impl<L, M> Consensus<L, M>
where
L: Log,
M: StateMachine,
{
/// Applies a client message to the consensus state machine.
pub fn apply_client_message<H: ConsensusHandler>(
&mut self,
handler: &mut H,
from: ClientId,
message: ClientRequest,
) -> Result<(), Error> {
let response = match message {
ClientRequest::Ping => Some(ClientResponse::Ping(self.ping_request())),
ClientRequest::Proposal(data) => {
let response = self.proposal_request(handler, from, data)?;
response.map(ClientResponse::Proposal)
}
ClientRequest::Query(data) => {
Some(ClientResponse::Query(self.query_request(from, &data)))
}
};
if let Some(response) = response {
handler.send_client_response(from, response)
}
handler.done();
Ok(())
}

//    fn ping_request(&self) -> PingResponse {
//PingResponse {
//term: self.current_term(),
//index: self.latest_log_index(),
//state: self.state.kind(),
//}
//}

//    /// Applies a client proposal to the consensus state machine.
//fn proposal_request<H: ConsensusHandler>(
//&mut self,
//handler: &mut H,
//from: ClientId,
//request: Vec<u8>,
//) -> Result<Option<CommandResponse>, Error> {
//let prev_log_index = self.latest_log_index();
//let prev_log_term = self.latest_log_term();
//let term = self.current_term();
//match &mut self.state {
//ConsensusState::Candidate(_) => Ok(Some(CommandResponse::UnknownLeader)),
//ConsensusState::Follower(ref state) => state
//.leader
//.map_or(Ok(Some(CommandResponse::UnknownLeader)), |leader| {
//Ok(Some(CommandResponse::NotLeader(leader)))
//}),
//ConsensusState::Leader(ref mut state) => {
//let log_index = prev_log_index + 1;
//debug!("ProposalRequest from client {}: entry {}", from, log_index);
//let leader_commit = self.commit_index;
//self.log
//.append_entries(
//log_index,
//Some(Entry::new(term, EntryData::Client(request.clone()))).into_iter(),
//)
//.map_err(|e| Error::PersistentLog(Box::new(e)))?;

//state.proposals.push_back((from, log_index));

//// the order of messages could broke here if we return Queued after calling
//// advance_commit_index since it queues some more packets for client,
//// we must first of all let client know that proposal has been received
//// and only confirm commits after that
//handler
//.send_client_response(from, ClientResponse::Proposal(CommandResponse::Queued));
//if self.peers.is_empty() {
//self.advance_commit_index(handler)?;
//} else {
//let message = AppendEntriesRequest {
//term,
//prev_log_index,
//prev_log_term,
//leader_commit,
//entries: vec![Entry::new(term, EntryData::Client(request))],
//};
//for &peer in &self.peers {
//if state.next_index(&peer.id) == log_index {
//handler.send_peer_message(
//peer.id,
//PeerMessage::AppendEntriesRequest(message.clone()),
//);
//state.set_next_index(peer.id, log_index + 1);
//}
//}
//self.advance_commit_index(handler)?;
//}
//// Since queued is already sent, we need no more messages for client
//Ok(None)
//}
//}
//}

//    /// Applies a client query to the state machine.
//fn query_request(&mut self, from: ClientId, request: &[u8]) -> CommandResponse {
//trace!("query from Client({})", from);
//match &mut self.state {
//ConsensusState::Candidate(_) => CommandResponse::UnknownLeader,
//ConsensusState::Follower(ref state) => state
//.leader
//.map_or(CommandResponse::UnknownLeader, |leader| {
//CommandResponse::NotLeader(leader)
//}),
//ConsensusState::Leader(_) => {
//// TODO(from original raft): This is probably not exactly safe.
//let result = self.state_machine.query(&request);
//CommandResponse::Success(result)
//}
//}
//}
}
*/

/// Timeout handling
impl<L, M> Consensus<L, M>
where
    L: Log,
    M: StateMachine,
{
    /*
        /// Triggered by external timeouts.
        /// Convenience function for handling any timeout.
        /// Will call either `heartbeat_timeout` or `election_timeout`
        pub fn apply_timeout<H: ConsensusHandler>(
        &mut self,
        handler: &mut H,
        timeout: ConsensusTimeout,
        ) -> Result<(), Error> {
        match timeout {
        ConsensusTimeout::Election => self.election_timeout(handler)?,
        ConsensusTimeout::Heartbeat(id) => {
        let request = self.heartbeat_timeout(id)?;
        // we get here only if we are leader
        let request = PeerMessage::AppendEntriesRequest(request);
        handler.send_peer_message(id, request);
        }
        };
        handler.done();
        Ok(())
        }

        /// Triggered by a heartbeat timeout for the peer.
        pub fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error> {
        if let ConsensusState::Leader(_) = self.state {
        debug!("HeartbeatTimeout for peer: {}", peer);
        Ok(AppendEntriesRequest {
        term: self.current_term(),
        prev_log_index: self.latest_log_index(),
        prev_log_term: self.with_log(|log| log.latest_log_term())?,
        leader_commit: self.commit_index,
        entries: Vec::new(),
        })
        } else {
        Err(Error::MustLeader)
        }
        }

        /// Triggered by an election timeout.
        pub fn election_timeout<H: ConsensusHandler>(&mut self, handler: &mut H) -> Result<(), Error> {
        match &mut self.state {
        ConsensusState::Leader(state) => {
        // leader uses election timeout when it has a remote being catched up
        if state.config_change.is_some() {
        // TODO option for number of timeouts
        if state.config_change.add_timeout().ok_or(Error::CatchUpBug)? >= 8 {
        // let handler know peer has failed
        handler.peer_failed(state.config_change.peer);
        // allowed number of timeouts passed without catching up
        state.config_change = None;
        return Err(Error::CatchUpFailed);
        } else {
        // remote still has time to catch up
        handler.set_timeout(ConsensusTimeout::Election);
        }
        } else {
        return Err(Error::MustNotLeader);
        }
        }
        ConsensusState::Follower(_) => unreachable!(),
        ConsensusState::Candidate(_) => {
        if self.peers.is_empty() || self.peers.len() == 1 && self.peers[0].id == self.id {
        // Solitary replica special case: we are the only peer in consensus
        // jump straight to Leader state.
        info!("Election timeout: transitioning to Leader due do solitary replica condition");
        assert!(self.log.voted_for().unwrap().is_none());

        self.log
        .inc_current_term()
        .map_err(|e| Error::PersistentLog(Box::new(e)))?;
        self.log
        .set_voted_for(self.id)
            .map_err(|e| Error::PersistentLog(Box::new(e)))?;
    } else {
        info!("Election timeout: transitioning to Candidate");
        self.transition_to_candidate(handler)?;

        return Ok(());
    }

    handler.done();
    }
    };

    // we only get here if we are candidate, and we need to release self.state before
    let old_state = self.state.clone();
    let latest_log_index = self
        .log
    .latest_log_index()
        .map_err(|e| Error::PersistentLog(Box::new(e)))?;

        self.state = ConsensusState::Leader(LeaderState::new(
                latest_log_index,
                self.peers.iter().map(|peer| &peer.id),
        ));

    handler.state_changed(old_state, &self.state);
    handler.clear_timeout(ConsensusTimeout::Election);
    Ok(())
        }
    */
}

/// State transitions handling
impl<L, M> State<L, M, ()>
where
    L: Log,
    M: StateMachine,
{
    //    /// Transitions the consensus state machine to Follower state with the provided term. The
    ///// `voted_for` field will be reset. The provided leader hint will replace the last known
    ///// leader.
    //fn transition_to_follower<H: ConsensusHandler>(
    //&mut self,
    //handler: &mut H,
    //term: Term,
    //leader: ServerId,
    //) -> Result<(), Error> {
    //self.log
    //.set_current_term(term)
    //.map_err(|e| Error::PersistentLog(Box::new(e)))?;
    //let old_state = self.state.clone();
    //self.state = ConsensusState::Follower(FollowerState {
    //leader: Some(leader),
    //});
    //handler.state_changed(old_state, &self.state);

    //for &peer in &self.peers {
    //handler.clear_timeout(ConsensusTimeout::Heartbeat(peer.id));
    //}

    //handler.set_timeout(ConsensusTimeout::Election);
    //Ok(())
    //}

    //    /// Transitions this consensus state machine to Leader state.
    //fn transition_to_leader<H: ConsensusHandler>(&mut self, handler: &mut H) -> Result<(), Error> {
    //trace!("transitioning to Leader");
    //let latest_log_index = self
    //.log
    //.latest_log_index()
    //.map_err(|e| Error::PersistentLog(Box::new(e)))?;
    //let old_state = self.state.clone();

    //self.state = ConsensusState::Leader(LeaderState::new(
    //latest_log_index,
    //self.peers.iter().map(|peer| &peer.id),
    //));
    //handler.state_changed(old_state, &self.state);

    //let message = AppendEntriesRequest {
    //term: self.current_term(),
    //prev_log_index: latest_log_index,
    //prev_log_term: self
    //.log
    //.latest_log_term()
    //.map_err(|e| Error::PersistentLog(Box::new(e)))?,
    //leader_commit: self.commit_index,
    //entries: Vec::new(),
    //};

    //for &peer in &self.peers {
    //handler.send_peer_message(peer.id, PeerMessage::AppendEntriesRequest(message.clone()));
    //handler.clear_timeout(ConsensusTimeout::Heartbeat(peer.id));
    //}
    //handler.clear_timeout(ConsensusTimeout::Election);
    //Ok(())
    //}

    /// Transitions the consensus state machine to Candidate state.
    fn transition_to_candidate<H: ConsensusHandler>(
        &mut self,
        handler: &mut H,
    ) -> Result<(), Error> {
        trace!("transitioning to Candidate");
        self.with_log_mut(|log| log.inc_current_term())?;
        let id = self.id;
        self.with_log_mut(|log| log.set_voted_for(id))?;
        let last_log_term = self.with_log(|log| log.latest_log_term())?;

        let old_state = self.state.clone();
        self.state = ConsensusState::Candidate(CandidateState::new());
        handler.state_changed(old_state, &self.state);
        if let ConsensusState::Candidate(ref mut state) = self.state {
            // always true
            state.record_vote(self.id);
        }

        let message = RequestVoteRequest {
            term: self.current_term(),
            last_log_index: self.latest_log_index(),
            last_log_term,
        };

        for &peer in &self.peers {
            handler.send_peer_message(peer.id, PeerMessage::RequestVoteRequest(message.clone()));
        }
        handler.set_timeout(ConsensusTimeout::Election);
        Ok(())
    }
}

///// Utility functions
//impl<L, M> State<L, M, ()>
//where
//L: Log,
//M: StateMachine,
//{
//pub fn peer_connected<H: ConsensusHandler>(
//&mut self,
//handler: &mut H,
//peer: ServerId,
//) -> Result<(), Error> {
//let new_peer = !self.peers.iter().any(|&p| p.id == peer);
//if new_peer {
//// This may still be correct peer, but it is was not added using AddServer API or was
//// removed already
//// the peer still can be the one that is going to catch up, so we skip this
//// check for a leader state
//// By this reason we don't panic here, returning an error
//debug!("New peer connected, but not found in consensus: {:?}", peer);
//}

//match &mut self.state {
//ConsensusState::Leader(state) => {
//let peer_index = state.next_index(&peer)?;

//// Send any outstanding entries to the peer, or an empty heartbeat if there are no
//// outstanding entries.
//let until_index = self.latest_log_index() + 1;

//let prev_log_index = peer_index - 1;
//let prev_log_term = if prev_log_index == LogIndex::from(0) {
//Term::from(0)
//} else {
//self.with_log(|log| log.term(prev_log_index))?
//};

//let mut message = AppendEntriesRequest {
//term: self.current_term(),
//prev_log_index,
//prev_log_term,
//leader_commit: self.commit_index,
//entries: Vec::new(),
//};

//for idx in peer_index.as_u64()..until_index.as_u64() {
//let mut entry = Entry::default();
//self.with_log_mut(|log| log.entry(LogIndex(idx), &mut entry))?;

//message.entries.push(entry);
//}

//// For stateless/lossy connections we cannot be sure if peer has received
//// our entries, so we call set_next_index only after response, which
//// is done in response processing code
////self.leader_state.set_next_index(peer, until_index);
//handler.send_peer_message(peer, PeerMessage::AppendEntriesRequest(message));
//}
//ConsensusState::Candidate(state) => {
//if new_peer {
//return Err(Error::UnknownPeer(peer));
//}
//// Resend the request vote request if a response has not yet been receieved.
//if state.peer_voted(peer) {
//return Ok(());
//}

//let message = RequestVoteRequest {
//term: self.current_term(),
//last_log_index: self.latest_log_index(),
//last_log_term: self.log.latest_log_term().unwrap(),
//};
//handler.send_peer_message(peer, PeerMessage::RequestVoteRequest(message));
//}
//ConsensusState::Follower(_) => {
//if new_peer {
//return Err(Error::UnknownPeer(peer));
//}
//// No message is necessary; if the peer is a leader or candidate they will send a
//// message.
//}

//ConsensusState::CatchingUp(_) => {
//// we don't now if it's leader or some other node from cluster, so we don't drop it
//// anyways, we only react on incoming messages and do not require action on any
//// connections
//}
//}

//handler.done();
//Ok(())
//}

/*
#[derive(Debug, Clone)]
/// Convenience wrapper over consensus, providing single handler
pub struct HandledConsensus<L, M, H> {
inner: Consensus<L, M>,

/// External handler of consensus responses
pub handler: H,
}

impl<L, M, H> HandledConsensus<L, M, H>
where
L: Log,
M: StateMachine,
H: ConsensusHandler,
{
pub fn new(
id: ServerId,
peers: Vec<Peer>,
log: L,
state_machine: M,
handler: H,
) -> Result<Self, Error> {
Ok(Self {
inner: Consensus::new(id, peers, log, state_machine)?,
handler,
})
}

pub fn init(&mut self) {
self.inner.init(&mut self.handler)
}

/// Applies a peer message to the consensus state machine.
pub fn apply_peer_message(
&mut self,
from: ServerId,
message: PeerMessage,
) -> Result<(), Error> {
self.inner
.apply_peer_message(&mut self.handler, from, message)
}

/// Applies a client message to the consensus state machine.
pub fn apply_client_message(
&mut self,
from: ClientId,
message: ClientRequest,
) -> Result<(), Error> {
self.inner
.apply_client_message(&mut self.handler, from, message)
}

/// Triggers a timeout for the peer.
pub fn apply_timeout(&mut self, timeout: ConsensusTimeout) -> Result<(), Error> {
self.inner.apply_timeout(&mut self.handler, timeout)
}

/// Triggers a heartbeat timeout for the peer.
pub fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error> {
self.inner.heartbeat_timeout(peer)
}

/// Triggered by an election timeout.
pub fn election_timeout(&mut self) -> Result<(), Error> {
self.inner.election_timeout(&mut self.handler)
}

pub fn peer_connected(&mut self, peer: ServerId) -> Result<(), Error> {
self.inner.peer_connected(&mut self.handler, peer)
}

/// Returns whether the consensus state machine is currently a Leader.
pub fn get_state(&self) -> ConsensusState {
    self.inner.get_state()
}

#[allow(dead_code)]
pub(crate) fn majority(&self) -> usize {
    self.inner.majority()
}

pub fn handler(&mut self) -> &mut H {
    &mut self.handler
}
}
*/

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::collections::VecDeque;

    use super::*;
    use crate::persistent_log::mem::MemLog;
    use crate::state_machine::null::NullStateMachine;

    use crate::handler::CollectHandler;
    use pretty_env_logger;

    #[derive(Debug)]
    struct TestHandler(CollectHandler);

    impl ConsensusHandler for TestHandler {
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

        fn state_changed(&mut self, old: ConsensusState, new: ConsensusState) {
            match (&old, &new) {
                (&ConsensusState::Leader, &ConsensusState::Candidate) => {
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
}
