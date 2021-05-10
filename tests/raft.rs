use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use crate::handler::*;
use crate::hash_machine::HashMachine;

use raft_consensus::message::*;
use raft_consensus::persistent_log::mem::MemLog;
use raft_consensus::*;

use raft_consensus::handler::{CollectHandler, Handler};

use log::{debug, info, trace};

pub type TestState = Raft<MemLog, HashMachine, TestHandler>;

#[derive(Clone, Debug)]
pub struct TestCluster {
    pub nodes: HashMap<ServerId, TestState>,
    pub election_timeouts: HashMap<ServerId, bool>,
    pub heartbeat_timeouts: HashMap<ServerId, Option<HashMap<ServerId, bool>>>,
    pub handler: TestHandler,
}

impl TestCluster {
    pub fn new(size: usize) -> Self {
        pretty_env_logger::try_init().unwrap_or_else(|_| ());
        let peers: Vec<Peer> = (0..size)
            .map(|i| Peer {
                id: ServerId(i as u64),
                metadata: format!("test peer #{}", i).as_bytes().to_vec(),
            })
            .collect();
        let mut nodes = HashMap::new();
        let mut handler = TestHandler::new();
        let mut election_timeouts = HashMap::new();
        let mut heartbeat_timeouts = HashMap::new();
        for peer in &peers {
            let store = MemLog::new();
            let machine = HashMachine::new();

            let mut builder = RaftBuilder::new(peer.id, store, machine);
            builder.with_bootstrap_config(ConsensusConfig::new(peers.iter().cloned()));
            election_timeouts.insert(peer.id, false);
            heartbeat_timeouts.insert(peer.id, None);

            handler.cur = peer.id;
            let consensus = builder.start(&mut handler).unwrap();
            nodes.insert(peer.id, consensus);
        }
        Self {
            nodes,
            handler,
            election_timeouts,
            heartbeat_timeouts,
        }
    }

    pub fn apply_actions(&mut self) {
        loop {
            trace!("apply step before {:?}", self.handler);
            self.apply_actions_step();
            trace!("apply step after {:?}", self.handler);

            for (id, timeouts) in &self.heartbeat_timeouts {
                self.handler.cur = *id;

                let node = self.nodes.get_mut(&id).unwrap();
                if let Some(timeouts) = timeouts {
                    for (tid, is_set) in timeouts {
                        if *is_set {
                            node.apply_timeout(&mut self.handler, Timeout::Heartbeat(*tid))
                                .unwrap();
                        }
                    }
                }
                self.handler.reset_cur()
            }

            for (id, timeout) in self.election_timeouts {
                //
            }

            trace!("apply step after timeouts {:?}", self.handler);
            if self.handler.queue.is_empty() {
                break;
            }
        }
    }

    pub fn apply_actions_step(&mut self) {
        self.handler.rotate_queue_ordered();
        while let Some(action) = self.handler.queue2.pop() {
            self.apply_action(action)
        }
        self.asset_leader_condition()
    }

    fn apply_action(&mut self, action: Action) {
        match action {
            Action::Empty => {
                panic!("empty action in queue");
            }
            Action::Peer(from, to, msg) => {
                self.handler.cur = to;
                let mut node = self.nodes.get_mut(&to).unwrap();
                node.apply_peer_message(&mut self.handler, from, &msg)
                    .unwrap();
                self.handler.reset_cur();
            }
            Action::SetTimeout(to, timeout) => {
                //self.handler.cur = to;
                match timeout {
                    Timeout::Election => {
                        self.election_timeouts.insert(to, true).unwrap(); // previous id must exist
                    }
                    Timeout::Heartbeat(id) => {
                        if let Some(timeouts) = self.heartbeat_timeouts.get_mut(&to).unwrap() {
                            timeouts.insert(id, true);
                        } else {
                            let mut timeouts = HashMap::new();
                            timeouts.insert(id, true);
                            *self.heartbeat_timeouts.get_mut(&to).unwrap() = Some(timeouts);
                        }
                    }
                }
            }
            Action::ClearTimeout(to, timeout) => match timeout {
                Timeout::Election => {
                    self.election_timeouts.insert(to, false).unwrap(); // previous id must exist
                }
                Timeout::Heartbeat(id) => {
                    if let Some(timeouts) = self.heartbeat_timeouts.get_mut(&to).unwrap() {
                        timeouts.insert(id, false);
                    } else {
                        let mut timeouts = HashMap::new();
                        timeouts.insert(id, false);
                        *self.heartbeat_timeouts.get_mut(&to).unwrap() = Some(timeouts);
                    }
                }
            },
            Action::Client(from, to, msg) => {
                todo!();
                //                self.handler.cur = to;
                //let node = self.nodes.get(&to).unwrap();
                //node.apply_client_message(&mut self.handler, from, msg)
                //.unwrap();
            }
            Action::Admin(from, to, msg) => {
                todo!();
                //self.handler.cur = to;
                //let node = self.nodes.get(&to).unwrap();
                //node.apply_admin_message(&mut self.handler, from, msg)
                //.unwrap();
            }
        }
        self.handler.reset_cur();
    }

    pub fn asset_leader_condition(&self) {
        let mut leaders = 0usize;
        for node in self.nodes.values() {
            if node.kind() == ConsensusState::Leader {
                leaders += 1
            }
            assert!(leaders <= 1);
        }
    }

    //fn apply_peer_messages(&mut self) {
    //trace!("apply peer messages");
    //let mut queue: VecDeque<(ServerId, ServerId, PeerMessage)> = VecDeque::new();
    //let mut timeouts: HashMap<ServerId, HashSet<ConsensusTimeout>> = HashMap::new();
    //let mut client_messages: HashMap<ClientId, Vec<ClientResponse>> = HashMap::new();
    //for (peer, mut consensus) in self.peers.iter_mut() {
    //for (to, messages) in consensus.handler.0.peer_messages.drain() {
    //for message in messages.into_iter() {
    //queue.push_back((peer.clone(), to, message));
    //}
    //}

    //let mut entry = timeouts.entry(peer.clone()).or_insert(HashSet::new());

    //for timeout in consensus.handler.0.timeouts.clone() {
    //if let ConsensusTimeout::Election = timeout {
    //entry.insert(timeout);
    //}
    //}

    //client_messages.extend(consensus.handler.0.client_messages.clone());
    //consensus.handler.clear();
    //}
    //trace!("Initial queue: {:?}", queue);
    //while let Some((from, to, message)) = queue.pop_front() {
    //let mut peer_consensus = self.peers.get_mut(&to).unwrap();
    //peer_consensus.apply_peer_message(from, message).unwrap();
    //for (to, messages) in peer_consensus.handler.0.peer_messages.drain() {
    //for message in messages.into_iter() {
    //queue.push_back((peer_consensus.inner.id.clone(), to, message));
    //}
    //}

    //trace!("Queue: {:?}", queue);
    //let mut entry = timeouts
    //.entry(peer_consensus.inner.id.clone())
    //.or_insert(HashSet::new());
    //for timeout in peer_consensus.handler.0.timeouts.clone() {
    //if let ConsensusTimeout::Election = timeout {
    //entry.insert(timeout);
    //}
    //}

    //client_messages.extend(peer_consensus.handler.0.client_messages.clone());
    //peer_consensus.handler.clear();
    //}
    //(timeouts, client_messages)
    //}

    //fn into_peers(self) -> HashMap<ServerId, TestPeer> {
    //self.peers
    //}

    //// Elect `leader` as the leader of a cluster with the provided followers.
    //// The leader and the followers must be in the same term.
    //fn elect_leader(&mut self, leader: ServerId) {
    //{
    //let leader_peer = self.peers.get_mut(&leader).unwrap();
    //leader_peer
    //.apply_timeout(ConsensusTimeout::Election)
    //.unwrap();
    //}
    ////let client_messages = apply_actions(leader, actions, peers);
    ////let client_messages = self.apply_peer_messages();
    //self.apply_peer_messages();
    //// TODO client messages
    //// assert!(client_messages.is_empty());
    //assert!(self.peers[&leader].is_leader());
    //}
}

//// Tests that a consensus state machine with no peers will transitition immediately to the
//// leader state upon the first election timeout.
//#[test]
//fn test_solitary_consensus_transition_to_leader() {
//let (_, mut peer) = TestCluster::new(1).into_peers().into_iter().next().unwrap();
//assert!(peer.is_follower());

//peer.apply_timeout(ConsensusTimeout::Election).unwrap();
//assert!(peer.is_leader());
//assert!(peer.handler.0.peer_messages.is_empty());
//assert!(peer.handler.0.client_messages.is_empty());
//// make sure all timeouts are clear
//for to in peer.handler.0.timeouts {
//assert!(peer.handler.0.clear_timeouts.iter().any(|&t| t == to))
//}
//}

///// A simple election test over multiple group sizes.
//#[test]
//fn test_election() {
//for group_size in 1..10 {
//trace!("Group size: {}", group_size);
//let mut cluster = TestCluster::new(group_size);
//let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
//let leader = &peer_ids[0];
//cluster.elect_leader(leader.clone());
//assert!(cluster.peers[leader].is_leader());
//for follower in peer_ids.iter().skip(1) {
//assert!(cluster.peers[follower].is_follower());
//}
//}
//}

///// Test the new node mechanism
///// Ensure the catching up node cathes up correctly and the cluster is able to
///// make it follower after catching up is done
//#[test]
//fn test_new_node() {
//// create a typical 3-node cluster first
//let mut cluster = TestCluster::new(3);
//let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
//let leader = &peer_ids[0];
//cluster.elect_leader(leader.clone());
//assert!(cluster.peers[leader].is_leader());
//for follower in peer_ids.iter().skip(1) {
//assert!(cluster.peers[follower].is_follower());
//}

//let mut leader = cluster.peers[leader];
////pub fn apply_add_server_message<H: ConsensusHandler>(
////&mut self,
////handler: &mut H,
////request: &AddServerRequest,
////) -> Result<ServerCommandResponse, Error> {

//leader.apply_add_server_message();
//}

///// Tests the Raft heartbeating mechanism. The leader receives a heartbeat
///// timeout, and in response sends an AppendEntries message to the follower.
///// The follower in turn resets its election timout, and replies to the
///// leader.
//#[test]
//fn test_heartbeat() {
//let mut cluster = TestCluster::new(2);
//let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
//let leader_id = &peer_ids[0];
//let follower_id = &peer_ids[1];
//cluster.elect_leader(leader_id.clone());

//let peer_message = {
//// Leader pings with a heartbeat timeout
//let leader = cluster.peers.get_mut(&leader_id).unwrap();
//leader
//.apply_timeout(ConsensusTimeout::Heartbeat(follower_id.clone()))
//.unwrap();

//let (to, peer_messages) = leader.handler.0.peer_messages.iter().next().unwrap();
//assert_eq!(*to, follower_id.clone());
//peer_messages[0].clone()
//};

//// Follower responds
//let follower_response = {
//let follower = cluster.peers.get_mut(&follower_id).unwrap();

//// Ensure follower has set it's election timeout
//follower
//.apply_peer_message(leader_id.clone(), peer_message)
//.unwrap();
//assert_eq!(follower.handler.0.timeouts[0], ConsensusTimeout::Election);

//let (to, peer_messages) = follower.handler.0.peer_messages.iter().next().unwrap();
//assert_eq!(*to, leader_id.clone());
//peer_messages[0].clone()
//};

//// Leader applies and sends back a heartbeat to establish leadership.
//let leader = cluster.peers.get_mut(&leader_id).unwrap();
//leader
//.apply_peer_message(follower_id.clone(), follower_response)
//.unwrap();
//let heartbeat_timeout = leader.handler.0.timeouts.pop().unwrap();
//assert_eq!(
//heartbeat_timeout,
//ConsensusTimeout::Heartbeat(follower_id.clone())
//);
//}

///// Emulates a slow heartbeat message in a two-node cluster.
/////
///// The initial leader (Consensus 0) sends a heartbeat, but before it is received by the follower
///// (Consensus 1), Consensus 1's election timeout fires. Consensus 1 transitions to candidate state
///// and attempts to send a RequestVote to Consensus 0. When the partition is fixed, the
///// RequestVote should prompt Consensus 0 to step down. Consensus 1 should send a stale term
///// message in response to the heartbeat from Consensus 0.
//#[test]
//fn test_slow_heartbeat() {
//let mut cluster = TestCluster::new(2);
//let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
//let peer_0 = &peer_ids[0];
//let peer_1 = &peer_ids[1];
//cluster.elect_leader(peer_0.clone());

//cluster
//.peers
//.get_mut(peer_0)
//.unwrap()
//.apply_timeout(ConsensusTimeout::Heartbeat(*peer_1))
//.unwrap();
//assert!(cluster.peers[peer_0].is_leader());

//cluster
//.peers
//.get_mut(peer_1)
//.unwrap()
//.apply_timeout(ConsensusTimeout::Election)
//.unwrap();
//assert!(cluster.peers[peer_1].is_candidate());
//cluster.apply_peer_messages();
//// Apply candidate messages.
//assert!(cluster.peers[peer_0].is_follower());
//assert!(cluster.peers[peer_1].is_leader());

//// Apply stale heartbeat.
//assert!(cluster.peers[peer_0].is_follower());
//assert!(cluster.peers[peer_1].is_leader());
//}

///// Tests that a client proposal is correctly replicated to peers, and the client is notified
///// of the success.
//#[test]
//fn test_proposal() {
//// Test various sizes.
//for size in 1..7 {
//trace!("testing size {} cluster", size);
//let mut cluster = TestCluster::new(size);
//let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
//let leader = &peer_ids[0];
//cluster.elect_leader(leader.clone());

//assert!(cluster.peers[leader].is_leader());

//let value = b"foo".to_vec();
//let proposal = ClientRequest::Proposal(value.clone());
//let client = ClientId::new();

//cluster
//.peers
//.get_mut(&leader)
//.unwrap()
//.apply_client_message(client, proposal)
//.unwrap();
//let (_, client_messages) = cluster.apply_peer_messages();
//assert_eq!(1, client_messages.len());

//for (_, peer) in cluster.peers {
//let mut entry = Vec::new();
//let term = peer.inner.log.entry(LogIndex(1), Some(&mut entry)).unwrap();
//assert_eq!(Term(1), term);
//assert_eq!(value, entry);
//}
//}
//}

//#[test]
//// Verify that out-of-order appends don't lead to the log tail being
//// dropped. See https://github.com/ktoso/akka-raft/issues/66; it's
//// not actually something that can happen in practice with TCP, but
//// wise to avoid it altogether.
//fn test_append_reorder() {
//let mut cluster = TestCluster::new(2);
//let peer_ids: Vec<ServerId> = cluster.peers.keys().cloned().collect();
//let follower = cluster.peers.get_mut(&ServerId(0)).unwrap();

//let value = b"foo".to_vec();
//let entries = vec![
//Entry::new(Term(1), value.clone()),
//Entry::new(Term(1), value.clone()),
//];
//let msg1 = PeerMessage::AppendEntriesRequest(AppendEntriesRequest {
//term: Term(1),
//prev_log_index: LogIndex(0),
//prev_log_term: Term(0),
//leader_commit: LogIndex(0),
//entries: entries.clone(),
//});

//let mut unordered = entries.clone();
//unordered.pop();
//let msg2 = PeerMessage::AppendEntriesRequest(AppendEntriesRequest {
//term: Term(1),
//prev_log_index: LogIndex(0),
//prev_log_term: Term(0),
//leader_commit: LogIndex(0),
//entries: unordered,
//});

//follower.apply_peer_message(peer_ids[1], msg1).unwrap();
//follower.apply_peer_message(peer_ids[1], msg2).unwrap();

//let mut entry1 = Vec::new();
//let term1 = follower
//.inner
//.log
//.entry(LogIndex(1), Some(&mut entry1))
//.unwrap();
//let mut entry2 = Vec::new();
//let term2 = follower
//.inner
//.log
//.entry(LogIndex(2), Some(&mut entry2))
//.unwrap();
//assert_eq!((Term(1), &value), (term1, &entry1));
//assert_eq!((Term(1), &value), (term2, &entry2));
//}
