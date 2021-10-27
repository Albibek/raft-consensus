use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::iter::FromIterator;

use crate::handler::*;
use crate::hash_machine::HashMachine;
use itertools::Itertools;

use raft_consensus::message::*;
use raft_consensus::persistent_log::mem::MemLog;
use raft_consensus::persistent_log::*;
use raft_consensus::*;

use raft_consensus::handler::{CollectHandler, Handler};

use log::{debug, info, trace};

pub type TestState = Raft<HashMachine<MemLog>, TestHandler>;

//#[derive(Clone, Debug, PartialEq, Eq)]
#[derive(Clone, Debug)]
pub struct TestCluster {
    pub nodes: HashMap<ServerId, TestState>,
    pub handler: TestHandler,
}

impl TestCluster {
    pub fn new(size: usize, chunked_machine: bool) -> Self {
        pretty_env_logger::try_init().unwrap_or_else(|_| ());
        let peers: Vec<Peer> = (0..size)
            .map(|i| Peer {
                id: ServerId(i as u64),
                metadata: format!("test peer #{}", i).as_bytes().to_vec(),
            })
            .collect();
        let mut nodes = HashMap::new();
        let mut handler = TestHandler::new(size);
        for peer in &peers {
            let store = MemLog::new();
            let machine = HashMachine::new(store, chunked_machine);

            let mut builder = RaftBuilder::new(peer.id, machine);
            builder.with_bootstrap_config(ConsensusConfig::new(peers.iter().cloned()));

            handler.cur = peer.id;
            let consensus = builder.start(&mut handler).unwrap();
            nodes.insert(peer.id, consensus);
        }
        Self { nodes, handler }
    }

    // start the consensus and  make everyone elect a leader
    // due to no election randomization (intentionally) the
    // leader will always be ServerId(0) after the kickstart
    pub fn kickstart(&mut self) {
        trace!("start {:?}", self.handler);
        // at the start all nodes have set their election timeouts
        // we only want one from id=0, so it could win the election
        // for doing that and considering sticky leader feature,
        // we must to play with election timeouts a bit

        // apply the timeout first
        self.apply_action(Action::Timeout(ServerId(0), Timeout::Election));
        self.apply_peer_packets();

        // due to sticky leader, the nodes will delay voting until their election timeouts
        // so we apply timeouts to get the election started
        self.apply_action(Action::Timeout(ServerId(1), Timeout::Election));
        self.apply_action(Action::Timeout(ServerId(2), Timeout::Election));

        self.apply_peer_packets();
        // consensus should have leader with id=0 elected at this point
        for (id, node) in &self.nodes {
            if id == &ServerId(0) {
                assert_eq!(node.kind(), ConsensusState::Leader);
            } else {
                assert_eq!(node.kind(), ConsensusState::Follower);
            }
        }
        // because of new term, leader MUST insert an empty entry to it's log instead of just pinging the followers,

        // let followers receive leader entries
        self.apply_peer_packets();

        // and confirm them
        self.apply_peer_packets();

        // we've missed the timeouts and need to push them back, so cluster
        // is good to correctly proceed

        // TODO: as of now leader clears it's election timeout, but there
        // are client API considerations that may have it always on in the future

        trace!("kickstart finished {:?}", self);
    }

    pub fn apply_peer_packets(&mut self) {
        while self.handler.peer_net_len() > 0 {
            let mut actions = Vec::new();
            for ((from, to), queue) in &mut self.handler.peer_network {
                while let Some(message) = queue.pop_front() {
                    actions.push(Action::Peer(*from, *to, message));
                }
            }
            while let Some(action) = actions.pop() {
                self.apply_action(action)
            }
        }
    }

    pub fn apply_heartbeats(&mut self) {
        let mut actions = Vec::new();
        for (at, timeouts) in &self.handler.heartbeat_timeouts {
            if let Some(timeouts) = timeouts {
                for (on, is_set) in timeouts {
                    if *is_set {
                        actions.push(Action::Timeout(*at, Timeout::Heartbeat(*on)));
                    }
                }
            }
        }

        while let Some(action) = actions.pop() {
            self.apply_action(action)
        }
    }

    pub fn apply_action(&mut self, action: Action) {
        match action {
            Action::Peer(from, to, msg) => {
                self.handler.cur = to;
                let node = self.nodes.get_mut(&to).unwrap();
                trace!("{} -> {} peer: {:?}", from, to, msg);
                node.apply_peer_message(&mut self.handler, from, &msg)
                    .unwrap();
                self.handler.reset_cur();
            }
            Action::Timeout(on, timeout) => {
                self.handler.cur = on;
                let node = self.nodes.get_mut(&on).unwrap();
                trace!("timeout {:?}", action);
                node.apply_timeout(&mut self.handler, timeout).unwrap();
                self.handler.reset_cur();
            }
            Action::Client(from, to, msg) => {
                self.handler.cur = to;
                trace!("{} -> {} client: {:?}", from, to, msg);
                let node = self.nodes.get_mut(&to).unwrap();
                node.apply_client_message(&mut self.handler, from, &msg)
                    .unwrap();
            }
            Action::Admin(from, to, msg) => {
                self.handler.cur = to;
                trace!("{} -> {}: admin {:?} ", from, to, msg);
                let node = self.nodes.get_mut(&to).unwrap();
                node.apply_admin_message(&mut self.handler, from, &msg)
                    .unwrap();
            }
        }
        self.handler.reset_cur();
    }

    pub fn assert_leader_condition(&mut self) {
        let mut leaders = HashMap::new();

        let admin_id = AdminId(uuid::Uuid::from_slice(&[0u8; 16]).unwrap());
        for (id, node) in &mut self.nodes {
            self.handler.cur = *id;
            node.apply_admin_message(&mut self.handler, admin_id, &AdminMessage::PingRequest)
                .unwrap();

            let (_, queue) = self.handler.admin_network.iter_mut().next().unwrap();
            let response = queue.pop_back().unwrap();
            if let AdminMessage::PingResponse(response) = response {
                let num_leaders = leaders.entry(response.term).or_insert(0usize);
                if response.state == ConsensusState::Leader {
                    *num_leaders += 1;
                }
            } else {
                panic!("non ping request for ping response");
            }
            self.handler.reset_cur();
        }

        for n in leaders.values() {
            if *n > 1 {
                trace!("states: {:?}", self.nodes.values().collect::<Vec<_>>());
            }
        }
    }

    pub fn assert_log_condition(&mut self) {
        for (id, node) in &mut self.nodes {
            let log = node.log().unwrap();
            let latest = log.latest_log_index().unwrap();
            trace!("id={} log latest {}", id, latest);
            for index in 1..(latest.as_u64() + 1) {
                let mut log_entry =
                    LogEntry::new_proposal(Term(0), Vec::new(), ClientId::default());
                log.read_entry(LogIndex(index), &mut log_entry).unwrap();
                trace!("id={} entry {:?}", id, log_entry);
            }
        }
    }

    pub fn assert_machine_condition(&mut self) {
        for (id, node) in &mut self.nodes {
            let machine = node.state_machine().unwrap();
            let result = machine.query(&[]);
            trace!("id={} state machine state: {:?}", id, result);
        }
    }
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
