use std::collections::HashMap;

use crate::testing::emulation::handler::*;
use crate::testing::emulation::hash_machine::HashMachine;
use bytes::Bytes;

use crate::message::*;
use crate::persistent_log::mem::MemLog;
use crate::persistent_log::*;
use crate::*;

use tracing::trace;

pub type TestLog = MemLog;
pub type TestMachine = HashMachine<TestLog>;
pub type TestState = Raft<TestMachine, TestHandler>;

/// An emulation of cluster of N nodes in consensus.
/// The network is emulated using handler::TestHandler.
/// The log in in memory, the state machine is XOR-hasher from hash_machine::HashMachine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestCluster {
    pub nodes: HashMap<ServerId, TestState>,
    pub handler: TestHandler,
    bootstrap_config: ConsensusConfig,
}

impl TestCluster {
    // TODO: make a builder, move chunking and tracing level inside
    /// Create a new cluster with n nodes. HashMachine supports snapshot streaming
    /// so there is a choice to do it.
    pub fn new(size: usize, chunked_machine: bool) -> Self {
        tracing_subscriber::fmt()
            .without_time()
            .with_max_level(tracing::Level::TRACE)
            .with_target(false)
            .try_init()
            // when initializing multiple clusters, we only need one global subscriber
            .unwrap_or(());

        let peers: Vec<Peer> = (0..size)
            .map(|i| Peer {
                id: ServerId(i as u64),
                metadata: format!("test peer #{}", i).as_bytes().to_vec(),
            })
            .collect();
        let mut cluster = Self {
            nodes: HashMap::new(),
            handler: TestHandler::new(size),
            bootstrap_config: ConsensusConfig::new(peers.iter().cloned()),
        };

        for peer in &peers {
            tracing::trace_span!("node", id = ?peer.id);
            cluster.add_node(peer.id, chunked_machine);
        }
        cluster
    }

    /// Add or replace a freshly created cluster node with empty log.
    /// This function only adds the node without changing cluster configutation
    /// on existing nodes or givning commands to them.
    pub fn add_node(&mut self, id: ServerId, chunked_machine: bool) {
        let store = MemLog::new();
        let machine = HashMachine::new(store, chunked_machine);

        let mut builder = RaftBuilder::new(id, machine);
        builder.with_bootstrap_config(self.bootstrap_config.clone());

        self.handler.cur = id;
        let consensus = builder.start(&mut self.handler).unwrap();
        self.nodes.insert(id, consensus);
    }

    /// start the consensus and  make everyone elect a leader
    /// due to no election timeout randomization (intentionally) the
    /// leader will always be ServerId(0) after the kickstart
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

        // TODO: as of now leader clears it's election timeout, but there
        // are client API considerations that may have it always on in the future

        trace!("kickstart finished {:?}", self);
    }

    /// Apply an arbitrary action to the consensus.
    /// Only node method will be called, no packets delivered.
    /// (there might be some enqueued though).
    pub fn apply_action(&mut self, action: Action) {
        match action {
            Action::Peer(from, to, msg) => {
                self.handler.cur = to;
                let node = self.nodes.get_mut(&to).unwrap();
                node.apply_peer_message(&mut self.handler, from, msg)
                    .unwrap();
                self.handler.reset_cur();
            }
            Action::Timeout(on, timeout) => {
                self.handler.cur = on;
                let node = self.nodes.get_mut(&on).unwrap();
                node.apply_timeout(&mut self.handler, timeout).unwrap();
                self.handler.reset_cur();
            }
            Action::Client(from, to, msg) => {
                self.handler.cur = to;
                let node = self.nodes.get_mut(&to).unwrap();
                node.apply_client_message(&mut self.handler, from, msg)
                    .unwrap();
            }
            Action::Admin(from, to, msg) => {
                self.handler.cur = to;
                let node = self.nodes.get_mut(&to).unwrap();
                node.apply_admin_message(&mut self.handler, from, msg)
                    .unwrap();
            }
        }
        self.handler.reset_cur();
    }

    /// Take all the peer protocol packets from network and apply them to the corresponding
    /// receivers. Repeat until there is no packets left.
    pub fn apply_peer_packets(&mut self) {
        trace!(n = self.handler.peer_net_len(), "applying packets");
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

    /// Fire all pending heartbeat timeouts on all nodes in cluster
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

    /// Call the compaction procedure on the specified nodes
    pub fn check_compaction(&mut self, nodes: &[ServerId], force: bool) -> Vec<bool> {
        let mut results = Vec::with_capacity(nodes.len());
        for id in nodes {
            let node = self.nodes.get_mut(id).unwrap();
            let res = node.check_compaction(&mut self.handler, force).unwrap();
            results.push(res)
        }
        results
    }

    /// Check if single leader condition is met
    pub fn assert_leader_condition(&mut self) {
        let mut leaders = HashMap::new();

        let admin_id = AdminId(uuid::Uuid::from_slice(&[0u8; 16]).unwrap());
        for (id, node) in &mut self.nodes {
            self.handler.cur = *id;
            node.apply_admin_message(&mut self.handler, admin_id, AdminMessage::PingRequest)
                .unwrap();

            let (_, queue) = self.handler.admin_network.iter_mut().next().unwrap();
            let response = queue.pop_back().unwrap();
            if let AdminMessage::PingResponse(response) = response {
                let num_leaders = leaders.entry(response.term).or_insert(0usize);
                if response.state == ConsensusState::Leader {
                    *num_leaders += 1;
                }
            } else {
                panic!("non ping response for ping request");
            }
            self.handler.reset_cur();
        }

        for n in leaders.values() {
            if *n > 1 {
                trace!("states: {:?}", self.nodes.values().collect::<Vec<_>>());
            }
        }
    }

    /// Check if log requirements are met
    pub fn assert_log_condition(&mut self) {
        for (id, node) in &mut self.nodes {
            let log = node.log().unwrap();
            let latest = log.latest_index().unwrap();
            trace!("id={} log latest {}", id, latest);
            for index in 1..(latest.as_u64() + 1) {
                let mut log_entry =
                    LogEntry::new_proposal(Term(0), Bytes::new(), ClientGuarantee::default());
                log.read_entry(LogIndex(index), &mut log_entry).unwrap();
                trace!("id={} entry {:?}", id, log_entry);
            }
        }
    }

    /// Check if state machines on all nodes are in equal state
    pub fn assert_machine_condition(&mut self) {
        for (id, node) in &mut self.nodes {
            let machine = node.state_machine_mut().unwrap();
            let result = machine.query(Bytes::new());
            trace!("id={} state machine state: {:?}", id, result);
        }
    }

    /// Get an immutable reference to node's log
    pub fn log_of(&self, id: ServerId) -> &TestLog {
        self.nodes.get(&id).unwrap().log().unwrap()
    }

    /// Get a mutable reference to node's log
    pub fn log_of_mut(&mut self, id: ServerId) -> &mut TestLog {
        self.nodes.get_mut(&id).unwrap().log_mut().unwrap()
    }

    /// Get an immutable reference to node's state machine
    pub fn machine_of(&self, id: ServerId) -> &TestMachine {
        self.nodes.get(&id).unwrap().state_machine().unwrap()
    }

    /// Get a mutable reference to node's state machine
    pub fn machine_of_mut(&mut self, id: ServerId) -> &mut TestMachine {
        self.nodes
            .get_mut(&id)
            .unwrap()
            .state_machine_mut()
            .unwrap()
    }
}
