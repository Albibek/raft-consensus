use std::collections::HashSet;
use std::collections::VecDeque;

use raft_consensus::message::*;
use raft_consensus::persistent_log::mem::MemLog;
use raft_consensus::state_machine::null::NullStateMachine;
use raft_consensus::*;

use raft_consensus::handler::{CollectHandler, Handler};

use log::{debug, info, trace};

#[derive(Debug, Clone)]
pub enum Action {
    Empty,
    Peer(ServerId, ServerId, PeerMessage),
    SetTimeout(ServerId, Timeout),
    ClearTimeout(ServerId, Timeout),
    FireTimeout(ServerId, Timeout),
    Client(ServerId, ClientId, ClientMessage),
    Admin(ServerId, AdminId, AdminMessage),
}

#[derive(Debug, Clone)]
pub struct TestHandler {
    pub cur: ServerId,
    pub queue: VecDeque<Action>,
    pub queue2: Vec<Action>,
}

impl Handler for TestHandler {
    /// Saves peer message to a vector
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage) {
        assert_ne!(self.cur, ServerId(u64::MAX));
        self.queue.push_back(Action::Peer(self.cur, id, message));
    }

    /// Saves client message to a vector
    fn send_client_message(&mut self, id: ClientId, message: ClientMessage) {
        assert_ne!(self.cur, ServerId(u64::MAX));
        self.queue.push_back(Action::Client(self.cur, id, message));
    }

    fn send_admin_message(&mut self, id: AdminId, message: AdminMessage) {
        assert_ne!(self.cur, ServerId(u64::MAX));
        self.queue.push_back(Action::Admin(self.cur, id, message));
    }

    /// Collects timeouts uniquely
    fn set_timeout(&mut self, timeout: Timeout) {
        assert_ne!(self.cur, ServerId(u64::MAX));
        self.queue.push_back(Action::SetTimeout(self.cur, timeout));
    }

    fn clear_timeout(&mut self, timeout: Timeout) {
        assert_ne!(self.cur, ServerId(u64::MAX));
        self.queue
            .push_back(Action::ClearTimeout(self.cur, timeout));
    }

    fn state_changed(&mut self, old: ConsensusState, new: ConsensusState) {
        assert_ne!(self.cur, ServerId(u64::MAX));
        match (old, new) {
            (ConsensusState::Leader, ConsensusState::Candidate) => {
                panic!("Bad state transition: leader to candidate")
            }
            (ConsensusState::Follower, ConsensusState::Leader) => {
                // this test is actually OK, but the implementation does the migration
                // throught the intermediate candidate state transition, so this must
                // conform too
                panic!("Bad state transition: follower to leader (ok for solitary transition)")
            }
            (old, new) => trace!("state transition {:?} -> {:?}", old, new),
        }
    }

    fn update_peers(&mut self, _peers: &ConsensusConfig) {
        assert_ne!(self.cur, ServerId(u64::MAX));
    }
}

impl TestHandler {
    pub fn new() -> Self {
        Self {
            cur: ServerId(u64::MAX),
            queue: VecDeque::new(),
            queue2: Vec::new(),
        }
    }

    pub fn fire_election_timeout(&mut self, id: ServerId) {
        //
    }

    pub fn rotate_queue_ordered(&mut self) {
        assert!(self.queue2.is_empty());
        self.queue2.resize(self.queue.len(), Action::Empty);
        let mut queue_index = 0usize;
        while let Some(next_action) = self.queue.pop_front() {
            self.queue2[queue_index] = next_action;
            queue_index += 1;
        }
    }

    // take all collected actions from the main queue and
    // push them to second queue, changing their order according to `permutation`
    pub fn rotate_queue(&mut self, permutation: &[usize]) {
        assert!(self.queue2.is_empty());
        self.queue2.resize(permutation.len(), Action::Empty);
        let mut queue_index = 0usize;
        while let Some(next_action) = self.queue.pop_front() {
            if let Some(pos) = permutation.iter().position(|idx| idx == &queue_index) {
                self.queue2[pos] = next_action
            }
            queue_index += 1;
        }
    }

    pub fn clear(&mut self) {
        self.queue.clear();
    }

    pub fn reset_cur(&mut self) {
        self.cur = ServerId(u64::MAX);
    }
}
