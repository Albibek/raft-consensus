use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use crate::message::*;
use crate::*;

use crate::handler::Handler;

use tracing::trace;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Peer(ServerId, ServerId, PeerMessage),
    Timeout(ServerId, Timeout),
    Client(ClientId, ServerId, ClientMessage),
    Admin(AdminId, ServerId, AdminMessage),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestHandler {
    pub cur: ServerId,

    pub peers: HashSet<ServerId>,
    pub clients: HashSet<ClientId>,
    pub admins: HashSet<AdminId>,

    pub peer_network: HashMap<(ServerId, ServerId), VecDeque<PeerMessage>>,
    /// handler is used only to send responses, not the requests
    /// so we don't emulate requests
    pub client_network: HashMap<(ServerId, ClientId), VecDeque<ClientMessage>>,
    pub admin_network: HashMap<(ServerId, AdminId), VecDeque<AdminMessage>>,

    pub election_timeouts: HashMap<ServerId, bool>,
    pub heartbeat_timeouts: HashMap<ServerId, Option<HashMap<ServerId, bool>>>,
    pub client_timeouts: HashMap<ServerId, bool>,
}

impl Handler for TestHandler {
    /// Saves peer message to a vector
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage) {
        assert_ne!(self.cur, ServerId(u64::MAX));
        let q = self.peer_network.get_mut(&(self.cur, id)).unwrap();
        q.push_back(message);
    }

    /// Saves client message to a vector
    fn send_client_message(&mut self, id: ClientId, message: ClientMessage) {
        assert_ne!(self.cur, ServerId(u64::MAX));
        let q = self.client_network.get_mut(&(self.cur, id)).unwrap();
        q.push_back(message);
    }

    fn send_admin_message(&mut self, id: AdminId, message: AdminMessage) {
        assert_ne!(self.cur, ServerId(u64::MAX));
        let q = self.admin_network.get_mut(&(self.cur, id)).unwrap();
        q.push_back(message);
    }

    /// Collects timeouts uniquely
    fn set_timeout(&mut self, timeout: Timeout) {
        assert_ne!(self.cur, ServerId(u64::MAX));
        //       self.queue.push_back(Action::SetTimeout(self.cur, timeout));

        // all timeouts being set or reset go to next step's queue
        if !self.heartbeat_timeouts.contains_key(&self.cur) {
            self.heartbeat_timeouts.insert(self.cur, None);
        }
        match timeout {
            Timeout::Election => {
                self.election_timeouts.insert(self.cur, true); // previous id must exist
            }
            Timeout::Heartbeat(id) => {
                if let Some(timeouts) = self.heartbeat_timeouts.get_mut(&self.cur).unwrap() {
                    timeouts.insert(id, true);
                } else {
                    let mut timeouts = HashMap::new();
                    timeouts.insert(id, true);
                    *self.heartbeat_timeouts.get_mut(&self.cur).unwrap() = Some(timeouts);
                }
            }
            Timeout::Client => {
                self.client_timeouts.insert(self.cur, true); // previous id must exist
            }
        }
    }

    fn clear_timeout(&mut self, timeout: Timeout) {
        assert_ne!(self.cur, ServerId(u64::MAX));

        if !self.heartbeat_timeouts.contains_key(&self.cur) {
            self.heartbeat_timeouts.insert(self.cur, None);
        }
        match timeout {
            Timeout::Election => {
                self.election_timeouts.insert(self.cur, false);
            }
            Timeout::Heartbeat(id) => {
                if let Some(timeouts) = self.heartbeat_timeouts.get_mut(&self.cur).unwrap() {
                    timeouts.insert(id, false);
                } else {
                    let mut timeouts = HashMap::new();
                    timeouts.insert(id, false);
                    *self.heartbeat_timeouts.get_mut(&self.cur).unwrap() = Some(timeouts);
                }
            }
            Timeout::Client => {
                self.client_timeouts.insert(self.cur, false);
            }
        }
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
            (old, new) => trace!("id={} state transition {:?} -> {:?}", self.cur, old, new),
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

            peers: HashSet::new(),
            clients: HashSet::new(),
            admins: HashSet::new(),

            peer_network: HashMap::new(),
            client_network: HashMap::new(),
            admin_network: HashMap::new(),

            election_timeouts: HashMap::new(),
            heartbeat_timeouts: HashMap::new(),
            client_timeouts: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, id: ServerId) {
        if self.peers.contains(&id) {
            // do not change existing nodes
            return;
        }

        if self.peers.len() == 0 {
            self.peers.insert(id);
            return;
        }

        // connect each other peer to the new one
        for peer in self.peers.iter().cloned() {
            self.peer_network.insert((peer, id), VecDeque::new());
            self.peer_network.insert((id, peer), VecDeque::new());
        }
        // then add the peer to list
        self.peers.insert(id);
    }

    pub fn add_client(&mut self, id: ClientId) {
        if self.clients.contains(&id) {
            // do not change existing nodes
            return;
        }
        self.clients.insert(id);

        for peer in self.peers.iter().cloned() {
            self.client_network.insert((peer, id), VecDeque::new());
        }
    }

    pub fn add_admin(&mut self, id: AdminId) {
        if self.admins.contains(&id) {
            // do not change existing nodes
            return;
        }
        self.admins.insert(id);

        for peer in self.peers.iter().cloned() {
            self.admin_network.insert((peer, id), VecDeque::new());
        }
    }

    pub fn reset_cur(&mut self) {
        self.cur = ServerId(u64::MAX);
    }

    pub fn peer_net_len(&self) -> usize {
        let mut len = 0;
        for (_, q) in &self.peer_network {
            len += q.len()
        }
        len
    }
}