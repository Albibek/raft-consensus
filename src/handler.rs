use std::collections::HashMap;
use std::fmt::Debug;

use crate::entry::ConsensusConfig;
use crate::message::*;
use crate::{AdminId, ClientId, ServerId};

/// Handler for actions returned from consensus
///
/// Timeout setting and clearing is expected to be overwriting, i.e. setting one if it was not set
/// and resetting otherwise, i.e. setting the new one from the start
pub trait Handler: Debug {
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage);
    fn send_client_message(&mut self, id: ClientId, message: ClientMessage);
    fn send_admin_message(&mut self, id: AdminId, message: AdminMessage);

    fn set_timeout(&mut self, timeout: Timeout);
    fn clear_timeout(&mut self, timeout: Timeout);

    /// Let handler know about new peers added by configuration change.
    /// The usual reaction could be, for example, to establish a connection to new peers getting
    /// exchanged messages witht hem.
    // Called when some follower(especially the catching one) receives the latest configuration change
    // so peer list may be counted updated and ready for sending messages.
    fn update_peers(&mut self, peers: &ConsensusConfig);

    #[allow(unused_variables)]
    /// Called when consensus goes to new state. Initializing new consensus does not call this function.
    fn state_changed(&mut self, old: ConsensusState, new: &ConsensusState) {}
}

/// A handler that collects all messages leaving processing of them untouched.
/// Note that `timeouts` vectors may intersect, that means both - clearing and setting a new timeout was requested.
#[derive(Debug)]
pub struct CollectHandler {
    pub peer_messages: HashMap<ServerId, Vec<PeerMessage>>,
    pub client_messages: HashMap<ClientId, Vec<ClientMessage>>,
    pub admin_messages: HashMap<AdminId, Vec<AdminMessage>>,
    pub timeouts: Vec<Timeout>,
    pub clear_timeouts: Vec<Timeout>,
    pub state: ConsensusState,
}

impl CollectHandler {
    pub fn new() -> Self {
        Self {
            peer_messages: HashMap::new(),
            client_messages: HashMap::new(),
            admin_messages: HashMap::new(),
            timeouts: Vec::new(),
            clear_timeouts: Vec::new(),
            state: ConsensusState::Follower,
        }
    }

    /// Delete all events
    pub fn clear(&mut self) {
        self.peer_messages.clear();
        self.client_messages.clear();
        self.timeouts.clear();
        self.clear_timeouts.clear();
    }
}

impl Default for CollectHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl Handler for CollectHandler {
    /// Saves peer message to a vector
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage) {
        let queue = self.peer_messages.entry(id).or_insert_with(Vec::new);
        queue.push(message);
    }

    /// Saves client message to a vector
    fn send_client_message(&mut self, id: ClientId, message: ClientMessage) {
        let queue = self.client_messages.entry(id).or_insert_with(Vec::new);
        queue.push(message);
    }

    /// Saves admin message to a vector
    fn send_admin_message(&mut self, id: AdminId, message: AdminMessage) {
        let queue = self.admin_messages.entry(id).or_insert_with(Vec::new);
        queue.push(message);
    }

    /// Collects timeouts uniquely
    fn set_timeout(&mut self, timeout: Timeout) {
        if !self.timeouts.iter().any(|&t| t == timeout) {
            self.timeouts.push(timeout);
        }
    }

    fn clear_timeout(&mut self, timeout: Timeout) {
        if !self.clear_timeouts.iter().any(|&t| t == timeout) {
            self.clear_timeouts.push(timeout);
        }
    }

    fn update_peers(&mut self, peers: &ConsensusConfig) {}

    fn state_changed(&mut self, _old: ConsensusState, new: &ConsensusState) {
        self.state = new.clone()
    }
}
