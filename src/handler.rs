use std::collections::HashMap;
use std::fmt::Debug;

use message::*;
use state::ConsensusState;
use {ClientId, ServerId};

/// Handler for actions returned from consensus
pub trait ConsensusHandler: Debug {
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage);
    fn send_client_response(&mut self, id: ClientId, message: ClientResponse);
    fn set_timeout(&mut self, timeout: ConsensusTimeout);
    fn clear_timeout(&mut self, timeout: ConsensusTimeout);

    #[allow(unused_variables)]
    /// Called when consensus goes to new state. Initializing new consensus does not call this function.
    fn state_changed(&mut self, old: ConsensusState, new: ConsensusState) {}

    /// Called when the particular event has been fully processed. Useful for doing actions in batches.
    fn done(&mut self) {}
}

/// A handler that collects all messages leaving processing of them untouched.
/// Note that timeouts vectors may intersect, that means both - clearing and setting a new timeout was requested.
#[derive(Debug)]
pub struct CollectHandler {
    pub peer_messages: HashMap<ServerId, Vec<PeerMessage>>,
    pub client_messages: HashMap<ClientId, Vec<ClientResponse>>,
    pub timeouts: Vec<ConsensusTimeout>,
    pub clear_timeouts: Vec<ConsensusTimeout>,
    pub state: ConsensusState,
}

impl CollectHandler {
    pub fn new() -> Self {
        Self {
            peer_messages: HashMap::new(),
            client_messages: HashMap::new(),
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

impl ConsensusHandler for CollectHandler {
    /// Saves peer message to a vector
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage) {
        let peer = self.peer_messages.entry(id).or_insert_with(Vec::new);
        peer.push(message);
    }

    /// Saves client message to a vector
    fn send_client_response(&mut self, id: ClientId, message: ClientResponse) {
        let client = self.client_messages.entry(id).or_insert_with(Vec::new);
        client.push(message);
    }

    /// Collects timeouts uniquely
    fn set_timeout(&mut self, timeout: ConsensusTimeout) {
        if !self.timeouts.iter().any(|&t| t == timeout) {
            self.timeouts.push(timeout);
        }
    }

    fn clear_timeout(&mut self, timeout: ConsensusTimeout) {
        if !self.clear_timeouts.iter().any(|&t| t == timeout) {
            self.clear_timeouts.push(timeout);
        }
    }

    fn state_changed(&mut self, _old: ConsensusState, new: ConsensusState) {
        self.state = new
    }
}
