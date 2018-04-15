use std::collections::HashMap;

use {ClientId, ServerId};
use message::*;
use consensus::ConsensusHandler;

/// A handler that collects all messages leaving processing of them untouched
/// Note that timeouts vectors may intersect, that means both - clearing and setting a new timeout was requested.
#[derive(Debug)]
pub struct CollectHandler {
    pub peer_messages: HashMap<ServerId, Vec<PeerMessage>>,
    pub client_messages: HashMap<ClientId, Vec<ClientResponse>>,
    pub timeouts: Vec<ConsensusTimeout>,
    pub clear_timeouts: Vec<ConsensusTimeout>,
}

impl CollectHandler {
    pub fn new() -> Self {
        Self {
            peer_messages: HashMap::new(),
            client_messages: HashMap::new(),
            timeouts: Vec::new(),
            clear_timeouts: Vec::new(),
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

impl ConsensusHandler for CollectHandler {
    /// Saves peer message to a vector
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage) {
        let peer = self.peer_messages.entry(id).or_insert(Vec::new());
        peer.push(message);
    }

    /// Saves client message to a vector
    fn send_client_response(&mut self, id: ClientId, message: ClientResponse) {
        let client = self.client_messages.entry(id).or_insert(Vec::new());
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
}
