use std::collections::HashMap;
use std::fmt::Debug;

use crate::message::*;
use crate::{ClientId, Peer, ServerId};

/// Handler for actions returned from consensus
///
/// Timeout setting and clearing is expected to be overwriting, i.e. setting one if it was not set
/// and resetting otherwise, i.e. setting the new one from the start
pub trait Handler: Debug {
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage);
    fn send_client_response(&mut self, id: ClientId, message: ClientResponse);
    fn set_timeout(&mut self, timeout: ConsensusTimeout);
    fn clear_timeout(&mut self, timeout: ConsensusTimeout);

    #[allow(unused_variables)]
    /// Called when consensus goes to new state. Initializing new consensus does not call this function.
    fn state_changed(&mut self, old: ConsensusStateKind, new: &ConsensusStateKind) {}

    /// called when peer caught the error where it should be restarted or failed
    fn peer_failed(&mut self, id: ServerId);
}

/// A handler that collects all messages leaving processing of them untouched.
/// Note that `timeouts` vectors may intersect, that means both - clearing and setting a new timeout was requested.
#[derive(Debug)]
pub struct CollectHandler {
    pub peer_messages: HashMap<ServerId, Vec<PeerMessage>>,
    pub client_messages: HashMap<ClientId, Vec<ClientResponse>>,
    pub timeouts: Vec<ConsensusTimeout>,
    pub clear_timeouts: Vec<ConsensusTimeout>,
    pub state: ConsensusStateKind,
}

impl CollectHandler {
    pub fn new() -> Self {
        Self {
            peer_messages: HashMap::new(),
            client_messages: HashMap::new(),
            timeouts: Vec::new(),
            clear_timeouts: Vec::new(),
            state: ConsensusStateKind::Follower,
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

    fn peer_failed(&mut self, _id: ServerId) {}

    fn clear_timeout(&mut self, timeout: ConsensusTimeout) {
        if !self.clear_timeouts.iter().any(|&t| t == timeout) {
            self.clear_timeouts.push(timeout);
        }
    }

    fn state_changed(&mut self, _old: ConsensusStateKind, new: &ConsensusStateKind) {
        self.state = new.clone()
    }

    //    fn ensure_connected(&mut self, peers: &[Peer]) -> Result<(), ()> {
    //Ok(())
    //    }
}
