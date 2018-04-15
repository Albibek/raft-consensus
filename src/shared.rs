use std::sync::{Arc, Mutex};

use {ClientId, ServerId};
use consensus::{ConsensusHandler, HandledConsensus};
use state_machine::StateMachine;
use persistent_log::Log;
use message::*;
use error::Error;

/// Convenience wrapper for multithreaded handling of consensus packages
/// Based on standard `Arc<Mutex<_>>` approach
#[derive(Debug, Clone)]
pub struct SharedConsensus<L, M, H> {
    inner: Arc<Mutex<HandledConsensus<L, M, H>>>,
}

impl<L, M, H> SharedConsensus<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: ConsensusHandler,
{
    pub fn new(consensus: HandledConsensus<L, M, H>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(consensus)),
        }
    }
    /// Calls initial actions which should be executed upon startup.
    pub fn init(&self) {
        self.inner.lock().unwrap().init()
    }

    /// Applies a peer message to the consensus state machine.
    pub fn apply_peer_message(&self, from: ServerId, message: PeerMessage) -> Result<(), Error> {
        self.inner.lock().unwrap().apply_peer_message(from, message)
    }

    /// Applies a client message to the consensus state machine.
    pub fn apply_client_message(
        &self,
        from: ClientId,
        message: ClientRequest,
    ) -> Result<(), Error> {
        self.inner
            .lock()
            .unwrap()
            .apply_client_message(from, message)
    }

    /// Triggers a timeout for the peer.
    pub fn apply_timeout(&self, timeout: ConsensusTimeout) -> Result<(), Error> {
        self.inner.lock().unwrap().apply_timeout(timeout)
    }

    /// Triggers a heartbeat timeout for the peer.
    pub fn heartbeat_timeout(&self, peer: ServerId) -> Result<AppendEntriesRequest, Error> {
        self.inner.lock().unwrap().heartbeat_timeout(peer)
    }

    pub fn election_timeout(&self) -> Result<(), Error> {
        self.inner.lock().unwrap().election_timeout()
    }
}
