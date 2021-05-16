#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};
use std::iter::FromIterator;

#[cfg(feature = "use_capnp")]
use capnp::message::{Allocator, Builder, HeapAllocator, Reader, ReaderSegments};

//#[cfg(feature = "use_capnp")]
//use crate::messages_capnp::{entry as entry_capnp, entry_data};

use crate::error::Error;
use crate::message::Timeout;

use crate::{ClientId, LogIndex, Peer, ServerId, Term};

pub use crate::handler::Handler;
pub use crate::persistent_log::Log;
pub use crate::state_machine::StateMachine;

// An interface to full cluster config, that always stores all
// nodes, including self, but always requires
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct ConsensusConfig {
    pub peers: Vec<Peer>,
}

impl ConsensusConfig {
    pub fn new<I: Iterator<Item = Peer>>(peers: I) -> Self {
        Self {
            peers: Vec::from_iter(peers),
        }
    }

    pub(crate) fn is_solitary(&self, this: ServerId) -> bool {
        self.peers.len() == 1 && self.peers[0].id == this
    }

    pub(crate) fn has_peer(&self, peer: ServerId) -> bool {
        self.peers.iter().any(|p| p.id == peer)
    }

    // Smart configuration change: adds peer if it was not in the list of peers
    // removes peer it it was there.
    // Returns true, if adding was done, false if removing was done
    pub(crate) fn add_or_remove_peer(&mut self, peer: Peer) -> Result<bool, Error> {
        if let Some(pos) = self
            .peers
            .iter()
            .position(|self_peer| self_peer.id == peer.id)
        {
            // peer esxisted - remove it
            if self.peers.len() == 1 {
                return Err(Error::LastNodeRemoval);
            }
            self.peers.swap_remove(pos);

            Ok(false)
        } else {
            // peer is not in list, adding a new one
            self.peers.push(peer);
            Ok(true)
        }
    }

    /// Get the cluster quorum majority size.
    pub(crate) fn majority(&self, this: ServerId) -> usize {
        let peers = self.peers.len();
        // paper 4.2.2: node shoult not include itself to majority if it is
        // not a cluster member, but still have to start voting
        if self.has_peer(this) {
            (peers >> 1) + 1
        } else {
            peers >> 1
        }
    }

    pub(crate) fn clear_heartbeats<H: Handler>(&self, handler: &mut H) {
        for peer in &self.peers {
            handler.clear_timeout(Timeout::Heartbeat(peer.id));
        }
    }

    pub(crate) fn with_remote_peers<F>(&self, this: &ServerId, mut f: F)
    where
        F: FnMut(&ServerId) -> Result<(), Error>,
    {
        self.peers
            .iter()
            .filter(|peer| &peer.id != this)
            .map(|peer| f(&peer.id))
            .last();
    }
}

//TODO
//#[cfg(test)]
//mod test {

//// Tests the majority function.
//#[test]
//fn test_majority() {
//let peers = TestCluster::new(1).peers;
//let majority = peers.values().next().unwrap().majority();
//assert_eq!(1, majority);

//let peers = TestCluster::new(2).peers;
//let majority = peers.values().next().unwrap().majority();
//assert_eq!(2, majority);

//let peers = TestCluster::new(3).peers;
//let majority = peers.values().next().unwrap().majority();
//assert_eq!(2, majority);
//let peers = TestCluster::new(4).peers;
//let majority = peers.values().next().unwrap().majority();
//assert_eq!(3, majority);
//}
//}
