#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};
use std::iter::FromIterator;

use crate::error::Error;
use crate::message::Timeout;

use crate::{Peer, ServerId};

pub use crate::handler::Handler;
pub use crate::persistent_log::Log;
pub use crate::state_machine::StateMachine;

/// Full cluster config replicated using Raft mechanism. Always stores all
/// nodes, including self.
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

    // Get the cluster quorum majority size.
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

/// Tunables for each node. This config is per node and not replicated.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsensusOptions {
    pub timeouts: SlowNodeTimeouts,
}


/// Raft paper describes a multiple-round method for catching up the log where the size of the
/// message is reduced each round due to number of log entries decreasing.
///
/// Still, paper says nothing about restoring snapshots on a catching up node.
/// Since a size of chunks are most probably not decreasing each send, there is no
/// point to measure them in rounds, so we introduce the fixed amount of election
/// timeouts which should be enough to install a full snapshot, or even several snapshots
/// if they go one after another.
/// There is also a possible state where node is flapping between catching up and
/// installing snapshot(for example snapshots install fast, but log catching
/// is slow). To avoid flapping, there is also state independent total_timeouts
/// counter, which decreases each election timeout regardless of the incoming messages.
///
/// timeouts are given per message, i.e. per snapshot chunk or a list of log entries,
/// each timeout is given as a number of election timeouts, so it is a bit random
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlowNodeTimeouts {
    pub max_snapshot_timeouts: u32,
    pub max_log_timeouts: u32,
    pub max_total_timeouts: u32,
}

impl Default for SlowNodeTimeouts {
    fn default() -> Self {
        Self {
            max_snapshot_timeouts: 40,
            max_log_timeouts: 20,
            // for regular network max election timeout will be around 500ms, but
            // not less than heartbeat timeout of 250ms, which means a median of
            // 375ms.
            // We should allow a big time for snapshot to settle in these conditions.
            // Let this be 10 minutes by default
            max_total_timeouts: 10u32 * 60 * 1000 / 375,
        }
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
