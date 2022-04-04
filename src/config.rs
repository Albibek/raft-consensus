#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};
use std::iter::FromIterator;

use crate::error::Error;
use crate::message::Timeout;

use crate::{debug_where, LogIndex, Peer, ServerId};

pub use crate::handler::Handler;
pub use crate::persistent_log::Log;
pub use crate::state_machine::StateMachine;

/// Full cluster config replicated using Raft mechanism. Always stores all
/// nodes, including self.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsensusConfig {
    /// all voting peers committed to config
    pub voters: Vec<Peer>,
    /// all voter candidates, not persisted until they are committed becoming
    /// voters
    pub volatile: Vec<Peer>,

    // TODO: non-voting nodes already committed to config
    //pub non_voters: Vec<Peer>,
    pending: PendingChange,
}

impl ConsensusConfig {
    pub fn uninitialized() -> Self {
        Self {
            voters: Vec::new(),
            volatile: Vec::new(),
            pending: PendingChange::default(),
        }
    }

    /// create new config with no volatile peers
    pub fn new<I: Iterator<Item = Peer>>(peers: I) -> Self {
        Self {
            voters: Vec::from_iter(peers),
            volatile: Vec::new(),
            pending: PendingChange::default(),
        }
    }

    /// Creates a pending config from self and the one provided
    pub(crate) fn update_from_new(&mut self, new_peers: &[Peer]) -> Result<(), Error> {
        if self.pending != PendingChange::Nothing {
            return Err(Error::ConfigChangeExists);
        }

        if new_peers.is_empty() {
            return Err(Error::LastNodeRemoval);
        }
        if (new_peers.len() as isize - self.voters.len() as isize).abs() > 1 {
            // We also support changing meta, i.e. when
            // no peer ids get changed.
            // This will require pushing and commiting the config entry
            // in the same way it is done when peers change.
            return Err(Error::ConfigChangeTooBig);
        }

        for peer in &self.voters {
            if !new_peers.iter().any(|new_peer| new_peer.id == peer.id) {
                if self.pending != PendingChange::Nothing {
                    return Err(Error::ConfigChangeTooBig);
                } else {
                    self.pending = PendingChange::Remove(peer.clone());
                }
            }
        }
        for new_peer in new_peers {
            if !self.has_peer(new_peer.id) {
                if self.pending != PendingChange::Nothing {
                    return Err(Error::ConfigChangeTooBig);
                } else {
                    self.pending = PendingChange::Add(new_peer.clone());
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn is_initialized(&self) -> bool {
        !self.voters.is_empty()
    }

    #[inline]
    pub(crate) fn is_solitary(&self, this: ServerId) -> bool {
        self.voters.len() == 1 && self.voters[0].id == this
    }

    #[inline]
    pub(crate) fn has_pending_removal(&self) -> bool {
        if let PendingChange::Remove(_) = self.pending {
            true
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn has_pending_removal_for(&self, id: ServerId) -> bool {
        if let PendingChange::Remove(peer) = &self.pending {
            id == peer.id
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn has_peer(&self, peer: ServerId) -> bool {
        self.voters.iter().any(|p| p.id == peer)
    }

    #[inline]
    // unlike remove_volatile we don't return error here because
    // this functions is called as a reaction to add_server_command, so
    // the message to admin can be returned instead
    pub(crate) fn add_volatile(&mut self, peer: Peer) -> bool {
        if let Some(pos) = self.volatile.iter().position(|peer| peer.id == peer.id) {
            return false;
        } else {
            self.volatile.push(peer);
            return true;
        }
    }

    #[inline]
    pub(crate) fn remove_volatile(&mut self, id: ServerId) -> Result<(), Error> {
        if let Some(pos) = self.volatile.iter().position(|peer| peer.id == id) {
            self.volatile.swap_remove(pos);
            Ok(())
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }

    #[inline]
    pub(crate) fn forget_volatile(&mut self) {
        self.pending = PendingChange::Nothing;
        self.volatile.clear()
    }

    pub(crate) fn create_config_update(&self) -> Vec<Peer> {
        let mut result = self.voters.clone();
        match &self.pending {
            PendingChange::Nothing => {}
            PendingChange::Add(peer) => result.push(peer.clone()),
            PendingChange::Remove(peer) => {
                if let Some(pos) = result.iter().position(|p| p.id == peer.id) {
                    result.swap_remove(pos);
                }
            }
        }
        result.sort();
        result
    }

    #[inline]
    pub(crate) fn new_pending_add(&mut self, id: ServerId) -> Result<(), Error> {
        if let Some(pos) = self.volatile.iter().position(|peer| peer.id == id) {
            let peer = self.volatile.swap_remove(pos);
            self.pending = PendingChange::Add(peer);
            Ok(())
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }

    pub(crate) fn has_pending_peer(&self, id: ServerId) -> bool {
        match &self.pending {
            PendingChange::Nothing => false,
            PendingChange::Add(peer) => peer.id == id,
            PendingChange::Remove(peer) => peer.id == id,
        }
    }

    pub(crate) fn has_changes(&self) -> bool {
        self.pending != PendingChange::Nothing
    }

    // Moves pending change into the main configuration
    pub(crate) fn commit_pending(&mut self) {
        let mut pending = PendingChange::Nothing;
        std::mem::swap(&mut pending, &mut self.pending);
        match pending {
            PendingChange::Nothing => {}
            PendingChange::Add(peer) => self.voters.push(peer),
            PendingChange::Remove(peer) => {
                if let Some(pos) = self.voters.iter().position(|p| p.id == peer.id) {
                    self.voters.swap_remove(pos);
                }
            }
        }
    }

    // Get the cluster quorum majority size.
    // Assumes config is already persisted.
    pub(crate) fn majority(&self) -> usize {
        let peers = self.voters.len();
        if peers == 1 {
            return 1;
        }
        let peers = match &self.pending {
            // there is no problem when no config changes happen
            PendingChange::Nothing => peers,

            // if new peer is being added, it must be counted, but only
            // if it is not added by mistake (which may be unreachable,
            // but we can handle it so why not)
            PendingChange::Add(added) => {
                if !self.has_peer(added.id) {
                    // only if the added peer
                    // was not in previous config
                    peers + 1
                } else {
                    peers
                }
            }

            // if the peer is being removed, it should not be included in the majority, but
            // only if it existed in previous configuration
            PendingChange::Remove(removed) => {
                if self.has_peer(removed.id) {
                    // only when config is already persisted as entry
                    // and if node existed in previous configuration
                    // we should not count it as majority
                    // it conforms 4.2.2 as well
                    peers - 1
                } else {
                    peers
                }
            }
        };
        peers >> 1
    }

    pub(crate) fn peer_votes_for_self(&self, latest_index: LogIndex, id: ServerId) -> bool {
        // 4.2.2 a server that is not part of its own latest configuration should still start
        // new elections, as it might still be needed until the Cnew entry is committed (as in Figure 4.6). It does
        // not count its own vote in elections unless it is part of its latest configuration.
        if let PendingChange::Remove(peer) = &self.pending {
            peer.id == id
        } else {
            true
        }
    }

    pub(crate) fn clear_heartbeats<H: Handler>(&self, handler: &mut H) {
        for peer in &self.voters {
            handler.clear_timeout(Timeout::Heartbeat(peer.id));
        }
    }

    pub(crate) fn with_all_peers<F>(&self, mut f: F)
    where
        F: FnMut(&ServerId) -> Result<(), Error>,
    {
        self.voters.iter().map(|peer| f(&peer.id)).last();
        if let PendingChange::Add(peer) = &self.pending {
            f(&peer.id);
        }
    }

    pub(crate) fn with_remote_peers<F>(&self, this: &ServerId, mut f: F)
    where
        F: FnMut(&ServerId) -> Result<(), Error>,
    {
        self.voters
            .iter()
            .filter(|peer| &peer.id != this)
            .map(|peer| f(&peer.id))
            .last();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum PendingChange {
    Nothing,
    Add(Peer),
    Remove(Peer),
}

impl Default for PendingChange {
    fn default() -> Self {
        PendingChange::Nothing
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
