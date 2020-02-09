use std::collections::{HashMap, HashSet, VecDeque};

use crate::message::ConsensusStateKind;
use crate::{ClientId, LogIndex, Peer, PeerStatus, ServerId};
/// Consensus modules can be in one of three state:
///
/// * `Follower` - which replicates AppendEntries requests and votes for it's leader.
/// * `Leader` - which leads the cluster by serving incoming requests, ensuring
///              data is replicated, and issuing heartbeats.
/// * `Candidate` -  which campaigns in an election and may become a `Leader`
///                  (if it gets enough votes) or a `Follower`, if it hears from
///                  a `Leader`.
/// * CatchingUp - which is currently trying to catch up the leader log
#[derive(Clone, Debug)]
pub enum ConsensusState {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
    CatchingUp(CatchingUpState),
}

impl ConsensusState {
    pub fn kind(&self) -> ConsensusStateKind {
        match self {
            ConsensusState::Follower(_) => ConsensusStateKind::Follower,
            ConsensusState::Candidate(_) => ConsensusStateKind::Candidate,
            ConsensusState::Leader(_) => ConsensusStateKind::Leader,
        }
    }

    pub fn is_follower(&self) -> bool {
        if let ConsensusState::Follower(_) = self {
            true
        } else {
            false
        }
    }

    pub fn is_candidate(&self) -> bool {
        if let ConsensusState::Candidate(_) = self {
            true
        } else {
            false
        }
    }

    pub fn is_leader(&self) -> bool {
        if let ConsensusState::Leader(_) = self {
            true
        } else {
            false
        }
    }
}

impl Default for ConsensusState {
    fn default() -> Self {
        ConsensusState::Follower(FollowerState::new())
    }
}

#[derive(Clone, Debug)]
pub struct CatchingUpRemote {
    pub peer: Peer,
    log_index: LogIndex,
    rounds_left: usize,
}

impl CatchingUpRemote {
    pub fn new(id: ServerId) -> Self {
        Self {
            peer: Peer {
                id,
                status: PeerStatus::FutureMember,
            },
            log_index: LogIndex(0),
            rounds_left: 10,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CatchingUpState {
    log_index: LogIndex,
    rounds_left: usize,
}

impl CatchingUpState {
    pub fn new() -> Self {
        Self {
            log_index: LogIndex(0),
            rounds_left: 10,
        }
    }
}

/// The state associated with a Raft consensus module in the `Leader` state.
#[derive(Clone, Debug)]
pub struct LeaderState {
    next_index: HashMap<ServerId, LogIndex>,
    match_index: HashMap<ServerId, LogIndex>,
    pub(crate) catching_up: Option<CatchingUpRemote>,
    /// Stores in-flight client proposals.
    pub(crate) proposals: VecDeque<(ClientId, LogIndex)>,
}

impl LeaderState {
    /// Returns a new `LeaderState` struct.
    ///
    /// # Arguments
    ///
    /// * `latest_log_index` - The index of the leader's most recent log entry at the
    ///                        time of election.
    /// * `peers` - The set of peer cluster members.
    pub(crate) fn new<'a, I: Iterator<Item = &'a ServerId>>(
        latest_log_index: LogIndex,
        peers: I,
    ) -> LeaderState {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        peers
            .map(|peer| {
                next_index.insert(peer.clone(), latest_log_index + 1);
                match_index.insert(peer.clone(), LogIndex::from(0));
            })
            .last();

        LeaderState {
            next_index,
            match_index,
            catching_up: None,
            proposals: VecDeque::new(),
        }
    }

    /// Returns the next log entry index of the follower.
    pub(crate) fn next_index(&mut self, follower: &ServerId) -> LogIndex {
        self.next_index[follower]
    }

    /// Sets the next log entry index of the follower.
    pub(crate) fn set_next_index(&mut self, follower: ServerId, index: LogIndex) {
        self.next_index.insert(follower, index);
    }

    /// Sets the index of the highest log entry known to be replicated on the
    /// follower.
    pub(crate) fn set_match_index(&mut self, follower: ServerId, index: LogIndex) {
        self.match_index.insert(follower, index);
    }

    /// Counts the number of followers containing the given log index.
    pub(crate) fn count_match_indexes(&self, index: LogIndex) -> usize {
        // +1 for self.
        self.match_index.values().filter(|&&i| i >= index).count() + 1
    }
}

/// The state associated with a Raft consensus module in the `Candidate` state.
#[derive(Clone, Debug)]
pub struct CandidateState {
    granted_votes: HashSet<ServerId>,
}

impl CandidateState {
    /// Creates a new `CandidateState`.
    pub(crate) fn new() -> CandidateState {
        CandidateState {
            granted_votes: HashSet::new(),
        }
    }

    /// Records a vote from `voter`.
    pub(crate) fn record_vote(&mut self, voter: ServerId) {
        self.granted_votes.insert(voter);
    }

    /// Returns the number of votes.
    pub(crate) fn count_votes(&self) -> usize {
        self.granted_votes.len()
    }

    /// Returns whether the peer has voted in the current election.
    pub(crate) fn peer_voted(&self, voter: ServerId) -> bool {
        self.granted_votes.contains(&voter)
    }
}

/// The state associated with a Raft consensus module in the `Follower` state.
#[derive(Clone, Debug)]
pub struct FollowerState {
    /// The most recent leader of the follower. The leader is not guaranteed to be active, so this
    /// should only be used as a hint.
    pub(crate) leader: Option<ServerId>,
}

impl FollowerState {
    /// Returns a new `FollowerState`.
    pub(crate) fn new() -> FollowerState {
        FollowerState { leader: None }
    }

    /// Sets a new leader.
    pub(crate) fn set_leader(&mut self, leader: ServerId) {
        self.leader = Some(leader);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use state::LeaderState;
    use {LogIndex, ServerId};

    /// Tests the `LeaderState`'s  `.count_match_indexes()` function and makes sure it adequately
    /// produces the correct values.
    #[test]
    fn test_count_match_indexes() {
        let index = LogIndex(0);
        let mut peers = HashSet::new();

        // All peers start at 0 index.
        let leader_state = LeaderState::new(index, &peers);
        // Should be one, since the leader node would be matched always.
        assert_eq!(1, leader_state.count_match_indexes(LogIndex(0)));

        peers.insert(ServerId(1));
        let leader_state = LeaderState::new(index, &peers);
        assert_eq!(2, leader_state.count_match_indexes(LogIndex(0)));

        peers.insert(ServerId(2));
        let leader_state = LeaderState::new(index, &peers);
        assert_eq!(3, leader_state.count_match_indexes(LogIndex(0)));

        peers.insert(ServerId(3));
        let mut leader_state = LeaderState::new(index, &peers);
        assert_eq!(4, leader_state.count_match_indexes(LogIndex(0)));

        leader_state.set_match_index(ServerId(1), LogIndex(1));
        leader_state.set_match_index(ServerId(2), LogIndex(1));
        assert_eq!(3, leader_state.count_match_indexes(LogIndex(1)));
    }
}
