use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::Error;
use log::{debug, info, trace};
use std::cmp;

use crate::consensus::State;
use crate::handler::ConsensusHandler;
use crate::message::*;
use crate::state::{ConsensusState, StateHandler};
use crate::{ClientId, ConsensusConfig, Entry, EntryData, LogIndex, Peer, ServerId, Term};

use crate::follower::FollowerState;
use crate::persistent_log::Log;
use crate::state_machine::StateMachine;

impl<L, M, H> StateHandler<L, M, H> for State<L, M, LeaderState>
where
    L: Log,
    M: StateMachine,
    H: ConsensusHandler,
{
    /// Apply an append entries request to the consensus state machine.
    fn append_entries_request(
        &mut self,
        handler: &mut H,
        from: ServerId,
        request: &AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, Option<ConsensusState<L, M>>), Error> {
        // when leader receives AppendEntries, this means another leader is in action somehow
        let leader_term = request.term;
        let current_term = self.current_term()?;

        if leader_term < current_term {
            return Ok((AppendEntriesResponse::StaleTerm(current_term), None));
        }

        if leader_term == current_term {
            // When new leader is at the same term, the single leader-per-term invariant is broken; there is a bug in the Raft
            // implementation.

            panic!("single leader condition is broken");
            //return Err(Error::AnotherLeader(from, current_term));
        }

        self.state.reset_catching_up();
        let ConsensusState::Follower(new_state) = self.to_follower(handler, leader_term)?;
        let (response, new_state) = new_state.append_entries_request(handler, from, request)?;

        Ok((response, new_state))
    }

    /// Apply an append entries response to the consensus state machine.
    ///
    /// The provided message may be initialized with a new AppendEntries request to send back to
    /// the follower in the case that the follower's log is behind.
    fn append_entries_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: &AppendEntriesResponse,
    ) -> Result<(Option<AppendEntriesRequest>, Option<ConsensusState<L, M>>), Error> {
        let current_term = self.current_term()?;
        let local_latest_log_index = self.latest_log_index()?;

        match *response {
            AppendEntriesResponse::Success(term, _)
            | AppendEntriesResponse::StaleTerm(term)
            | AppendEntriesResponse::InconsistentPrevEntry(term, _)
                if current_term < term =>
            {
                // some node has received message with term higher than ours,
                // that means some other leader appeared in consensus,
                // we should downgrade to follower immediately
                let new_state = self.to_follower(handler, current_term)?;
                return Ok((None, Some(new_state)));
            }
            AppendEntriesResponse::Success(term, _)
            | AppendEntriesResponse::StaleTerm(term)
            | AppendEntriesResponse::InconsistentPrevEntry(term, _)
                if current_term > term =>
            {
                // some follower confirmed message we've sent at previous term
                // it is ok for us
                return Ok((None, None));
            }
            AppendEntriesResponse::Success(_, follower_latest_log_index) => {
                if follower_latest_log_index > local_latest_log_index {
                    // some follower has too high index in it's log
                    // it can only happen by mistake or incorrect log on a follower
                    // but we cannot fix it from here, so we only can report

                    return Err(Error::BadFollowerIndex);
                }

                self.state.set_match_index(from, follower_latest_log_index);
                self.try_advance_commit_index(handler)?;
            }
            AppendEntriesResponse::InconsistentPrevEntry(_, next_index) => {
                self.state.set_next_index(from, next_index)?;
            }
            AppendEntriesResponse::StaleEntry => {
                return Ok((None, None));
            }
            AppendEntriesResponse::StaleTerm(_) => {
                // The peer is reporting a stale term, but the term number matches the local term.
                // Ignore the response, since it is to a message from a prior term, and this server
                // has already transitioned to the new term.

                return Ok((None, None));
            }
        }

        // catching up peer is handled internally by state fucntions
        let next_index = self.state.next_index(&from)?;

        if next_index <= local_latest_log_index {
            // If the peer is behind, send it entries to catch up.
            trace!(
                "AppendEntriesResponse: peer {} is missing at least {} entries; \
                     sending missing entries",
                from,
                local_latest_log_index - next_index
            );
            let prev_log_index = next_index - 1;
            let prev_log_term = if prev_log_index == LogIndex(0) {
                Term(0)
            } else {
                self.with_log(|log| log.term(prev_log_index))?
            };

            let from_index = next_index;
            let until_index = local_latest_log_index + 1;

            let mut message = AppendEntriesRequest {
                term: current_term,
                prev_log_index,
                prev_log_term,
                entries: Vec::new(),
                leader_commit: self.commit_index,
            };

            // as of now we push all the entries, regardless of amount
            // this may be kind of dangerous sometimes because of amount being too big
            // but most probably snapshotting whould solve it
            for idx in from_index.as_u64()..until_index.as_u64() {
                let mut entry = Entry::default();

                self.with_log(|log| log.entry(LogIndex(idx), &mut entry))?;

                message.entries.push(entry);
            }

            self.state
                .set_next_index(from, local_latest_log_index + 1)?;

            if let Err(_) = self.state.update_rounds(from) {
                handler.peer_failed(from);
            }

            Ok((Some(message), None))
        } else {
            if let Some(ref mut catching_up) = self.state.catching_up {
                if from == catching_up.peer.id {
                    // the catching up remote has catched up
                    // we should begin committing the new config
                    self.state.catching_up_ready();

                    //FIXME:
                    todo!("add config change entry to log and distribute it over cluster");
                }
            }

            // since the peer is caught up, set a heartbeat timeout.
            handler.set_timeout(ConsensusTimeout::Heartbeat(from));
            Ok((None, None))
        }
    }

    /// Applies a peer request vote request to the consensus state machine.
    fn request_vote_request(
        &mut self,
        handler: &mut H,
        candidate: ServerId,
        request: &RequestVoteRequest,
    ) -> Result<(RequestVoteResponse, Option<ConsensusState<L, M>>), Error>
where {
        // FIXME:
        // there is also leader change, meaning all uncommitted configuration changes
        // must be revert (QUESTION: does this repend on candidate's log index or not?)
        self.state.reset_catching_up(handler)?;
        self.common_request_vote_request(handler, candidate, request)
    }

    /// Applies a request vote response to the consensus state machine.
    fn request_vote_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: &RequestVoteResponse,
    ) -> Result<Option<ConsensusState<L, M>>, Error> {
    }

    // Timeout handling
    /// Handles heartbeat timeout event
    fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error> {}
    fn election_timeout(&mut self, handler: &mut H) -> Result<(), Error> {}

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error> {}

    // Configuration change messages
    fn add_server_request(
        &mut self,
        handler: &mut H,
        request: &AddServerRequest,
    ) -> Result<ServerCommandResponse, Error> {
    }

    /// Applies a client proposal to the consensus state machine.
    fn client_proposal_request(
        &mut self,
        handler: &mut H,
        from: ClientId,
        request: Vec<u8>,
    ) -> Result<Option<CommandResponse>, Error> {
    }

    fn client_query_request(&mut self, from: ClientId, request: &[u8]) -> CommandResponse {}
}

impl<L, M> State<L, M, LeaderState>
where
    L: Log,
    M: StateMachine,
{
    fn try_advance_commit_index<H: ConsensusHandler>(
        &mut self,
        handler: &mut H,
    ) -> Result<(), Error> {
        let majority = self.majority()?;
        // Here we try to move commit index to one that the majority of peers in cluster already have
        let latest_log_index = self.latest_log_index()?;

        while self.commit_index < latest_log_index {
            if self.state.count_match_indexes(self.commit_index + 1) >= majority {
                self.commit_index = self.commit_index + 1;
                debug!("commit index advanced to {}", self.commit_index);
            } else {
                break; // If there isn't a majority now, there won't be one later.
            }
        }

        let results = self.apply_commits();

        // As long as we know it, we send the connected clients the notification
        // about their proposals being committed
        // TODO: fix client proposals being out of order (think if it is possible)
        while let Some(&(client, index)) = self.state.proposals.get(0) {
            if index <= self.commit_index {
                trace!("responding to client {} for entry {}", client, index);

                // We know that there will be an index here since it was commited
                // and the index is less than that which has been commited.
                let result = &results[&index];
                handler.send_client_response(
                    client,
                    ClientResponse::Proposal(CommandResponse::Success(result.clone())),
                );
                self.state.proposals.pop_front();
            } else {
                break;
            }
        }

        Ok(())
    }
}

/// The state associated with a Raft consensus module in the `Leader` state.
#[derive(Clone, Debug)]
pub(crate) struct LeaderState {
    next_index: HashMap<ServerId, LogIndex>,
    match_index: HashMap<ServerId, LogIndex>,
    /// stores pending config change making sure there can only be one at a time
    pub(crate) catching_up: Option<ConfigChange>,
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

    /// Returns the next log entry index of the follower or a catching up peer.
    pub(crate) fn next_index(&mut self, follower: &ServerId) -> Result<LogIndex, Error> {
        if let Some(index) = self.next_index.get(follower) {
            return Ok(index);
        }

        if let Some(ConfigChange {
            peer,
            stage: ConfigChangeStage::CatchingUp(peer_index, _),
        }) = self.config_change
        {
            if peer == follower {
                // the index is requested for a catching up peer
                return Ok(peer_index);
            }
        }

        Err(Error::UnknownPeer(follower.clone()))
    }

    /// Sets the next log entry index of the follower or a catching up peer.
    pub(crate) fn set_next_index(
        &mut self,
        follower: ServerId,
        index: LogIndex,
    ) -> Result<(), Error> {
        if let Some(mut stored_index) = self.get_mut(follower) {
            self.next_index.insert(follower, index);
            *stored_index = index;
            return Ok(());
        }

        if let Some(
            ref mut
            config_change
            @
            ConfigChange {
                peer,
                stage: ConfigChangeStage::CatchingUp(peer_index, rounds),
            },
        ) = self.config_change
        {
            if peer == follower {
                // the index is set for a catching up peer
                // since set_index happens on each confirmation we also increase the round number
                config_change.stage = ConfigChangeStage::CatchingUp(index, rounds);
                return Ok(());
            }
        }

        Err(Error::UnknownPeer(follower))
    }

    pub(crate) fn update_rounds(&mut self, catching_up: ServerId) -> Result<(), Error> {
        if let Some(
            ref mut
            config_change
            @
            ConfigChange {
                peer,
                stage: ConfigChangeStage::CatchingUp(peer_index, rounds),
            },
        ) = self.config_change
        {
            if peer == follower {
                // the index is set for a catching up peer
                // since set_index happens on each confirmation we also increase the round number
                if let Some(result) = config_change.update_rounds() {
                    if result {
                        return Ok(());
                    } else {
                        return Err(Error::CatchUpFailed);
                    }
                }
            }
        }
        Ok(())
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

    pub(crate) fn start_config_change_unchecked(&mut self, new_node_id: ServerId) {
        self.config_change = Some(ConfigChange::new(new_node_id))
    }
}

#[derive(Clone, Debug)]
pub enum ConfigChangeStage {
    CatchingUp {
        index: LogIndex,
        rounds: usize,
        response_this_timeout: bool, // flag to measure round time
    },
    Committing(Vec<Peer>), // previous config
}

#[derive(Clone, Debug)]
pub struct ConfigChange {
    pub(crate) peer: Peer,
    pub(crate) stage: ConfigChangeStage,
}

impl ConfigChange {
    pub(crate) fn new(id: ServerId) -> Self {
        Self {
            peer: Peer {
                id,
                status: PeerStatus::FutureMember,
            },
            stage: ConfigChangeStage::CatchingUp {
                index: LogIndex(0),
                rounds: 10,
                response_this_timeout: false,
            },
        }
    }

    pub(crate) fn handle_timeout(&mut self) {
        if let ConfigChangeStage::CatchingUp {
            response_this_timeout,
            ..
        } = &mut self.stage
        {
            *response_this_timeout = false;
        } else {
            panic!("IMPLEMENTATION BUG: update_rounds called during wrong stage")
        }
    }

    /// returns if node is
    /// * too slow (None)
    /// * did not catch up Some(false)
    /// * caught up Some(true)
    pub(crate) fn update_rounds(&mut self, new_index: LogIndex) -> Option<bool> {
        if let ConfigChangeStage::CatchingUp {
            index,
            rounds,
            response_this_timeout,
        } = &mut self.stage
        {
            *index = new_index;
            if *rounds == 0 {
                return None;
            } else {
                *rounds -= 1;
                if *response_this_timeout {
                    self.stage = ConfigChangeStage::Committing(Vec::new());
                    // node has caught up because we already had reponse within this timeout
                    return Some(true);
                } else {
                    *response_this_timeout = true;
                    return Some(false);
                }
            }
        } else {
            panic!("IMPLEMENTATION BUG: update_rounds called during wrong stage")
        }
    }
}
