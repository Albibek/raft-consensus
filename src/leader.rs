use std::collections::{HashMap, VecDeque};

use crate::error::*;
use log::{debug, info, trace};

use crate::entry::{ConsensusConfig, Entry, EntryData};
use crate::handler::Handler;
use crate::message::*;
use crate::raft::CurrentState;
use crate::state::State;
use crate::state_impl::StateImpl;
use crate::{ClientId, LogIndex, Peer, ServerId, Term};

use crate::follower::Follower;
use crate::persistent_log::Log;
use crate::state_machine::StateMachine;

static MAX_ROUNDS: u32 = 10;
static MAX_TIMEOUTS: u32 = 20;

impl<L, M, H> StateImpl<L, M, H> for State<L, M, H, Leader>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    fn append_entries_request(
        self,
        handler: &mut H,
        from: ServerId,
        request: &AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, CurrentState<L, M, H>), Error> {
        // when leader receives AppendEntries, this means another leader is in action somehow
        let leader_term = request.term;
        let current_term = self.current_term()?;

        if leader_term < current_term {
            return Ok((AppendEntriesResponse::StaleTerm(current_term), self.into()));
        }

        if leader_term == current_term {
            // When new leader is at the same term, the single leader-per-term invariant is broken; there is a bug in the Raft
            // implementation.

            panic!("BUG: single leader condition is broken");
            //return Err(Error::AnotherLeader(from, current_term));
        }

        let mut new_state = self.leader_into_follower(handler, leader_term)?;
        let (response, new_state) = new_state.append_entries_request(handler, from, request)?;

        Ok((response, new_state))
    }

    /// The provided message may be initialized with a new AppendEntries request to send back to
    /// the follower in the case that the follower's log is behind.
    fn append_entries_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: &AppendEntriesResponse,
    ) -> Result<(Option<AppendEntriesRequest>, CurrentState<L, M, H>), Error> {
        let current_term = self.current_term()?;
        let local_latest_log_index = self.latest_log_index()?;

        // First, check if the response is from the correct term
        // and is not some error
        match response {
            AppendEntriesResponse::Success(term, _)
            | AppendEntriesResponse::StaleTerm(term)
            | AppendEntriesResponse::InconsistentPrevEntry(term, _)
                if &current_term < term =>
            {
                // some node has received message with term higher than ours,
                // that means some other leader appeared in consensus,
                // we should downgrade to follower immediately
                let new_state = self.leader_into_follower(handler, current_term)?;
                return Ok((None, new_state.into()));
            }

            AppendEntriesResponse::Success(term, _)
            | AppendEntriesResponse::StaleTerm(term)
            | AppendEntriesResponse::InconsistentPrevEntry(term, _)
                if &current_term > term =>
            {
                // some follower confirmed message we've sent at previous term
                // it is ok for us
                return Ok((None, self.into()));
            }
            AppendEntriesResponse::StaleEntry => {
                return Ok((None, self.into()));
            }
            AppendEntriesResponse::StaleTerm(_) => {
                // The peer is reporting a stale term, but the term number matches the local term.
                // Ignore the response, since it is to a message from a prior term, and this server
                // has already transitioned to the new term.

                return Ok((None, self.into()));
            }
            AppendEntriesResponse::InconsistentPrevEntry(_, next_index) => {
                self.state_data.set_next_index(from, *next_index)?;
            }
            AppendEntriesResponse::Success(_, follower_latest_log_index) => {
                if *follower_latest_log_index > local_latest_log_index {
                    // some follower has too high index in it's log
                    // it can only happen by mistake or incorrect log on a follower
                    // but we cannot fix it from here, so we only can report

                    return Err(Error::BadFollowerIndex);
                }

                // catching up node will be handled internally
                self.state_data
                    .set_match_index(from, *follower_latest_log_index);

                if self.state_data.has_follower(&from) {
                    // advance commit only if response was from follower
                    self.try_advance_commit_index(handler)?;
                } else if self.state_data.is_catching_up(&from) {
                    // having success from the catching up node, means
                    // it has catched up the last round
                    // the exception here is teh very first "ping"
                    // request, it will go to Some(false) because
                    // last round check was not set to true intentionally
                    match self.state_data.update_rounds()? {
                        CatchUpStatus::CaughtUp => {
                            // switch to new stage
                            self.state_data.begin_config_change(
                                local_latest_log_index + 1,
                                &mut self.config,
                            )?;

                            // the new config is definitely the latest one
                            let (log_index, message) =
                                self.add_next_entry(EntryData::Config(self.config.clone(), true))?;

                            self.send_append_entries_request(handler, log_index, message)?;
                            return Ok((None, self.into()));
                        }
                        CatchUpStatus::NotYet => {
                            trace!("catching up node did not catch this round, continuing the catching up process");
                            // nothing shoule be done here: the rounds has been rotated,
                            // the rest of logic is the same as for a regular follower,
                            // which is written below
                        }
                        CatchUpStatus::TooSlow => {
                            trace!("catching up node was too slow, configuration change cancelled");
                            let mut config = self.config.clone();
                            self.with_log_mut(|log| log.read_latest_config(&mut config))?;
                            self.state_data.reset_config_change(handler);
                            return Ok((None, self.into()));
                        }
                    }
                } else {
                    return Err(Error::UnknownPeer(from.clone()));
                }
            }
        }

        // after all checks and returns we get to this point
        // when follower is at the correct term and it's latest index
        // is considered by leader
        //
        // So, the only thing left to decide is which messages to send to it next

        // catching up peer is handled internally by state funtion
        let next_index = self.state_data.next_index(&from)?;

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
                // non-existence of index is definitely a bug here
                self.with_log(|log| log.term_at(prev_log_index))?
                    .ok_or(Error::log_broken(prev_log_index, module_path!()))?
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
            // but most probably snapshotting would solve it
            for idx in from_index.as_u64()..until_index.as_u64() {
                let mut entry = Entry::default();

                self.with_log_mut(|log| log.entry(LogIndex(idx), &mut entry))?;

                message.entries.push(entry);
            }
            self.state_data
                .set_next_index(from, local_latest_log_index + 1)?;

            Ok((Some(message), self.into()))
        } else {
            if self.state_data.is_catching_up(&from) {
                // the catching up remote has catched up
                // we should begin committing the new config
            }

            todo!("remove the election timeout to stop counting rounds for catching up follower");
            //FIXME:
            todo!("add config change entry to log and distribute it over cluster");

            // since the peer is caught up, set a heartbeat timeout.
            handler.set_timeout(Timeout::Heartbeat(from));
            Ok((None, self.into()))
        }
    }

    /// Applies a peer vote request to the consensus.
    fn request_vote_request(
        self,
        handler: &mut H,
        candidate: ServerId,
        request: &RequestVoteRequest,
    ) -> Result<(Option<RequestVoteResponse>, CurrentState<L, M, H>), Error> {
        // To avoid disrupting leader while configuration changes, node should ignore or delay vote requests
        // coming within election timeout unless there is special flag set signalling
        // a remote node is allowed such disruption
        //
        // But leader does not set the election timeout (it actually does, but for another purpose, meaning
        // that it  may be set or not at the time of receiveing this message)
        // Given so, receiving such a packet is only valid when disruption was allowed explicitly
        if request.is_voluntary_step_down {
            let (response, new_state) = self.common_request_vote_request(
                handler,
                candidate,
                request,
                ConsensusState::Leader,
            )?;
            Ok((Some(response), new_state))
        } else {
            Ok((None, self.into()))
        }
    }

    /// Applies a request vote response to the consensus state machine.
    fn request_vote_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: &RequestVoteResponse,
    ) -> Result<CurrentState<L, M, H>, Error> {
        let local_term = self.current_term()?;
        let voter_term = response.voter_term();
        if local_term < voter_term {
            // Responder has a higher term number. The election is compromised; abandon it and
            // revert to follower state with the updated term number. Any further responses we
            // receive from this election term will be ignored because the term will be outdated.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            info!(
                "received RequestVoteResponse from {{ id: {}, term: {} }} with newer term; transitioning to follower",
                from, voter_term
            );
            let follower = self.into_follower(handler, ConsensusState::Leader, voter_term)?;
            Ok(follower.into())
        } else {
            // local_term > voter_term: ignore the message; it came from a previous election cycle
            // local_term = voter_term: since state is not candidate, it's ok because some votes
            // can come after we became follower or leader

            Ok(self.into())
        }
    }

    // Timeout handling
    fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error> {
        debug!("heartbeat timeout for peer: {}", peer);
        Ok(AppendEntriesRequest {
            term: self.current_term()?,
            prev_log_index: self.latest_log_index()?,
            prev_log_term: self.with_log(|log| log.latest_log_term())?,
            leader_commit: self.commit_index,
            entries: Vec::new(),
        })
    }

    fn election_timeout(mut self, handler: &mut H) -> Result<CurrentState<L, M, H>, Error> {
        if self.state_data.config_change.is_some() {
            if self.state_data.catching_up_timeout()? {
                // have more timeouts left
            } else {
                //  no timeouts left for node
                self.state_data.reset_config_change(handler);
            }
            Ok(StateImpl::<L, M, H>::into_consensus_state(self))
        } else {
            debug!("BUG: election timeout called on leader without catch up node");
            return Err(Error::MustNotLeader);
        }
    }

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error> {
        // According to Raft 4.1 last paragraph, servers should receive any RPC call
        // from any server, because they may be a new ones which current server doesn't know
        // about yet

        let new_peer = !self.config.has_peer(&peer) && !self.state_data.is_catching_up(&peer);
        if new_peer {
            // This may still be correct peer, but it is was not added using AddServer API or was
            // removed already
            // the peer still can be the one that is going to catch up, so we skip this
            // check for a leader state
            // By this reason we do not panic here nor return an error
            debug!("New peer connected, but not found in consensus: {:?}", peer);
            return Ok(());
        }

        // send a ping to a peer to get back into request-responce cycle
        let message = AppendEntriesRequest {
            term: self.current_term()?,
            prev_log_index: self.latest_log_index()?,
            prev_log_term: self.with_log(|log| log.latest_log_term())?,
            leader_commit: self.commit_index,
            entries: Vec::new(),
        };

        // For stateless/lossy connections we cannot be sure if peer has received
        // our entries, so we call set_next_index only after response, which
        // is done in response processing code
        handler.send_peer_message(peer, PeerMessage::AppendEntriesRequest(message));

        // TODO: probably return the message here
        Ok(())
    }

    // Configuration change messages
    fn add_server_request(
        &mut self,
        handler: &mut H,
        request: &AddServerRequest,
    ) -> Result<ConfigurationChangeResponse, Error> {
        if self.state_data.config_change.is_some() {
            Ok(ConfigurationChangeResponse::AlreadyPending)
        } else {
            self.state_data.config_change = Some(ConfigChange::new(request.id));
            // we can send a ping request to receive the log index at the catching up node
            // the first success will not be counted as a good round because
            // response_this_timeout is set to false at this point
            // TODO: this roundtrip cost us 1 round, it should be solved by increasing
            // a number of rounds by 1
            //
            // we need this heartbeat as a way to initiate the exchange between leader
            // and a catching up, because if catching up, for example connects
            // before the add_server command is given, it's initial message may be already
            // ignored by consensus
            let message = AppendEntriesRequest {
                term: self.current_term()?,
                // we know nothing about the node at the start, so we send 0 as index and term
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                leader_commit: self.commit_index,
                entries: Vec::new(),
            };

            handler.send_peer_message(request.id, PeerMessage::AppendEntriesRequest(message));
            handler.set_timeout(Timeout::Election);
            Ok(ConfigurationChangeResponse::Started)
        }
    }

    /// Applies a client proposal to the consensus state machine.
    fn client_proposal_request(
        &mut self,
        handler: &mut H,
        from: ClientId,
        request: &ClientRequest,
    ) -> Result<ClientResponse, Error> {
        let (log_index, message) = self.add_next_entry(EntryData::Client(request.data))?;

        debug!("proposal request from client {}: entry {}", from, log_index);
        self.state_data.proposals.push_back((from, log_index));
        self.send_append_entries_request(handler, log_index, message)?;

        Ok(ClientResponse::Queued)
    }

    fn client_query_request(&mut self, from: ClientId, request: &ClientRequest) -> ClientReponse {
        // TODO: this is not totally safe because we don't implement RegisterClientRPC yet
        // so messages can be duplicated
        trace!("query from client {}", from);
        let result = self.state_machine.query(request.data);
        ClientResponse::Success(result)
    }

    fn ping_request(&self) -> Result<PingResponse, Error> {
        self.common_client_ping_request(ConsensusState::Leader)
    }

    fn into_consensus_state(self) -> CurrentState<L, M, H> {
        CurrentState::Leader(self)
    }
}

impl<L, M, H> State<L, M, H, Leader>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    fn leader_into_follower(
        self,
        handler: &mut H,
        leader_term: Term,
    ) -> Result<State<L, M, H, Follower>, Error> {
        self.state_data.reset_config_change(handler);
        self.into_follower(handler, ConsensusState::Leader, leader_term)
    }

    fn try_advance_commit_index(&mut self, handler: &mut H) -> Result<(), Error> {
        let majority = self.config.majority();
        // Here we try to move commit index to one that the majority of peers in cluster already have
        let latest_log_index = self.latest_log_index()?;

        while self.commit_index < latest_log_index {
            if self.state_data.count_match_indexes(self.commit_index + 1) >= majority {
                self.commit_index = self.commit_index + 1;
                debug!("commit index advanced to {}", self.commit_index);
            } else {
                break; // If there isn't a majority now, there won't be one later.
            }
        }

        let mut results = self.apply_commits(true);

        // As long as we know it, we send the connected clients the notification
        // about their proposals being committed
        //
        // A note about client proposals ordering: they may come out of order obviously (via parallel
        // TCP connectinos, for example), but the consensus should not be responsible for that ordering
        // because state machine may or may not depend on it. Consensus still tries to help the
        // client with the ordering sending a ClientResponse::Queued for each request
        //
        // What the consensus should really be responsible is the same ordering on the followers.
        // And since it is leader that applies and commits the commands, the order
        // they come will be kept as intended, as it's enqueued in self.state.proposals
        // so we don't need to do any special handling of it outside the client state machine aplication
        while let Some(&(client, index)) = self.state_data.proposals.get(0) {
            if index <= self.commit_index {
                trace!("responding to client {} for entry {}", client, index);
                // state machine's apply have to return a vector, that means the
                // results will always contain the index required, so we
                // can safely unwrap it, otherwise it's a bug and we should panic
                // We know that there will be an index here since it was commited
                // and the index is less than that which has been commited.
                handler.send_client_message(
                    client,
                    ClientMessage::ClientProposalResponse(ClientResponse::Success(
                        results.remove(&index).unwrap(),
                    )),
                );
                self.state_data.proposals.pop_front();
            } else {
                break;
            }
        }

        Ok(())
    }

    /// appends a single entry to a local log obeying all the Raft rules (log indexing etc)
    /// and sends the request to the followers
    fn add_next_entry(
        &mut self,
        entry_data: EntryData,
    ) -> Result<(LogIndex, AppendEntriesRequest), Error> {
        let prev_log_index = self.latest_log_index()?;
        let prev_log_term = self.latest_log_term()?;
        let term = self.current_term()?;

        let log_index = prev_log_index + 1;
        let leader_commit = self.commit_index;
        let entry = Entry::new(term, entry_data);
        self.with_log_mut(|log| log.append_entries(log_index, Some(&entry).into_iter()))?;

        let message = AppendEntriesRequest {
            term,
            prev_log_index,
            prev_log_term,
            leader_commit,
            entries: vec![entry],
        };

        Ok((log_index, message))
    }

    fn send_append_entries_request(
        &mut self,
        handler: &mut H,
        log_index: LogIndex,
        message: AppendEntriesRequest,
    ) -> Result<(), Error> {
        if !self.config.is_solitary(&self.id) {
            // solitary consensus can just advance, no messages required
            //
            // fan out the request to all followers that are catched up enough
            self.config.with_remote_peers(&self.id, |id| {
                if self.state_data.next_index(&id)? == log_index {
                    handler
                        .send_peer_message(*id, PeerMessage::AppendEntriesRequest(message.clone()));
                    self.state_data.set_next_index(*id, log_index + 1);
                }
                Ok(())
            });
        }

        if self.config.majority() == 1 {
            // for a solitary consensus or a 2-peer cluster current node aready has a majority,
            // (because of it's own log commit)
            // so there is a reason to advance the index in case the proposal queue is empty
            //
            // otherwise, there is no point in this because the majority is not achieved yet
            self.try_advance_commit_index(handler)?;
        }

        Ok(())
    }
}

/// The state associated with a Raft consensus module in the `Leader` state.
#[derive(Clone, Debug)]
pub(crate) struct Leader {
    next_index: HashMap<ServerId, LogIndex>,
    match_index: HashMap<ServerId, LogIndex>,
    /// stores pending config change making sure there can only be one at a time
    pub(crate) config_change: Option<ConfigChange>,
    /// Stores in-flight client proposals.
    pub(crate) proposals: VecDeque<(ClientId, LogIndex)>,
}

impl Leader {
    /// Returns a new `LeaderState` struct.
    ///
    /// # Arguments
    ///
    /// * `latest_log_index` - The index of the leader's most recent log entry at the
    ///                        time of election.
    /// * `peers` - The set of peer cluster members.
    pub(crate) fn new(
        latest_log_index: LogIndex,
        config: &ConsensusConfig,
        own_id: &ServerId,
    ) -> Leader {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        config.with_remote_peers(own_id, |id| {
            next_index.insert(*id, latest_log_index + 1);
            match_index.insert(*id, LogIndex::from(0));
            Ok(())
        });

        Leader {
            next_index,
            match_index,
            config_change: None,
            proposals: VecDeque::new(),
        }
    }

    fn has_follower(&mut self, follower: &ServerId) -> bool {
        self.match_index.contains_key(follower)
    }

    fn is_catching_up(&self, node: &ServerId) -> bool {
        if let Some(catching_up) = &self.config_change {
            node == &catching_up.peer.id
        } else {
            false
        }
    }

    fn has_catching_up_node(&self) -> bool {
        if let Some(ConfigChange {
            peer,
            stage: ConfigChangeStage::CatchingUp { .. },
        }) = self.config_change
        {
            true
        } else {
            false
        }
    }

    fn reset_config_change<H: Handler>(&mut self, handler: &mut H) {
        match self.config_change.take() {
            Some(ConfigChange {
                peer,
                stage: ConfigChangeStage::CatchingUp { .. },
            }) => {
                //trace!("configuration change cancelled because of leader changing to follower");
                // TODO: notify admin about cancelling
                handler.clear_timeout(Timeout::Heartbeat(peer.id));
                handler.clear_timeout(Timeout::Election);
            }
            Some(ConfigChange {
                peer,
                stage: ConfigChangeStage::Committing { .. },
            }) => {}
            None => {}
        }
    }

    /// Returns the next log entry index of the follower or a catching up peer.
    fn next_index(&mut self, follower: &ServerId) -> Result<LogIndex, Error> {
        if let Some(index) = self.next_index.get(follower) {
            return Ok(*index);
        }

        if let Some(ConfigChange {
            peer,
            stage: ConfigChangeStage::CatchingUp { index, .. },
        }) = self.config_change
        {
            if &peer.id == follower {
                // the index is requested for a catching up peer
                return Ok(index);
            }
        }

        Err(Error::UnknownPeer(follower.clone()))
    }

    /// Sets the next log entry index of the follower or a catching up peer.
    fn set_next_index(&mut self, follower: ServerId, index: LogIndex) -> Result<(), Error> {
        if let Some(stored_index) = self.next_index.get_mut(&follower) {
            *stored_index = index;
            return Ok(());
        }

        if let Some(ConfigChange {
            peer,
            stage:
                ConfigChangeStage::CatchingUp {
                    index: cf_index,
                    rounds,
                    ..
                },
        }) = &mut self.config_change
        {
            if peer.id == follower {
                *cf_index = index;
                return Ok(());
            }
        }

        Err(Error::UnknownPeer(follower))
    }

    /// Sets the index of the highest log entry known to be replicated on the
    /// follower.
    fn set_match_index(&mut self, follower: ServerId, index: LogIndex) {
        self.match_index.insert(follower, index);
    }

    /// config_index should be brought to save the log index the new config will
    /// be committed to leader log at (in case of catching up success)
    fn update_rounds(&mut self) -> Result<CatchUpStatus, Error> {
        if let Some(ConfigChange {
            stage:
                ConfigChangeStage::CatchingUp {
                    rounds,
                    response_this_timeout,
                    timeouts,
                    ..
                },
            ..
        }) = &mut self.config_change
        {
            if *rounds == 0 {
                Ok(CatchUpStatus::TooSlow)
            } else {
                *rounds -= 1;
                if *response_this_timeout {
                    // node has caught up because we already had response within this timeout
                    Ok(CatchUpStatus::CaughtUp)
                } else {
                    *response_this_timeout = true;
                    *timeouts = 20;
                    Ok(CatchUpStatus::NotYet)
                }
            }
        } else {
            //panic!("IMPLEMENTATION BUG: update_rounds called during wrong stage")
            Err(Error::unreachable(module_path!()))
        }
    }

    /// Migrates configuration change state to begin committing a modification of config after
    /// cathching up node was ready
    /// Checks previous config for existing of peer being processed, adds peer if it didn't exist
    /// Removes peer if it existed in previous config
    fn begin_config_change(
        &mut self,
        config_index: LogIndex,
        current_config: &mut ConsensusConfig,
    ) -> Result<(), Error> {
        if let Some(ConfigChange { peer, stage, .. }) = &mut self.config_change {
            if current_config.add_or_remove_peer(peer.clone())? {
                // peer did not exist - added
            } else {
                // peer existed - remove it
                todo!("removing server is not implemented yet, must think about giving away leadership");
            }
            *stage = ConfigChangeStage::Committing(current_config.clone(), config_index);
            Ok(())
        } else {
            Err(Error::Critical(CriticalError::Unreachable(module_path!())))
        }
    }

    fn catching_up_timeout(&mut self) -> Result<bool, Error> {
        if let Some(ConfigChange {
            stage:
                ConfigChangeStage::CatchingUp {
                    response_this_timeout,
                    timeouts,
                    ..
                },
            ..
        }) = &mut self.config_change
        {
            *response_this_timeout = false;
            if *timeouts > 0 {
                *timeouts -= 1;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Err(Error::Critical(CriticalError::Unreachable(module_path!())))
        }
    }

    /// Counts the number of followers containing the given log index.
    fn count_match_indexes(&self, index: LogIndex) -> usize {
        // +1 is for self
        self.match_index.values().filter(|&&i| i >= index).count() + 1
    }
}

pub(crate) enum CatchUpStatus {
    TooSlow,
    NotYet,
    CaughtUp,
}

#[derive(Clone, Debug)]
pub(crate) enum ConfigChangeStage {
    CatchingUp {
        index: LogIndex,
        rounds: u32,
        timeouts: u32,
        response_this_timeout: bool, // flag to measure round time
    },
    Committing(ConsensusConfig, LogIndex), // previous config
}

#[derive(Clone, Debug)]
pub(crate) struct ConfigChange {
    pub(crate) peer: Peer,
    pub(crate) stage: ConfigChangeStage,
}

impl ConfigChange {
    pub(crate) fn new(id: ServerId) -> Self {
        Self {
            peer: Peer { id },
            stage: ConfigChangeStage::CatchingUp {
                index: LogIndex(0),
                rounds: MAX_ROUNDS,
                timeouts: MAX_TIMEOUTS,
                response_this_timeout: false,
            },
        }
    }
}
