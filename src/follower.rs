use std::cmp;
use std::marker::PhantomData;

use crate::error::*;
use log::{debug, info, trace};

use crate::handler::Handler;
use crate::message::*;
use crate::persistent_log::{Log, LogEntryMeta};
use crate::raft::CurrentState;
use crate::state::State;
use crate::state_impl::StateImpl;
use crate::state_machine::StateMachine;
use crate::{AdminId, ClientId, LogIndex, ServerId, Term};

use crate::candidate::Candidate;

/// Follower replicates AppendEntries requests and votes for it's leader.
impl<M, H> StateImpl<M, H> for State<M, H, Follower>
where
    M: StateMachine,
    H: Handler,
{
    fn append_entries_request(
        mut self,
        handler: &mut H,
        from: ServerId,
        request: &AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, CurrentState<M, H>), Error> {
        let leader_term = request.term;
        let current_term = self.current_term()?;

        if leader_term < current_term {
            return Ok((AppendEntriesResponse::StaleTerm(current_term), self.into()));
        }

        self.state_data.set_leader(from);

        let message = self.append_entries(request, current_term)?;

        // ensure to reset timer at the very end of potentially long disk operation
        handler.set_timeout(Timeout::Election);

        // heartbeat invalidates all the delayed voting requests because receiving it
        // means leader is not lost for this node therefore voting should not start
        if self.state_data.delayed_vote_request.is_some() {
            self.state_data.delayed_vote_request = None
        }

        Ok((message, self.into()))
    }

    fn append_entries_response(
        self,
        _handler: &mut H,
        _from: ServerId,
        _response: &AppendEntriesResponse,
    ) -> Result<(Option<PeerMessage>, CurrentState<M, H>), Error> {
        // the follower can only receive responses in case the node was a leader some time ago
        // and sent requests which triggered the reponse while response was held somewhere in network
        //
        // at the moment of being non-leader it has nothing to do with them, expept ignoring
        Ok((None, self.into()))
    }

    fn request_vote_request(
        mut self,
        handler: &mut H,
        candidate: ServerId,
        request: &RequestVoteRequest,
    ) -> Result<(Option<RequestVoteResponse>, CurrentState<M, H>), Error> {
        if !self.state_data.can_vote {
            debug!("non-voting node received a voting request");
            return Ok((None, self.into()));
        }
        // To avoid disrupting leader while configuration changes, node should ignore or delay vote requests
        // coming within election timeout unless there is special flag set signalling
        // the leadership was given away voluntarily

        // Followers are always "inside" the election timeout (because they become candidates
        // otherwise). So for a follower this request means it was given a possibility to become a
        // leader faster than others.
        if request.is_voluntary_step_down {
            let (response, new_state) = self.common_request_vote_request(
                handler,
                candidate,
                request,
                ConsensusState::Follower,
            )?;
            Ok((Some(response), new_state))
        } else {
            // Otherwise, the follower can either ignore/deny or delay the voting request.
            //
            // The ignoring/denying scenario makes first election unlikely to be finished effectively
            // (because all other followers would ignore the request), so the voting will require
            // additional requests from each of the followers having the election timeout expiring.
            //
            // The delaying scenario makes the request on non-first followers unneeded, because
            // they already know who to vote for and can vote immediately

            if self.state_data.delayed_vote_request.is_none() {
                // rewriting request or not does not bring any difference, because election timeout
                // is randomized, so there is no preference for a follower
                // so we only avoid some negligible byte copying
                self.state_data.delayed_vote_request = Some((candidate, request.clone()));
            }
            Ok((None, self.into()))
        }
    }

    fn request_vote_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: &RequestVoteResponse,
    ) -> Result<CurrentState<M, H>, Error> {
        let local_term = self.current_term()?;
        let voter_term = response.voter_term();
        if local_term < voter_term {
            // Responder has a higher term number. The election is compromised; abandon it and
            // revert to follower state with the updated term number. Any further responses we
            // receive from this election term will be ignored because the term will be outdated.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            info!(
                "received RequestVoteResponse from Consensus {{ id: {}, term: {} }} \
                with newer term; transitioning to Follower",
                from, voter_term
            );
            let follower = self.into_follower(handler, ConsensusState::Follower, voter_term)?;
            Ok(follower.into())
        } else {
            // local_term > voter_term: ignore the message; it came from a previous election cycle
            // local_term = voter_term: since state is not candidate, it's ok because some votes
            // can come after we became follower or leader

            Ok(self.into())
        }
    }

    fn timeout_now(self, handler: &mut H) -> Result<CurrentState<M, H>, Error> {
        let candidate = self.into_candidate(handler, true)?;
        Ok(candidate.into())
    }

    fn install_snapshot_request(
        mut self,
        _handler: &mut H,
        _from: ServerId,
        request: &InstallSnapshotRequest,
    ) -> Result<(PeerMessage, CurrentState<M, H>), Error> {
        let current_term = self.current_term()?;

        if request.term < current_term {
            let message = InstallSnapshotResponse::StaleTerm(current_term);
            return Ok((PeerMessage::InstallSnapshotResponse(message), self.into()));
        }

        if let Some(ref last_config) = request.last_config {
            self.log_mut()
                .set_latest_config(last_config, request.snapshot_index)
                .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;
            self.config = last_config.clone();
        }

        let next_chunk_request = self
            .state_machine
            .write_snapshot_chunk(request.snapshot_index, &request.chunk_data)
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

        if next_chunk_request.is_none() {
            // the follower has finished writing the last chunk of a snapshot
            // we don't need old records in log now
            self.log_mut()
                .discard_since(request.snapshot_index)
                .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;
        }

        let message = InstallSnapshotResponse::Success(
            current_term,
            request.snapshot_index,
            next_chunk_request,
        );
        Ok((PeerMessage::InstallSnapshotResponse(message), self.into()))
    }

    fn install_snapshot_response(
        self,
        _handler: &mut H,
        from: ServerId,
        response: &InstallSnapshotResponse,
    ) -> Result<(Option<PeerMessage>, CurrentState<M, H>), Error> {
        // whatever the response is, the follower cannot process it because
        // the node in follower state does not now about snapshot states of other nodes
        // TODO: we could do some term checks and reply with StaleTerm for example,
        // but there are cases where follower could be behind so most probably
        // we should not
        trace!(
            "unknown snapshot response from {} on follower: {:?}",
            from,
            response
        );
        return Ok((None, self.into()));
    }

    // Timeout handling
    /// Handles heartbeat timeout event
    fn heartbeat_timeout(&mut self, _peer: ServerId) -> Result<AppendEntriesRequest, Error> {
        Err(Error::MustLeader)
    }

    fn election_timeout(mut self, handler: &mut H) -> Result<CurrentState<M, H>, Error> {
        if !self.state_data.can_vote {
            // non voters should not set election timeout
            return Err(Error::unreachable(module_path!()));
        }

        // election timeout is never down, but we want handler to randomize it every tick
        // TODO: probably make this an option
        handler.set_timeout(Timeout::Election);
        if let Some((from, ref request)) = self.state_data.delayed_vote_request.take() {
            // Since voting has started already, we must process the delayed vote request.
            // For the node this means it has to vote for another candidate instead of itself
            // This in turn means, that becoming candidate may not be required

            // we must send response to any request, regardless of our own state
            let (response, new_state) =
                self.common_request_vote_request(handler, from, request, ConsensusState::Follower)?;

            // but after that we must decide if we should really become a candidate
            // this depends on what response we are sending
            // general rule: we want it if request was bad for some reason, meaning election must
            // start as required
            let become_candidate = match response {
                RequestVoteResponse::StaleTerm(_) => true,
                RequestVoteResponse::InconsistentLog(_) => true,
                RequestVoteResponse::Granted(_) => false,
                RequestVoteResponse::AlreadyVoted(_) => false,
            };

            handler.send_peer_message(from, PeerMessage::RequestVoteResponse(response));

            if become_candidate {
                if let CurrentState::Follower(new_state) = new_state {
                    let candidate = new_state.into_candidate(handler, false)?;
                    Ok(candidate.into())
                } else {
                    Err(Error::unreachable(module_path!()))
                }
            } else {
                Ok(new_state)
            }
        } else {
            let candidate = self.into_candidate(handler, false)?;
            let maybe_leader = candidate.try_solitary_leader(handler)?;
            Ok(maybe_leader.into())
        }
    }

    fn check_compaction(&mut self, _handler: &mut H, __force: bool) -> Result<bool, Error> {
        self.common_check_compaction(true)
    }

    fn client_proposal_request(
        &mut self,
        _handler: &mut H,
        _from: ClientId,
        _request: ClientRequest,
    ) -> Result<ClientResponse, (Error, ClientRequest)> {
        self.state_data
            .leader
            .map_or(Ok(ClientResponse::UnknownLeader), |leader| {
                Ok(ClientResponse::NotLeader(leader))
            })
    }

    fn client_query_request(
        &mut self,
        from: ClientId,
        _request: &ClientRequest,
    ) -> Result<ClientResponse, Error> {
        // TODO: introduce an option for allowing the response from the potentially
        // older state of the machine
        // With such option it could be possible to request the state machine on the follower
        // having eventual consistency as the result
        trace!("query from client {}", from);
        Ok(self
            .state_data
            .leader
            .map_or(ClientResponse::UnknownLeader, |leader| {
                ClientResponse::NotLeader(leader)
            }))
    }

    fn ping_request(&self) -> Result<PingResponse, Error> {
        self.common_client_ping_request(ConsensusState::Follower)
    }

    // Admin messages
    fn add_server_request(
        &mut self,
        _handler: &mut H,
        _from: AdminId,
        _request: &AddServerRequest,
    ) -> Result<ConfigurationChangeResponse, Error> {
        // TODO: forward to leader
        self.state_data
            .leader
            .map_or(Ok(ConfigurationChangeResponse::UnknownLeader), |leader| {
                Ok(ConfigurationChangeResponse::NotLeader(leader))
            })
    }

    fn step_down_request(
        &mut self,
        _handler: &mut H,
        from: AdminId,
        _request: Option<ServerId>,
    ) -> Result<ConfigurationChangeResponse, Error> {
        trace!("query from client {}", from);
        self.state_data
            .leader
            .map_or(Ok(ConfigurationChangeResponse::UnknownLeader), |leader| {
                Ok(ConfigurationChangeResponse::NotLeader(leader))
            })
    }

    // Utility messages and actions
    fn peer_connected(&mut self, _handler: &mut H, _peer: ServerId) -> Result<(), Error> {
        // followers don't send messages, so they don't care about other peers' connections
        Ok(())
    }

    fn into_consensus_state(self) -> CurrentState<M, H> {
        CurrentState::Follower(self)
    }

    fn client_timeout(&mut self, _handler: &mut H) -> Result<(), Error> {
        Ok(())
    }
}

impl<M, H> State<M, H, Follower>
where
    M: StateMachine,
    H: Handler,
{
    /// AppendEntries processing function for
    pub(crate) fn append_entries(
        &mut self,
        request: &AppendEntriesRequest,
        current_term: Term,
    ) -> Result<AppendEntriesResponse, Error> {
        let leader_term = request.term;
        if current_term < leader_term {
            self.log_mut()
                .set_current_term(leader_term)
                .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;
        }

        let leader_prev_log_index = request.prev_log_index;
        let leader_prev_log_term = request.prev_log_term;
        let current_term = self.current_term()?;

        let latest_log_index = self.latest_log_index()?;

        // check for gaps
        if latest_log_index < leader_prev_log_index {
            // If the previous entries index was not the same we'd leave a gap! Reply failure.
            return Ok(AppendEntriesResponse::InconsistentPrevEntry(
                current_term,
                latest_log_index,
            ));
        }

        let existing_term = if leader_prev_log_index == LogIndex::from(0) {
            // zeroes mean the very start, when everything is empty at leader and follower
            // we better leave this here as special case rather than requiring it from
            // persistent log implementation
            Some(Term::from(0))
        } else {
            // try to find term of specified log index and check if there is such entry
            self.log()
                .term_of(leader_prev_log_index)
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?
        };

        if existing_term
            .map(|term| term != leader_prev_log_term)
            .unwrap_or(true)
        {
            // If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it

            self.log_mut()
                .discard_until(leader_prev_log_index)
                .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;

            return Ok(AppendEntriesResponse::InconsistentPrevEntry(
                current_term,
                latest_log_index,
            ));
        }

        // now, for non-empty entries list
        // append only those that needs to be applied
        let AppendEntriesRequest {
                        entries,
                        ..
                            //term , prev_log_index, prev_log_term, leader_commit, entries
                    }
                    = request;
        let num_entries = entries.len();

        let new_latest_log_index = leader_prev_log_index + num_entries as u64;
        if new_latest_log_index < self.min_index {
            // Stale entry; ignore. This guards against overwriting a
            // possibly committed part of the log if messages get
            // rearranged; see ktoso/akka-raft#66.
            return Ok(AppendEntriesResponse::StaleEntry);
        }

        let mut append_index = leader_prev_log_index + 1;

        // for a heartbeat request this cycle will be skipped
        // but all other check and actions MUST be performed
        // because due to Raft's nature the follower's state machine(not log though) is always 1 commit behind the leader
        // until it receives the next AppendEntries message, be it heartbeat or whatever else
        for entry in entries.iter() {
            // with each send, leader marks config entries as active if they really take place
            // as current cluster configuration, this way the follower can skip using previous
            // inactive entries as actual ones and will not connect to some old nodes
            if let EntryData::Config(config, is_active, _) = &entry.data {
                if *is_active {
                    self.config = config.clone();
                }
            }

            self.log_mut()
                .append_entry(append_index, &entry.as_entry_ref())
                .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;

            append_index = append_index + 1;
        }

        self.min_index = new_latest_log_index;

        // We are matching the leader's log up to and including `new_latest_log_index`.
        self.commit_index = cmp::min(request.leader_commit, new_latest_log_index);
        self.apply_follower_commits()?;

        Ok(AppendEntriesResponse::Success(
            current_term,
            new_latest_log_index,
        ))
    }

    /// Applies all committed but unapplied log entries to the state machine ignoring the results.
    pub(crate) fn apply_follower_commits(&mut self) -> Result<(), Error> {
        let entry_index = self
            .state_machine
            .last_applied()
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;
        trace!(
            "applying commits from {} to {}",
            &entry_index,
            &self.commit_index
        );
        while entry_index < self.commit_index {
            let entry_index = entry_index + 1;
            let entry_kind = self
                .log()
                .entry_meta_at(entry_index)
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
            //     let mut entry = LogEntry::default();
            //self.log
            //.read_entry(self.last_applied, &mut entry)
            //         .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;

            match entry_kind {
                LogEntryMeta::Empty => {}
                LogEntryMeta::Proposal(_) => {
                    self.state_machine
                        .apply(entry_index, false)
                        .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;
                }
                LogEntryMeta::Config(config, _) => {
                    // we only saved configuration to log(an an entry) and memory, but has not persisted it yet
                    // this is what we have to do now
                    self.log_mut()
                        .set_latest_config(&config, entry_index)
                        .map_err(|e| {
                            Error::Critical(CriticalError::PersistentLogWrite(Box::new(e)))
                        })?;
                }
            }
        }

        Ok(())
    }

    fn into_candidate(
        self,
        handler: &mut H,
        voluntary: bool,
    ) -> Result<State<M, H, Candidate>, Error> {
        trace!("id={} transitioning to candidate", self.id);
        let state_data = Candidate::new();
        let mut candidate = State {
            id: self.id,
            config: self.config,
            state_machine: self.state_machine,
            commit_index: self.commit_index,
            min_index: self.min_index,
            _h: PhantomData,
            state_data,
            options: self.options,
        };

        handler.state_changed(ConsensusState::Follower, ConsensusState::Candidate);
        candidate.start_election(handler, voluntary)?;
        Ok(candidate)
    }
}

/// The state associated with a Raft consensus module in the `Follower` state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Follower {
    // The most recent leader of the follower. The leader is not guaranteed to be active, so this
    // should only be used as a hint.
    leader: Option<ServerId>,

    delayed_vote_request: Option<(ServerId, RequestVoteRequest)>,

    // makes node catching up or non-voting
    can_vote: bool,
}

impl Follower {
    /// Returns a new `FollowerState`.
    pub(crate) fn new(can_vote: bool) -> Follower {
        Follower {
            leader: None,
            delayed_vote_request: None,
            can_vote,
        }
    }

    /// Sets a new leader.
    pub(crate) fn set_leader(&mut self, leader: ServerId) {
        self.leader = Some(leader);
    }
}
