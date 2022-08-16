use std::cmp;
use std::marker::PhantomData;

use crate::{debug_where, error::*, ConsensusConfig};
use tracing::{debug, info, trace, warn};

use crate::handler::Handler;
use crate::message::*;
use crate::persistent_log::{Log, LogEntry, LogEntryData};
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
        request: AppendEntriesRequest,
    ) -> Result<(Option<AppendEntriesResponse>, CurrentState<M, H>), Error> {
        let leader_term = request.term;
        let current_term = self.current_term()?;

        if leader_term < current_term {
            return Ok((
                Some(AppendEntriesResponse::StaleTerm(current_term)),
                self.into(),
            ));
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
        _response: AppendEntriesResponse,
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
        request: RequestVoteRequest,
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
        response: RequestVoteResponse,
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
        request: InstallSnapshotRequest,
    ) -> Result<(PeerMessage, CurrentState<M, H>), Error> {
        let current_term = self.current_term()?;

        if request.term < current_term {
            let message = InstallSnapshotResponse::StaleTerm(current_term);
            return Ok((PeerMessage::InstallSnapshotResponse(message), self.into()));
        }

        let next_chunk_request = self
            .state_machine
            .write_snapshot_chunk(
                request.snapshot_index,
                request.snapshot_term,
                request.last_config.clone(),
                request.force_reset,
                &request.chunk_data,
            )
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

        if next_chunk_request.is_none() {
            // the follower has finished writing the last chunk of a snapshot
            // we don't need old records in log now
            self.log_mut()
                .discard_until(request.snapshot_index, request.snapshot_term)
                .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;

            if request.force_reset {
                // if leader requested us to make a full reset (for example, leader sees us as a bad
                // follower) we also have to discard all new entries too, if there are any
                if self.latest_log_index > request.snapshot_index {
                    self.log_mut()
                        .discard_since(request.snapshot_index + 1)
                        .map_err(|e| {
                            Error::Critical(CriticalError::PersistentLogWrite(Box::new(e)))
                        })?;
                }
                if let Some(last_config) = request.last_config {
                    // on reset apply config from snapshot without any checks
                    self.config = ConsensusConfig::new(last_config.into_iter());
                    self.log_mut()
                        .set_latest_config_view(request.snapshot_index, LogIndex(0))
                        .map_err(|e| {
                            Error::Critical(CriticalError::PersistentLogWrite(Box::new(e)))
                        })?;
                }
                self.latest_config_index = LogIndex(0);
            } else {
                // We've received tha last chunk of snapshot, and leader doesn't want us to reset it.
                //
                // State machine is allowed to not accept a snapshot if reset was not required,
                // which means we need to update the snapshot status first
                let mut info = self
                    .state_machine
                    .snapshot_info()
                    .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;
                let (committed_config_index, pending_config_index) = self
                    .log()
                    .latest_config_view()
                    .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
                let current_config_index = self.latest_config_index;
                let (new_committed_config_index, new_pending_config_index) = if let Some(info) =
                    info.take()
                {
                    // if there is any snapshot available, check if current config
                    // indices were replaced by the latest write
                    if committed_config_index != LogIndex(0) {
                        // check our own config indices after log cutting

                        // all our configs are deprecated by snapshot
                        if committed_config_index < info.index && pending_config_index < info.index
                        {
                            (info.index, info.index)
                        } else if committed_config_index < info.index {
                            // only committed config is replaced by snapshot
                            (info.index, pending_config_index)
                        } else {
                            // nothing is replaced by snapshot
                            (committed_config_index, pending_config_index)
                        }
                    } else {
                        // we had no config index set, but we may have a pending one
                        if committed_config_index != pending_config_index {
                            (info.index, pending_config_index)
                        } else {
                            // none of config indices was wet
                            (info.index, info.index)
                        }
                    }
                } else {
                    // without config in snapshot leave everything as is
                    (committed_config_index, pending_config_index)
                };
                if new_pending_config_index <= self.latest_log_index {
                    self.latest_config_index = new_pending_config_index;
                } else {
                    self.latest_config_index = new_committed_config_index;
                }

                if committed_config_index != new_committed_config_index
                    || pending_config_index != new_pending_config_index
                {
                    // save new indices to log if snapshot have changed them
                    self.log_mut()
                        .set_latest_config_view(
                            new_committed_config_index,
                            new_pending_config_index,
                        )
                        .map_err(|e| {
                            Error::Critical(CriticalError::PersistentLogWrite(Box::new(e)))
                        })?;
                }

                if current_config_index != self.latest_config_index {
                    self.update_latest_config(info.as_ref().map(|info| info.config.as_slice()))?;
                }
            }

            debug!(
                snapshot_index = %request.snapshot_index,
                snapshot_term = %request.snapshot_term,
                zero_log_index = %self.zero_log_index,
                latest_log_index = %self.latest_log_index,
                "follower finished receiving snapshot"
            );
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
        response: InstallSnapshotResponse,
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
        if let Some((from, request)) = self.state_data.delayed_vote_request.take() {
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
    ) -> Result<ClientResponse, Error> {
        self.state_data
            .leader
            .map_or(Ok(ClientResponse::UnknownLeader), |leader| {
                Ok(ClientResponse::NotLeader(leader))
            })
    }

    fn client_query_request(
        &mut self,
        from: ClientId,
        _request: ClientRequest,
    ) -> Result<ClientResponse, Error> {
        // TODO: introduce an option for allowing the response from the potentially
        // older state of the machine
        // With such option it could be possible to request the state machine on the follower
        // having eventual consistency as the result
        // TODO:
        // When option is not set, the request should be proxied to the assumed leader
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
        _request: AddServerRequest,
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

    fn on_any_event(&mut self) -> Result<(), Error> {
        self.common_on_any_event()?;
        //if self.latest_log_index < self.commit_index {
        // this is a rare, but possible situation:
        // we are catching up, our commit index was taken from leader, when
        // leader sent some entries.
        // At the time of this function call some or all of these entries
        // have been persisted in background.
        // This may mean that we have some persistent entries, ready to be applied, but
        // not applied yet. Without this check they will only be applied
        // after receiving a packet from the leader. But if we have an external tick,
        // we may do it earlier, making load a bit more distributed
        //
        // (This is commented, because the current tick(heartbeat timer) is almost the same interval
        // as leader's ping. Furthermore, somebody may prefer a batched state machine applies
        // because this may be faster in some cases)
        //self.apply_follower_commits()?;
        //}
        Ok(())
    }
}

impl<M, H> State<M, H, Follower>
where
    M: StateMachine,
    H: Handler,
{
    pub(crate) fn initialize(mut self, handler: &mut H) -> Result<CurrentState<M, H>, Error> {
        self.update_log_view()?;

        let (latest_config_index, pending_config_index) = self
            .log()
            .latest_config_view()
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;

        // Pending config may still be in view, even after restart.
        // Though our active config must be persisted
        let actual_config_index = if pending_config_index != LogIndex(0)
            && pending_config_index <= self.latest_log_index
        {
            pending_config_index
        } else if latest_config_index != LogIndex(0) {
            self.latest_config_index = latest_config_index;
            latest_config_index
        } else {
            LogIndex(0)
        };

        if actual_config_index != LogIndex(0) {
            let mut config_entry = LogEntry::default();
            if
            // try reading log from index that is probably volatile
            !self
                .log()
                .read_entry(actual_config_index, &mut config_entry)
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?
            {
                // reading volatile may not give success, in that case, try reading the
                // entry that must be persistent
                if !self
                    .log()
                    .read_entry(latest_config_index, &mut config_entry)
                    .map_err(|e| Error::PersistentLogRead(Box::new(e)))?
                {
                    return Err(Error::Critical(CriticalError::FollowerLogBroken(
                        latest_config_index,
                        "log refused to read persisted config entry",
                    )));
                }
            }

            if let LogEntryData::Config(peers) = config_entry.data {
                self.config = ConsensusConfig::new(peers.into_iter());
            } else {
                return Err(Error::Critical(CriticalError::LogInconsistentEntry(
                    actual_config_index,
                )));
            }
        } // else -> use bootstrap config which is already in self.config

        // out voting status is determined by the presence of our id in
        // active config
        self.state_data.can_vote =
            self.config.has_peer(self.id) || self.config.has_pending_peer(self.id);

        if self.state_data.can_vote {
            handler.set_timeout(Timeout::Election);
        }

        let info = self
            .state_machine
            .snapshot_info()
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;
        if let Some(info) = info {
            self.min_index = info.index;
        }

        Ok(self.into_consensus_state())
    }

    pub(crate) fn append_entries(
        &mut self,
        request: AppendEntriesRequest,
        current_term: Term,
    ) -> Result<Option<AppendEntriesResponse>, Error> {
        let leader_term = request.term;
        if current_term < leader_term {
            // always set the current term from the leader
            self.log_mut()
                .set_current_term(leader_term)
                .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;
        }

        let leader_prev_log_index = request.prev_log_index;
        let leader_prev_log_term = request.prev_log_term;
        let current_term = self.current_term()?;

        // Check for gaps and possibility to find the corresponding entry in term
        if self.latest_log_index < leader_prev_log_index
            || leader_prev_log_index < self.zero_log_index
        {
            trace!(
                leader_prev_log_index = %leader_prev_log_index,
                zero_log_index = %self.zero_log_index,
                latest_log_index = %self.latest_log_index,
                "leader sent prev_entry not in follower's log",
            );
            // If the previous entries index was not the same we would leave a gap.
            return Ok(Some(AppendEntriesResponse::InconsistentIndex(
                current_term,
                self.latest_log_index,
                self.latest_volatile_log_index,
            )));
        }

        // try to find term of specified log index and check if there such entry exists
        let existing_term = self
            .log()
            .term_of(leader_prev_log_index)
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;

        if existing_term != leader_prev_log_term {
            // An existing entry in persistent log conflicts with a new one (same index but different terms):
            // we have to delete the existing entries after AND INCLUDING the zero entry
            // LogIndex(0) is not possible here because term will always match (equal to Term(0))
            //
            // Regardless of the result we got above (except the unrecoverable failures)
            // we cannot predict leader's pre-previous entry term, so this if's return will
            // not depend on the next if block, except for somea criticals
            //
            if leader_prev_log_index == self.zero_log_index {
                // Leader have sent the entries starting from follower's zero index, meaning the
                // whole log is to be discarded anyways.

                // But there is still hope that the zero entry and term are correct inside the snapshot.
                // With log already potentially empty, the snapshot is the only source of truth.
                // So we query its information
                let info = self
                    .state_machine
                    .snapshot_info()
                    .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;
                if let Some(info) = info {
                    // Now, having the indexes from the snapshot (which is now the only possible source of truth):
                    // * check if the term and index match the leader:
                    if info.index == leader_prev_log_index && info.term == leader_prev_log_term {
                        // The indexes in snapshot match!
                        // We can discard the whole log, seting the correct indexes and proceed
                        // with applying entries
                        warn!(
                            local_zero_index = %self.zero_log_index,
                            local_zero_term = %existing_term,
                            snapshot_index = %info.index,
                            snapshot_term = %info.term,
                            "follower snapshot is out of sync with persistent log, log is discarded");
                        self.log_mut()
                            .discard_until(info.index, info.term)
                            .map_err(|e| {
                                Error::Critical(CriticalError::PersistentLogWrite(Box::new(e)))
                            })?;
                    } else {
                        // Snapshot is also bad. We can only report the error, making the
                        // caller reset the follower.

                        return Err(Error::Critical(CriticalError::SnapshotAndLogCorrupted));
                    }
                } else {
                    // We are still in the branch where the term in log did not match the one from leader. But
                    // now we don't have any sources of truth on the state machine.
                    // For LogIndex(0) such situation would be correct, but the term  for LogIndex(0) must always be Term(0),
                    // meaning we cannot get here when log is at LogIndex(0)
                    // This definitely means the snapshot absence is not normal: the state cannot
                    // be restored.
                    trace!(
                        local_zero_index = %self.zero_log_index,
                        local_zero_term = %existing_term,
                        "follower snapshot is absent while log index does not match term");
                    return Err(Error::Critical(CriticalError::SnapshotExpected));
                }
            } else {
                // Here the term in log do not match the one on leader, but the snapshot is somewhere behind
                // the leader's prev_entry.
                // This means, that our log's tail starting from leader_prev_log_index is
                // inconsistent, so we must delete it and ask a leader for pre-previous entry until
                // we find matching terms.

                self.log_mut()
                    .discard_since(leader_prev_log_index)
                    .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;
            }

            return Ok(Some(AppendEntriesResponse::InconsistentIndex(
                current_term,
                leader_prev_log_index - 1,
                leader_prev_log_index - 1,
            )));
        }

        // Starting from here, we know that the terms are correct, meaning the entries can be
        // appended.
        // Though, due to follower making snapshots independently of the leader,
        // the incoming request from it may have some extra entries, that follower
        // does not need.
        // We should also consider the bug described in ktoso/akka-raft#66:
        // Due to packet reordering it is possible that follower will receive
        // AppendEntries with the list of entries shorter than the previously received.
        // This means we cannot mindlessly delete the rest of the log starting from prevIndex
        // instead, we need to compare terms and indexes in the incoming packet and only rewrite
        // entries starting from the one that does not match or does not exist in the follower log

        todo!("rethink the part above in terms of ktoso/akka-raft#66 and deleting already committed entries");
        todo!("find correct behaviour for heartbeats");

        let AppendEntriesRequest { entries, .. } = request;
        let num_entries = entries.len();

        //let new_latest_log_index = leader_prev_log_index + num_entries as u64;
        //if new_latest_log_index < self.min_index {
        //// Stale entry; ignore. This guards against overwriting a
        //// possibly committed part of the log if messages get
        //// rearranged; see ktoso/akka-raft#66.
        //return Ok(None);
        //}

        //let append_index = leader_prev_log_index + 1;
        let entries_last_index = leader_prev_log_index + num_entries as u64;
        let entries_first_index = leader_prev_log_index + 1;

        let new_commit_index = cmp::min(request.leader_commit, self.latest_log_index);

        let append_index = if self.latest_volatile_log_index == leader_prev_log_index {
            // the most often case: everything is normal, entries are appended
            // as intended, nothing to discard
            self.latest_volatile_log_index + 1
        } else if self.latest_volatile_log_index > leader_prev_log_index {
            // entries overlaps with the log somehow, we need to know how exactly
            // and which indexes are correct
            //
            // TODO: while we don't have to overwrite anything before commit index,
            // this case is only possible with bad leader and packet reordering
            // Ideally, we should check the entries terms and indexes and ignore the packet if all of them match
            // and to return a critical error if indexes don't match

            // obviously, there may be a non-overlapping part, that is not written yet at all
            let overlapping_len = self.latest_volatile_log_index - leader_prev_log_index;

            let overlapping_start = leader_prev_log_index + 1;
            let overlapping_end = leader_prev_log_index + overlapping_len as u64;

            let mut entry_index = overlapping_len as usize - 1;
            let mut index = overlapping_end;
            let mut overwrite_index = None;

            while index <= overlapping_start {
                let term = self
                    .log()
                    .term_of(index)
                    .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
                if term != entries[entry_index].term {
                    overwrite_index = Some(entry_index);
                    todo!("enforce overwriting");
                }
                index = index - 1;
                entry_index = entry_index - 1;
            }
            todo!()
        } else {
            // self.latest_volatile_log_index < leader_prev_log_index */
            return Ok(Some(AppendEntriesResponse::InconsistentIndex(
                current_term,
                leader_prev_log_index - 1,
                leader_prev_log_index - 1,
            )));
        };

        todo!("this is an old code, may be based on incorrect assumptions");
        // for a heartbeat request appending entries may be skipped,
        // but all other check and actions laying afterwards still has be performed
        // because due to Raft's nature the follower's state machine(not log though) is always 1 commit behind the leader
        // until it receives the next AppendEntries message, be it heartbeat or whatever else
        let mut updated_config_index = None;
        let mut log_entries = Vec::with_capacity(entries.len());
        let mut entry_index = append_index;
        for entry in entries.into_iter() {
            // with each send, leader marks config entries as active if they really take place
            // as current cluster configuration, this way the follower can skip using previous
            // inactive entries as actual ones and will not connect to some old nodes
            if let EntryData::Config(config, is_active) = &entry.data {
                if *is_active {
                    updated_config_index = Some(entry_index);
                }
            }
            entry_index = entry_index + 1;

            log_entries.push(entry.into());
        }

        trace!("appending at {}: {:?}", append_index, &log_entries);
        self.log_mut()
            .append_entries(append_index, &log_entries)
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;
        self.update_log_view()?;
        self.min_index = self.latest_log_index;

        // We are matching the leader's log up to and including self.latest_log_index, which
        // may have been moved forward after appending
        self.commit_index = cmp::min(request.leader_commit, self.latest_log_index);
        self.apply_follower_commits()?;

        // we should only work with configs after we've moved our commit index
        // because our current config may have become committed at this point
        if let Some(updated_config_index) = updated_config_index {
            // persist new index as latest, make previous index become previous
            if self.latest_config_index > self.commit_index {
                // self.latest_config_index MUST be committed at this point
                // otherwise this is an error: new config cannot come until
                // the previous one is committed
                return Err(Error::unreachable(debug_where!()));
            }
            let latest_config_index = self.latest_config_index; // bind required to pass the index as self is borrowed mutably
            self.log_mut()
                .set_latest_config_view(latest_config_index, updated_config_index)
                .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;

            self.latest_config_index = updated_config_index;

            // Now, there is a case where the new config comes into view already,
            // for example if append_entries was synchronous.
            // This will be checked on the next event and does not affect
            // us because we only should answer to the same leader we received AppendEntries from
        }

        Ok(Some(AppendEntriesResponse::Success(
            current_term,
            self.latest_log_index,
            self.latest_volatile_log_index,
        )))
    }

    /// Applies all committed but unapplied log entries to the state machine ignoring the results.
    pub(crate) fn apply_follower_commits(&mut self) -> Result<(), Error> {
        trace!(to = ?self.commit_index, "applying entries");
        self.state_machine
            .apply(self.commit_index)
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

        // TODO: when having clients connected to follower, deal with their
        // expected proposals the same way as it is done on leader

        Ok(())
    }

    fn into_candidate(
        self,
        handler: &mut H,
        voluntary: bool,
    ) -> Result<State<M, H, Candidate>, Error> {
        trace!("transitioning to candidate");
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
            zero_log_index: self.zero_log_index,
            latest_log_index: self.latest_log_index,
            latest_volatile_log_index: self.latest_volatile_log_index,
            latest_config_index: self.latest_config_index,
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
    pub(crate) fn new() -> Follower {
        Follower {
            leader: None,
            delayed_vote_request: None,
            can_vote: false,
        }
    }

    /// Sets a new leader.
    pub(crate) fn set_leader(&mut self, leader: ServerId) {
        self.leader = Some(leader);
    }
}
