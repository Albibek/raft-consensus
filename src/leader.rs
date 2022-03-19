use std::collections::{BTreeMap, VecDeque};

use crate::debug_where;
use crate::error::*;
use crate::state_machine::SnapshotInfo;

use crate::config::{ConsensusConfig, SlowNodeTimeouts};
use crate::handler::Handler;
use crate::message::*;
use crate::persistent_log::{LogEntry, LogEntryData};
use crate::raft::CurrentState;
use crate::state::State;
use crate::state_impl::StateImpl;
use crate::{AdminId, ClientId, LogIndex, Peer, ServerId, Term};

use crate::follower::Follower;
use crate::persistent_log::{Log, LogEntryMeta};
use crate::state_machine::StateMachine;

use bytes::Bytes;
use tracing::{debug, info, trace};

const MAX_ROUNDS: u32 = 10;
const MAX_TOTAL_TIMEOUTS: u32 = 20;
const MAX_SNAPSHOT_TIMEOUTS: u32 = 10;
const MAX_ENTRIES_TIMEOUTS: u32 = 10;

impl<M, H> StateImpl<M, H> for State<M, H, Leader>
where
    M: StateMachine,
    H: Handler,
{
    fn append_entries_request(
        self,
        handler: &mut H,
        from: ServerId,
        request: AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, CurrentState<M, H>), Error> {
        // when leader receives AppendEntries, this means another leader is in action somehow
        let leader_term = request.term;
        let current_term = self.current_term()?;

        if leader_term < current_term {
            return Ok((AppendEntriesResponse::StaleTerm(current_term), self.into()));
        }

        if leader_term == current_term {
            // When new leader is at the same term, the single leader-per-term invariant is broken;
            // there is a bug in the Raft implementation.
            return Err(Error::Critical(CriticalError::AnotherLeader(
                from,
                current_term,
            )));
        }

        let new_state = self.leader_into_follower(handler, leader_term)?;
        let (response, new_state) = new_state.append_entries_request(handler, from, request)?;

        Ok((response, new_state))
    }

    /// The provided message may be initialized with a new AppendEntries request to send back to
    /// the follower in the case that the follower's log is behind.
    fn append_entries_response(
        mut self,
        handler: &mut H,
        from: ServerId,
        response: AppendEntriesResponse,
    ) -> Result<(Option<PeerMessage>, CurrentState<M, H>), Error> {
        let current_term = self.current_term()?;
        let local_latest_log_index = self.latest_log_index;

        // peer successfully received last AppendEntries, consider it alive and running
        // within current election timeout
        self.state_data.mark_alive(from);

        // First, check if the response is from the correct term
        // and is not some error
        // We are ok to respond to some peers not in config in case they were there
        // in previous terms
        match response {
            AppendEntriesResponse::Success(follower_term, _, _)
            | AppendEntriesResponse::StaleTerm(follower_term)
            | AppendEntriesResponse::InconsistentPrevEntry(follower_term, _, _)
                if current_term < follower_term =>
            {
                debug!("leader found a peer with the higher term, stepping down");
                // some node has received message with term higher than ours,
                // that means some other leader appeared in consensus,
                // we should downgrade to follower immediately
                let new_state = self.leader_into_follower(handler, current_term)?;
                Ok((None, new_state.into()))
            }

            AppendEntriesResponse::Success(follower_term, _, _)
            | AppendEntriesResponse::StaleTerm(follower_term)
            | AppendEntriesResponse::InconsistentPrevEntry(follower_term, _, _)
                if current_term > follower_term =>
            {
                // some follower confirmed message we've sent at previous term
                // it is ok for us
                Ok((None, self.into()))
            }
            AppendEntriesResponse::StaleEntry => Ok((None, self.into())),
            AppendEntriesResponse::StaleTerm(_) => {
                // The peer is reporting a stale term, but the term number matches the local term.
                // Ignore the response, since it is to a message from a prior term, and this server
                // has already transitioned to the new term.

                Ok((None, self.into()))
            }
            AppendEntriesResponse::InconsistentPrevEntry(
                _follower_term,
                follower_last_index,
                follower_volatile_index,
            ) => {
                let message = self.next_entries_or_snapshot(
                    handler,
                    from,
                    follower_last_index,
                    follower_volatile_index,
                    current_term,
                )?;
                Ok((message, self.into()))
            }
            AppendEntriesResponse::Success(
                _,
                follower_latest_log_index,
                follower_latest_volatile_log_index,
            ) => {
                trace!(
                    follower_latest_log_index = %follower_latest_log_index,
                    local_latest_log_index = %local_latest_log_index,
                    "follower responded success, checking next entry"
                );
                if follower_latest_volatile_log_index > self.latest_volatile_log_index {
                    // some follower has too high index in it's log
                    // it can only happen by mistake or incorrect log on a follower
                    // we only can report such follower and let the caller process the error

                    // TODO reset follower by sending a snapshot to it
                    // or starting the log from the beginning
                    return Err(Error::BadFollowerIndex);
                }

                // now we need the following things considering follower status

                // for everyone:
                //  * update their indices in the state
                //  * decide what message should be next for the follower

                // regardless of the follower type: update the indices metadata
                // in the leader state
                let new_commit_index = self.update_follower_indices(
                    from,
                    follower_latest_volatile_log_index + 1,
                    follower_latest_log_index,
                );

                // we match to make sure we process all possible follower states
                match self.state_data.follower_status_kind(from) {
                    FollowerStatusKind::Voter => {
                        // having None in new_commit_index is unreachable because of check
                        // of follower existence in follower_status_kind
                        let new_commit_index =
                            new_commit_index.ok_or(Error::unreachable(debug_where!()))?;
                        if new_commit_index != self.commit_index {
                            // if commit index has changed: let the persistence layer know
                            // that the range can be applied
                            let (instant_commit, step_down_required) = self.apply_commits(
                                self.commit_index,
                                new_commit_index,
                                &mut handler,
                            )?;
                            self.commit_index = new_commit_index;
                            // At this point no commits is guaranteed because state machine may be
                            // async. BUT: we count majority considering all the followers, not
                            // only the leader, so the commit on leader's local state machine is
                            // not required to be persisted for majority of the cluster to proceed.

                            if instant_commit {
                                // client requested the data to be committed on all the followers
                                // as soon as possible, so we need to send them either a
                                // ping message or the next entries without waiting for the heartbeat timeout
                                //
                                // if we send the next entries, the commit may become delayed
                                // because of delays on reading the log on leader along with
                                // the follower, which will try to write them into the log before
                                // sending the confirmation so we should prefer the ping message
                                //
                                // after the ping the leader will receive the responses
                                // so the next entries will anyways be sent shortly after one
                                // network round-robin
                                // TODO: in future the appendEntries may be replaced with some special packet (like
                                // LeaderCommit) which may be shorter since it does
                                // not require replying and only intended for follower to
                                // try moving its commit index

                                for id in self.state_data.iter_alive() {
                                    let heartbeat = self.new_heartbeat_request(&id)?;
                                    handler.send_peer_message(
                                        id,
                                        PeerMessage::AppendEntriesRequest(heartbeat),
                                    );
                                }
                            }
                            if step_down_required {
                                trace!(
                                    "latest configuration change required the leader to step down"
                                );
                                // become follower that will instantly become candidate on timeout
                                let new_state = self.leader_into_follower(handler, current_term)?;
                                Ok((None, new_state.timeout_now(handler)?))
                            } else {
                                let message =
                                    self.next_entries_or_snapshot(handler, from, current_term)?;
                                Ok((message, self.into()))
                            }
                        } else {
                            // commit index did not change
                            let message =
                                self.next_entries_or_snapshot(handler, from, current_term)?;
                            Ok((message, self.into()))
                        }
                    }

                    FollowerStatusKind::CatchingUp => {
                        // for any catching up state: update the rounds and see if
                        // the follower is the one that caught up now
                        // if it is:
                        // * for voting followers: initiate the configuration change
                        // * for non-voting followers: update follower state to NonVoter

                        trace!(
                            "follower is catching up: {:?}",
                            self.state_data.config_change
                        );
                        // For the catching up node, receiving a number of entries means it
                        // has made some progress,
                        let message = self.check_catch_up_status(
                            handler,
                            from,
                            current_term,
                            local_latest_log_index,
                        )?;
                        Ok((message, self.into()))
                    }
                    FollowerStatusKind::NonVoter => {
                        // for non-voting followers nothing is required except the next packet
                        let message = self.next_entries_or_snapshot(handler, from, current_term)?;
                        Ok((message, self.into()))
                    }
                    FollowerStatusKind::Unknown => {
                        trace!(
                            id = ?from,
                            "ignored success response from unknown peer"
                        );
                        Ok((None, self.into()))
                        // TODO: think if we should return error here
                        // since it's recoverable there is only a small difference between
                        // giving a debug message ourselves or let the caller know the
                        // peer is "strange"
                        // Since the peer is not always invalid (may be a delayed package for example)
                        // the caller may be confused with the error and break consensus just
                        // by not processing the return value properly
                        //Err(Error::UnknownPeer(from.clone()))
                    }
                }
            }
        }
    }

    /// Applies a peer vote request to the consensus.
    fn request_vote_request(
        self,
        handler: &mut H,
        candidate: ServerId,
        request: RequestVoteRequest,
    ) -> Result<(Option<RequestVoteResponse>, CurrentState<M, H>), Error> {
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

    fn timeout_now(self, _handler: &mut H) -> Result<CurrentState<M, H>, Error> {
        info!("ignored timeout_now");
        Ok(self.into())
    }

    fn install_snapshot_request(
        self,
        handler: &mut H,
        from: ServerId,
        request: InstallSnapshotRequest,
    ) -> Result<(PeerMessage, CurrentState<M, H>), Error> {
        // this is the same logic as in append_entries_request
        let leader_term = request.term;
        let current_term = self.current_term()?;

        // in case of delayed request it is ok to receive something from former leader
        if leader_term < current_term {
            return Ok((
                PeerMessage::InstallSnapshotResponse(InstallSnapshotResponse::StaleTerm(
                    current_term,
                )),
                self.into(),
            ));
        }

        if leader_term == current_term {
            // When new leader is at the same term, the single leader-per-term invariant is broken;
            // there is a bug in the Raft implementation.
            return Err(Error::Critical(CriticalError::AnotherLeader(
                from,
                current_term,
            )));
        }

        let new_state = self.leader_into_follower(handler, leader_term)?;
        let (response, new_state) = new_state.install_snapshot_request(handler, from, request)?;

        Ok((response, new_state))
    }

    fn install_snapshot_response(
        mut self,
        handler: &mut H,
        from: ServerId,
        response: InstallSnapshotResponse,
    ) -> Result<(Option<PeerMessage>, CurrentState<M, H>), Error> {
        let current_term = self.current_term()?;
        let local_latest_log_index = self.latest_log_index;

        // we receive this when some node had received InstallSnapshotRequest
        match response {
            InstallSnapshotResponse::Success(term, _, _)
            | InstallSnapshotResponse::StaleTerm(term)
                if &current_term < &term =>
            {
                // some node has received message with term higher than ours,
                // that means some other leader appeared in consensus,
                // we should downgrade to follower immediately
                let new_state = self.leader_into_follower(handler, current_term)?;
                Ok((None, new_state.into()))
            }

            InstallSnapshotResponse::Success(term, _, _)
            | InstallSnapshotResponse::StaleTerm(term)
                if &current_term > &term =>
            {
                // some follower confirmed message we've sent at previous term
                // it is ok for us
                Ok((None, self.into()))
            }
            InstallSnapshotResponse::StaleTerm(_) => {
                // The peer is reporting a stale term, but the term number matches the local term.
                // Ignore the response, since it is to a message from a prior term, and this node
                // has already transitioned to the new term.

                Ok((None, self.into()))
            }
            InstallSnapshotResponse::Success(
                _follower_term,
                follower_snapshot_index,
                next_chunk_request,
            ) => {
                // don't waste state machine resources on unknown peers
                if !self.state_data.has_any_follower(from) {
                    return Err(Error::UnknownPeer(from));
                }

                if let Some(next_chunk_request) = next_chunk_request {
                    // receiving Some means there is more chunks required from the state machine
                    match self.new_install_snapshot(
                        Some(next_chunk_request.as_slice()),
                        local_latest_log_index,
                    ) {
                        Err(Error::SnapshotExpected) => {
                            // TODO: Getting here means snapshot was taken and started to be sent, but
                            // disappeared for some reason. In this situation we cannot determine
                            // whether the next chunk should be sent and can only start over.
                            // In future we could try recovering by taking snapshot again
                            // with take_snapshot and trying to send new one from scratch. But since conditions
                            // where this could happen are not very clean at the moment, we
                            // prefer to return an error here.

                            return Err(Error::SnapshotExpected);
                        }
                        Ok((message, _info)) => {
                            // this behaviour is same for catching-up and follower nodes
                            // and means there are more snapshots to send
                            if self.state_data.is_catching_up(from)? {
                                self.state_data.update_rounds(from)?;
                            }
                            return Ok((
                                Some(PeerMessage::InstallSnapshotRequest(message)),
                                self.into(),
                            ));
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }

                // Receiving None from the follower OR from the read_next_chunk
                // means the follower has received the last chunk of the snapshot.
                // Even if the snapshot is not applied on the follower, we clear the status
                // because the leader is using this flag only for its own local snapshot "soft-locking".
                self.state_data.clear_pending_snapshot(&from);

                // If snapshot was done at some other index, we expect the follower
                // to re-read them from the state machine and put in the response
                // This will mean we have the actual values we can update locally
                // Since snapshots are only taken using commit index or lower,
                // advancing an index on a single follower cannot advance index of the
                // consensus. This means there is no need to call try_advance_commit_index,
                // but the follower matching status has to be saved.
                self.state_data.update_follower_indices(
                    from,
                    follower_snapshot_index + 1,
                    follower_snapshot_index,
                )?;
                // Now, if the follower was a catching-up future voter,
                // check and update its state first
                if self.state_data.is_catching_up(from)? {
                    let message = self.check_catch_up_status(
                        handler,
                        from,
                        current_term,
                        local_latest_log_index,
                    )?;
                    Ok((message, self.into()))
                } else {
                    Ok((
                        self.next_entries_or_snapshot(handler, from, current_term)?,
                        self.into(),
                    ))
                }
            }
        }
    }

    // Timeout handling
    fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error> {
        debug!("heartbeat timeout for peer: {}", peer);
        self.state_data.forget_alive(peer);
        self.state_data.decrease_timeout(peer);
        self.new_heartbeat_request(&peer)
    }

    fn election_timeout(mut self, handler: &mut H) -> Result<CurrentState<M, H>, Error> {
        // election timeout is never down, but we want handler to radomize it
        handler.set_timeout(Timeout::Election);
        if self.state_data.config_change.is_some() {
            if self.state_data.catching_up_timeout()? {
                // have more timeouts left
            } else {
                //  no timeouts left for node
                self.state_data.reset_config_change(handler);
            }
        }
        // for solitary consensus the count(=1) will be equal to majority(=1), so
        // the step down will not be performed
        if self.state_data.alive_voters_count() < self.config.majority() {
            debug!("leader did not receive enough responses from followers, stepping down to avoid stale leadership");
            let term = self.current_term()?;
            Ok(CurrentState::Follower(
                self.leader_into_follower(handler, term)?,
            ))
        } else {
            Ok(self.into())
        }
    }

    fn check_compaction(&mut self, _handler: &mut H, force: bool) -> Result<bool, Error> {
        // leader may have snapshots in progress of being sent to slow followers
        // or catching up nodes.  In that case we prefer not to take snapshot if it is
        // not forced. But there is an exclusion here: same index.
        //
        // Due to the implementation of state machine's chunk metadata mechanism,
        // we can keep sending all previoius chunks if we guarantee that new snapshots
        // are only taken if leader's commit index has changed (i.e. new data has been applied to
        // state machine).  With this implemented we can use leader's commit index as a kind of
        // snapshot ID

        // all common checks like self.commit_index == 0 are done in state::common_check_compaction

        // with pending snapshots, but without forcing - do not request it, hoping
        // slow followers will catch up
        if !force && self.state_data.have_pending_snapshots() {
            trace!("not taking snapshot because leader has snapshots pending and snapshot was not forced");
            return Ok(false);
        }

        // without pending snapshots there is no problem: just do the snapshot regardless of forcing
        if self.state_data.have_pending_snapshots() {
            return self.common_check_compaction(force);
        }

        // the last case: force and pending_snapshots
        let snapshot_changed = self.common_check_compaction(force)?;

        Ok(snapshot_changed)
    }

    /// Applies a client proposal to the consensus state machine. May be applied with the delay
    /// depending on urgency
    fn client_proposal_request(
        &mut self,
        handler: &mut H,
        from: ClientId,
        request: ClientRequest,
    ) -> Result<ClientResponse, Error> {
        // write the request to log anyways

        match request.urgency {
            Urgency::Fast => {
                // append an entry to the log and send the message right away
                let mut message = AppendEntriesRequest::new(1);
                let log_index = self.add_new_entry(
                    LogEntryData::Proposal(request.data, request.urgency),
                    Some(&mut message),
                )?;
                debug!(
                    "proposal request from client {} assigned idx {}",
                    from, log_index
                );

                self.send_append_entries_request(handler, log_index, message)?;

                self.state_data.proposals.push_front(from);
                Ok(ClientResponse::Queued(log_index))
            }

            Urgency::Log => {
                let log_index = self
                    .add_new_entry(LogEntryData::Proposal(request.data, request.urgency), None)?;
                debug!(
                    "proposal request from client {} assigned idx {}",
                    from, log_index
                );

                self.state_data.proposals.push_front(from);
                // write to log, but don't send append_entries_request right away
                Ok(ClientResponse::Queued(log_index))
            }
            Urgency::Batch => {
                self.state_data
                    .client_batch
                    .push((from, LogEntryData::Proposal(request.data, request.urgency)));
                if self.state_data.client_batch.len() == 1 {
                    handler.set_timeout(Timeout::Client);
                }

                Ok(ClientResponse::BatchQueued)
            }
        }
    }

    /// Applies client query to the state machine, urgency is ignored
    fn client_query_request(
        &mut self,
        from: ClientId,
        request: ClientRequest,
    ) -> Result<ClientResponse, Error> {
        // TODO: linerability for clients
        trace!("query from client {}", from);
        let result = self
            .state_machine
            .query(request.data)
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;
        Ok(ClientResponse::Success(result))
    }

    fn ping_request(&self) -> Result<PingResponse, Error> {
        self.common_client_ping_request(ConsensusState::Leader)
    }

    // Configuration change messages
    fn add_server_request(
        &mut self,
        handler: &mut H,
        from: AdminId,
        request: AddServerRequest,
    ) -> Result<ConfigurationChangeResponse, Error> {
        debug!(
            requested_id = %request.id,
            requested_info = ?request.info,
            "add server requested");
        // check if first empty entry has been committed by consensus, do not allow config change until it has
        let latest_log_term = self.latest_log_term()?;
        let current_term = self.current_term()?;
        if latest_log_term < current_term {
            return Ok(ConfigurationChangeResponse::LeaderJustChanged);
        }

        if self.state_data.config_change.is_some() {
            Ok(ConfigurationChangeResponse::AlreadyPending)
        } else {
            let info = self
                .state_machine
                .snapshot_info()
                .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;
            let snapshot_index = if let Some(SnapshotInfo { index, .. }) = info {
                index
            } else if self.zero_log_index != LogIndex(0) {
                // state machine may be broken, storing no snapshot
                // when it is expected
                return Err(Error::Critical(CriticalError::SnapshotExpected));
            } else {
                // local node has no snapshot yet
                LogIndex(0)
            };

            self.state_data.config_change = Some(ConfigChange::new(
                request.id,
                from,
                snapshot_index,
                request.info,
                self.options.timeouts.clone(),
            ));

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

    fn step_down_request(
        &mut self,
        handler: &mut H,
        from: AdminId,
        request: Option<ServerId>,
    ) -> Result<ConfigurationChangeResponse, Error> {
        trace!("request to step down from {}", from);
        if let Some(id) = request {
            if self.config.has_peer(id) {
                handler.send_peer_message(id, PeerMessage::TimeoutNow);
                Ok(ConfigurationChangeResponse::Started)
            } else {
                Ok(ConfigurationChangeResponse::BadPeer)
            }
        } else {
            // when request does not specify the node to give up leader to,
            // choose the node, where leader is more likely to be elected,
            // i.e. the one whichsi alive and whose log is the most far forward
            let mut max_index = LogIndex(0);
            let best_follower = self.state_data.farthest_alive_voter(self.id);
            if let Some(id) = best_follower {
                handler.send_peer_message(id, PeerMessage::TimeoutNow);
                Ok(ConfigurationChangeResponse::Started)
            } else {
                Ok(ConfigurationChangeResponse::NoAliveFollowers)
            }
        }
    }

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error> {
        // According to Raft 4.1 last paragraph, servers should receive any RPC call
        // from any server, because they may be a new ones which current server doesn't know
        // about yet

        if !self.state_data.has_any_follower(peer) {
            // This may still be correct peer, but it is was not added using AddServer API or was
            // removed already
            // the peer still can be the one that is going to catch up, so we skip this
            // check for a leader state
            // By this reason we do not panic here nor return an error
            debug!(
                "New peer connected, but not found in configuration: {:?}",
                peer
            );
            return Ok(());
        }

        // send a ping to a peer to get into request-responce cycle faster
        let message = self.new_heartbeat_request(&peer)?;

        // For stateless/lossy connections we cannot be sure if peer has received
        // our entries, so we call set_next_index only after response, which
        // is done in response processing code
        handler.send_peer_message(peer, PeerMessage::AppendEntriesRequest(message));

        // TODO: probably return the message here
        Ok(())
    }

    fn into_consensus_state(self) -> CurrentState<M, H> {
        CurrentState::Leader(self)
    }

    fn client_timeout(&mut self, handler: &mut H) -> Result<(), Error> {
        if self.state_data.client_batch.is_empty() {
            return Err(Error::BadLeaderBatch);
        }

        let term = self.current_term()?;
        let prev_log_index = self.latest_log_index;

        let log_index = prev_log_index + 1;

        let mut entries = Vec::new();
        while let Some((client_id, data)) = self.state_data.client_batch.pop() {
            entries.push(LogEntry { term, data });
            self.state_data.proposals.push_front(client_id);
        }

        trace!(index = %log_index, ?entries, "appending batched clients proposals to log");
        self.log_mut()
            .append_entries(log_index, &entries)
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;

        handler.clear_timeout(Timeout::Client);
        Ok(())
    }

    fn on_any_event(&mut self) -> Result<(), Error> {
        self.common_on_any_event()?;
        // at this point we know some new information about our log
        // which may have moved forward
        // so we need to save this information as we were follower (paper chapter 10) so
        // our match index was counted
        self.state_data
            .set_own_match_index(self.id, self.latest_log_index);
        Ok(())
    }
}

impl<M, H> State<M, H, Leader>
where
    M: StateMachine,
    H: Handler,
{
    fn leader_into_follower(
        mut self,
        handler: &mut H,
        leader_term: Term,
    ) -> Result<State<M, H, Follower>, Error> {
        self.state_data.reset_config_change(handler);
        self.into_follower(handler, ConsensusState::Leader, leader_term)
    }

    // Updates the information about follower's last known indices
    // return the updated commit index if follower match index changed
    //
    // Returns None only if follower did not exist
    fn update_follower_indices(
        &mut self,
        follower_id: ServerId,
        next_index: LogIndex,
        match_index: LogIndex,
    ) -> Option<LogIndex> {
        let majority = self.config.majority();
        self.state_data.update_follower_indices_state(
            follower_id,
            next_index,
            match_index,
            self.commit_index,
            majority as u64,
        )
    }

    // while advancing index, the step down may be required, if one of the committed entries
    // contains the configuration change, which removes the current node
    // the return value is the flags in order `(step_down_required, instant_commit)`
    // showing if stepping down have to happen and if instant commit is required for
    // the updated commit index
    fn apply_commits(
        &mut self,
        first_index: LogIndex,
        last_index: LogIndex,
        handler: &mut H,
    ) -> Result<(bool, bool), Error> {
        let mut entry_index = first_index;
        while entry_index <= last_index {
            // so we read all committed entries and check if any of them have to be
            // replied to clients
            let entry_kind = self
                .log()
                .entry_meta_at(entry_index)
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;

            trace!(
                "leader applying entry at {}: {:?}",
                &entry_index,
                &entry_kind
            );

            match entry_kind {
                LogEntryMeta::Empty => {
                    self.state_machine
                        .apply(entry_index, false)
                        .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;
                }
                LogEntryMeta::Proposal(urgency) => {
                    let result = self
                        .state_machine
                        .apply(entry_index, true)
                        .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

                    if urgency == Urgency::Fast {
                        // if any of the entries was requested with fast guarantee,
                        // ping the followers without waiting for the timeouts
                        // to make the entry committed as fast as possible
                        instant_commit = true;
                    }
                    // As long as we know it, we've sent the connected clients the notification
                    // about their proposals being queued
                    //
                    // Client proposals may come not in the same order clients expect. For example, via parallel
                    // TCP connections. TODO: with linerability it will be possible to process
                    // requests correctly

                    let client_id = match self.state_data.proposals.pop_back() {
                        Some(id) => id,
                        None => return Err(Error::unreachable(debug_where!())),
                    };
                    if let Some(response) = result {
                        trace!(
                            "responding to client {} for entry {}",
                            client_id,
                            entry_index
                        );

                        handler.send_client_message(
                            client_id,
                            ClientMessage::ClientProposalResponse(ClientResponse::Success(
                                response,
                            )),
                        );
                    } else {
                        return Err(Error::unreachable(debug_where!()));
                    }
                }
                LogEntryMeta::Config(config) => {
                    self.state_machine
                        .apply(entry_index, false)
                        .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

                    if self.config.has_pending_peer(self.id) && self.config.has_pending_removal() {
                        step_down_required = true;
                    } else {
                        // when applying multiple configuration entries (shoult not happen actually, but just in
                        // case), reset the step down requirements it it was canceled by the
                        // following entry
                        step_down_required = false;
                    }

                    // applying a config entry means the config probably was a pending one
                    // we detect this by checking current indices
                    if entry_index == self.latest_config_index {
                        // this entry was a pending config, so we:
                        // make our in-memory config non-pending
                        self.config.commit_pending();

                        // commit updated indices to log, setting both of them to the index
                        // of current entry
                        self.log_mut()
                            .set_latest_config_view(
                                self.latest_config_index,
                                self.latest_config_index,
                            )
                            .map_err(|e| {
                                Error::Critical(CriticalError::PersistentLogWrite(Box::new(e)))
                            })?;
                    }
                }
            }

            entry_index = entry_index + 1;
        }

        Ok((step_down_required, instant_commit))
    }

    /// based on follower indices, decide if we should send the follower
    /// new entries or snapshot
    /// also, update the corresponding indices in follower state
    fn next_entries_or_snapshot(
        &mut self,
        handler: &mut H,
        from: ServerId,
        follower_last_index: LogIndex,
        follower_last_volatile_index: LogIndex,
        current_term: Term,
    ) -> Result<Option<PeerMessage>, Error> {
        if follower_last_index == self.latest_log_index {
            // peer is in sync with leader, only catching up peers require atention
            if self.state_data.is_catching_up(from)? {
                return Err(Error::unreachable(debug_where!()));
            }

            // since the peer has caught up, set a heartbeat timeout.
            handler.set_timeout(Timeout::Heartbeat(from));
            Ok(None)
        } else if follower_last_volatile_index < self.zero_log_index {
            // follower is behind out snapshot, so we cannot send it any data
            // except for the snapshot because we don't have data before zero_log_index
            trace!(
                peer_match_index = %follower_last_index,
                local_latest_log_index = %self.latest_log_index,
                "peer is behind leader and requires snapshot because it is out of log index range"
            );
            //if self.state_data.get_pending_snapshot(&from) {
            // We may already have a snapshot being sent to follower, but received ApendEntries from
            // it with lower index than we have.
            // Possible reasons and solutions:
            // 1. AppendEntriesResponse was delayed and we already sending a snapshot to
            //    this follower
            // 2. Follower received the last chunk, but our log went too far from the last snapshot
            // we've sent and we already took a new snapshot
            //
            // In both these cases it leads to the same action: send a first chunk of the current snapshot.
            // For case (1) this can be handled by the state machine metadata mechanism
            // For case (2) it is just natural and the snapshot is really required

            // So, if no case is missed in the reasoning above, it's always the same action
            // regardless of the follower state, meanin gwe don't need the condition
            //}
            self.state_data.set_pending_snapshot(&from);
            match self.new_install_snapshot(None, self.latest_log_index) {
                Ok((message, _info)) => Ok(Some(PeerMessage::InstallSnapshotRequest(message))),
                Err(e @ Error::SnapshotExpected) => {
                    // missing a snapshot for any reason at this point means something wrong with the state
                    // machine, because the followers being at wrong index can only happen if
                    // snapshots existed before

                    Err(e)
                }
                Err(e) => return Err(e),
            }
        } else {
            // follower index is behind leader's index, meaning we have entries
            // to send

            // since follower already has an entry at `follower_last_volatile_index`,
            // we put this index as prev_log_index
            let prev_log_index = follower_last_volatile_index;
            let prev_log_term = self
                .state_machine
                .log()
                .term_of(prev_log_index)
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;

            let mut entries = Vec::new();
            self.fill_entries(&mut entries, follower_last_volatile_index + 1)?;
            trace!(
                num_entries = ?{entries.len()},
                "follower is missing log entries, sending them",
            );

            self.state_data.update_follower_indices(
                from,
                // save follower's next index considering a number of entries put
                follower_last_volatile_index + (entries.len() as u64) + 1,
                follower_last_index,
            );
            if entries.is_empty() {
                // depending on settings and log's state, fill_entries may return zero entries,
                // i.e. if log has only volatile entries, that cannot be read
                Ok(None)
            } else {
                // there is no need to update follower next index because it is
                // always set to self.latest_log_index + 1
                // TODO: in some future, if we limit the max number of entries or
                // kind of that, the index will have to be calculated in other way
                let message = AppendEntriesRequest {
                    term: current_term,
                    prev_log_index,
                    prev_log_term,
                    leader_commit: self.commit_index,
                    entries,
                };

                Ok(Some(PeerMessage::AppendEntriesRequest(message)))
            }
        }
    }

    // read a chunk from state machine and put it into InstallSnapshot packet
    // if snapshot is not required or cannot be made, None is returned
    fn new_install_snapshot(
        &self,
        chunk_request: Option<&[u8]>,
        latest_log_index: LogIndex,
    ) -> Result<(InstallSnapshotRequest, SnapshotInfo), Error> {
        let info = self
            .state_machine
            .snapshot_info()
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?
            .ok_or(Error::SnapshotExpected)?;

        let chunk_data = self
            .state_machine
            .read_snapshot_chunk(chunk_request)
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

        let term = self.current_term()?;
        let last_log_term = self.latest_log_term()?;

        let last_config = if chunk_request.is_some() {
            Some(self.config.peers.clone())
        } else {
            None
        };

        let message = InstallSnapshotRequest {
            term,
            last_config,
            snapshot_index: info.index,
            snapshot_term: info.term,
            force_reset: false,
            chunk_data,
        };
        Ok((message, info))
    }

    fn new_heartbeat_request(&self, peer: &ServerId) -> Result<AppendEntriesRequest, Error> {
        // next_index is never LogIndex(0) by algorithm definition
        // (state_data is initialized by self.latest_log_index + 1)
        let follower_last_known_index = self.state_data.next_index(peer)? - 1;

        let (prev_log_index, prev_log_term) = if follower_last_known_index < self.zero_log_index {
            // follower's index is behind the snapshot
            // so we can only send our zero index and its term
            (
                self.zero_log_index,
                self.log()
                    .term_of(self.zero_log_index)
                    .map_err(|e| Error::PersistentLogRead(Box::new(e)))?,
            )
        } else if follower_last_known_index > self.latest_log_index {
            // this should not be possible because of the checks done
            // before calling update_follower_indices
            return Err(Error::unreachable(debug_where!()));
        } else {
            (
                follower_last_known_index,
                self.log()
                    .term_of(follower_last_known_index)
                    .map_err(|e| Error::PersistentLogRead(Box::new(e)))?,
            )
        };

        Ok(AppendEntriesRequest {
            term: self.current_term()?,
            prev_log_index,
            prev_log_term,
            leader_commit: self.commit_index,
            entries: Vec::new(),
        })
    }

    fn check_catch_up_status(
        &mut self,
        handler: &mut H,
        from: ServerId,
        current_term: Term,
        local_latest_log_index: LogIndex,
    ) -> Result<Option<PeerMessage>, Error> {
        // having success from the catching up node, means
        // it has catched up the last round
        // the exception here is the very first "ping"
        // request, it will go to Some(false) because
        // last round check was not set to true intentionally
        match self.state_data.update_catch_up_status(from)? {
            CatchUpStatus::CaughtUp => {
                // switch to new stage
                let admin_id = self
                    .state_data
                    .begin_config_change(local_latest_log_index + 1, &mut self.config)?;

                let config = self.config.clone();

                // the config is already modified, just add it as an entry
                let mut message = AppendEntriesRequest::new(1);

                let log_index =
                    self.add_new_entry(LogEntryData::Config(config), Some(&mut message))?;
                self.send_append_entries_request(handler, log_index, message)?;

                handler.set_timeout(Timeout::Heartbeat(from));
                Ok(None)
            }
            CatchUpStatus::NotYet => {
                trace!(
                    "catching up node did not catch this round, continuing the catching up process"
                );

                let message = self.next_entries_or_snapshot(handler, from, current_term)?;

                // We need to maintain correct states here. Possible cases:
                match &message {
                    Some(PeerMessage::InstallSnapshotRequest(req)) => {
                        // Node is going to receive InstallSnapshotRequest message. Not depending on which chunk or
                        // snapshot count it is, the state must migrate to snapshotting at snapshot
                        // index
                        if let Some(config_change) = &mut self.state_data.config_change {
                            config_change.transition_to_snapshotting(req.snapshot_index)?;
                            Ok(message)
                        } else {
                            Err(Error::unreachable(debug_where!()))
                        }
                    }
                    Some(PeerMessage::AppendEntriesRequest(_)) => {
                        // Node receives AppendEntriesRequest message. State must be CatchingUp.
                        if let Some(config_change) = &mut self.state_data.config_change {
                            config_change.transition_to_catching_up()?;
                            Ok(message)
                        } else {
                            Err(Error::unreachable(debug_where!()))
                        }
                    }
                    _ => Err(Error::unreachable(debug_where!())),
                }
            }
            CatchUpStatus::TooSlow => {
                trace!("catching up node was too slow, configuration change cancelled");
                // The new config is only stored in the volatile state and is not
                // distributed to followers yet, so dropping it is enough, nothing to restore here
                self.state_data.reset_config_change(handler);
                Ok(None)
            }
        }
    }

    /// fetch the required number entries from the log and push them to provided vector
    fn fill_entries(&self, entries: &mut Vec<Entry>, from: LogIndex) -> Result<(), Error> {
        // TODO: limit entry bytes or something
        // as of now we push all the entries, regardless of amount
        // this may be kind of dangerous sometimes because of amount being too big
        // but most probably snapshotting would solve it
        let until = self.latest_volatile_log_index;

        for index in from.as_u64()..until.as_u64() {
            let mut log_entry = LogEntry::new_proposal(Term(0), Bytes::new(), Urgency::default());

            // read until log lets us read volatile entries
            if !self
                .log()
                .read_entry(LogIndex(index), &mut log_entry)
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?
            {
                if index > self.latest_log_index.as_u64() {
                    break;
                } else {
                    return Err(Error::Critical(CriticalError::FollowerLogBroken(
                        LogIndex(index),
                        "log refused to read persisted entry",
                    )));
                }
            }
            let mut entry: Entry = log_entry.into();
            if self.latest_config_index.as_u64() == index {
                entry.set_config_active(true);
            }

            entries.push(entry);
        }
        Ok(())
    }

    /// Appends a single entry to a leader's log obeying all the Raft rules (log indexing etc).
    /// Optionally, fills a message making it ready to be sent to followers. When request is None,
    /// the entry is still written to log, but will be send to the followers using the heartbeat mechanism.
    ///
    /// Returns the log index of appended entry (if append was synchronous)
    pub(crate) fn add_new_entry(
        &mut self,
        entry_data: LogEntryData,
        request: Option<&mut AppendEntriesRequest>,
    ) -> Result<LogIndex, Error> {
        let term = self.current_term()?;

        let entry_log_index = self.latest_log_index + 1;
        let leader_commit = self.commit_index;

        let log_entry = LogEntry {
            term,
            data: entry_data,
        };

        let prev_log_term = self.latest_log_term()?;

        trace!(at = %entry_log_index, entry = ?log_entry, "appending single entry to log");
        self.log_mut()
            .append_entries(entry_log_index, &[log_entry.clone()])
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;

        let mut entry: Entry = log_entry.into();
        // the entry is the new addition, so if it's a configuration change, than it is definitely a new active configuration
        entry.set_config_active(true);

        // even if the entry did not land in our log, we still can send it to the follower
        // setting the correct indices
        // To do this, we must NOT update our latest_log_index after appending even into_follower
        // the append realy happened
        if let Some(mut request) = request {
            request.term = term;
            request.prev_log_index = entry_log_index - 1;
            request.prev_log_term = prev_log_term;
            request.leader_commit = leader_commit;
            request.entries.push(entry);
        };

        // in case entries are appended synchronously, re-read the log updating the indices
        // and remember the new match index for self
        self.update_log_view()?;
        self.state_data
            .set_match_index(self.id, self.latest_log_index);

        Ok(entry_log_index)
    }

    fn send_append_entries_request(
        &mut self,
        handler: &mut H,
        log_index: LogIndex,
        message: AppendEntriesRequest,
    ) -> Result<(), Error> {
        if !self.config.is_solitary(self.id) {
            // fan out the request to all followers that are catched up enough
            for peer in &self.config.peers {
                if peer.id != self.id {
                    let id = peer.id;

                    trace!(
                        "index on {:?} = {:?} == {:?}",
                        &id,
                        self.state_data.next_index(&id),
                        log_index
                    );
                    if self.state_data.next_index(&id).unwrap() == log_index {
                        handler.send_peer_message(
                            id,
                            PeerMessage::AppendEntriesRequest(message.clone()),
                        );
                        self.state_data.set_next_index(id, log_index + 1)?;
                    }
                }
            }
        }

        todo!("deal with solitary consensus the other way");
        if self.config.majority() == 1 {
            // for a solitary consensus or a 2-peer cluster current node aready has a majority,
            // (because of it's own log commit)
            // so there is a reason to advance the index in case the proposal queue is empty
            //
            // otherwise, there is no point in this because the majority is not achieved yet
            //
            // instant commit is (obviously) not required
            let (step_down_required, _) = self.try_advance_commit_index(handler)?;
            if step_down_required {
                return Err(Error::unreachable(debug_where!()));
            }
        }
        Ok(())
    }
}

/// The state associated with a Raft consensus module in the `Leader` state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Leader {
    followers: BTreeMap<ServerId, FollowerInfo>,
    // optimization for updating commit index
    max_follower_index: LogIndex,

    client_batch: Vec<(ClientId, LogEntryData)>,
    proposals: VecDeque<ClientId>,

    // Liearizability marker set at the beginning of each term
    read_index: Option<LogIndex>,

    /// stores pending config change making sure there can only be one at a time
    pub(crate) config_change: Option<ConfigChange>,
}

impl Leader {
    /// Returns a new `LeaderState` struct.
    ///
    /// # Arguments
    ///
    /// * `latest_log_index` - The index of the leader's most recent log entry at the
    ///                        time of election.
    /// * `peers` - The set of peer cluster members.
    pub(crate) fn new(latest_log_index: LogIndex, config: &ConsensusConfig) -> Leader {
        let mut followers = BTreeMap::new();

        config.with_all_peers(|id| {
            // at the moment config only contains voting peers
            // TODO: persist non-voters to config as well
            followers.insert(*id, FollowerInfo::new(latest_log_index + 1, true));
            Ok(())
        });

        Leader {
            followers,
            max_follower_index: LogIndex(0),
            client_batch: Vec::new(),
            proposals: VecDeque::new(),
            read_index: None,
            config_change: None,
        }
    }

    fn has_any_follower(&mut self, id: ServerId) -> bool {
        self.followers.contains_key(&id)
    }

    fn has_voting_follower(&mut self, id: ServerId) -> bool {
        if let Some(info) = self.followers.get(&id) {
            match info.status {
                FollowerStatus::Voter { .. } => true,
                FollowerStatus::CatchingUpSnapshot { .. } => false,
                FollowerStatus::CatchingUpEntries { .. } => false,
                FollowerStatus::NonVoter => false,
            }
        } else {
            false
        }
    }

    fn is_catching_up(&self, id: ServerId) -> Result<bool, Error> {
        if let Some(info) = self.followers.get(&id) {
            Ok(match info.status {
                FollowerStatus::Voter { .. } => false,
                FollowerStatus::CatchingUpSnapshot { .. } => true,
                FollowerStatus::CatchingUpEntries { .. } => true,
                FollowerStatus::NonVoter => false,
            })
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }

    /*
     *    fn next_index(&self, follower: &ServerId) -> Result<LogIndex, Error> {
     *        if let Some(index) = self.next_index.get(follower) {
     *            return Ok(*index);
     *        }
     *
     *        match &self.config_change {
     *            Some(ConfigChange {
     *                peer,
     *                stage: ConfigChangeStage::Snapshotting { .. },
     *                ..
     *            }) => {
     *                if &peer.id == follower {
     *                    // the snapshotting follower has no index set up yet
     *                    return Ok(LogIndex(0));
     *                }
     *            }
     *            Some(ConfigChange {
     *                peer,
     *                stage: ConfigChangeStage::CatchingUp { index, .. },
     *                ..
     *            }) => {
     *                if &peer.id == follower {
     *                    // the index is requested for a catching up peer
     *                    return Ok(*index);
     *                }
     *            }
     *            Some(ConfigChange {
     *                peer,
     *                stage: ConfigChangeStage::Committing(_, index),
     *                ..
     *            }) => {
     *                if &peer.id == follower {
     *                    // peer has caught up and waiting for config to be
     *                    // committed by the followers
     *                    return Ok(*index);
     *                }
     *            }
     *            [>Some(_) => {<]
     *            [>trace!(?self.config_change);<]
     *            [>return Err(Error::unreachable(debug_where!()));<]
     *            [>}<]
     *            //_ => (),
     *            None => {
     *                return Err(Error::unreachable(debug_where!()));
     *            }
     *        }
     *
     *        Err(Error::UnknownPeer(follower.clone()))
     *    }
     */

    // Updates the information about follower's last known indices
    // return the updated commit index if follower match index changed
    //
    // Returns None only if follower did not exist
    fn update_follower_indices_state(
        &mut self,
        follower_id: ServerId,
        next_index: LogIndex,
        match_index: LogIndex,
        prev_commit_index: LogIndex,
        majority: u64,
    ) -> Option<LogIndex> {
        let mut update_required = false;
        if let Some(info) = self.followers.get_mut(&follower_id) {
            info.next_index = next_index;
            if let FollowerStatus::Voter {
                match_index: prev_match_index,
            } = info.status
            {
                if match_index != prev_match_index {
                    prev_match_index = match_index;
                    if match_index > self.max_follower_index {
                        self.max_follower_index = match_index
                    }
                    update_required = true;
                }
            }
        } else {
            return None;
        }

        if update_required {
            // This cool algorithm allows finding a majority using the least possible amount of iterations
            // by checking each follower's index instead of doing minus 1 each pass
            let mut current_checked_index = self.max_follower_index;
            let mut step = self.max_follower_index - prev_commit_index;
            let mut count = 0;
            loop {
                // TODO unit test this shit
                for info in self.followers.values() {
                    if let FollowerStatus::Voter { match_index } = info.status {
                        if match_index >= current_checked_index {
                            count += 1;
                        }
                        if match_index < current_checked_index
                            && step > current_checked_index - match_index
                        {
                            step = current_checked_index - match_index
                        }
                    }
                    if count >= majority {
                        return Some(current_checked_index);
                    } else if prev_commit_index + step > current_checked_index {
                        // no valid indices left
                        return Some(prev_commit_index);
                    } else {
                        current_checked_index = current_checked_index - step;
                        step = current_checked_index - prev_commit_index;
                    }
                }
            }
        } else {
            Some(prev_commit_index)
        }
    }

    fn follower_status_kind(&self, follower_id: ServerId) -> FollowerStatusKind {
        self.followers
            .get(&follower_id)
            .map(|info| info.status)
            .into()
    }
    /*
     *    // Sets the next log entry index of the follower or a catching up peer.
     *    fn set_next_index(&mut self, follower: ServerId, index: LogIndex) -> Result<(), Error> {
     *        if let Some(stored_index) = self.next_index.get_mut(&follower) {
     *            trace!(id = %follower, index = %index, "moved next index for follower");
     *            *stored_index = index;
     *            return Ok(());
     *        }
     *
     *        // Node is not in follower list, check node's catching up state
     *        if let Some(config_change) = &mut self.config_change {
     *            match config_change {
     *                ConfigChange {
     *                    peer,
     *                    stage:
     *                        ConfigChangeStage::Snapshotting {
     *                            index: cf_index, ..
     *                        },
     *                    ..
     *                } => {
     *                    // setting index while node is snapshotting means node
     *                    // finished a snapshot and can migrate to catching up
     *                    if peer.id == follower {
     *                        if cf_index == &index {
     *                            config_change.transition_to_catching_up()?;
     *                            return Ok(());
     *                        } else {
     *                            trace!(id = %follower, index = %index, "moved next index for catching up node");
     *                            *cf_index = index;
     *                            return Ok(());
     *                        }
     *                    } else {
     *                        return Err(Error::unreachable(debug_where!()));
     *                    }
     *                }
     *                ConfigChange {
     *                    peer,
     *                    stage:
     *                        ConfigChangeStage::CatchingUp {
     *                            index: cf_index, ..
     *                        },
     *                    ..
     *                } => {
     *                    if peer.id == follower {
     *                        *cf_index = index;
     *                        return Ok(());
     *                    }
     *                }
     *
     *                ConfigChange {
     *                    peer,
     *                    stage: ConfigChangeStage::Committing(_, cf_index),
     *                    ..
     *                } => {
     *                    if peer.id == follower {
     *                        *cf_index = index
     *                        //return Err(Error::unreachable(debug_where!()));
     *                    }
     *                }
     *            }
     *        }
     *
     *        Err(Error::UnknownPeer(follower))
     *    }
     *
     */

    fn set_own_match_index(&mut self, own_id: ServerId, index: LogIndex) -> Result<(), Error> {
        if let Some(info) = self.followers.get_mut(&own_id) {
            if let FollowerStatus::Voter { match_index } = info.status {
                match_index = match_index;
                Ok(())
            } else {
                Err(Error::unreachable(debug_where!()))
            }
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }

    ///// Sets the index of the highest log entry known to be replicated on the
    ///// follower.
    //fn set_match_index(&mut self, node: ServerId, index: LogIndex) {
    //if let Some(stored_index) = self.match_index.get_mut(&node) {
    //trace!(id = %node, index = %index, "moved match index for node");
    //*stored_index = index;
    //} else {
    //// match indexes are used in log commitment counting,
    //// so only followers should participate there
    //trace!("not setting up match index for catching up node");
    //}
    //}

    fn reset_config_change<H: Handler>(&mut self, handler: &mut H) {
        todo!();
        /*
         *match self.config_change.take() {
         *    Some(ConfigChange {
         *        peer,
         *        stage: ConfigChangeStage::Snapshotting { .. },
         *        ..
         *    })
         *    | Some(ConfigChange {
         *        peer,
         *        stage: ConfigChangeStage::CatchingUp { .. },
         *        ..
         *    }) => {
         *        debug!(
         *            "config change has been canceled for catching-up node {}",
         *            peer.id
         *        );
         *        handler.clear_timeout(Timeout::Heartbeat(peer.id));
         *    }
         *    Some(ConfigChange {
         *        stage: ConfigChangeStage::Committing { .. },
         *        ..
         *    }) => {}
         *    None => {}
         *}
         */
    }

    fn have_pending_snapshots(&self) -> bool {
        self.followers
            .values()
            .any(|info| info.has_pending_snapshot)
    }

    fn set_pending_snapshot(&mut self, id: &ServerId) -> Result<(), Error> {
        if let Some(info) = self.followers.get_mut(id) {
            info.has_pending_snapshot = true;
            Ok(())
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }

    fn clear_pending_snapshot(&mut self, id: &ServerId) -> Result<(), Error> {
        if let Some(info) = self.followers.get_mut(id) {
            info.has_pending_snapshot = false;
            Ok(())
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }

    fn mark_alive(&mut self, id: ServerId) {
        if let Some(info) = self.followers.get_mut(&id) {
            info.response_this_timeout = true;
        }
        // we don't care if we received a message from some unaccounted peer
    }

    fn forget_alive(&mut self, id: ServerId) {
        if let Some(info) = self.followers.get_mut(&id) {
            info.response_this_timeout = false;
        }
    }

    fn iter_alive<'a>(&'a self) -> impl Iterator<Item = ServerId> + 'a {
        self.followers
            .iter()
            .filter(|(id, info)| info.response_this_timeout)
            .map(|(id, info)| *id)
    }

    fn decrease_timeout(&mut self, id: ServerId) {
        let mut remove = false;
        if let Some(info) = self.followers.get_mut(&id) {
            if info.status.decrease_timeout() {
                // peer was lost
                remove = info.become_lost();
            }
        }
        if remove {
            trace!(peer = %id, "catching up peer has timed out, removing");
            self.followers.remove(&id);
        }
    }

    fn farthest_alive_voter<'a>(&self, leader_id: ServerId) -> Option<ServerId> {
        let mut last_follower = None;
        let mut max_index = LogIndex(0);
        for (id, info) in &self.followers {
            if info.response_this_timeout && *id != leader_id {
                if let FollowerStatus::Voter { match_index } = info.status {
                    if match_index > max_index {
                        last_follower = Some(*id);
                        max_index = match_index;
                    }
                }
            }
        }
        last_follower
    }

    fn alive_voters_count(&self) -> usize {
        self.followers
            .values()
            .filter(|info| {
                if let FollowerStatus::Voter { .. } = info.status {
                    info.response_this_timeout
                } else {
                    false
                }
            })
            .count()
    }

    fn update_catch_up_status(&mut self, id: ServerId) -> Result<CatchUpStatus, Error> {
        if let Some(info) = self.followers.get_mut(&id) {
            match info.status {
                FollowerStatus::Voter { .. } | FollowerStatus::NonVoter => {
                    Err(Error::unreachable(debug_where!()))
                }
                FollowerStatus::CatchingUpSnapshot { timeouts_left } => todo!(),
                FollowerStatus::CatchingUpEntries {
                    rounds_left,
                    timeouts_left,
                } => todo!(),
            }
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }

    /*
     *    fn update_rounds_old(&mut self) -> Result<CatchUpStatus, Error> {
     *        match &mut self.config_change {
     *            Some(ConfigChange {
     *                stage:
     *                    ConfigChangeStage::Snapshotting {
     *                        response_this_timeout,
     *                        timeouts,
     *                        ..
     *                    },
     *                max_snapshot_timeouts,
     *                ..
     *            }) => {
     *                *response_this_timeout = true;
     *                *timeouts = *max_snapshot_timeouts;
     *
     *                // node receiving snapshots is never ready for the config change, because it needs
     *                // a log entries
     *                Ok(CatchUpStatus::NotYet)
     *            }
     *
     *            Some(ConfigChange {
     *                stage:
     *                    ConfigChangeStage::CatchingUp {
     *                        rounds,
     *                        response_this_timeout,
     *                        timeouts,
     *                        ..
     *                    },
     *                max_log_timeouts,
     *                ..
     *            }) => {
     *                if *rounds == 0 {
     *                    Ok(CatchUpStatus::TooSlow)
     *                } else {
     *                    *rounds -= 1;
     *                    if *response_this_timeout {
     *                        // node has caught up because we already had response within this timeout
     *                        Ok(CatchUpStatus::CaughtUp)
     *                    } else {
     *                        *response_this_timeout = true;
     *                        *timeouts = *max_log_timeouts;
     *                        Ok(CatchUpStatus::NotYet)
     *                    }
     *                }
     *            }
     *            Some(_) => Err(Error::unreachable(debug_where!())),
     *            None => {
     *                //panic!("IMPLEMENTATION BUG: update_rounds called during wrong stage")
     *                Err(Error::unreachable(debug_where!()))
     *            }
     *        }
     *    }
     */

    /// Migrates configuration change state to begin committing a modification of config after
    /// cathching up node was ready
    /// Checks previous config for existing of peer being processed, adds peer if it didn't exist
    /// Removes peer if it existed in previous config
    fn begin_config_change(
        &mut self,
        config_index: LogIndex,
        current_config: &mut ConsensusConfig,
    ) -> Result<AdminId, Error> {
        if let Some(ConfigChange {
            peer,
            stage,
            admin_id,
            ..
        }) = &mut self.config_change
        {
            if current_config.add_or_remove_peer(peer.clone())? {
                // peer did not exist - added
            } else {
                // peer existed - this cannot happen because the function is only called
                // when catching up node being added is caught up
                return Err(Error::Critical(CriticalError::Unreachable(debug_where!())));
            }
            *stage = ConfigChangeStage::Committing(current_config.clone(), config_index);
            Ok(admin_id.clone())
        } else {
            Err(Error::Critical(CriticalError::Unreachable(debug_where!())))
        }
    }

    // Logic for expiring timeouts on all stages
    fn catching_up_timeout(&mut self) -> Result<bool, Error> {
        match &mut self.config_change {
            Some(ConfigChange {
                stage:
                    ConfigChangeStage::Snapshotting {
                        response_this_timeout,
                        timeouts,
                        ..
                    },
                total_timeouts_left: total_timeouts,
                ..
            }) => {
                *response_this_timeout = false;
                if *total_timeouts > 0 {
                    *total_timeouts -= 1;
                } else {
                    return Ok(false);
                }

                if *timeouts > 0 {
                    *timeouts -= 1;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Some(ConfigChange {
                stage:
                    ConfigChangeStage::CatchingUp {
                        response_this_timeout,
                        timeouts,
                        ..
                    },
                total_timeouts_left: total_timeouts,
                ..
            }) => {
                *response_this_timeout = false;
                if *total_timeouts > 0 {
                    *total_timeouts -= 1;
                } else {
                    return Ok(false);
                }

                if *timeouts > 0 {
                    *timeouts -= 1;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            _ => Err(Error::Critical(CriticalError::Unreachable(debug_where!()))),
        }
    }

    /*
     * // Counts the number of followers containing the given log index.
     *fn count_match_indexes(&self, index: LogIndex) -> usize {
     *    // +1 is for self
     *    //self.match_index.values().filter(|&&i| i >= index).count() + 1
     *    self.match_index.values().filter(|&&i| i >= index).count()
     *}
     */
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FollowerInfo {
    next_index: LogIndex,
    response_this_timeout: bool,
    has_pending_snapshot: bool,
    status: FollowerStatus,
}

impl FollowerInfo {
    fn new_saved_voter(next_index: LogIndex) -> Self {
        Self {
            next_index,
            response_this_timeout: true,
            has_pending_snapshot: false,
            status: FollowerStatus::new_saved_voter(),
        }
    }

    fn new_candidate_voter() -> Self {
        Self {
            next_index: LogIndex(0),
            response_this_timeout: true,
            has_pending_snapshot: false,
            status: FollowerStatus::new_candidate_voter(),
        }
    }

    fn new_non_voter() -> Self {
        Self {
            next_index: LogIndex(0),
            response_this_timeout: true,
            has_pending_snapshot: false,
            status: FollowerStatus::new_non_voter(),
        }
    }

    fn become_lost(&mut self) -> bool {
        let remove;
        (self.status, remove) = match self.status {
            FollowerStatus::Voter { .. } => (FollowerStatus::LostVoter, false),
            FollowerStatus::NonVoter { .. } => (FollowerStatus::LostVoter, false),
            FollowerStatus::CatchingUpEntries { .. }
            | FollowerStatus::CatchingUpSnapshot { .. } => (self.status.clone(), false),
            // you can never become more lost than now
            FollowerStatus::LostVoter => (FollowerStatus::LostVoter, false),
            FollowerStatus::LostNonVoter => (FollowerStatus::LostNonVoter, false),
        };
        remove
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FollowerStatus {
    // A valid voter allowed to vote
    Voter {
        match_index: LogIndex,
    },
    // A future voter at the stage of receiving
    // an initial snapshot
    CatchingUpSnapshot {
        total_timeouts_left: u32,
        timeouts_left: u32,
    },
    // A future voter at the stage of capturing the entries
    // after the snapshot is installed
    CatchingUpEntries {
        rounds_left: u32,
        timeouts_left: u32,
        total_timeouts_left: u32,
    },
    // A valid follower, which is not expected to vote, meaning the leader does not care
    // about its timeouts or caught-up status
    NonVoter,
    LostVoter,
    LostNonVoter,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FollowerStatusKind {
    Voter,
    CatchingUp,
    NonVoter,
    Unknown,
    LostVoter,
    LostNonVoter,
}

impl From<Option<FollowerStatus>> for FollowerStatusKind {
    fn from(status: Option<FollowerStatus>) -> Self {
        match status {
            Some(FollowerStatus::Voter { .. }) => FollowerStatusKind::Voter,
            Some(FollowerStatus::CatchingUpSnapshot { .. }) => FollowerStatusKind::CatchingUp,
            Some(FollowerStatus::CatchingUpEntries { .. }) => FollowerStatusKind::CatchingUp,
            Some(FollowerStatus::NonVoter) => FollowerStatusKind::NonVoter,
            Some(FollowerStatus::LostVoter) => FollowerStatusKind::LostVoter,
            Some(FollowerStatus::LostNonVoter) => FollowerStatusKind::LostNonVoter,
            None => FollowerStatusKind::Unknown,
        }
    }
}

impl FollowerStatus {
    fn new_saved_voter() -> Self {
        FollowerStatus::Voter {
            match_index: LogIndex(0),
        }
    }

    fn new_candidate_voter() -> Self {
        FollowerStatus::CatchingUpSnapshot {
            total_timeouts_left: MAX_TOTAL_TIMEOUTS,
            timeouts_left: MAX_SNAPSHOT_TIMEOUTS,
        }
    }

    fn new_non_voter() -> Self {
        FollowerStatus::NonVoter
    }

    fn update_rounds(&mut self) -> Result<(), Error> {
        if let FollowerStatus::CatchingUpEntries { rounds_left, .. } = self {
            todo!()
            //if *rounds_left > 0 {
            //}
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }

    // Counts a number of election timeouts left for the follower
    // and returns true if follower has exceeded this number, which
    // will mean it was lost
    fn decrease_timeout(&mut self) -> bool {
        match self {
            FollowerStatus::CatchingUpSnapshot {
                total_timeouts_left,
                timeouts_left,
            } => {
                if *total_timeouts_left > 0 && *timeouts_left > 0 {
                    *total_timeouts_left -= 1;
                    *timeouts_left -= 1;
                    false
                } else {
                    true
                }
            }
            FollowerStatus::CatchingUpEntries {
                total_timeouts_left,
                timeouts_left,
                ..
            } => {
                if *total_timeouts_left > 0 && *timeouts_left > 0 {
                    *total_timeouts_left -= 1;
                    *timeouts_left -= 1;
                    false
                } else {
                    true
                }
            }
            _ => false,
        }
    }

    fn to_catching_up(self) -> Result<Self, Error> {
        if let FollowerStatus::CatchingUpSnapshot {
            total_timeouts_left,
            timeouts_left,
        } = self
        {
            Ok(FollowerStatus::CatchingUpEntries {
                total_timeouts_left,
                rounds_left: MAX_ROUNDS,
                timeouts_left: MAX_ENTRIES_TIMEOUTS,
            })
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }

    fn to_voter(self) -> Result<Self, Error> {
        if let FollowerStatus::CatchingUpEntries { .. } = self {
            Ok(FollowerStatus::new_saved_voter())
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }
}

pub(crate) enum CatchUpStatus {
    TooSlow,
    NotYet(Option<FollowerStatus>),
    CaughtUp,
}

/*
 *#[derive(Clone, Debug, PartialEq, Eq)]
 *pub(crate) enum ConfigChangeStage {
 *    Snapshotting {
 *        index: LogIndex,
 *        timeouts: u32,
 *        response_this_timeout: bool,
 *    },
 *    CatchingUp {
 *        index: LogIndex,
 *        rounds: u32,
 *        timeouts: u32,
 *        response_this_timeout: bool, // flag to measure round time
 *    },
 *    Committing(ConsensusConfig, LogIndex), // previous config
 *}
 *
 * // TODO: we could adjust the amount of timeouts automatically, based on measurements
 * // done for snapshotting active followers
 *#[derive(Clone, Debug, PartialEq, Eq)]
 *pub(crate) struct ConfigChange {
 *    pub(crate) peer: Peer,
 *    pub(crate) stage: ConfigChangeStage,
 *    admin_id: AdminId,
 *    total_timeouts_left: u32,
 *
 *    // TODO: these are kinda options, we must move them somewhere
 *    // into non-replicated consensus config
 *    max_snapshot_timeouts: u32,
 *    max_log_timeouts: u32,
 *}
 *
 *impl ConfigChange {
 *    pub(crate) fn new(
 *        new_peer: ServerId,
 *        admin_id: AdminId,
 *        index: LogIndex,
 *        metadata: Vec<u8>,
 *        timeouts: SlowNodeTimeouts,
 *    ) -> Self {
 *        let SlowNodeTimeouts {
 *            max_snapshot_timeouts,
 *            max_log_timeouts,
 *            max_total_timeouts,
 *        } = timeouts;
 *        Self {
 *            peer: Peer {
 *                id: new_peer,
 *                metadata,
 *            },
 *            stage: ConfigChangeStage::Snapshotting {
 *                index,
 *                timeouts: max_snapshot_timeouts,
 *                response_this_timeout: false,
 *            },
 *            admin_id,
 *            total_timeouts_left: max_total_timeouts,
 *
 *            max_log_timeouts,
 *            max_snapshot_timeouts,
 *        }
 *    }
 *
 *    fn transition_to_catching_up(&mut self) -> Result<(), Error> {
 *        let new_stage = match self.stage {
 *            ConfigChangeStage::CatchingUp { .. } => {
 *                return Ok(());
 *            }
 *            ConfigChangeStage::Snapshotting {
 *                index,
 *                response_this_timeout,
 *                ..
 *            } => ConfigChangeStage::CatchingUp {
 *                index,
 *                rounds: MAX_ROUNDS,
 *                timeouts: self.max_log_timeouts,
 *                response_this_timeout,
 *            },
 *            _ => return Err(Error::unreachable(debug_where!())),
 *        };
 *        self.stage = new_stage;
 *        Ok(())
 *    }
 *
 *    fn transition_to_snapshotting(&mut self, index: LogIndex) -> Result<(), Error> {
 *        let new_stage = match self.stage {
 *            ConfigChangeStage::Snapshotting { .. } => {
 *                return Ok(());
 *            }
 *            ConfigChangeStage::CatchingUp {
 *                response_this_timeout,
 *                ..
 *            } => ConfigChangeStage::Snapshotting {
 *                index,
 *                timeouts: self.max_snapshot_timeouts,
 *                response_this_timeout,
 *            },
 *            _ => return Err(Error::unreachable(debug_where!())),
 *        };
 *        self.stage = new_stage;
 *        Ok(())
 *    }
 *}
 */
