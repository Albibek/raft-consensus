use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::*;
use crate::state_machine::SnapshotInfo;
use log::{debug, info, trace};

use crate::config::{ConsensusConfig, SlowNodeTimeouts};
use crate::handler::Handler;
use crate::message::*;
use crate::persistent_log::{LogEntry, LogEntryData, LogEntryDataRef, LogEntryRef};
use crate::raft::CurrentState;
use crate::state::State;
use crate::state_impl::StateImpl;
use crate::{AdminId, ClientId, LogIndex, Peer, ServerId, Term};

use crate::follower::Follower;
use crate::persistent_log::Log;
use crate::state_machine::StateMachine;

static MAX_ROUNDS: u32 = 10;

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
        response: &AppendEntriesResponse,
    ) -> Result<(Option<PeerMessage>, CurrentState<L, M, H>), Error> {
        let current_term = self.current_term()?;
        let local_latest_log_index = self.latest_log_index()?;

        // First, check if the response is from the correct term
        // and is not some error
        // We are ok to respond to some peers not in config in case they were there
        // in previous terms
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
                Ok((None, new_state.into()))
            }

            AppendEntriesResponse::Success(term, _)
            | AppendEntriesResponse::StaleTerm(term)
            | AppendEntriesResponse::InconsistentPrevEntry(term, _)
                if &current_term > term =>
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
            AppendEntriesResponse::InconsistentPrevEntry(_, next_index) => {
                self.state_data.set_next_index(from, *next_index)?;
                let message = self.next_entries_or_snapshot(
                    handler,
                    from,
                    current_term,
                    local_latest_log_index,
                )?;
                Ok((message, self.into()))
            }
            AppendEntriesResponse::Success(_, follower_latest_log_index) => {
                // peer successfully received last AppendEntries, consider it alive and running
                // within current election timeout
                self.state_data.peers_alive.insert(from);
                trace!(
                    "follower OK, checking next entry: {} {}",
                    *follower_latest_log_index,
                    local_latest_log_index
                );
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
                    let step_down_required = self.try_advance_commit_index(handler)?;
                    if step_down_required {
                        // become follower that will instantly become candidate on timeout
                        let new_state = self.leader_into_follower(handler, current_term)?;
                        Ok((None, new_state.timeout_now(handler)?))
                    } else {
                        let message = self.next_entries_or_snapshot(
                            handler,
                            from,
                            current_term,
                            local_latest_log_index,
                        )?;
                        Ok((message, self.into()))
                    }
                } else if self.state_data.is_catching_up(from) {
                    // For the catching up node, receiving a number of entries means it
                    // has made some progress,
                    let message = self.check_catch_up_status(
                        handler,
                        from,
                        current_term,
                        local_latest_log_index,
                    )?;
                    Ok((message, self.into()))
                } else {
                    Err(Error::UnknownPeer(from.clone()))
                }
            }
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

    fn timeout_now(self, _handler: &mut H) -> Result<CurrentState<L, M, H>, Error> {
        info!("TimeoutNow on leader ignored");
        Ok(self.into())
    }

    fn install_snapshot_request(
        self,
        handler: &mut H,
        from: ServerId,
        request: &InstallSnapshotRequest,
    ) -> Result<(PeerMessage, CurrentState<L, M, H>), Error> {
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
        response: &InstallSnapshotResponse,
    ) -> Result<(Option<PeerMessage>, CurrentState<L, M, H>), Error> {
        let current_term = self.current_term()?;
        let local_latest_log_index = self.latest_log_index()?;

        // we receive this when some node had received InstallSnapshotRequest
        match response {
            InstallSnapshotResponse::Success(term, _, _)
            | InstallSnapshotResponse::StaleTerm(term)
                if &current_term < term =>
            {
                // some node has received message with term higher than ours,
                // that means some other leader appeared in consensus,
                // we should downgrade to follower immediately
                let new_state = self.leader_into_follower(handler, current_term)?;
                Ok((None, new_state.into()))
            }

            InstallSnapshotResponse::Success(term, _, _)
            | InstallSnapshotResponse::StaleTerm(term)
                if &current_term > term =>
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
                if !self.state_data.has_follower(&from) && !self.state_data.is_catching_up(from) {
                    return Err(Error::UnknownPeer(from));
                }

                match self.new_install_snapshot(next_chunk_request.as_ref().map(|v| v.as_slice())) {
                    Err(Error::SnapshotExpected) => {
                        // snapshot may stop existing in very rare, mostly unreachable cases like fast
                        // restarts

                        // we can only delay sending by retrying a heartbeat message to the
                        // follower, so the next response could trigger this same logic again
                        todo!("do this only for non-catching up followers");
                        handler.set_timeout(Timeout::Heartbeat(from));
                        Ok((None, self.into()))
                    }
                    Ok(Some((message, _info))) => {
                        // this behaviour is same for catching-up and follower nodes
                        if self.state_data.is_catching_up(from) {
                            self.state_data.update_rounds()?;
                        }
                        Ok((
                            Some(PeerMessage::InstallSnapshotRequest(message)),
                            self.into(),
                        ))
                    }
                    Ok(None) => {
                        // the sending of snapshot has finished, which means
                        // that log entries can be sent now
                        // so we clean the snapshot status and set the follower's index,
                        // which should be after the snapshot
                        //
                        // the next_entries_or_snapshot will now be able to correctly
                        // determine, if another snapshot or AppendEntriesRequest should be sent
                        self.state_data.pending_snapshots.remove(&from);

                        // This will update data for the state on both follower types
                        self.state_data
                            .set_next_index(from, *follower_snapshot_index + 1)?;
                        if self.state_data.has_follower(&from) {
                            // Snapshots are only taken using commit index,
                            // so advancing an index on a single follower cannot change
                            // majority and advance index on the whole consensus.
                            // This means we don't need to call try_advance_commit_index,
                            // but we need to save the follower status to the state
                            self.state_data
                                .set_match_index(from, *follower_snapshot_index);

                            Ok((
                                self.next_entries_or_snapshot(
                                    handler,
                                    from,
                                    current_term,
                                    local_latest_log_index,
                                )?,
                                self.into(),
                            ))
                        } else if self.state_data.is_catching_up(from) {
                            // this function includes checking the snapshot state
                            let message = self.check_catch_up_status(
                                handler,
                                from,
                                current_term,
                                local_latest_log_index,
                            )?;
                            Ok((message, self.into()))
                        } else {
                            Err(Error::unreachable(module_path!()))
                        }
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    // Timeout handling
    fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error> {
        debug!("heartbeat timeout for peer: {}", peer);
        self.state_data.peers_alive.remove(&peer);
        Ok(AppendEntriesRequest {
            term: self.current_term()?,
            prev_log_index: self.latest_log_index()?,
            prev_log_term: self.latest_log_term()?,
            leader_commit: self.commit_index,
            entries: Vec::new(),
        })
    }

    fn election_timeout(mut self, handler: &mut H) -> Result<CurrentState<L, M, H>, Error> {
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
        if self.state_data.peers_alive.len() < self.config.majority(self.id) {
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
        // or catching up nodes.  In that case we prefer not to taks snapshot if it is
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
        if !force && !self.state_data.pending_snapshots.is_empty() {
            trace!("not taking snapshot because leader has snapshots pending and snapshot was not forced");
            return Ok(false);
        }

        // without pending snapshots there is not problem: just do it regardless of forcing
        if !self.state_data.pending_snapshots.is_empty() {
            return self.common_check_compaction(force);
        }

        // the last case: force and pending_snapshots
        let snapshot_changed = self.common_check_compaction(force)?;

        Ok(snapshot_changed)
    }

    /// Applies a client proposal to the consensus state machine.
    fn client_proposal_request(
        &mut self,
        handler: &mut H,
        from: ClientId,
        request: &ClientRequest,
    ) -> Result<ClientResponse, Error> {
        // append an entry to the log
        let (log_index, message) =
            self.add_new_entry(LogEntryDataRef::Proposal(request.data.as_slice()))?;

        debug!("proposal request from client {}: entry {}", from, log_index);
        self.state_data.proposals.push_back((from, log_index));
        self.send_append_entries_request(handler, log_index, message)?;

        Ok(ClientResponse::Queued)
    }

    fn client_query_request(
        &mut self,
        from: ClientId,
        request: &ClientRequest,
    ) -> Result<ClientResponse, Error> {
        // TODO: linerability for clients
        trace!("query from client {}", from);
        let result = self
            .state_machine
            .query(request.data.as_slice())
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
        request: &AddServerRequest,
    ) -> Result<ConfigurationChangeResponse, Error> {
        // check if first empty entry has been committed by consensus, do not allow config change until it has
        let latest_log_index = self.latest_log_index()?;
        let latest_log_term = self
            .with_log(|log| log.term_of(latest_log_index))?
            .ok_or(Error::log_broken(latest_log_index, module_path!()))?;
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
            } else {
                // local node has no snapshot yet
                LogIndex(0)
            };

            self.state_data.config_change = Some(ConfigChange::new(
                request.id,
                snapshot_index,
                request.info.clone(),
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
            // ie. the one which log is ths closest to the leader
            let mut sid = self.id;
            let mut max_index = LogIndex(0);
            for (id, index) in &self.state_data.match_index {
                if *index > max_index {
                    sid = *id;
                    max_index = *index;
                }
            }
            if sid == self.id {
                Err(Error::unreachable(module_path!()))
            } else {
                handler.send_peer_message(sid, PeerMessage::TimeoutNow);
                Ok(ConfigurationChangeResponse::Started)
            }
        }
    }

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error> {
        // According to Raft 4.1 last paragraph, servers should receive any RPC call
        // from any server, because they may be a new ones which current server doesn't know
        // about yet

        let new_peer = !self.config.has_peer(peer) && !self.state_data.is_catching_up(peer);
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
            prev_log_term: self.latest_log_term()?,
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
        mut self,
        handler: &mut H,
        leader_term: Term,
    ) -> Result<State<L, M, H, Follower>, Error> {
        self.state_data.reset_config_change(handler);
        self.into_follower(handler, ConsensusState::Leader, leader_term)
    }

    // while advancing index, the step down may be required, if one of the committed entries
    // contains the configuration change, which removes the current node
    // the return value is the flag showing if stepping down have to happen
    fn try_advance_commit_index(&mut self, handler: &mut H) -> Result<bool, Error> {
        let majority = self.config.majority(self.id);
        // Here we try to move commit index to one that the majority of peers in cluster already have
        let latest_log_index = self.latest_log_index()?;

        while self.commit_index < latest_log_index {
            if self.state_data.count_match_indexes(self.commit_index + 1) >= majority {
                self.commit_index = self.commit_index + 1;
                trace!("commit index advanced to {}", self.commit_index);
            } else {
                trace!("commit index not advanced: {}", self.commit_index);
                return Ok(false); // If there isn't a majority now, there will non be one until next message
            }
        }

        let mut step_down_required = false;
        // Being here means the commit index has advanced and we can confirm clients
        // that their proposals are committed
        while self.last_applied < self.commit_index {
            let entry_index = self.last_applied + 1;
            // so we read all committed entries and check if any of them have to be
            // replied to clients
            let mut entry = LogEntry::default();
            self.log
                .read_entry(entry_index, &mut entry)
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;

            match entry.data {
                LogEntryData::Empty => {}
                LogEntryData::Proposal(data) => {
                    let result = self
                        .state_machine
                        .apply(&data, true)
                        .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

                    // As long as we know it, we've sent the connected clients the notification
                    // about their proposals being queued
                    //
                    // Client proposals may come not in the same order clients expect. For example, via parallel
                    // TCP connections. TODO: with linerability it will be possible to process
                    // requests correctly

                    // the queued entries have to exist in the queue to be committed,
                    // so popping a wrong index means a bug
                    if let Some(&(client, index)) = self.state_data.proposals.get(0) {
                        trace!("responding to client {} for entry {}", client, index);

                        handler.send_client_message(
                            client,
                            ClientMessage::ClientProposalResponse(ClientResponse::Success(result)),
                        );
                        self.state_data.proposals.pop_front();
                    } else {
                        return Err(Error::unreachable(module_path!()));
                    }
                }
                LogEntryData::Config(config) => {
                    // we only saved configuration to log and memory, but has not persisted it yet
                    // this is what we have to do now
                    self.log
                        .set_latest_config(&config, entry_index)
                        .map_err(|e| {
                            Error::Critical(CriticalError::PersistentLogWrite(Box::new(e)))
                        })?;

                    if !config.peers.iter().any(|peer| peer.id == self.id) {
                        step_down_required = true;
                    } else {
                        // when applying multiple configuration entries (shoult not happen actually, but just in
                        // case), reset the step down requirements it it was canceled by the
                        // following entry
                        step_down_required = false;
                    }
                }
            }
            self.last_applied = entry_index;
        }

        Ok(step_down_required)
    }

    fn next_entries_or_snapshot(
        &mut self,
        handler: &mut H,
        from: ServerId,
        current_term: Term,
        local_latest_log_index: LogIndex,
    ) -> Result<Option<PeerMessage>, Error> {
        let follower_last_index = if let Some(index) = self.state_data.match_index.get(&from) {
            *index
        } else {
            return Err(Error::unreachable(module_path!()));
        };

        if follower_last_index < local_latest_log_index {
            let first_log_index = self
                .log
                .first_log_index()
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
            if follower_last_index < first_log_index {
                trace!(
                    "peer {} is behind leader and requires snapshot because it is out of log index range {} < {}",
                    from,
                    local_latest_log_index, follower_last_index
                );
                if self.state_data.pending_snapshots.contains(&from) {
                    // We were already sending snapshot to follower, but received ApendEntries from
                    // it with lower index than we have.
                    // Possible reasons and solutions:
                    // 1. AppendEntriesResponse was delayed and we already sending a snapshot to
                    //    this follower
                    // 2. Follower received the last chunk, but our log went too far from the last snapshot
                    // we've sent and we already took a new snapshot
                    //
                    // In both these cases it is ok to sent a first chunk of the current snapshot.
                    // For case (1) this can be handled by the state machine metadata mechanism
                    // For case (2) it is just natural and the snapshot is really required

                    // So, if no case is missed in the reasoning above, it's always the same action
                    // regardless of the follower state
                }
                self.state_data.pending_snapshots.insert(from);
                todo!("sending snapshot to catching up node");
                match self.new_install_snapshot(None) {
                    Ok(Some((message, _info))) => {
                        Ok(Some(PeerMessage::InstallSnapshotRequest(message)))
                    }
                    Ok(None) | Err(Error::SnapshotExpected) => {
                        debug!("no snapshot at leader while follower is behind, delaying InstallSnapshot for {}", from);
                        // this should be some kind of impossible or a very rare situation: follower is
                        // already behind, but the leader's snapshot is still not ready. Would probably
                        // mean the leader is slow at creating the snapshot. All we can do here is to
                        // let a follower wait for leader to recover
                        //
                        // we will retry  to send a snapshot to the follower after a heartbeat timeout
                        // we need the heartbeat message
                        //
                        todo!("no timewout for catchin up nodes, fail catch up instead");
                        handler.set_timeout(Timeout::Heartbeat(from));
                        Ok(None)
                    }
                    Err(e) => return Err(e),
                }
            } else {
                // The peer is in log index range, but behind: send it entries to catch up.
                trace!(
                    "peer {} is missing at least {} log entries, sending them",
                    from,
                    local_latest_log_index - follower_last_index
                );
                let prev_log_index = follower_last_index - 1;
                let prev_log_term = if prev_log_index == LogIndex(0) {
                    Term(0)
                } else {
                    // non-existence of index is definitely a bug here
                    self.with_log(|log| log.term_of(prev_log_index))?
                        .ok_or(Error::log_broken(prev_log_index, module_path!()))?
                };

                let mut message = AppendEntriesRequest {
                    term: current_term,
                    prev_log_index,
                    prev_log_term,
                    leader_commit: self.commit_index,
                    entries: Vec::new(),
                };

                self.fill_entries(
                    &mut message.entries,
                    follower_last_index,
                    local_latest_log_index + 1,
                )?;

                self.state_data
                    .set_next_index(from, local_latest_log_index + 1)?;

                Ok(Some(PeerMessage::AppendEntriesRequest(message)))
            }
        } else {
            // peer is in sync with leader, only catching up peers require action
            if self.state_data.is_catching_up(from) {
                return Err(Error::unreachable(module_path!()));
            }

            // since the peer has caught up, set a heartbeat timeout.
            handler.set_timeout(Timeout::Heartbeat(from));
            Ok(None)
        }
    }

    // read a chunk from state machine and put it into InstallSnapshot packet
    // if snapshot is not required or cannot be made, None is returned
    fn new_install_snapshot(
        &self,
        chunk_request: Option<&[u8]>,
    ) -> Result<Option<(InstallSnapshotRequest, SnapshotInfo)>, Error> {
        let info = self
            .state_machine
            .snapshot_info()
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?
            .ok_or(Error::SnapshotExpected)?;

        let chunk_data = self
            .state_machine
            .read_snapshot_chunk(chunk_request)
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

        if let Some(chunk_data) = chunk_data {
            let term = self.current_term()?;
            let last_log_index = self.latest_log_index()?;
            let last_log_term = self
                .with_log(|log| log.term_of(last_log_index))?
                .ok_or(Error::log_broken(last_log_index, module_path!()))?;

            let last_config = if chunk_request.is_some() {
                Some(self.config.clone())
            } else {
                None
            };

            let message = InstallSnapshotRequest {
                term,
                last_log_index,
                last_log_term,
                leader_commit: self.commit_index,
                last_config,
                snapshot_index: info.index,
                chunk_data,
            };
            Ok(Some((message, info)))
        } else {
            Ok(None)
        }
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
        match self.state_data.update_rounds()? {
            CatchUpStatus::CaughtUp => {
                // switch to new stage
                self.state_data
                    .begin_config_change(local_latest_log_index + 1, &mut self.config)?;

                // TODO
                // too bad, we cannot borrow config from self at the same time with
                // running something on self.log
                // let's hope borrowing rules change someday (2021 edition promises it!)
                // until that time it's not very bad to clone a config
                let config = self.config.clone();

                // the config is already modified, just add it as an entry
                let (log_index, message) = self.add_new_entry(LogEntryDataRef::Config(&config))?;
                self.send_append_entries_request(handler, log_index, message)?;

                handler.set_timeout(Timeout::Heartbeat(from));
                Ok(None)
            }
            CatchUpStatus::NotYet => {
                trace!(
                    "catching up node did not catch this round, continuing the catching up process"
                );

                // We need to maintain correct states here. Possible cases:
                match self.next_entries_or_snapshot(
                    handler,
                    from,
                    current_term,
                    local_latest_log_index,
                )? {
                    m
                    @
                    Some(PeerMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                        snapshot_index,
                        ..
                    })) => {
                        // Node is going to receive InstallSnapshotRequest message. Not depending on which chunk or
                        // snapshot count it is, the state must migrate to snapshotting at snapshot
                        // index
                        if let Some(config_change) = &mut self.state_data.config_change {
                            config_change.transition_to_snapshotting(snapshot_index)?;
                            Ok(m)
                        } else {
                            Err(Error::unreachable(module_path!()))
                        }
                    }
                    m @ Some(PeerMessage::AppendEntriesRequest(_)) => {
                        // Node receives AppendEntriesRequest message. State must be CatchingUp.
                        if let Some(config_change) = &mut self.state_data.config_change {
                            config_change.transition_to_catching_up()?;
                            Ok(m)
                        } else {
                            Err(Error::unreachable(module_path!()))
                        }
                    }
                    _ => Err(Error::unreachable(module_path!())),
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

    // fetch required entries from the log and push them to provided vector
    fn fill_entries(
        &self,
        entries: &mut Vec<Entry>,
        from: LogIndex,
        until: LogIndex,
    ) -> Result<(), Error> {
        // as of now we push all the entries, regardless of amount
        // this may be kind of dangerous sometimes because of amount being too big
        // but most probably snapshotting would solve it
        for index in from.as_u64()..until.as_u64() {
            let mut log_entry = LogEntry::new_proposal(Term(0), Vec::new());
            self.log
                .read_entry(LogIndex(index), &mut log_entry)
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
            let mut entry: Entry = log_entry.into();
            if self
                .log
                .latest_config_index()
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?
                .map(|idx| idx.as_u64() == index)
                .unwrap_or(false)
            {
                entry.set_config_active(true);
            }

            entries.push(entry);
        }
        Ok(())
    }

    /// appends a single entry to a leader's log obeying all the Raft rules (log indexing etc)
    /// returns a message ready to send to followers
    pub(crate) fn add_new_entry<'a>(
        &mut self,
        entry_data: LogEntryDataRef<'a>,
    ) -> Result<(LogIndex, AppendEntriesRequest), Error> {
        let prev_log_index = self.latest_log_index()?;
        let prev_log_term = self.latest_log_term()?;
        let term = self.current_term()?;

        let log_index = prev_log_index + 1;
        let leader_commit = self.commit_index;

        //      self.with_log_mut(|log| log.append_entries(log_index, Some(&entry).into_iter()))?;
        let log_entry = LogEntryRef {
            term,
            data: entry_data,
        };
        self.log
            .append_entry(log_index, &log_entry)
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;

        let mut entry: Entry = log_entry.into();
        // this is the new addition, so if it's a config, than it is definitely a new active one
        entry.set_config_active(true);
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
        if !self.config.is_solitary(self.id) {
            // solitary consensus can just advance, no messages required
            // fan out the request to all followers that are catched up enough
            for peer in &self.config.peers {
                if peer.id != self.id {
                    let id = peer.id;
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

        if self.config.majority(self.id) == 1 {
            // for a solitary consensus or a 2-peer cluster current node aready has a majority,
            // (because of it's own log commit)
            // so there is a reason to advance the index in case the proposal queue is empty
            //
            // otherwise, there is no point in this because the majority is not achieved yet
            let step_down_required = self.try_advance_commit_index(handler)?;
            if step_down_required {
                return Err(Error::unreachable(module_path!()));
            }
        }
        Ok(())
    }
}

/// The state associated with a Raft consensus module in the `Leader` state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Leader {
    next_index: HashMap<ServerId, LogIndex>,
    match_index: HashMap<ServerId, LogIndex>,
    peers_alive: HashSet<ServerId>,
    pending_snapshots: HashSet<ServerId>,
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
            peers_alive: HashSet::new(),
            config_change: None,
            proposals: VecDeque::new(),
            pending_snapshots: HashSet::new(),
        }
    }

    fn has_follower(&mut self, follower: &ServerId) -> bool {
        self.match_index.contains_key(follower)
    }

    fn is_catching_up(&self, node: ServerId) -> bool {
        if let Some(catching_up) = &self.config_change {
            node == catching_up.peer.id
        } else {
            false
        }
    }

    /// Returns the next log entry index of the follower or a catching up peer.
    /// Used only in AppendEntries context
    fn next_index(&mut self, follower: &ServerId) -> Result<LogIndex, Error> {
        if let Some(index) = self.next_index.get(follower) {
            return Ok(*index);
        }

        match &self.config_change {
            Some(ConfigChange {
                peer,
                stage: ConfigChangeStage::CatchingUp { index, .. },
                ..
            }) => {
                if &peer.id == follower {
                    // the index is requested for a catching up peer
                    return Ok(*index);
                }
            }
            Some(_) => {
                return Err(Error::unreachable(module_path!()));
            }
            _ => (),
        }

        Err(Error::UnknownPeer(follower.clone()))
    }

    /// Sets the next log entry index of the follower or a catching up peer.
    fn set_next_index(&mut self, follower: ServerId, index: LogIndex) -> Result<(), Error> {
        if let Some(stored_index) = self.next_index.get_mut(&follower) {
            *stored_index = index;
            return Ok(());
        }

        match &mut self.config_change {
            Some(
                config_change
                @
                ConfigChange {
                    peer,
                    stage:
                        ConfigChangeStage::Snapshotting {
                            index: cf_index, ..
                        },
                    ..
                },
            ) => {
                // setting index while node is snapshotting means node
                // finished a snapshot and can migrate to catching up
                if peer.id == follower {
                    if cf_index == &index {
                        config_change.transition_to_catching_up()?;
                        return Ok(());
                    }
                }
            }
            Some(ConfigChange {
                peer,
                stage:
                    ConfigChangeStage::CatchingUp {
                        index: cf_index, ..
                    },
                ..
            }) => {
                if peer.id == follower {
                    *cf_index = index;
                    return Ok(());
                }
            }

            Some(ConfigChange { peer, .. }) => {
                if peer.id == follower {
                    return Err(Error::unreachable(module_path!()));
                }
            }
            _ => (),
        }

        Err(Error::UnknownPeer(follower))
    }

    /// Sets the index of the highest log entry known to be replicated on the
    /// follower.
    fn set_match_index(&mut self, follower: ServerId, index: LogIndex) {
        self.match_index.insert(follower, index);
    }

    fn reset_config_change<H: Handler>(&mut self, handler: &mut H) {
        match self.config_change.take() {
            Some(ConfigChange {
                peer,
                stage: ConfigChangeStage::Snapshotting { .. },
                ..
            })
            | Some(ConfigChange {
                peer,
                stage: ConfigChangeStage::CatchingUp { .. },
                ..
            }) => {
                debug!(
                    "config change has been canceled for catching-up node {}",
                    peer.id
                );
                handler.clear_timeout(Timeout::Heartbeat(peer.id));
            }
            Some(ConfigChange {
                stage: ConfigChangeStage::Committing { .. },
                ..
            }) => {}
            None => {}
        }
    }

    fn update_rounds(&mut self) -> Result<CatchUpStatus, Error> {
        match &mut self.config_change {
            Some(ConfigChange {
                stage:
                    ConfigChangeStage::Snapshotting {
                        response_this_timeout,
                        timeouts,
                        ..
                    },
                max_snapshot_timeouts,
                ..
            }) => {
                *response_this_timeout = true;
                *timeouts = *max_snapshot_timeouts;

                // node receiving snapshots is never ready for the config change, because it needs
                // a log entries
                Ok(CatchUpStatus::NotYet)
            }

            Some(ConfigChange {
                stage:
                    ConfigChangeStage::CatchingUp {
                        rounds,
                        response_this_timeout,
                        timeouts,
                        ..
                    },
                max_log_timeouts,
                ..
            }) => {
                if *rounds == 0 {
                    Ok(CatchUpStatus::TooSlow)
                } else {
                    *rounds -= 1;
                    if *response_this_timeout {
                        // node has caught up because we already had response within this timeout
                        Ok(CatchUpStatus::CaughtUp)
                    } else {
                        *response_this_timeout = true;
                        *timeouts = *max_log_timeouts;
                        Ok(CatchUpStatus::NotYet)
                    }
                }
            }
            Some(_) => Err(Error::unreachable(module_path!())),
            None => {
                //panic!("IMPLEMENTATION BUG: update_rounds called during wrong stage")
                Err(Error::unreachable(module_path!()))
            }
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
                total_timeouts,
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
                total_timeouts,
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
            _ => Err(Error::Critical(CriticalError::Unreachable(module_path!()))),
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConfigChangeStage {
    Snapshotting {
        index: LogIndex,
        timeouts: u32,
        response_this_timeout: bool,
    },
    CatchingUp {
        index: LogIndex,
        rounds: u32,
        timeouts: u32,
        response_this_timeout: bool, // flag to measure round time
    },
    Committing(ConsensusConfig, LogIndex), // previous config
}

// TODO: we could adjust the amount of timeouts automatically, based on measurements
// done for snapshotting active followers
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConfigChange {
    pub(crate) peer: Peer,
    pub(crate) stage: ConfigChangeStage,
    total_timeouts: u32,
    max_snapshot_timeouts: u32,
    max_log_timeouts: u32,
}

impl ConfigChange {
    pub(crate) fn new(
        id: ServerId,
        index: LogIndex,
        metadata: Vec<u8>,
        timeouts: SlowNodeTimeouts,
    ) -> Self {
        let SlowNodeTimeouts {
            max_snapshot_timeouts,
            max_log_timeouts,
            max_total_timeouts,
        } = timeouts;
        Self {
            peer: Peer { id, metadata },
            stage: ConfigChangeStage::Snapshotting {
                index,
                timeouts: max_snapshot_timeouts,
                response_this_timeout: false,
            },
            total_timeouts: max_total_timeouts,

            max_log_timeouts,
            max_snapshot_timeouts,
        }
    }

    fn transition_to_catching_up(&mut self) -> Result<(), Error> {
        let new_stage = match self.stage {
            ConfigChangeStage::CatchingUp { .. } => {
                return Ok(());
            }
            ConfigChangeStage::Snapshotting {
                index,
                response_this_timeout,
                ..
            } => ConfigChangeStage::CatchingUp {
                index,
                rounds: MAX_ROUNDS,
                timeouts: self.max_log_timeouts,
                response_this_timeout: false,
            },
            _ => return Err(Error::unreachable(module_path!())),
        };
        self.stage = new_stage;
        Ok(())
    }

    fn transition_to_snapshotting(&mut self, index: LogIndex) -> Result<(), Error> {
        let new_stage = match self.stage {
            ConfigChangeStage::Snapshotting { .. } => {
                return Ok(());
            }
            ConfigChangeStage::CatchingUp {
                response_this_timeout,
                ..
            } => ConfigChangeStage::Snapshotting {
                index,
                timeouts: self.max_snapshot_timeouts,
                response_this_timeout: false,
            },
            _ => return Err(Error::unreachable(module_path!())),
        };
        self.stage = new_stage;
        Ok(())
    }
}
