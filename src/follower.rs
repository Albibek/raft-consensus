use std::cmp;

use log::{debug, info, trace};

use crate::error::Error;
use crate::handler::ConsensusHandler;
use crate::message::*;
use crate::{ClientId, ConsensusConfig, Entry, EntryData, LogIndex, Peer, ServerId, Term};

use crate::persistent_log::Log;
use crate::state_machine::StateMachine;

use crate::consensus::State;
use crate::state::{ConsensusState, StateHandler};

impl<L, M, H> StateHandler<L, M, H> for State<L, M, FollowerState>
where
    L: Log,
    M: StateMachine,
    H: ConsensusHandler,
{
    fn append_entries_request(
        &mut self,
        handler: &mut H,
        from: ServerId,
        request: AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, Option<ConsensusState<L, M>>), Error> {
        let leader_term = request.term;
        let current_term = self.current_term()?;

        if leader_term < current_term {
            return Ok((AppendEntriesResponse::StaleTerm(current_term), None));
        }

        self.state.set_leader(from);

        // this function is shared between follower and catching_up states
        let message = self.append_entries(request, handler, current_term)?;

        // ensure to reset timer at the very end of potentially long disk operation
        handler.set_timeout(ConsensusTimeout::Election);
        Ok((message, None))
    }

    fn append_entries_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: AppendEntriesResponse,
    ) -> Result<(Option<AppendEntriesRequest>, Option<ConsensusState<L, M>>), Error> {
        // the follower can only receive responses in case the node was a leader some time ago
        // and sent requests which triggered the reponse while response was held somewhere in network
        //
        // at the moment of being non-leader it has nothing to do with them, expept ignoring
        Ok((None, None))
    }

    fn request_vote_request(
        &mut self,
        handler: &mut H,
        candidate: ServerId,
        request: RequestVoteRequest,
    ) -> Result<(Option<RequestVoteResponse>, Option<ConsensusState<L, M>>), Error> {
        self.common_request_vote_request(handler, candidate, request, ConsensusStateKind::Follower)
    }

    fn request_vote_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: RequestVoteResponse,
    ) -> Result<Option<ConsensusState<L, M>>, Error> {
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
            let follower_state =
                self.to_follower(handler, ConsensusStateKind::Follower, voter_term)?;
            Ok(Some(follower_state))
        } else {
            // local_term > voter_term: ignore the message; it came from a previous election cycle
            // local_term = voter_term: since state is not candidate, it's ok because some votes
            // can come after we became follower or leader

            Ok(None)
        }
    }

    // Timeout handling
    /// Handles heartbeat timeout event
    fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error> {
        Err(Error::MustLeader)
    }

    fn election_timeout(&mut self, handler: &mut H) -> Result<Option<ConsensusState<L, M>>, Error> {
        todo!("handle election timeout on follower")

            debug!("election timeout on follower: transitioning to candidate");
        let new_state = self.into_candidate(
    }

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error> {
        // According to Raft 4.1 last paragraph, servers should receive any RPC call
        // from any server, because they may be a new ones which current server doesn't know
        // about yet

        let new_peer = !self.peers.iter().any(|&p| p.id == peer);
        if new_peer {
            debug!("new peer connected, but not found in consensus: {:?}", peer);
        }

        Ok(())
    }

    // Configuration change messages
    fn add_server_request(
        &mut self,
        handler: &mut H,
        request: &AddServerRequest,
    ) -> Result<ServerCommandResponse, Error> {
        todo!("process add_server request")
    }
    fn client_ping_request(&self) -> PingResponse {
        self.common_client_ping_request()
    }

    /// Applies a client proposal to the consensus state machine.
    fn client_proposal_request(
        &mut self,
        handler: &mut H,
        from: ClientId,
        request: Vec<u8>,
    ) -> Result<Option<CommandResponse>, Error> {
        let prev_log_index = self.latest_log_index();
        let prev_log_term = self.latest_log_term();
        let term = self.current_term();
        state
            .leader
            .map_or(Ok(Some(CommandResponse::UnknownLeader)), |leader| {
                Ok(Some(CommandResponse::NotLeader(leader)))
            })
    }

    fn client_query_request(&mut self, from: ClientId, request: &[u8]) -> CommandResponse {
        trace!("query from Client({})", from);
        self.state
            .leader
            .map_or(CommandResponse::UnknownLeader, |leader| {
                CommandResponse::NotLeader(leader)
            })
    }

    //    fn kind(&self) -> ConsensusStateKind {
    //ConsensusStateKind::Follower
    //}
}

impl<L, M> State<L, M, FollowerState>
where
    L: Log,
    M: StateMachine,
{
    /// AppendEntries processing function for both follower and catching up node
    pub(crate) fn append_entries<H: ConsensusHandler>(
        &mut self,
        request: AppendEntriesRequest,
        handler: &mut H,
        current_term: Term,
    ) -> Result<AppendEntriesResponse, Error> {
        let leader_term = request.term;
        if current_term < leader_term {
            self.with_log(|log| log.set_current_term(leader_term))?;
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
                    leader_prev_log_index,
            ));
        }

        let existing_term = if leader_prev_log_index == LogIndex::from(0) {
            // zeroes mean the very start, when everything is empty at leader and follower
            // we better leave this here as special case rather than requiring it from
            // persistent log implementation
            Some(Term::from(0))
        } else {
            // try to find term of specified log index and check if there is such entry
            self.with_log(|log| log.term(leader_prev_log_index))?
        };

        if existing_term
            .map(|term| term != leader_prev_log_term)
                .unwrap_or(true)
        {
            // If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it

            self.with_log(|log| log.discard_since(leader_prev_log_index))?;

            // TODO we could send a hint to a leader about existing index, so leader
            // could send the entries starting from this index instead of decrementing it each time
            return Ok(AppendEntriesResponse::InconsistentPrevEntry(
                    current_term,
                    leader_prev_log_index,
            ));
        }

        // empty entries means heartbeat message
        if request.entries.is_empty() {
            // reply success right away
            return Ok(AppendEntriesResponse::Success(
                    current_term,
                    latest_log_index,
            ));
        }

        // now, for non-empty entries
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

        self.with_log(|log| log.append_entries(leader_prev_log_index + 1, entries.into_iter()))?;
        self.min_index = new_latest_log_index;
        // We are matching the leader's log up to and including `new_latest_log_index`.
        self.commit_index = cmp::min(request.leader_commit, new_latest_log_index);
        self.apply_commits(false);

        // among the changes a config change may have come
        // so we ask the handler to check it and maybe do the connection
        // with new peers or disconnect the removed ones
        handler.ensure_connected(&self.peers);

        Ok(AppendEntriesResponse::Success(
                current_term,
                new_latest_log_index,
        ))
    }

    /// Transitions the consensus state machine to Candidate state.

    fn into_candidate<H: ConsensusHandler>(
        &mut self,
        handler: &mut H,
    ) -> Result<ConsensusState<L, M>, Error> {
        todo!("write candidate transitioning");
        trace!("transitioning to Candidate");
        self.with_log_mut(|log| log.inc_current_term())?;
        let id = self.id;
        self.with_log_mut(|log| log.set_voted_for(id))?;
        let last_log_term = self.with_log(|log| log.latest_log_term())?;

        let old_state = self.state.clone();
        self.state = ConsensusState::Candidate(CandidateState::new());
        handler.state_changed(old_state, &self.state);
        if let ConsensusState::Candidate(ref mut state) = self.state {
            // always true
            state.record_vote(self.id);
        }

        let message = RequestVoteRequest {
            term: self.current_term(),
            last_log_index: self.latest_log_index(),
            last_log_term,
        };

        for &peer in &self.peers {
            handler.send_peer_message(peer.id, PeerMessage::RequestVoteRequest(message.clone()));
        }
        handler.set_timeout(ConsensusTimeout::Election);
        Ok(())
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
