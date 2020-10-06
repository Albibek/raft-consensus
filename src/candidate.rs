use std::collections::{HashMap, HashSet, VecDeque};

use log::{debug, info, trace};
use std::cmp;

use crate::error::Error;
use crate::handler::ConsensusHandler;
use crate::message::*;
use crate::{ClientId, ConsensusConfig, Entry, EntryData, LogIndex, Peer, ServerId, Term};

use crate::persistent_log::Log;
use crate::state::{ConsensusState, StateHandler};
use crate::state_machine::StateMachine;

use crate::consensus::State;

impl<L, M, H> StateHandler<L, M, H> for State<L, M, CandidateState>
where
    L: Log,
    M: StateMachine,
    H: ConsensusHandler,
{
    fn append_entries_request(
        &mut self,
        handler: &mut H,
        from: ServerId,
        request: &AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, Option<ConsensusState<L, M>>), Error> {
        let leader_term = request.term;
        let current_term = self.current_term()?;

        if leader_term < current_term {
            return Ok((AppendEntriesResponse::StaleTerm(current_term), None));
        }

        // receiving AppendEntries for candidate means new leader was found,
        // so it must become follower now
        //
        let ConsensusState::Follower(new_state) = self.to_follower(handler, leader_term)?;
        let (response, new_state) = new_state.append_entries_request(handler, from, request)?;
        Ok((response, new_state))
    }

    fn append_entries_response<S>(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: &AppendEntriesResponse,
    ) -> Result<(Option<AppendEntriesRequest>, Option<ConsensusState<L, M>>), Error>
    where
        S: StateHandler<L, M, H>,
    {
        // the candidate can only receive responses in case the node was a leader some time ago
        // and sent requests which triggered the reponse while response was held somewhere in network
        //
        // at the moment of being non-leader it has nothing to do with them, expept ignoring
        Ok((None, None))
    }

    /// Applies a peer request vote request to the consensus state machine.
    fn request_vote_request(
        &mut self,
        handler: &mut H,
        candidate: ServerId,
        request: &RequestVoteRequest,
    ) -> Result<(RequestVoteResponse, Option<ConsensusState<L, M>>), Error> {
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

impl<L, M> State<L, M, CandidateState>
where
    L: Log,
    M: StateMachine,
{
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
