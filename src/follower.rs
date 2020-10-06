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
        request: &AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, Option<ConsensusState<L, M>>), Error> {
        let leader_term = request.term;
        let current_term = self.current_term()?;

        if leader_term < current_term {
            return Ok((AppendEntriesResponse::StaleTerm(current_term), None));
        }

        self.state.set_leader(from);

        let message = self.follower_append_entries(request, current_term)?;
        handler.set_timeout(ConsensusTimeout::Election);
        Ok((message, None))
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
        request: &RequestVoteRequest,
    ) -> Result<(RequestVoteResponse, Option<ConsensusState<L, M>>), Error> {
    }

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

impl<L, M> State<L, M, FollowerState>
where
    L: Log,
    M: StateMachine,
{
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
