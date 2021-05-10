use crate::{error::Error, AdminId};

use crate::handler::Handler;
use crate::message::*;
use crate::persistent_log::Log;
use crate::state_machine::StateMachine;
use crate::{ClientId, ServerId};

use crate::raft::CurrentState;

/// Applies a peer message to the consensus
pub(crate) fn apply_peer_message<L, M, S: StateImpl<L, M, H>, H: Handler>(
    s: S,
    handler: &mut H,
    from: ServerId,
    message: &PeerMessage,
) -> Result<CurrentState<L, M, H>, Error>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    match message {
        PeerMessage::AppendEntriesRequest(request) => {
            // request produces response and optionally - new state
            let (response, new) = s.append_entries_request(handler, from, request)?;

            handler.send_peer_message(from, PeerMessage::AppendEntriesResponse(response));
            Ok(new)
        }

        PeerMessage::AppendEntriesResponse(response) => {
            // response may produce a new request as an answer
            let (request, new) = s.append_entries_response(handler, from, response)?;
            if let Some(message) = request {
                handler.send_peer_message(from, PeerMessage::AppendEntriesRequest(message));
            }
            Ok(new)
        }

        PeerMessage::RequestVoteRequest(request) => {
            // vote request always produces response and optionally - state change
            let (response, new) = s.request_vote_request(handler, from, request)?;
            if let Some(message) = response {
                handler.send_peer_message(from, PeerMessage::RequestVoteResponse(message));
            }
            Ok(new)
        }

        PeerMessage::RequestVoteResponse(response) => {
            // request vote response does not produce new requests, but may produce new state
            let new = s.request_vote_response(handler, from, response)?;
            //(None, new)
            Ok(new)
        }
    }
}

pub(crate) fn apply_timeout<L, M, S: StateImpl<L, M, H>, H: Handler>(
    mut s: S,
    handler: &mut H,
    timeout: Timeout,
) -> Result<CurrentState<L, M, H>, Error>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    match timeout {
        Timeout::Election => s.election_timeout(handler),
        Timeout::Heartbeat(id) => {
            let request = s.heartbeat_timeout(id)?;
            let request = PeerMessage::AppendEntriesRequest(request);
            handler.send_peer_message(id, request);
            Ok(s.into_consensus_state())
        }
    }
}

/// Applies a client message to the consensus state machine.
pub(crate) fn apply_client_message<L, M, S: StateImpl<L, M, H>, H: Handler>(
    s: &mut S,
    handler: &mut H,
    from: ClientId,
    message: &ClientMessage,
) -> Result<(), Error>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    match message {
        ClientMessage::ClientProposalRequest(req) => {
            let response = s.client_proposal_request(handler, from, req)?;
            handler.send_client_message(from, ClientMessage::ClientProposalResponse(response));
            Ok(())
        }
        ClientMessage::ClientProposalResponse(req) => {
            // TODO: message proxying
            Ok(())
        }
        ClientMessage::ClientQueryRequest(req) => {
            let response = s.client_proposal_request(handler, from, req)?;
            handler.send_client_message(from, ClientMessage::ClientQueryResponse(response));
            Ok(())
        }
        ClientMessage::ClientQueryResponse(_req) => {
            // TODO: message proxying
            Ok(())
        }
    }
}

pub(crate) fn apply_admin_message<L, M, S: StateImpl<L, M, H>, H: Handler>(
    s: &mut S,
    handler: &mut H,
    from: AdminId,
    message: &AdminMessage,
) -> Result<(), Error>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    match message {
        AdminMessage::AddServerRequest(request) => {
            let message = s.add_server_request(handler, request)?;
            handler.send_admin_message(from, AdminMessage::AddServerResponse(message));
            Ok(())
        }
        AdminMessage::PingRequest => {
            let message = s.ping_request()?;
            handler.send_admin_message(from, AdminMessage::PingResponse(message));
            Ok(())
        }
        AdminMessage::AddServerResponse(_) => {
            // TODO: message proxying
            Ok(())
        }
        AdminMessage::RemoveServerRequest(_) => {
            todo!("implement removal");
        }
        AdminMessage::RemoveServerResponse(_) => {
            todo!("implement removal");
        }
        AdminMessage::StepDownRequest(_) => {
            todo!("implement step down");
        }
        AdminMessage::StepDownResponse(_) => {
            todo!("implement step down");
        }
        AdminMessage::PingResponse(_) => {
            // TODO: message proxying
            Ok(())
        }
    }
}

/// This trait defines a consensus behaviour that should be supported in each particular state
pub(crate) trait StateImpl<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    ///////////////////////////
    // AppendEntriesRPC
    /// Apply an append entries request to the consensus state machine.
    fn append_entries_request(
        self,
        handler: &mut H,
        from: ServerId,
        request: &AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, CurrentState<L, M, H>), Error>;

    /// Apply an append entries response to the consensus state machine.
    ///
    /// The provided message may be initialized with a new AppendEntries request to send back to
    /// the follower in the case that the follower's log is behind.
    fn append_entries_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: &AppendEntriesResponse,
    ) -> Result<(Option<AppendEntriesRequest>, CurrentState<L, M, H>), Error>;

    ///////////////////////////
    // RequestVoteRPC
    /// Applies a peer request vote request to the consensus state machine.
    fn request_vote_request(
        self,
        handler: &mut H,
        candidate: ServerId,
        request: &RequestVoteRequest,
    ) -> Result<(Option<RequestVoteResponse>, CurrentState<L, M, H>), Error>;

    /// Applies a request vote response to the consensus state machine.
    fn request_vote_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: &RequestVoteResponse,
    ) -> Result<CurrentState<L, M, H>, Error>;

    ///////////////////////////
    // Timeouts
    ///////////////////////////

    /// Handles heartbeat timeout event
    fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error>;
    fn election_timeout(self, handler: &mut H) -> Result<CurrentState<L, M, H>, Error>;

    ///////////////////////////
    // Client RPC messages
    ///////////////////////////

    // Applies a client proposal to the state machine handled by consensus
    fn client_proposal_request(
        &mut self,
        handler: &mut H,
        from: ClientId,
        request: &ClientRequest,
    ) -> Result<ClientResponse, Error>;

    // Requests some client state from the state machine handled by consensus
    fn client_query_request(&mut self, from: ClientId, request: &ClientRequest) -> ClientResponse;

    ///////////////////////////
    // Administration RPC
    ///////////////////////////

    fn ping_request(&self) -> Result<PingResponse, Error>;

    fn add_server_request(
        &mut self,
        handler: &mut H,
        request: &AddServerRequest,
    ) -> Result<ConfigurationChangeResponse, Error>;

    //    fn remove_server_request(
    //&mut self,
    //handler: &mut H,
    //request: &RemoveServerRequest,
    //) -> Result<ConfigurationChangeResponse, Error>;

    //    fn step_down_request(
    //&mut self,
    //handler: &mut H,
    //request: &RemoveServerRequest,
    //) -> Result<ConfigurationChangeResponse, Error>;

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error>;

    fn into_consensus_state(self) -> CurrentState<L, M, H>;
}
