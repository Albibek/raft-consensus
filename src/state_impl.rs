use crate::{error::Error, AdminId};

use crate::handler::Handler;
use crate::message::*;

use crate::state_machine::StateMachine;
use crate::{ClientId, ServerId};

use crate::raft::CurrentState;

/// Applies a peer message to the consensus in state specified by S
pub(crate) fn apply_peer_message<M, S: StateImpl<M, H>, H: Handler>(
    mut s: S,
    handler: &mut H,
    from: ServerId,
    message: PeerMessage,
) -> Result<CurrentState<M, H>, Error>
where
    M: StateMachine,
    H: Handler,
{
    s.on_any_event()?;
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
                handler.send_peer_message(from, message);
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
            Ok(new)
        }
        PeerMessage::TimeoutNow => {
            let new = s.timeout_now(handler)?;
            Ok(new)
        }
        PeerMessage::InstallSnapshotRequest(request) => {
            let (response, new) = s.install_snapshot_request(handler, from, request)?;
            handler.send_peer_message(from, response);
            Ok(new)
        }
        PeerMessage::InstallSnapshotResponse(response) => {
            let (request, new) = s.install_snapshot_response(handler, from, response)?;
            if let Some(request) = request {
                handler.send_peer_message(from, request);
            }
            Ok(new)
        }
    }
}

/// Apply a timeout to consensus in state S
pub(crate) fn apply_timeout<M, S: StateImpl<M, H>, H: Handler>(
    mut s: S,
    handler: &mut H,
    timeout: Timeout,
) -> Result<CurrentState<M, H>, Error>
where
    M: StateMachine,
    H: Handler,
{
    s.on_any_event()?;
    match timeout {
        Timeout::Election => s.election_timeout(handler),
        Timeout::Heartbeat(id) => {
            let request = s.heartbeat_timeout(id)?;
            let request = PeerMessage::AppendEntriesRequest(request);
            handler.send_peer_message(id, request);
            Ok(s.into_consensus_state())
        }
        Timeout::Client => {
            s.client_timeout(handler)?;
            Ok(s.into_consensus_state())
        }
    }
}

/// Apply a client message to the consensus in state S
pub(crate) fn apply_client_message<M, S: StateImpl<M, H>, H: Handler>(
    s: &mut S,
    handler: &mut H,
    from: ClientId,
    message: ClientMessage,
) -> Result<(), Error>
where
    M: StateMachine,
    H: Handler,
{
    s.on_any_event()?;
    match message {
        ClientMessage::ClientProposalRequest(req) => {
            let response = s.client_proposal_request(handler, from, req)?;
            handler.send_client_message(from, ClientMessage::ClientProposalResponse(response));
            Ok(())
        }
        ClientMessage::ClientProposalResponse(req) => {
            // TODO: message proxying
            // don't use todo!() here, because some trailing responses
            // could still reach this code
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

pub(crate) fn apply_admin_message<M, S: StateImpl<M, H>, H: Handler>(
    s: &mut S,
    handler: &mut H,
    from: AdminId,
    message: AdminMessage,
) -> Result<(), Error>
where
    M: StateMachine,
    H: Handler,
{
    s.on_any_event()?;
    match message {
        AdminMessage::AddServerRequest(request) => {
            let message = s.add_server_request(handler, from, request)?;
            handler.send_admin_message(from, AdminMessage::AddServerResponse(message));
            Ok(())
        }
        AdminMessage::PingRequest => {
            let message = s.ping_request()?;
            handler.send_admin_message(from, AdminMessage::PingResponse(message));
            Ok(())
        }
        AdminMessage::AddServerResponse(_) => {
            todo!("implement add_server proxying");
            Ok(())
        }
        AdminMessage::RemoveServerRequest(_) => {
            todo!("implement removal");
        }
        AdminMessage::RemoveServerResponse(_) => {
            todo!("implement removal");
        }
        AdminMessage::StepDownRequest(request) => {
            let message = s.step_down_request(handler, from, request)?;
            handler.send_admin_message(from, AdminMessage::StepDownResponse(message));
            Ok(())
        }
        AdminMessage::StepDownResponse(request) => {
            todo!("implement step down proxying");
        }
        AdminMessage::PingResponse(_) => {
            todo!("implement ping proxying");
            Ok(())
        }
    }
}

/// This trait defines a consensus behaviour that should be supported in each particular state
pub(crate) trait StateImpl<M, H>
where
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
        request: AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, CurrentState<M, H>), Error>;

    /// Apply an append entries response to the consensus.
    fn append_entries_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: AppendEntriesResponse,
    ) -> Result<(Option<PeerMessage>, CurrentState<M, H>), Error>;

    ///////////////////////////
    // RequestVoteRPC
    /// Applies a peer request vote request to the consensus.
    fn request_vote_request(
        self,
        handler: &mut H,
        candidate: ServerId,
        request: RequestVoteRequest,
    ) -> Result<(Option<RequestVoteResponse>, CurrentState<M, H>), Error>;

    /// Applies a request vote response to the consensus.
    fn request_vote_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: RequestVoteResponse,
    ) -> Result<CurrentState<M, H>, Error>;

    /// Applies a preliminary election timeout response to the consensus
    fn timeout_now(self, handler: &mut H) -> Result<CurrentState<M, H>, Error>;

    /// Apply an append entries request to the consensus state machine.
    fn install_snapshot_request(
        self,
        handler: &mut H,
        from: ServerId,
        request: InstallSnapshotRequest,
    ) -> Result<(PeerMessage, CurrentState<M, H>), Error>;

    /// Apply an append entries response to the consensus.
    fn install_snapshot_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: InstallSnapshotResponse,
    ) -> Result<(Option<PeerMessage>, CurrentState<M, H>), Error>;

    ///////////////////////////
    // Timeouts
    ///////////////////////////

    /// Handles heartbeat timeout event
    fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error>;

    /// Handles election timeout
    fn election_timeout(self, handler: &mut H) -> Result<CurrentState<M, H>, Error>;

    /// Handles client requests timeout if batching is enabled
    fn client_timeout(&mut self, handler: &mut H) -> Result<(), Error>;

    fn check_compaction(&mut self, handler: &mut H, force: bool) -> Result<bool, Error>;
    ///////////////////////////
    // Client RPC messages
    ///////////////////////////

    // Applies a client proposal to the state machine handled by consensus
    // Client requests can be really big, so we require them to be sent by value to avoid copying.
    // Hence the return value contains the original response in case of any errors.
    fn client_proposal_request(
        &mut self,
        handler: &mut H,
        from: ClientId,
        request: ClientRequest,
    ) -> Result<ClientResponse, Error>;

    // Requests some client state from the state machine handled by consensus
    fn client_query_request(
        &mut self,
        from: ClientId,
        request: ClientRequest,
    ) -> Result<ClientResponse, Error>;

    ///////////////////////////
    // Administration RPC
    ///////////////////////////

    fn ping_request(&self) -> Result<PingResponse, Error>;

    fn add_server_request(
        &mut self,
        handler: &mut H,
        from: AdminId,
        request: AddServerRequest,
    ) -> Result<ConfigurationChangeResponse, Error>;

    //    fn remove_server_request(
    //&mut self,
    //handler: &mut H,
    //request: &RemoveServerRequest,
    //) -> Result<ConfigurationChangeResponse, Error>;

    fn step_down_request(
        &mut self,
        handler: &mut H,
        from: AdminId,
        request: Option<ServerId>,
    ) -> Result<ConfigurationChangeResponse, Error>;

    // Utility messages and actions
    fn on_any_event(&mut self) -> Result<(), Error>;
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error>;

    fn into_consensus_state(self) -> CurrentState<M, H>;
}
