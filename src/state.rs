use crate::error::Error;
use crate::message::ConsensusStateKind;

use crate::handler::ConsensusHandler;
use crate::message::*;
use crate::{ClientId, ServerId};

use crate::consensus::State;

use crate::candidate::CandidateState;
use crate::follower::FollowerState;
use crate::leader::LeaderState;

/// Consensus can be in one of three state:
///
/// * `Follower` - which replicates AppendEntries requests and votes for it's leader.
/// * `Leader` - which leads the cluster by serving incoming requests, ensuring
///              data is replicated, and issuing heartbeats.
/// * `Candidate` -  which campaigns in an election and may become a `Leader`
///                  (if it gets enough votes) or a `Follower`, if it hears from
///                  a `Leader`.
/// foreign to the node, because node that didn't catch up is not considered to be in cluster yet
pub(crate) enum ConsensusState<L, M> {
    Leader(State<L, M, LeaderState>),
    Follower(State<L, M, FollowerState>),
    Candidate(State<L, M, CandidateState>),
}

/// Applies a peer message to the consensus
pub(crate) fn apply_peer_message<L, M, S: StateHandler<L, M, H>, H: ConsensusHandler>(
    s: &mut S,
    handler: &mut H,
    from: ServerId,
    message: PeerMessage,
) -> Result<(Option<PeerMessage>, Option<ConsensusState<L, M>>), Error> {
    let message = message; // This enforces a by-value move making clippy happy
    let result = match message {
        PeerMessage::AppendEntriesRequest(request) => {
            // request produces response and optionally - new state
            let (response, new) = s.append_entries_request(handler, from, request)?;
            (Some(PeerMessage::AppendEntriesResponse(response)), new)
        }

        PeerMessage::AppendEntriesResponse(response) => {
            // response may produce a new request as an answer
            let (request, new) = s.append_entries_response(handler, from, response)?;
            (request.map(PeerMessage::AppendEntriesRequest), new)
        }

        PeerMessage::RequestVoteRequest(request) => {
            // vote request always produces response and optionally - state change
            let (response, new) = s.request_vote_request(handler, from, request)?;
            (response.map(PeerMessage::RequestVoteResponse), new)
        }

        PeerMessage::RequestVoteResponse(response) => {
            // request vote response does not produce new requests, but may produce new state
            let new = s.request_vote_response(handler, from, response)?;
            (None, new)
        }
    };
    Ok(result)
}

pub(crate) fn apply_timeout<L, M, S: StateHandler<L, M, H>, H: ConsensusHandler>(
    s: &mut S,
    handler: &mut H,
    timeout: ConsensusTimeout,
) -> Result<Option<ConsensusState<L, M>>, Error> {
    match timeout {
        ConsensusTimeout::Election => s.election_timeout(handler),
        ConsensusTimeout::Heartbeat(id) => {
            let request = s.heartbeat_timeout(id)?;
            let request = PeerMessage::AppendEntriesRequest(request);
            handler.send_peer_message(id, request);
            Ok(None)
        }
    }
}

/// Applies a client message to the consensus state machine.
pub(crate) fn apply_client_message<L, M, S: StateHandler<L, M, H>, H: ConsensusHandler>(
    s: &mut S,
    handler: &mut H,
    from: ClientId,
    message: ClientRequest,
) -> Result<ClientResponse, Error> {
    Ok(match message {
        ClientRequest::Ping => ClientResponse::Ping(s.client_ping_request()?),
        ClientRequest::Proposal(data) => {
            let response = s.client_proposal_request(handler, from, data)?;
            ClientResponse::Proposal(response)
        }
        ClientRequest::Query(data) => ClientResponse::Query(s.client_query_request(from, &data)),
    })
}

pub fn apply_config_change_message<L, M, S: StateHandler<L, M, H>, H: ConsensusHandler>(
    s: &mut S,
    handler: &mut H,
    request: &AddServerRequest,
) -> Result<ServerCommandResponse, Error> {
    s.add_server_request(handler, request)
}

/// This trait defines a consensus behaviour in the each
pub(crate) trait StateHandler<L, M, H: ConsensusHandler> {
    // AppendEntriesRPC
    /// Apply an append entries request to the consensus state machine.
    fn append_entries_request(
        &mut self,
        handler: &mut H,
        from: ServerId,
        request: AppendEntriesRequest,
    ) -> Result<(AppendEntriesResponse, Option<ConsensusState<L, M>>), Error>;

    /// Apply an append entries response to the consensus state machine.
    ///
    /// The provided message may be initialized with a new AppendEntries request to send back to
    /// the follower in the case that the follower's log is behind.
    fn append_entries_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: AppendEntriesResponse,
    ) -> Result<(Option<AppendEntriesRequest>, Option<ConsensusState<L, M>>), Error>;

    // RequestVoteRPC
    /// Applies a peer request vote request to the consensus state machine.
    fn request_vote_request(
        &mut self,
        handler: &mut H,
        candidate: ServerId,
        request: RequestVoteRequest,
    ) -> Result<(Option<RequestVoteResponse>, Option<ConsensusState<L, M>>), Error>;

    /// Applies a request vote response to the consensus state machine.
    fn request_vote_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: RequestVoteResponse,
    ) -> Result<Option<ConsensusState<L, M>>, Error>;

    // Timeouts
    /// Handles heartbeat timeout event
    fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error>;
    fn election_timeout(&mut self, handler: &mut H) -> Result<Option<ConsensusState<L, M>>, Error>;

    // Configuration change RPC
    fn add_server_request(
        &mut self,
        handler: &mut H,
        request: &AddServerRequest,
    ) -> Result<ServerCommandResponse, Error>;

    // Client RPC messages
    fn client_ping_request(&self) -> Result<PingResponse, Error>;

    // Applies a client proposal to the state machine handled by consensus
    fn client_proposal_request(
        &mut self,
        handler: &mut H,
        from: ClientId,
        request: Vec<u8>,
    ) -> Result<CommandResponse, Error>;

    // Requests some client state from the state machine handled by consensus
    fn client_query_request(&mut self, from: ClientId, request: &[u8]) -> CommandResponse;

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use state::LeaderState;
    use {LogIndex, ServerId};

    /// Tests the `LeaderState`'s  `.count_match_indexes()` function and makes sure it adequately
    /// produces the correct values.
    #[test]
    fn test_count_match_indexes() {
        let index = LogIndex(0);
        let mut peers = HashSet::new();

        // All peers start at 0 index.
        let leader_state = LeaderState::new(index, &peers);
        // Should be one, since the leader node would be matched always.
        assert_eq!(1, leader_state.count_match_indexes(LogIndex(0)));

        peers.insert(ServerId(1));
        let leader_state = LeaderState::new(index, &peers);
        assert_eq!(2, leader_state.count_match_indexes(LogIndex(0)));

        peers.insert(ServerId(2));
        let leader_state = LeaderState::new(index, &peers);
        assert_eq!(3, leader_state.count_match_indexes(LogIndex(0)));

        peers.insert(ServerId(3));
        let mut leader_state = LeaderState::new(index, &peers);
        assert_eq!(4, leader_state.count_match_indexes(LogIndex(0)));

        leader_state.set_match_index(ServerId(1), LogIndex(1));
        leader_state.set_match_index(ServerId(2), LogIndex(1));
        assert_eq!(3, leader_state.count_match_indexes(LogIndex(1)));
    }
}
