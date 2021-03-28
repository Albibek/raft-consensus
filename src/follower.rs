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
        let message = self.follower_append_entries(request, current_term)?;
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
        request: &RequestVoteRequest,
    ) -> Result<(RequestVoteResponse, Option<ConsensusState<L, M>>), Error> {
        self.common_request_vote_request(handler, candidate, request)
    }

    fn request_vote_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: &RequestVoteResponse,
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
    fn election_timeout(&mut self, handler: &mut H) -> Result<(), Error> {
        unreachable!()
    }

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error> {
        // According to Raft 4.1 last paragraph, servers should receive any RPC call
        // from any server, because they may be a new ones which current server doesn't know
        // about yet

        let new_peer = !self.peers.iter().any(|&p| p.id == peer);
        if new_peer {
            // This may still be correct peer, but it is was not added using AddServer API or was
            // removed already
            // the peer still can be the one that is going to catch up, so we skip this
            // check for a leader state
            // By this reason we don't panic here, returning an error
            debug!("New peer connected, but not found in consensus: {:?}", peer);
        }
        //   if new_peer {
        //return Err(Error::UnknownPeer(peer));
        //}
        // No message is necessary; if the peer is a leader or candidate they will send a
        // message.
        handler.done();
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
