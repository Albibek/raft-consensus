use std::collections::HashSet;
use std::marker::PhantomData;

use crate::error::*;
use log::{debug, info, trace};

use crate::handler::Handler;
use crate::leader::Leader;
use crate::message::*;
use crate::persistent_log::Log;
use crate::raft::CurrentState;
use crate::state::State;
use crate::state_impl::StateImpl;
use crate::state_machine::StateMachine;
use crate::{ClientId, ServerId, Term};

impl<L, M, H> StateImpl<L, M, H> for State<L, M, H, Candidate>
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
        let leader_term = request.term;
        let current_term = self.current_term()?;

        if leader_term < current_term {
            return Ok((AppendEntriesResponse::StaleTerm(current_term), self.into()));
        }

        // receiving AppendEntries for candidate means new leader was found,
        // so it must become follower now
        let follower = self.into_follower(handler, ConsensusState::Candidate, leader_term)?;

        // after becoming follower it should process the request
        follower.append_entries_request(handler, from, request)
    }

    fn append_entries_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: &AppendEntriesResponse,
    ) -> Result<(Option<AppendEntriesRequest>, CurrentState<L, M, H>), Error> {
        // the candidate can only receive responses in case the node was a leader some time ago
        // and sent requests which triggered the reponse while response was held somewhere in network
        //
        // at the moment of being non-leader it has nothing to do with them, expept ignoring
        Ok((None, self.into()))
    }

    /// Applies a peer request vote request to the consensus state machine.
    fn request_vote_request(
        self,
        handler: &mut H,
        candidate: ServerId,
        request: &RequestVoteRequest,
    ) -> Result<(Option<RequestVoteResponse>, CurrentState<L, M, H>), Error> {
        // To avoid disrupting leader while configuration changes, node should ignore or delay vote requests
        // coming within election timeout unless there is special flag set signalling
        // the leadership was given away voluntarily

        // On candidate node disrupting of a leader is not possible because voting has
        // already started, so the flag has no meaning
        let (response, new_state) = self.common_request_vote_request(
            handler,
            candidate,
            request,
            ConsensusState::Candidate,
        )?;
        Ok((Some(response), new_state))
    }

    /// Applies a request vote response to the consensus state machine.
    fn request_vote_response(
        mut self,
        handler: &mut H,
        from: ServerId,
        response: &RequestVoteResponse,
    ) -> Result<CurrentState<L, M, H>, Error> {
        debug!("RequestVoteResponse from peer {}", from);

        let local_term = self.current_term()?;
        let voter_term = response.voter_term();
        let majority = self.config.majority(self.id);
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
            let follower = self.into_follower(handler, ConsensusState::Candidate, voter_term)?;
            Ok(follower.into())
        } else if local_term > voter_term {
            // Ignore this message; it came from a previous election cycle.
            Ok(self.into())
        } else {
            // local_term == voter_term
            // A vote was received!
            if let RequestVoteResponse::Granted(_) = response {
                self.state_data.record_vote(from);
                if self.state_data.count_votes() >= majority {
                    info!(
                        "election for term {} won; transitioning to Leader",
                        local_term
                    );
                    let new_state = self.into_leader(handler)?;
                    Ok(new_state.into())
                } else {
                    Ok(self.into())
                }
            } else {
                Ok(self.into())
            }
        }
    }

    // Timeout handling
    /// Handles heartbeat timeout event
    fn heartbeat_timeout(&mut self, _peer: ServerId) -> Result<AppendEntriesRequest, Error> {
        Err(Error::MustLeader)
    }

    fn election_timeout(mut self, handler: &mut H) -> Result<CurrentState<L, M, H>, Error> {
        if self.config.is_solitary(self.id) {
            // Solitary replica special case: we are the only peer in consensus
            // jump straight to Leader state.
            trace!("election timeout: transitioning to leader due do solitary replica condition");
            assert!(self.with_log(|log| log.voted_for())?.is_none()); // there cannot be anyone to vote for us

            //self.with_log(|log| log.inc_current_term())?;
            //self.with_log(|log| log.set_voted_for(self.id))?;
            let leader = self.into_leader(handler)?;
            Ok(leader.into())
        } else {
            trace!("election timeout on candidate: restarting election");
            self.start_election(handler)?;
            Ok(self.into())
        }
    }

    // Utility messages and actions
    fn peer_connected(&mut self, _handler: &mut H, _peer: ServerId) -> Result<(), Error> {
        // TODO: we could resend `RequestVoteRequest`s to a peer, like previous implementation did,
        // but we intentionally do not do this, leaving all the retrying strategies to external implementation
        // which may decide to have different startegies of retrying
        // Sending nothing should not break the consensus, except probably delaying the
        // voting process for one more election timeout

        Ok(())
    }

    // Configuration change messages
    fn add_server_request(
        &mut self,
        _handler: &mut H,
        _request: &AddServerRequest,
    ) -> Result<ConfigurationChangeResponse, Error> {
        Ok(ConfigurationChangeResponse::UnknownLeader)
    }

    /// Applies a client proposal to the consensus state machine.
    fn client_proposal_request(
        &mut self,
        _handler: &mut H,
        _from: ClientId,
        _request: &ClientRequest,
    ) -> Result<ClientResponse, Error> {
        Ok(ClientResponse::UnknownLeader)
    }

    fn client_query_request(
        &mut self,
        _from: ClientId,
        _request: &ClientRequest,
    ) -> ClientResponse {
        ClientResponse::UnknownLeader
    }

    fn ping_request(&self) -> Result<PingResponse, Error> {
        self.common_client_ping_request(ConsensusState::Candidate)
    }

    fn into_consensus_state(self) -> CurrentState<L, M, H> {
        CurrentState::Candidate(self)
    }
}

impl<L, M, H> State<L, M, H, Candidate>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    // Checks if consensus is solitary, allowing instant transition to the leader without waiting
    // an additional election timeout
    //
    // Must be used only from follower placed in candidate handler to avoid breaking the state transitioning:
    // direct follower -> leader is only possible in solitary consensus condition,
    // so we expect follower to call this right after transitioning to candidate
    // rather than making into_leader usable in any state being callable from anywhere
    pub(crate) fn try_solitary_leader(
        mut self,
        handler: &mut H,
    ) -> Result<CurrentState<L, M, H>, Error> {
        if self.config.is_solitary(self.id) {
            // Solitary replica special case: we are the only peer in consensus
            // jump straight to Leader state.
            info!("transitioning to leader due do solitary replica condition");
            assert!(self.with_log(|log| log.voted_for())?.is_none());

            self.inc_current_term()?;
            let id = self.id;
            self.with_log_mut(|log| log.set_voted_for(Some(id)))?;
            let leader = self.into_leader(handler)?;
            Ok(leader.into())
        } else {
            Ok(self.into())
        }
    }

    pub(crate) fn start_election(&mut self, handler: &mut H) -> Result<(), Error> {
        self.inc_current_term()?;
        let id = self.id;
        self.with_log_mut(|log| log.set_voted_for(Some(id)))?;
        let last_log_term = self.latest_log_term()?;

        let message = RequestVoteRequest {
            term: self.current_term()?,
            last_log_index: self.latest_log_index()?,
            last_log_term,
            is_voluntary_step_down: false,
        };

        self.config.with_remote_peers(&self.id, |id| {
            handler.send_peer_message(*id, PeerMessage::RequestVoteRequest(message.clone()));
            Ok(())
        });

        handler.set_timeout(Timeout::Election);
        Ok(())
    }

    // Only candidates can transition to leader
    pub(crate) fn into_leader(self, handler: &mut H) -> Result<State<L, M, H, Leader>, Error> {
        trace!("transitioning to leader");
        let latest_log_index = self.with_log(|log| log.latest_log_index())?;
        let current_term = self.current_term()?;
        let latest_log_term = self.latest_log_term()?;
        let commit_index = self.commit_index;

        handler.state_changed(ConsensusState::Candidate, ConsensusState::Leader);

        // transition to new state
        let state = Leader::new(latest_log_index, &self.config, &self.id);
        let leader = State {
            id: self.id,
            config: self.config,
            log: self.log,
            state_machine: self.state_machine,
            commit_index,
            min_index: self.min_index,
            last_applied: self.last_applied,
            _h: PhantomData,
            state_data: state,
        };

        // send a ping to all peers
        let message = AppendEntriesRequest {
            term: current_term,
            prev_log_index: latest_log_index,
            prev_log_term: latest_log_term,
            leader_commit: commit_index,
            entries: Vec::new(),
        };

        leader.config.with_remote_peers(&leader.id, |id| {
            handler.send_peer_message(*id, PeerMessage::AppendEntriesRequest(message.clone()));
            handler.set_timeout(Timeout::Heartbeat(*id));
            Ok(())
        });

        handler.clear_timeout(Timeout::Election);

        Ok(leader)
    }
}

/// The state associated with a Raft consensus module in the `Candidate` state.
#[derive(Clone, Debug)]
pub struct Candidate {
    granted_votes: HashSet<ServerId>,
}

impl Candidate {
    /// Creates a new `CandidateState`.
    pub(crate) fn new() -> Candidate {
        Candidate {
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
}
