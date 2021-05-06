use std::collections::HashSet;

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
use crate::{ClientId, LogIndex, Peer, ServerId, Term};

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
            return Ok((
                AppendEntriesResponse::StaleTerm(current_term),
                self.into_consensus_state(),
            ));
        }

        // receiving AppendEntries for candidate means new leader was found,
        // so it must become follower now
        if let CurrentState::Follower(mut new_state) =
            self.to_follower(handler, ConsensusStateKind::Candidate, leader_term)?
        {
            let (response, new_state) = new_state.append_entries_request(handler, from, request)?;
            Ok((response, new_state))
        } else {
            Err(Error::unreachable(module_path!()))
        }
    }

    fn append_entries_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: AppendEntriesResponse,
    ) -> Result<(Option<AppendEntriesRequest>, Option<CurrentState<L, M, H>>), Error> {
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
        request: RequestVoteRequest,
    ) -> Result<(Option<RequestVoteResponse>, Option<CurrentState<L, M, H>>), Error> {
        // To avoid disrupting leader while configuration changes, node should ignore or delay vote requests
        // coming within election timeout unless there is special flag set signalling
        // the leadership was given away voluntarily

        // On candidate node disrupting of a leader is not possible because voting has
        // already started, so the flag has no meaning
        let (response, new_state) = self.common_request_vote_request(
            handler,
            candidate,
            request,
            ConsensusStateKind::Candidate,
        )?;
        Ok((Some(response), new_state))
    }

    /// Applies a request vote response to the consensus state machine.
    fn request_vote_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: RequestVoteResponse,
    ) -> Result<Option<CurrentState<L, M, H>>, Error> {
        debug!("RequestVoteResponse from peer {}", from);

        let local_term = self.current_term()?;
        let voter_term = response.voter_term();
        let majority = self.config.majority();
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
                self.to_follower(handler, ConsensusStateKind::Candidate, voter_term)?;
            Ok(Some(follower_state))
        } else if local_term > voter_term {
            // Ignore this message; it came from a previous election cycle.
            Ok(None)
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
                    let new_state = self.into_leader(handler, voter_term)?;
                    Ok(Some(new_state))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
    }

    // Timeout handling
    /// Handles heartbeat timeout event
    fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error> {
        Err(Error::MustLeader)
    }

    fn election_timeout(mut self, handler: &mut H) -> Result<CurrentState<L, M, H>, Error> {
        if self.config.is_solitary(&self.id) {
            // Solitary replica special case: we are the only peer in consensus
            // jump straight to Leader state.
            info!("election timeout: transitioning to leader due do solitary replica condition");
            assert!(self.with_log(|log| log.voted_for())?.is_none()); // there cannot be anyone to vote for us

            //self.with_log(|log| log.inc_current_term())?;
            //self.with_log(|log| log.set_voted_for(self.id))?;
            let current_term = self.with_log(|log| log.current_term())?;
            let new_state = self.into_leader(handler, current_term)?;
            Ok(new_state)
        } else {
            info!("election timeout on candidate: restarting election");
            self.with_log_mut(|log| log.inc_current_term())?;
            self.with_log_mut(|log| log.set_voted_for(self.id))?;
            handler.set_timeout(ConsensusTimeout::Election);
            todo!("send vote requests");
            Ok(StateImpl::<L, M, H>::into_consensus_state(self))
        }
    }

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error> {
        //  let new_peer = !self.peers.iter().any(|&p| p.id == peer);
        //if new_peer {
        //// This may still be correct peer, but it is was not added using AddServer API or was
        //// removed already
        //// the peer still can be the one that is going to catch up, so we skip this
        //// check for a leader state
        //// By this reason we don't panic here, returning an error
        //debug!("New peer connected, but not found in consensus: {:?}", peer);
        //}
        //if new_peer {
        //return Err(Error::UnknownPeer(peer));
        //}
        //// Resend the request vote request if a response has not yet been receieved.
        //if state.peer_voted(peer) {
        //return Ok(());
        //}

        //let message = RequestVoteRequest {
        //term: self.current_term(),
        //last_log_index: self.latest_log_index(),
        //last_log_term: self.log.latest_log_term().unwrap(),
        //};
        //handler.send_peer_message(peer, PeerMessage::RequestVoteRequest(message));
        //handler.done();

        // TODO: we could resend some data to a peer, but we intentionally do not do this,
        // leaving this to the implementation which may decide to have different startegies
        // of retrying
        // Sending nothing should not break the consensus, except probably delaying the
        // voting process for one more iteration

        Ok(())
    }

    // Configuration change messages
    fn add_server_request(
        &mut self,
        handler: &mut H,
        request: &AddServerRequest,
    ) -> Result<AdminCommandResponse, Error> {
        todo!("process add_server request")
    }

    fn client_ping_request(&self) -> Result<PingResponse, Error> {
        self.common_client_ping_request(ConsensusStateKind::Candidate)
    }

    /// Applies a client proposal to the consensus state machine.
    fn client_proposal_request(
        &mut self,
        handler: &mut H,
        from: ClientId,
        request: Vec<u8>,
    ) -> Result<CommandResponse, Error> {
        Ok(CommandResponse::UnknownLeader)
    }

    fn client_query_request(&mut self, from: ClientId, request: &[u8]) -> CommandResponse {
        // TODO: introduce an option for allowing the response from the potentially
        // older state of the machine
        // With such option it could be possible to reply the machine on the follower
        trace!("query from client {}", from);
        CommandResponse::UnknownLeader
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
        &mut self,
        handler: &mut H,
    ) -> Result<Option<CurrentState<L, M, H>>, Error> {
        if self.config.is_solitary(&self.id) {
            // Solitary replica special case: we are the only peer in consensus
            // jump straight to Leader state.
            info!("election timeout: transitioning to Leader due do solitary replica condition");
            assert!(self.with_log(|log| log.voted_for())?.is_none());

            self.with_log_mut(|log| log.inc_current_term())?;
            self.with_log_mut(|log| log.set_voted_for(self.id))?;
            let current_term = self.with_log(|log| log.current_term())?;
            let new_state = self.into_leader(handler, current_term)?;
            return Ok(Some(new_state));
        } else {
            Ok(None)
        }
    }

    // Only candidates can transition to leader
    pub(crate) fn into_leader(
        &mut self,
        handler: &mut H,
        leader_term: Term,
    ) -> Result<CurrentState<L, M, H>, Error> {
        trace!("transitioning to Leader");
        let latest_log_index = self.with_log(|log| log.latest_log_index())?;

        handler.state_changed(ConsensusStateKind::Candidate, &ConsensusStateKind::Leader);

        let message = AppendEntriesRequest {
            term: self.current_term()?,
            prev_log_index: latest_log_index,
            prev_log_term: self.with_log(|log| log.latest_log_term())?,
            leader_commit: self.commit_index,
            entries: Vec::new(),
        };

        self.config.with_remote_peers(&self.id, |id| {
            handler.send_peer_message(*id, PeerMessage::AppendEntriesRequest(message.clone()));
            handler.set_timeout(ConsensusTimeout::Heartbeat(*id));
            Ok(())
        });

        handler.clear_timeout(ConsensusTimeout::Election);
        let state = Leader::new(latest_log_index, &self.config, &self.id);

        let leader_state = State {
            id: self.id,
            config: self.config.clone(),
            log: self.log,
            state_machine: self.state_machine,
            commit_index: self.commit_index,
            min_index: self.min_index,
            last_applied: self.last_applied,
            state_data: state,
        };
        Ok(CurrentState::Leader(leader_state))
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

    /// Returns whether the peer has voted in the current election.
    pub(crate) fn peer_voted(&self, voter: ServerId) -> bool {
        self.granted_votes.contains(&voter)
    }
}
