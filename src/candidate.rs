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

    fn append_entries_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: &AppendEntriesResponse,
    ) -> Result<(Option<AppendEntriesRequest>, Option<ConsensusState<L, M>>), Error> {
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
        self.common_request_vote_request(handler, candidate, request)
    }

    /// Applies a request vote response to the consensus state machine.
    fn request_vote_response(
        &mut self,
        handler: &mut H,
        from: ServerId,
        response: &RequestVoteResponse,
    ) -> Result<Option<ConsensusState<L, M>>, Error> {
        debug!("RequestVoteResponse from peer {}", from);

        let local_term = self.current_term();
        let voter_term = response.voter_term();
        let majority = self.majority();
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
            let follower_state = self.to_follower(handler, voter_term)?;
            Ok(Some(follower_state))
        } else if local_term > voter_term {
            // Ignore this message; it came from a previous election cycle.
            Ok(None)
        } else {
            // A vote was received!
            if let RequestVoteResponse::Granted(_) = response {
                self.state.record_vote(from);
                if self.state.count_votes() >= majority {
                    info!(
                        "election for term {} won; transitioning to Leader",
                        local_term
                    );
                    let new_state = self.into_leader(handler)?;
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

    fn election_timeout(&mut self, handler: &mut H) -> Result<(), Error> {
        if self.peers.is_empty() || self.peers.len() == 1 && self.peers[0].id == self.id {
            // Solitary replica special case: we are the only peer in consensus
            // jump straight to Leader state.
            info!("Election timeout: transitioning to Leader due do solitary replica condition");
            assert!(self.log.voted_for().unwrap().is_none());

            self.log
                .inc_current_term()
                .map_err(|e| Error::PersistentLog(Box::new(e)))?;
            self.log
                .set_voted_for(self.id)
                .map_err(|e| Error::PersistentLog(Box::new(e)))?;
        } else {
            info!("Election timeout: transitioning to Candidate");
            self.transition_to_candidate(handler)?;

            return Ok(());
        }

        handler.done();
        todo!("become leader?");
        // we only get here if we are candidate, and we need to release self.state before
        //            let old_state = self.state.clone();
        //let latest_log_index = self
        //.log
        //.latest_log_index()
        //.map_err(|e| Error::PersistentLog(Box::new(e)))?;

        //self.state = ConsensusState::Leader(LeaderState::new(
        //latest_log_index,
        //self.peers.iter().map(|peer| &peer.id),
        //));

        //handler.state_changed(old_state, &self.state);
        //handler.clear_timeout(ConsensusTimeout::Election);

        Ok(())
    }

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error> {
        let new_peer = !self.peers.iter().any(|&p| p.id == peer);
        if new_peer {
            // This may still be correct peer, but it is was not added using AddServer API or was
            // removed already
            // the peer still can be the one that is going to catch up, so we skip this
            // check for a leader state
            // By this reason we don't panic here, returning an error
            debug!("New peer connected, but not found in consensus: {:?}", peer);
        }
        if new_peer {
            return Err(Error::UnknownPeer(peer));
        }
        // Resend the request vote request if a response has not yet been receieved.
        if state.peer_voted(peer) {
            return Ok(());
        }

        let message = RequestVoteRequest {
            term: self.current_term(),
            last_log_index: self.latest_log_index(),
            last_log_term: self.log.latest_log_term().unwrap(),
        };
        handler.send_peer_message(peer, PeerMessage::RequestVoteRequest(message));
        handler.done();
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
        //        let prev_log_index = self.latest_log_index();
        //let prev_log_term = self.latest_log_term();
        //        let term = self.current_term();
        Ok(Some(CommandResponse::UnknownLeader))
    }

    fn client_query_request(&mut self, from: ClientId, request: &[u8]) -> CommandResponse {
        trace!("query from Client({})", from);
        CommandResponse::UnknownLeader
    }

    fn kind(&self) -> ConsensusStateKind {
        ConsensusStateKind::Candidate
    }
}

impl<L, M> State<L, M, CandidateState>
where
    L: Log,
    M: StateMachine,
{
    // Only candidates can transition to leader
    pub(crate) fn into_leader<H: ConsensusHandler>(
        &mut self,
        handler: &mut H,
        leader_term: Term,
    ) -> Result<ConsensusState<L, M>, Error> {
        trace!("transitioning to Leader");
        let latest_log_index = self
            .log
            .latest_log_index()
            .map_err(|e| Error::PersistentLog(Box::new(e)))?;
        let old_state = self.state.clone();

        self.state = ConsensusState::Leader(LeaderState::new(
            latest_log_index,
            self.peers.iter().map(|peer| &peer.id),
        ));
        handler.state_changed(old_state, &self.state);

        let message = AppendEntriesRequest {
            term: self.current_term(),
            prev_log_index: latest_log_index,
            prev_log_term: self
                .log
                .latest_log_term()
                .map_err(|e| Error::PersistentLog(Box::new(e)))?,
            leader_commit: self.commit_index,
            entries: Vec::new(),
        };

        for &peer in &self.peers {
            handler.send_peer_message(peer.id, PeerMessage::AppendEntriesRequest(message.clone()));
            handler.clear_timeout(ConsensusTimeout::Heartbeat(peer.id));
        }
        handler.clear_timeout(ConsensusTimeout::Election);
        Ok(())
    }
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
