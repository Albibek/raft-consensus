use std::cmp;
use std::marker::PhantomData;

use crate::error::*;
use log::{debug, info, trace};

use crate::{ClientId, LogIndex, Peer, ServerId, Term};

use crate::handler::Handler;
use crate::message::*;
use crate::persistent_log::Log;
use crate::raft::CurrentState;
use crate::state::State;
use crate::state_impl::StateImpl;
use crate::state_machine::StateMachine;

use crate::candidate::Candidate;

/// Follower replicates AppendEntries requests and votes for it's leader.
impl<L, M, H> StateImpl<L, M, H> for State<L, M, H, Follower>
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

        self.state_data.set_leader(from);

        let message = self.append_entries(request, handler, current_term)?;

        // ensure to reset timer at the very end of potentially long disk operation
        handler.set_timeout(Timeout::Election);

        // heartbeat invalidates all the delayed voting requests because receiving it
        // means leader is not lost for this node therefore voting should not start
        if self.state_data.delayed_vote_request.is_some() {
            self.state_data.delayed_vote_request = None
        }

        Ok((message, self.into()))
    }

    fn append_entries_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: &AppendEntriesResponse,
    ) -> Result<(Option<AppendEntriesRequest>, CurrentState<L, M, H>), Error> {
        // the follower can only receive responses in case the node was a leader some time ago
        // and sent requests which triggered the reponse while response was held somewhere in network
        //
        // at the moment of being non-leader it has nothing to do with them, expept ignoring
        Ok((None, self.into()))
    }

    fn request_vote_request(
        self,
        handler: &mut H,
        candidate: ServerId,
        request: &RequestVoteRequest,
    ) -> Result<(Option<RequestVoteResponse>, CurrentState<L, M, H>), Error> {
        if !self.state_data.can_vote {
            debug!("catching up node received a voting request");
            return Ok((None, self.into()));
        }
        // To avoid disrupting leader while configuration changes, node should ignore or delay vote requests
        // coming within election timeout unless there is special flag set signalling
        // the leadership was given away voluntarily

        // Followers are always "inside" the election timeout (because they become candidates
        // otherwise). So for a follower this request means it was given a possibility to become a
        // leader faster than others.
        if request.is_voluntary_step_down {
            let (response, new_state) = self.common_request_vote_request(
                handler,
                candidate,
                request,
                ConsensusState::Follower,
            )?;
            Ok((Some(response), new_state))
        } else {
            // Otherwise, the follower can either ignore/deny or delay the voting request.
            //
            // The ignoring/denying scenario makes first election unlikely to be finished effectively
            // (because all other followers would ignore the request), so the voting will require
            // additional requests from each of the followers having the election timeout expiring.
            //
            // The delaying scenario makes the request on non-first followers unneeded, because
            // they already know who to vote for and can vote immediately

            if self.state_data.delayed_vote_request.is_none() {
                // rewriting request or not does not bring any difference, because election timeout
                // is randomized, so there is no preference for a follower
                // so we only avoid some negligible byte copying
                self.state_data.delayed_vote_request = Some((candidate, request.clone()));
            }
            Ok((None, self.into()))
        }
    }

    fn request_vote_response(
        self,
        handler: &mut H,
        from: ServerId,
        response: &RequestVoteResponse,
    ) -> Result<CurrentState<L, M, H>, Error> {
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
            let follower = self.into_follower(handler, ConsensusState::Follower, voter_term)?;
            Ok(follower.into())
        } else {
            // local_term > voter_term: ignore the message; it came from a previous election cycle
            // local_term = voter_term: since state is not candidate, it's ok because some votes
            // can come after we became follower or leader

            Ok(self.into())
        }
    }

    // Timeout handling
    /// Handles heartbeat timeout event
    fn heartbeat_timeout(&mut self, peer: ServerId) -> Result<AppendEntriesRequest, Error> {
        Err(Error::MustLeader)
    }

    fn election_timeout(mut self, handler: &mut H) -> Result<CurrentState<L, M, H>, Error> {
        if let Some((from, ref request)) = self.state_data.delayed_vote_request.take() {
            // voting has started already, process the vote request first
            let (response, new_state) =
                self.common_request_vote_request(handler, from, request, ConsensusState::Follower)?;
            handler.send_peer_message(from, PeerMessage::RequestVoteResponse(response));

            if new_state.is_follower() {
                // new state means we must become totally new follower, meaning becoming candidate
                // is not required
                Ok(new_state)
            } else {
                // Even though we have voted for another candidate, there stil may be things to be
                // done, like increasing local term.
                // Most probably this branch is unreachable, because delayed request will
                // always have the term greater than ours.
                // TODO: think the cases this branch could be reached (candidate_term <= local_term)
                // At the moment we choose the safer way of starting the election in a regular way
                // rather than staying a follower
                let candidate = self.into_candidate(handler)?;
                Ok(candidate.into())
            }
        } else {
            let candidate = self.into_candidate(handler)?;
            Ok(candidate.into())
        }
    }

    // Utility messages and actions
    fn peer_connected(&mut self, handler: &mut H, peer: ServerId) -> Result<(), Error> {
        // followers don't send messages, so they don't care about other peers' connections
        Ok(())
    }

    // Configuration change messages
    fn add_server_request(
        &mut self,
        handler: &mut H,
        request: &AddServerRequest,
    ) -> Result<ConfigurationChangeResponse, Error> {
        self.state_data
            .leader
            .map_or(Ok(ConfigurationChangeResponse::UnknownLeader), |leader| {
                Ok(ConfigurationChangeResponse::NotLeader(leader))
            })
        //TODO: Proxy a command to leader if it is known
    }

    /// Applies a client proposal to the consensus state machine.
    fn client_proposal_request(
        &mut self,
        handler: &mut H,
        from: ClientId,
        request: &ClientRequest,
    ) -> Result<ClientResponse, Error> {
        let prev_log_index = self.latest_log_index();
        let prev_log_term = self.latest_log_term();
        let term = self.current_term();
        self.state_data
            .leader
            .map_or(Ok(ClientResponse::UnknownLeader), |leader| {
                Ok(ClientResponse::NotLeader(leader))
            })
    }

    fn client_query_request(&mut self, from: ClientId, request: &ClientRequest) -> ClientResponse {
        // TODO: introduce an option for allowing the response from the potentially
        // older state of the machine
        // With such option it could be possible to reply the machine on the follower
        trace!("query from client {}", from);
        self.state_data
            .leader
            .map_or(ClientResponse::UnknownLeader, |leader| {
                ClientResponse::NotLeader(leader)
            })
    }

    fn ping_request(&self) -> Result<PingResponse, Error> {
        self.common_client_ping_request(ConsensusState::Follower)
    }

    fn into_consensus_state(self) -> CurrentState<L, M, H> {
        CurrentState::Follower(self)
    }
}

impl<L, M, H> State<L, M, H, Follower>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    /// AppendEntries processing function for
    pub(crate) fn append_entries(
        &mut self,
        request: &AppendEntriesRequest,
        handler: &mut H,
        current_term: Term,
    ) -> Result<AppendEntriesResponse, Error> {
        let leader_term = request.term;
        if current_term < leader_term {
            self.with_log_mut(|log| log.set_current_term(leader_term))?;
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
                latest_log_index,
            ));
        }

        let existing_term = if leader_prev_log_index == LogIndex::from(0) {
            // zeroes mean the very start, when everything is empty at leader and follower
            // we better leave this here as special case rather than requiring it from
            // persistent log implementation
            Some(Term::from(0))
        } else {
            // try to find term of specified log index and check if there is such entry
            self.with_log(|log| log.term_at(leader_prev_log_index))?
        };

        if existing_term
            .map(|term| term != leader_prev_log_term)
            .unwrap_or(true)
        {
            // If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it

            self.with_log(|log| log.discard_log_since(leader_prev_log_index))?;

            // TODO we could send a hint to a leader about existing index, so leader
            // could send the entries starting from this index instead of decrementing it each time
            return Ok(AppendEntriesResponse::InconsistentPrevEntry(
                current_term,
                latest_log_index,
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

        self.with_log_mut(|log| log.append_entries(leader_prev_log_index + 1, entries.iter()))?;
        self.min_index = new_latest_log_index;

        // We are matching the leader's log up to and including `new_latest_log_index`.
        self.commit_index = cmp::min(request.leader_commit, new_latest_log_index);
        self.apply_commits(false);

        Ok(AppendEntriesResponse::Success(
            current_term,
            new_latest_log_index,
        ))
    }

    fn into_candidate(mut self, handler: &mut H) -> Result<State<L, M, H, Candidate>, Error> {
        let mut state_data = Candidate::new();
        state_data.record_vote(self.id);
        let mut candidate = State {
            id: self.id,
            config: self.config,
            log: self.log,
            state_machine: self.state_machine,
            commit_index: self.commit_index,
            min_index: self.min_index,
            last_applied: self.last_applied,
            _h: PhantomData,
            state_data,
        };

        candidate.with_log_mut(|log| log.read_latest_config(&mut candidate.config))?;
        candidate.with_log_mut(|log| log.inc_current_term())?;
        let id = candidate.id;
        candidate.with_log_mut(|log| log.set_voted_for(id))?;
        let last_log_term = candidate.with_log(|log| log.latest_log_term())?;

        let message = RequestVoteRequest {
            term: candidate.current_term()?,
            last_log_index: candidate.latest_log_index()?,
            last_log_term,
            is_voluntary_step_down: false,
        };

        candidate.config.with_remote_peers(&candidate.id, |id| {
            handler.send_peer_message(*id, PeerMessage::RequestVoteRequest(message.clone()));
            Ok(())
        });

        handler.state_changed(ConsensusState::Follower, &ConsensusState::Candidate);
        handler.set_timeout(Timeout::Election);
        Ok(candidate)
    }
}

/// The state associated with a Raft consensus module in the `Follower` state.
#[derive(Clone, Debug)]
pub struct Follower {
    /// The most recent leader of the follower. The leader is not guaranteed to be active, so this
    /// should only be used as a hint.
    pub(crate) leader: Option<ServerId>,

    delayed_vote_request: Option<(ServerId, RequestVoteRequest)>,
    can_vote: bool,
}

impl Follower {
    /// Returns a new `FollowerState`.
    pub(crate) fn new(can_vote: bool) -> Follower {
        Follower {
            leader: None,
            delayed_vote_request: None,
            can_vote,
        }
    }

    /// Sets a new leader.
    pub(crate) fn set_leader(&mut self, leader: ServerId) {
        self.leader = Some(leader);
    }
}
