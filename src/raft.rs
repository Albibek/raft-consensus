use std::{marker::PhantomData, mem};

use tracing::trace;

use crate::error::{CriticalError, Error};
use crate::handler::Handler;
use crate::message::*;

use crate::state::State;
use crate::state_impl::{
    apply_admin_message, apply_client_message, apply_peer_message, apply_timeout, StateImpl,
};

use crate::candidate::Candidate;
use crate::config::{ConsensusConfig, ConsensusOptions};
use crate::follower::Follower;
use crate::leader::Leader;
use crate::state_machine::StateMachine;
use crate::{AdminId, ClientId, LogIndex, Peer, ServerId};

#[derive(Clone, Debug)]
pub struct RaftBuilder<M>
where
    M: StateMachine,
{
    id: ServerId,
    state_machine: M,
    bootstrap_config: ConsensusConfig,
    force_bootstrap: bool,
    can_vote: Option<bool>,
    state_options: ConsensusOptions,
}

impl<M> RaftBuilder<M>
where
    M: StateMachine,
{
    pub fn new(id: ServerId, state_machine: M) -> Self {
        Self {
            id,
            state_machine,
            bootstrap_config: ConsensusConfig::new(
                vec![Peer {
                    id,
                    metadata: Vec::new(),
                }]
                .into_iter(),
            ),
            force_bootstrap: false,
            can_vote: None,
            state_options: ConsensusOptions::default(),
        }
    }

    /// provide config to be used if it cannot be read from log
    pub fn with_bootstrap_config(&mut self, config: ConsensusConfig) {
        self.bootstrap_config = config
    }

    pub fn with_force_bootstrap_config(&mut self, force: bool) {
        self.force_bootstrap = force
    }

    /// Enforce voting permission for node. By default, permission will be determined
    /// from consensus config and node ID
    pub fn with_vote_permission(&mut self, can_vote: bool) {
        self.can_vote = Some(can_vote)
    }

    /// Defines a number of election timeouts the catching up node can process a single
    /// AppendEntriesRequest
    pub fn with_log_timeouts(&mut self, log_timeouts: u32) {
        self.state_options.timeouts.max_log_timeouts = log_timeouts
    }

    /// Defines a number of election timeouts the catching up node can process a single
    /// chunk of snapshot
    pub fn with_snapshot_timeouts(&mut self, snapshot_timeouts: u32) {
        self.state_options.timeouts.max_snapshot_timeouts = snapshot_timeouts
    }

    /// Defines a number of election timeouts the catching up can catch up in total
    pub fn with_catch_up_timeouts(&mut self, total_timeouts: u32) {
        self.state_options.timeouts.max_total_timeouts = total_timeouts
    }

    pub fn start<H: Handler>(self, handler: &mut H) -> Result<Raft<M, H>, Error> {
        let Self {
            id,
            state_machine,
            bootstrap_config,
            force_bootstrap,
            can_vote,
            state_options,
        } = self;

        let mut state = State {
            id,
            config: bootstrap_config,
            state_machine,
            commit_index: LogIndex(0),
            min_index: LogIndex(0),
            state_data: Follower::new(),
            options: state_options,
            zero_log_index: LogIndex(0),
            latest_log_index: LogIndex(0),
            latest_volatile_log_index: LogIndex(0),
            latest_config_index: LogIndex(0),
            _h: PhantomData,
        };

        trace!(?id, "new consensus initialized");
        Ok(Raft {
            state: state.initialize(handler)?,
            _h: PhantomData,
        })
    }
}

/// An instance of a Raft consensus' single node.
/// The node API controls a client state machine, to which it applies entries in a globally consistent order.
///
/// Each event incoming from outside, like timer or a consensus packet, should be passed to
/// corresponsing method. After that the consensus reaction will be made through handler function
/// calls
// This structure is just a facade, hiding the internal state transitions from the caller.
// At the same time all the internal convenience and requirements like StateImpl correctness is
// left intact and mofifiable
//
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Raft<M, H>
where
    M: StateMachine,
    H: Handler,
{
    state: CurrentState<M, H>,
    _h: std::marker::PhantomData<H>,
}

impl<M, H> Raft<M, H>
where
    M: StateMachine,
    H: Handler,
{
    /// Apply a timeout event to the consensus
    // timeout is just the enum, it's easier to pass it by value
    pub fn apply_timeout(&mut self, handler: &mut H, timeout: Timeout) -> Result<(), Error> {
        let span = tracing::trace_span!(
            "timeout",
            state = %self.kind_str_short(),
            id = self.state.id().as_u64(),
            timeout = ?timeout
        );
        let _guard = span.enter();
        self.state.apply_timeout(handler, timeout)
    }

    /// Apply a standard API peer message to the consensus
    pub fn apply_peer_message(
        &mut self,
        handler: &mut H,
        from: ServerId,
        message: PeerMessage,
    ) -> Result<(), Error> {
        let span = tracing::trace_span!(
            "peer",
            state = %self.kind_str_short(),
            id = self.state.id().as_u64(),
            from = from.as_u64()
        );
        let _guard = span.enter();

        trace!(msg = ?message);
        self.state.apply_peer_message(handler, from, message)
    }

    /// Apply a client API message to the consensus
    /// Replies should also be processed, i.e. when proxying between
    /// follower and leader takes place
    pub fn apply_client_message(
        &mut self,
        handler: &mut H,
        from: ClientId,
        message: ClientMessage,
    ) -> Result<(), Error> {
        let span = tracing::trace_span!(
            "client",
            state = %self.kind_str_short(),
            id = self.state.id().as_u64(),
            client = %from
        );
        let _guard = span.enter();
        self.state.apply_client_message(handler, from, message)
    }

    /// Apply an admin command API request message to the consensus
    pub fn apply_admin_message(
        &mut self,
        handler: &mut H,
        from: AdminId,
        request: AdminMessage,
    ) -> Result<(), Error> {
        let span = tracing::trace_span!(
            "admin",
            state = %self.kind_str_short(),
            id = self.state.id().as_u64(),
            client = %from
        );
        let _guard = span.enter();
        self.state.apply_admin_message(handler, from, request)
    }

    /// Initiate a check for log compaction procedure. Since many compaction strategies may exist
    /// depending on the log and state machine implementations, the decision of the moments
    /// to take the snapshot (i.e. timing, byte sizes of log and snapshot, etc.)
    /// is left to a caller.
    ///
    /// The force flag requests an enforcing of the snapshot, but may still provide
    /// the refusal to make it at the moment.
    ///
    /// The return value will show if the snapshoting procedure happened.
    pub fn check_compaction(&mut self, handler: &mut H, force: bool) -> Result<bool, Error> {
        let span = tracing::trace_span!(
            "check_compaction",
            state = %self.kind_str_short(),
            id = self.state.id().as_u64(),
        );
        let _guard = span.enter();
        self.state.check_compaction(handler, force)
    }
}

/// Some functions to use for querying log and state machine externally
impl<M, H> Raft<M, H>
where
    M: StateMachine,
    H: Handler,
{
    /// borrow the consensus log
    pub fn log(&self) -> Option<&M::Log> {
        match self.state {
            CurrentState::Lost => None,
            CurrentState::Leader(ref s) => Some(&s.state_machine.log()),
            CurrentState::Follower(ref s) => Some(&s.state_machine.log()),
            CurrentState::Candidate(ref s) => Some(&s.state_machine.log()),
        }
    }

    ///
    pub fn log_mut(&mut self) -> Option<&mut M::Log> {
        match self.state {
            CurrentState::Lost => None,
            CurrentState::Leader(ref mut s) => Some(s.state_machine.log_mut()),
            CurrentState::Follower(ref mut s) => Some(s.state_machine.log_mut()),
            CurrentState::Candidate(ref mut s) => Some(s.state_machine.log_mut()),
        }
    }

    pub fn state_machine(&self) -> Option<&M> {
        match self.state {
            CurrentState::Lost => None,
            CurrentState::Leader(ref s) => Some(&s.state_machine),
            CurrentState::Follower(ref s) => Some(&s.state_machine),
            CurrentState::Candidate(ref s) => Some(&s.state_machine),
        }
    }

    pub fn state_machine_mut(&mut self) -> Option<&mut M> {
        match self.state {
            CurrentState::Lost => None,
            CurrentState::Leader(ref mut s) => Some(&mut s.state_machine),
            CurrentState::Follower(ref mut s) => Some(&mut s.state_machine),
            CurrentState::Candidate(ref mut s) => Some(&mut s.state_machine),
        }
    }

    pub fn into_inner(self) -> Option<(ServerId, M)> {
        let Self { state, .. } = self;
        match state {
            CurrentState::Lost => None,
            CurrentState::Leader(State {
                id, state_machine, ..
            }) => Some((id, state_machine)),
            CurrentState::Follower(State {
                id, state_machine, ..
            }) => Some((id, state_machine)),
            CurrentState::Candidate(State {
                id, state_machine, ..
            }) => Some((id, state_machine)),
        }
    }
}

/// These a special functions only available in debug builds.
/// They can be only used for testing and like looking into log or state machine
/// but not allowed in production to avoid unintentional damage
#[cfg(debug_assertions)]
impl<M, H> Raft<M, H>
where
    M: StateMachine,
    H: Handler,
{
    pub fn kind(&self) -> ConsensusState {
        match self.state {
            CurrentState::Lost => panic!("state is lost"),
            CurrentState::Leader(_) => ConsensusState::Leader,
            CurrentState::Follower(_) => ConsensusState::Follower,
            CurrentState::Candidate(_) => ConsensusState::Candidate,
        }
    }

    pub fn kind_str_short(&self) -> &'static str {
        match self.state {
            CurrentState::Lost => "FAILED",
            CurrentState::Leader(_) => "L",
            CurrentState::Follower(_) => "F",
            CurrentState::Candidate(_) => "C",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CurrentState<M, H>
where
    M: StateMachine,
    H: Handler,
{
    Leader(State<M, H, Leader>),
    Follower(State<M, H, Follower>),
    Candidate(State<M, H, Candidate>),
    Lost,
}

macro_rules! proxy_state {
    ($i:ident, $s:ident, $e:expr) => {
        match $i {
            CurrentState::Lost => Err(Error::Critical(CriticalError::Unrecoverable)),
            CurrentState::Leader($s) => Ok($e),
            CurrentState::Candidate($s) => Ok($e),
            CurrentState::Follower($s) => Ok($e),
        }
    };
}

impl<M, H, T> From<State<M, H, T>> for CurrentState<M, H>
where
    M: StateMachine,
    H: Handler,
    State<M, H, T>: StateImpl<M, H>,
{
    fn from(t: State<M, H, T>) -> Self {
        t.into_consensus_state()
    }
}

impl<M, H> CurrentState<M, H>
where
    M: StateMachine,
    H: Handler,
{
    fn pop(&mut self) -> Result<Self, Error> {
        let mut s = CurrentState::Lost;
        if let CurrentState::Lost = self {
            return Err(Error::Critical(CriticalError::Unrecoverable));
        }

        mem::swap(&mut s, self);
        Ok(s)
    }

    fn apply_timeout(&mut self, handler: &mut H, timeout: Timeout) -> Result<(), Error> {
        let prev_state = self.pop()?;
        let new_state = proxy_state!(prev_state, s, apply_timeout(s, handler, timeout)?)?;
        *self = new_state;
        Ok(())
    }

    fn apply_peer_message(
        &mut self,
        handler: &mut H,
        from: ServerId,
        message: PeerMessage,
    ) -> Result<(), Error> {
        let prev_state = self.pop()?;
        let new_state = proxy_state!(
            prev_state,
            s,
            apply_peer_message(s, handler, from, message)?
        )?;
        *self = new_state;
        Ok(())
    }

    fn apply_client_message(
        &mut self,
        handler: &mut H,
        from: ClientId,
        message: ClientMessage,
    ) -> Result<(), Error> {
        proxy_state!(self, s, apply_client_message(s, handler, from, message)?)
    }

    fn apply_admin_message(
        &mut self,
        handler: &mut H,
        from: AdminId,
        request: AdminMessage,
    ) -> Result<(), Error> {
        proxy_state!(self, s, apply_admin_message(s, handler, from, request)?)
    }
    pub fn check_compaction(&mut self, handler: &mut H, force: bool) -> Result<bool, Error> {
        // we don't need packet conversions here, so no generic function required
        proxy_state!(self, s, s.check_compaction(handler, force)?)
    }

    fn id(&self) -> ServerId {
        match self {
            // TODO impl tracing::Value to avoid unreachable
            CurrentState::Lost => unreachable!(),
            CurrentState::Leader(s) => s.id,
            CurrentState::Candidate(s) => s.id,
            CurrentState::Follower(s) => s.id,
        }
    }
}
