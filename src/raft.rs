use std::{marker::PhantomData, mem};

use log::trace;

use crate::error::{CriticalError, Error};
use crate::handler::Handler;
use crate::message::*;

use crate::state::State;
use crate::state_impl::{
    apply_admin_message, apply_client_message, apply_peer_message, apply_timeout, StateImpl,
};

use crate::candidate::Candidate;
use crate::config::ConsensusConfig;
use crate::follower::Follower;
use crate::leader::Leader;
use crate::persistent_log::Log;
use crate::persistent_log::{LogEntry, LogEntryData};
use crate::state_machine::StateMachine;
use crate::{AdminId, ClientId, LogIndex, Peer, ServerId};

#[derive(Clone, Debug)]
pub struct RaftBuilder<L, M>
where
    L: Log,
    M: StateMachine,
{
    id: ServerId,
    log: L,
    state_machine: M,
    bootstrap_config: ConsensusConfig,
    force_bootstrap: bool,
    can_vote: Option<bool>,
}

impl<L, M> RaftBuilder<L, M>
where
    L: Log,
    M: StateMachine,
{
    pub fn new(id: ServerId, log: L, state_machine: M) -> Self {
        Self {
            id,
            log,
            state_machine,
            bootstrap_config: ConsensusConfig {
                peers: vec![Peer {
                    id: ServerId(0),
                    metadata: Vec::new(),
                }],
            },
            force_bootstrap: false,
            can_vote: None,
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

    pub fn start<H: Handler>(self, handler: &mut H) -> Result<Raft<L, M, H>, Error> {
        let Self {
            id,
            log,
            state_machine,
            bootstrap_config,
            force_bootstrap,
            can_vote,
        } = self;

        let config = if let Some(index) = log
            .latest_config_index()
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?
        {
            if index == LogIndex(0) || force_bootstrap {
                bootstrap_config
            } else {
                let mut entry = LogEntry::default();
                log.read_entry(index, &mut entry)
                    .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
                if let LogEntryData::Config(config) = entry.data {
                    config
                } else {
                    return Err(Error::unreachable(module_path!()));
                }
            }
        } else {
            bootstrap_config
        };

        let can_vote = if let Some(can_vote) = can_vote {
            can_vote
        } else {
            config.has_peer(id)
        };

        if can_vote {
            handler.set_timeout(Timeout::Election);
        }

        let state = State {
            id,
            config,
            log,
            state_machine,
            commit_index: LogIndex(0),
            // TODO: after snapshots min_index should be the index from snapshot
            min_index: LogIndex(0),
            last_applied: LogIndex(0),
            state_data: Follower::new(can_vote),
            _h: PhantomData,
        };
        trace!("raft id={} initialized", id);
        Ok(Raft {
            state: state.into(),
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Raft<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    state: CurrentState<L, M, H>,
    _h: std::marker::PhantomData<H>,
}

impl<L, M, H> Raft<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    /// Apply a timeout event to the consensus
    // timeout is just the enum, it's easier to pass it by value
    pub fn apply_timeout(&mut self, handler: &mut H, timeout: Timeout) -> Result<(), Error> {
        self.state.apply_timeout(handler, timeout)
    }

    /// Apply a standard API peer message to the consensus
    pub fn apply_peer_message(
        &mut self,
        handler: &mut H,
        from: ServerId,
        message: &PeerMessage,
    ) -> Result<(), Error> {
        self.state.apply_peer_message(handler, from, message)
    }

    /// Apply a client API message to the consensus
    /// Replies should also be processed, i.e. when proxying between
    /// follower and leader takes place
    pub fn apply_client_message(
        &mut self,
        handler: &mut H,
        from: ClientId,
        message: &ClientMessage,
    ) -> Result<(), Error> {
        self.state.apply_client_message(handler, from, message)
    }

    /// Apply an admin command API request message to the consensus
    pub fn apply_admin_message(
        &mut self,
        handler: &mut H,
        from: AdminId,
        request: &AdminMessage,
    ) -> Result<(), Error> {
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
        self.state.check_compaction(handler, force)
    }
}

/// Some functions to use for querying log and state machine externally
impl<L, M, H> Raft<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    /// borrow the consensus log
    pub fn log(&self) -> Option<&L> {
        match self.state {
            CurrentState::Lost => None,
            CurrentState::Leader(ref s) => Some(&s.log),
            CurrentState::Follower(ref s) => Some(&s.log),
            CurrentState::Candidate(ref s) => Some(&s.log),
        }
    }

    ///
    pub fn log_mut(&mut self) -> Option<&mut L> {
        match self.state {
            CurrentState::Lost => None,
            CurrentState::Leader(ref mut s) => Some(&mut s.log),
            CurrentState::Follower(ref mut s) => Some(&mut s.log),
            CurrentState::Candidate(ref mut s) => Some(&mut s.log),
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

    pub fn into_inner(self) -> Option<(ServerId, L, M)> {
        let Self { state, .. } = self;
        match state {
            CurrentState::Lost => None,
            CurrentState::Leader(State {
                id,
                log,
                state_machine,
                ..
            }) => Some((id, log, state_machine)),
            CurrentState::Follower(State {
                id,
                log,
                state_machine,
                ..
            }) => Some((id, log, state_machine)),
            CurrentState::Candidate(State {
                id,
                log,
                state_machine,
                ..
            }) => Some((id, log, state_machine)),
        }
    }
}

/// These a special functions only available in debug builds.
/// They can be only used for testing and like looking into log or state machine
/// but not allowed in production to avoid unintentional damage
#[cfg(debug_assertions)]
impl<L, M, H> Raft<L, M, H>
where
    L: Log,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CurrentState<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: Handler,
{
    Leader(State<L, M, H, Leader>),
    Follower(State<L, M, H, Follower>),
    Candidate(State<L, M, H, Candidate>),
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

impl<L, M, H, T> From<State<L, M, H, T>> for CurrentState<L, M, H>
where
    L: Log,
    M: StateMachine,
    H: Handler,
    State<L, M, H, T>: StateImpl<L, M, H>,
{
    fn from(t: State<L, M, H, T>) -> Self {
        t.into_consensus_state()
    }
}

impl<L, M, H> CurrentState<L, M, H>
where
    L: Log,
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
        message: &PeerMessage,
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
        message: &ClientMessage,
    ) -> Result<(), Error> {
        proxy_state!(self, s, apply_client_message(s, handler, from, message)?)
    }

    fn apply_admin_message(
        &mut self,
        handler: &mut H,
        from: AdminId,
        request: &AdminMessage,
    ) -> Result<(), Error> {
        proxy_state!(self, s, apply_admin_message(s, handler, from, request)?)
    }
    pub fn check_compaction(&mut self, handler: &mut H, force: bool) -> Result<bool, Error> {
        // we don't need packet conversions here, so no generic function required
        proxy_state!(self, s, s.check_compaction(handler, force)?)
    }
}
