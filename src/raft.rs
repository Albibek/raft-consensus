use std::mem;

use crate::error::{CriticalError, Error};
use crate::handler::Handler;
use crate::message::*;

use crate::state::State;
use crate::state_impl::{
    apply_admin_message, apply_client_message, apply_peer_message, apply_timeout, StateImpl,
};

use crate::{ClientId, ServerId};

use crate::candidate::Candidate;
use crate::follower::Follower;
use crate::leader::Leader;
use crate::persistent_log::Log;
use crate::state_machine::StateMachine;

/// An instance of a Raft consensus' single node.
/// The node API controls a client state machine, to which it applies entries in a globally consistent order.
///
/// Each event incoming from outside, like timer or a consensus packet, should be passed to
/// corresponsing method. After that the consensus reaction will be made through handler function
/// calls
// This structure is just a facade, hiding the internal state transitions from the caller.
// At the same time all the internal convenience and requirements like StateImpl correctness is
// left intact and mofifiable
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
    pub(crate) fn apply_peer_message(
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
    fn apply_client_message(
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
        request: &AdminMessage,
    ) -> Result<(), Error> {
        self.state.apply_admin_message(handler, request)
    }
}

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
        request: &AdminMessage,
    ) -> Result<(), Error> {
        proxy_state!(self, s, apply_admin_message(s, handler, request)?)
    }

    pub(crate) fn is_leader(&self) -> bool {
        if let CurrentState::Leader(_) = self {
            true
        } else {
            false
        }
    }

    pub(crate) fn is_follower(&self) -> bool {
        if let CurrentState::Follower(_) = self {
            true
        } else {
            false
        }
    }

    pub(crate) fn is_candidate(&self) -> bool {
        if let CurrentState::Candidate(_) = self {
            true
        } else {
            false
        }
    }
}
