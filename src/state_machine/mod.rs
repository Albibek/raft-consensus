//! A `StateMachine` is a single instance of a distributed application. It is the consensus
//! responsibility to take commands from the `Client` and apply them to each `StateMachine`
//! instance in a globally consistent order.
//!
//! The `StateMachine` is interface is intentionally generic so that any distributed application
//! needing consistent state can be built on it. For instance, a distributed hash table
//! application could implement `StateMachine`, with commands corresponding to `insert`, and
//! `remove`. Raft consensus would guarantee that the same order of `insert` and `remove`
//! commands would be seen by all consensus modules.

pub mod channel;
pub mod null;

pub use crate::state_machine::channel::ChannelStateMachine;
pub use crate::state_machine::null::NullStateMachine;

/// This trait is meant to be implemented such that the commands issued to it via `apply()` will
/// be reflected in the consuming application. Commands sent via `apply()` will be committed
/// in the cluster.
///
/// Note that the implementor is responsible for **not crashing** the state machine. The production
/// implementation is recommended not to use `.unwrap()`, `.expect()` or anything else that leads to `panic!()`
pub trait StateMachine {
    /// Applies a command to the state machine.
    /// if results_required is true, should return an application-specific result value.
    ///
    /// The situation when results are not requred happens on follower who only applies the log ant does not
    /// send any response to client). In such case the function may return empty vector to avoid allocation.
    fn apply(&mut self, command: &[u8], results_required: bool) -> Vec<u8>;

    /// Queries a value of the state machine. Does not go through the durable log, or mutate the state machine.
    /// Returns an application-specific result value.
    fn query(&self, query: &[u8]) -> Vec<u8>;

    /// Takes a snapshot of the state machine.
    fn snapshot(&self) -> Vec<u8>;

    /// Restores a snapshot of the state machine.
    fn restore_snapshot(&mut self, snapshot: &[u8]) -> ();
}
