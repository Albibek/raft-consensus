//! A `StateMachine` is a single instance of a distributed application. It is the consensus
//! responsibility to take commands from the `Client` and apply them to each `StateMachine`
//! instance in a globally consistent order.
//!
//! The `StateMachine` is interface is intentionally generic so that any distributed application
//! needing consistent state can be built on it. For instance, a distributed hash table
//! application could implement `StateMachine`, with commands corresponding to `insert`, and
//! `remove`. Raft consensus would guarantee that the same order of `insert` and `remove`
//! commands would be seen by all consensus modules.

// FIXME pub mod channel;
pub mod null;

//pub use crate::state_machine::channel::ChannelStateMachine;
pub use crate::state_machine::null::NullStateMachine;
use crate::LogIndex;

pub struct SnapshotInfo {
    /// Index the snapshot was taken at
    pub index: LogIndex,
    /// Size of snapshot in bytes (for decisions about snapshotting)
    pub size: usize,
    /// Number of chunks in snapshot (0 means single chunk and will be treated the same way as 1)
    pub chunks: usize,
    /// Indicates that snapshot was finished
    pub finished: bool,
    /// Any additional data, this state machine may want to share with another nodes' machines
    pub metadata: Option<Vec<u8>>,
}

/// This trait is meant to be implemented such that the commands issued to it via `apply()` will
/// be reflected in the consuming application. Commands sent via `apply()` will be committed
/// in the cluster.
///
/// To provide snapshotting, the state machine should operate on two snapshots:
/// * Current: requested by node and made by machine itself
/// * New: sent by leader to catching up nodes and slow followers. Should replace the current when
/// finished
///
/// Snapshots are read and written in chunks. State machine should decide the chunk size and the
/// need for chunks at all. State machine should not do snapshots without command from consensus.
///
/// Note that the implementor is responsible for **not crashing** the state machine. The production
/// implementation is recommended not to use `.unwrap()`, `.expect()` or anything else that leads to `panic!()`
/// Instead, the functions should return the StateMachine::Error. Any kind of error will be treated by consensus as
/// unrecoverable.
pub trait StateMachine {
    type Error: std::error::Error + Sized + 'static;
    /// Applies a command to the state machine.
    /// if results_required is true, should return an application-specific result value.
    ///
    /// The situation when results are not requred happens on follower who only applies the entry from log, but does
    /// not require sending any response. In such case the function may return an empty vector to avoid allocation.
    fn apply(&mut self, command: &[u8], results_required: bool) -> Result<Vec<u8>, Self::Error>;

    /// Queries a value of the state machine. Does not go through the durable log, or mutate the state machine.
    /// Returns an application-specific result value.
    fn query(&self, query: &[u8]) -> Result<Vec<u8>, Self::Error>;

    /// Should return information about current snapshot, if any.
    /// There are cases where metadata is not required. `meta_required` flag indicates,
    /// that metadata field will not e read and may be set to None or empty vector to avoid allocations.
    fn snapshot_info(&self, meta_required: bool) -> Result<Option<SnapshotInfo>, Self::Error>;

    /// Should take a snapshot of the state machine saving the snapshot's index.
    fn take_snapshot(&mut self, index: LogIndex) -> Result<(), Self::Error>;

    /// Should give away a next chunk of the current snapshot.
    fn read_snapshot_chunk(&self, chunk: usize) -> Result<Vec<u8>, Self::Error>;

    /// Should initiate a procedure of creating a new snapshot taken at external state machine
    /// Any previous new snapshots may be discarded at will.
    fn init_new_snapshot(&mut self, info: SnapshotInfo) -> Result<(), Self::Error>;

    /// Should write a part of externally initiated new snapshot and make it current if
    /// all chunks are received
    fn write_snapshot_chunk(&mut self, chunk: usize, chunk_bytes: &[u8])
        -> Result<(), Self::Error>;
}
