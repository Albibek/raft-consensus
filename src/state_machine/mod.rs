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

    /// Size of snapshot in bytes (for decisions about snapshotting at the consensus level, i.e.
    /// comparing log size with snapshot size)
    pub size: usize,
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
/// State machine should not do snapshots without command from consensus.
///
/// Snapshots can be read and written in chunks. State machine should decide the chunk shape, specifically, their size,
/// sequence and the need for chunks at all. Since the consensus follows the reactive model, requests and responses for
/// chunks can only be sent sequentially, i.e. every next request will only be sent after receiving a response. This
/// means all kinds of windowing strategies should be performed on a consensus calling level by
/// emulating an `InstallSnapshotResponse` messages.
///
/// It is always the leader's state machine that decides that snapshot is
/// complete, which means every correct response from InstallSnapsnot will be delivered to it.
/// Only after the snapshot is transferred to follower completely and the very last chunk of it
/// is confirmed, the leader will consider follower index changed and will recount the majority
/// according to this new data.
///
/// For a state machine implementation this means
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
    fn snapshot_info(&self) -> Result<Option<SnapshotInfo>, Self::Error>;

    /// Should take a snapshot of the state machine saving the snapshot's index.
    /// The snapshot is expected to be taken synchronously, implementor must expect,
    /// that read_snapshot_chunk will be called right away after this function
    fn take_snapshot(&mut self, index: LogIndex) -> Result<(), Self::Error>;

    /// Should give away the next chunk of the current snapshot based on previous chunk data.
    /// Option in query can be used to receive the first chunk.
    /// Returning None means there is no chunks left, in which case the snapshot will be considered read completely.
    /// Returning None as the first chunk will be equivalent to not having an active snapshot
    /// ready, in which case the snapshot sending will be delayed to the heartbeat timeout.
    ///
    /// Note that any possible shapshot metadata useful for the remote side, i.e. to create
    /// a correct query for the next chunk, has to be inside the returned vector.
    fn read_snapshot_chunk(&self, query: Option<&[u8]>) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Should write a part of externally initiated new snapshot make it current if
    /// all chunks are received. Is is also up to the implementation to decide if the snapshot
    /// is a new one and how it should be recreated based on log index.
    ///
    /// The index must be saved and returned from snapshot_info. The calling side
    /// considers index to be written right after the function returns None.
    ///
    /// The returned value is a request for the next chunk or None if the
    /// chunk is the last one.
    fn write_snapshot_chunk(
        &mut self,
        index: LogIndex,
        chunk_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, Self::Error>;
}
