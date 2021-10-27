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
use crate::persistent_log::Log;
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
/// Since the leader side must have the knowlege that snapshot is complete, every successfull result of `write_next_chunk`
/// will be sent to it. In case when None is returned, the None value is not passed to the leader's state machine
/// `read_next_chunk` function, meaning, the follower state machine MUST knows there will be no chunks from the chunk_data
/// message itself.
///
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
    /// The persistent log used by this state machine.
    ///
    /// Using and assotiated type allows state machine to do some optimizations, like not copying
    /// bytes between log and the machine itself, for example, if the log and state machine are
    /// implemented using the same storage engine.
    /// Those implementors who don't want this kind of specificity, may always fall toa generic
    /// implementation.
    type Log: Log;
    type Error: std::error::Error + Sized + 'static;

    /// Should return a read-only reference to the log, for consensus to use it directly
    fn log(&self) -> &Self::Log;

    /// Should return a mutable reference to the log, for consensus to use it directly
    fn log_mut(&mut self) -> &mut Self::Log;

    /// Applies a command to the state machine. A command should be already stored in the specified log index.
    /// Asynchronous machines may decide if last_applied should be increased at once, but if not,
    /// they shoul expect the same index to be requested multiple times.
    ///
    /// If `results_required` is true, should return an application-specific result value which will
    /// be forwarded to client. `results_required` may be false when the command is applied on a follower which is not
    /// expected to reply to a client.
    /// Function may return None in cases where result is not required or in any other situation.
    /// For example a client, which does not expect answer at all or uses `query()` for polling
    /// state machine.
    fn apply(
        &mut self,
        index: LogIndex,
        results_required: bool,
    ) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Queries a value of the state machine. Does not go through the durable log, or mutate the state machine.
    /// Returns an application-specific result value.
    fn query(&self, query: &[u8]) -> Result<Vec<u8>, Self::Error>;

    /// Must return currently applied index. The index must have the same persistency as the
    /// state machine itself and will be used by a leader to decide which log entries to
    /// delete from log.
    /// Tthat is, if a state machine is working asynchronously, its last_applied
    /// index may be behind conensus commit_index. In that case any unapplied entry
    /// will be applied even if it was tried before. This is a state machine's responsibility
    /// to decide on properly processing such duplicate applications.
    fn last_applied(&self) -> Result<LogIndex, Self::Error>;

    /// Should return information about current snapshot, if any.
    fn snapshot_info(&self) -> Result<Option<SnapshotInfo>, Self::Error>;

    /// Should take a snapshot of the state machine saving the snapshot's index.
    /// The snapshot is expected to be taken synchronously, implementor must expect,
    /// that read_snapshot_chunk will be called right away after this function
    fn take_snapshot(&mut self, index: LogIndex) -> Result<(), Self::Error>;

    /// Should give away the next chunk of the current snapshot based on previous chunk data.
    /// None value as a request means the first chunk is requested, so there is no follower side
    /// requests yet.
    /// Note that any possible shapshot metadata useful for the remote side, i.e. to create
    /// a correct request for the next chunk or to understand that there is no more chunks left,
    /// has to be inside the returned vector.
    fn read_snapshot_chunk(&self, request: Option<&[u8]>) -> Result<Vec<u8>, Self::Error>;

    /// Should write a part of externally initiated new snapshot make it current if
    /// all chunks are received. Is is also up to the implementation to decide if the snapshot
    /// is a new one and how it should be recreated based on log index.
    ///
    /// The index must be saved and returned from snapshot_info. The calling side
    /// considers index to be written right after the function returns None.
    ///
    /// The returned value is a request for the next chunk or None if the
    /// chunk is the last one. Any returned value including None will be sent to leader
    /// to let leader know that snapshot is done, but None will not be passed to leader's state
    /// machine, so it should not be confused with first chunk request.
    fn write_snapshot_chunk(
        &mut self,
        index: LogIndex,
        chunk_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, Self::Error>;
}
