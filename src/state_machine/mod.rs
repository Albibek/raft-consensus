//! A `StateMachine` is a single instance of a distributed application. It is the consensus
//! responsibility to take commands from the `Client` and apply them to each `StateMachine`
//! instance in a globally consistent order.

// FIXME pub mod channel;
pub mod null;

use bytes::Bytes;

//pub use crate::state_machine::channel::ChannelStateMachine;
use crate::persistent_log::Log;
pub use crate::state_machine::null::NullStateMachine;
use crate::{LogIndex, Peer, Term};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// Last index of the entry contained in the snapshot
    pub index: LogIndex,

    /// Term of the last entry contained in the snapshot
    pub term: Term,

    /// The latest consensus config known to snapshot
    pub config: Vec<Peer>,

    /// Size of snapshot in bytes (for decisions about snapshotting at the consensus level,
    /// for example, comparing log size with snapshot size)
    pub size: usize,
}

impl Default for SnapshotInfo {
    fn default() -> Self {
        Self {
            index: LogIndex(0),
            term: Term(0),
            config: Vec::new(),
            size: 0,
        }
    }
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
    /// they should expect the same index to be requested multiple times along with having gaps
    /// between machine's last_applied and the requested index.
    ///
    /// If `results_required` is true, should return an implementation-specific result value which will
    /// be forwarded to client. `results_required` may be false when the command is applied on a follower which is not
    /// expected to reply to a client.
    ///
    /// It should be noted, that despite of possible machine asynchronousness, the result have to
    /// be returned immediately, i.e. potentially before the real persistence. For example, it could be a good
    /// idea to have some "queued" mesage, or just returning None and requiring clients to poll the
    /// machine later.
    ///
    /// Function may return None in cases where result is delayed, not required or, in any other situation.
    /// In case of None being returned, no response will be sent to the client after calling this
    /// function.
    fn apply(
        &mut self,
        index: LogIndex,
        results_required: bool,
    ) -> Result<Option<Bytes>, Self::Error>;

    /// Queries a value of the state machine. Goes right from the clients, bypassing the persistent log, but
    /// can mutate the state machine if required by the implementation.
    ///
    /// Returns a state machine specific result value.
    fn query(&mut self, query: Bytes) -> Result<Bytes, Self::Error>;

    /// Must return currently applied index. The index must have the same persistency as the
    /// state machine itself and will be used by a leader to decide which log entries to
    /// delete from log.
    /// If a state machine is working asynchronously, its last_applied
    /// index may be behind conensus commit_index. In that case any unapplied entry
    /// will be applied even if it was tried before. This is a state machine's responsibility
    /// to decide on properly processing such duplicate applications.
    fn last_applied(&self) -> Result<LogIndex, Self::Error>;

    /// Should return information about current snapshot, if any.
    /// Must return only totally completed snapshot. The implementation must be aware, that
    /// log entries preceeding (inclusively) the returned index will be asked to be discarded
    /// in log.
    fn snapshot_info(&self) -> Result<Option<SnapshotInfo>, Self::Error>;

    /// Should take a snapshot of the state machine saving the snapshot's index and term.
    /// Implementation may decide the index to take snapshot at itself, but only at earlier or
    /// equal index, never later. The term must be taken from the entry at the index the snapshot
    /// is taken from.
    /// If asynchronous snapshotting is performed, the implementation have to be ready,
    /// that the attempts
    fn take_snapshot(&mut self, index: LogIndex, term: Term) -> Result<(), Self::Error>;

    /// Should give away the next chunk of the current snapshot based on previous chunk data.
    /// None value as a request means the first chunk is requested, so there is no follower side
    /// requests yet.
    /// Note that the returned value may contain any possible shapshot metadata useful for the remote side,
    /// i.e. to create a correct request for the next chunk or to understand that there is no more chunks left.
    fn read_snapshot_chunk(&self, request: Option<&[u8]>) -> Result<Vec<u8>, Self::Error>;

    /// Should write a part of externally initiated new snapshot.
    ///
    /// Numbering of chunks is left to implementation and.
    /// After receiving the last chunk:
    /// * If force flag is set, than the snapshot must be made current at
    /// the required index even if there was some previously taken
    /// snapshots or if there is other inconsistency.
    /// * If force flag is false, then is is up to the implementation to decide on
    /// whatever should be done on the snapshot. Still, it is good to know, that
    /// most probably snapshot_info will be called after writing the last chunk right away.
    ///
    /// The snapshot is expected to be consistent with index term and provided config.
    /// These values must be persisted and returned from snapshot_info if corresponsing
    /// snapshot is in place.
    ///
    /// The returned value have to be a request for the next chunk or None if no more
    /// chunks are required from the remote side. On follower this last None value will
    /// also be sent to leader, but will not be passed to read_snapshot_chunk at leader's state machine.
    fn write_snapshot_chunk(
        &mut self,
        index: LogIndex,
        term: Term,
        config: Option<Vec<Peer>>,
        force: bool,
        chunk_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Expected to flush all the pending state on disk, if any.
    ///
    /// Function is not called by consensus and only used when state machine is tested. It also
    /// may be useful in other scenarios, calling it is not prohibited.
    /// The default implementation always succeds without action.
    fn sync(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}
