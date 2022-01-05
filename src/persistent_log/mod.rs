//! The persistent storage of Raft state.
//!
//! In your consuming application you may want to implement this trait on one of your structures to
//! have your own facility for storing Raft log
//!
//! *Note:* This is not necessary in your consuming application. The `Log` is meant to be
//! internally used by the library, while letting this library authors no to be opinionated about
//! how data is stored.

// reimports

// TODO: feature gate all types of logs
//FIXME
//pub mod fs;
pub mod mem;

use crate::message::ClientGuarantee;
// FIXME:
//pub use persistent_log::fs::FsLog;
pub use crate::persistent_log::mem::MemLog;

// this module imports
use std::fmt::Debug;

use crate::config::ConsensusConfig;
use crate::message::peer::{Entry, EntryData};
use crate::{LogIndex, ServerId, Term};

use bytes::Bytes;
#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

use thiserror::Error as ThisError;

/// A layer of persistence to store Raft log and state data.
/// Should implement a log storage for appending raft log entries
/// along with storing iseveral specific values, like voted_for, current cluster configuration.
///
/// The implementor is responsible for keeping ordering and leaving no gaps in log.
///
/// For example when batching is used, the entries may not be persisted right away. In that case,
/// latest_log_index should not forward to a new value until the corresponsing entries
/// are synced to disk.
///
/// The latter automatically means, that the tradeoff between cluster stability
/// and probable performance benefits lays on the implementor.
/// Since Raft always counts on entries persisted to the log, it does not manage the
/// indexes of the tail (i.e. just appended) entries. If implementation wants
/// to guarantee the persistence of these entries, it should not return from
/// the function until the entries are synced properly, or should not shift its
/// latest index towards entries being persisted yet. To avoid resending of these
/// entries, the latest_volatile_index is used. The index returned by this function
/// will be sent to leader, and leader will try to put only entries following
/// this index.
///
/// *Important edge cases to consider*
/// All of thefollowing MUST be implemented correctly. It is strongly
/// recommended to unit-test the implementation using the testing::log::LogTester
/// module (enabled by the "testing" feature).
///
/// * Log indexes in Raft start with 1. LogIndex(0) is reserved as special and
/// means the freshly initialized log with no entries (old or new). The entry
/// data of a term of entry at LogIndex(0) will never be requested.
/// * Due to snapshotting, the log may become containing no entries at all. This means the
/// return values from first_index, last_index and term_of may become confusing due to
/// the log entry itself not existing in log while its meta information is still required.
/// So to clear things out the implementation is expected to return the following:
///     * fisrt_index must return the index of last seen entry
///     * latest_index must return the index of last seen entry
///     * latest_volatile_index may return the higher index, than last_log
///     * term_of must return the term of last seen entry; this is the ONLY case
///   It is also expected from the implementation that these values are persisted between log
///   restarts, that is when the empty log which had entries before is initialized again,
///   the indexes have to remain as they were before the restart. In this case, the
///   need of retransferring snapshot will be eliminated which may sava a potentially
///   big amount of network traffic.
///
///   This edge case is the only one where any metadata of non-existing entry is
///   requested. The consensus gives guarantee to do all other function calls or arguments
///   only on existing values. This means the implementation is free to return an error
///   if it couldn't found the entry, which, in turn will mean either a bug or data corruption
///   in the implementaion itself or a bug in the consensus implementation.
pub trait Log {
    type Error: std::error::Error + Sized + 'static;

    /// Should return the index of the latest log entry ever appended to log. To avoid data losses
    /// this index should point the data that is guaranteed to be persisted.
    /// Should return LogIndex(0) if the log is freshly created.
    fn latest_index(&self) -> Result<LogIndex, Self::Error>;

    /// May be overriden  to return the index that was lately received by the log, but may not be yet persisted.
    /// Will be used as a tip only, to avoid resending excessive data to followers.
    fn latest_volatile_index(&self) -> Result<LogIndex, Self::Error> {
        self.latest_index()
    }

    /// Should return the index of the first log entry (0 if the log is empty).
    fn first_index(&self) -> Result<LogIndex, Self::Error>;

    /// Should return term corresponding to the entry at `index`.
    /// The consensus guarantees to always request a term of the existing
    /// entry
    ///
    /// Just for the record, the term at LogIndex(0) should be Term(0), but
    /// this is also promised to not be requested.
    fn term_of(&self, index: LogIndex) -> Result<Term, Self::Error>;

    /// Delete or mark as invalid all entries since specified log index, inclusively.
    /// Return the new index. It is allowed to return any index lower than requested.
    fn discard_since(&mut self, index: LogIndex) -> Result<LogIndex, Self::Error>;

    /// The implementation may consider all entries until specified index inclusively
    /// (i.e. discarding the entry at `index` too) as compacted into a snapshot.
    /// Still, it is allowed to leave any number of entries before `index`.
    fn discard_until(&mut self, index: LogIndex) -> Result<(), Self::Error>;

    /// Should read the entry at the provided log index into an entry provided by reference.
    /// Consensus guarantees that no entries at indexes higher than latest_log_index will be read.
    /// If index is too low, LogEntry should be set to LogEntryData::Empty
    fn read_entry(&self, index: LogIndex, dest: &mut LogEntry) -> Result<(), Self::Error>;

    /// Should return the metadata of the entry at the specified index. No indexes higher
    /// than latest_log_index will be requested.
    fn entry_meta_at(&self, index: LogIndex) -> Result<LogEntryMeta, Self::Error>;

    /// Must append an entry to the log. The entries sync to disk and index shifting should be decided by implementation,
    /// but the entries must be persisted with no gaps and incoming ordering.
    /// In case of applying verlapping entries, all previous ones must be considered invalid and
    /// have to be overwritten. The consensus guarantees that only existing entries (i.e. stored
    /// at or after the first index) will be overwritten.
    ///
    /// `start` is pointing at the index where the first entry should be placed at. Consensus
    /// guarantees the absence of gaps with regards to `discard_since` function.
    ///
    /// Implementations are recommended to NOT rely on term being always increasing because
    /// of possible overflows in future implementations of consensus.
    ///
    /// Configuration change entries should only be persisted, but not marked as latest
    /// affecting further calls to `latest_config*` functions. Such marking will be done later,
    /// when configuration is committed by calling set_latest_config function explicitly.
    fn append_entries(&mut self, start: LogIndex, entries: &[LogEntry]) -> Result<(), Self::Error>;

    /// Should return the latest known term.
    /// For the empty log should be Term(0).
    fn current_term(&self) -> Result<Term, Self::Error>;

    /// Should set the current term to the provided value.
    /// The implementations are recommended to NOT assume the term to be always greater
    /// than or equal to the previous term, due to possible future overflows.
    /// Recommended to be synchronous, delays in persisting term may break the
    /// consensus.
    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error>;

    /// Returns the id of the candidate node voted for in the current term, if any.
    fn voted_for(&self) -> Result<Option<ServerId>, Self::Error>;

    /// Should save a provided candidate id the node voted for the current term.
    /// None means voted_for should be unset(i.e. cleared)
    fn set_voted_for(&mut self, server: Option<ServerId>) -> Result<(), Self::Error>;

    /// Should persist the provided configuration and its index.
    ///
    /// The provided values should be persisted separately from the original config entry in the log,
    /// which may not exist there at the moment of calling (i.e. because of snapshotting).
    /// Still, the consensus will guarantee that the configuration at the index is the
    /// same as the one in the log.
    ///
    /// Recommended to be synchronous, delays in persisting may break the
    /// consensus.
    fn set_latest_config(
        &mut self,
        config: &ConsensusConfig,
        index: LogIndex,
    ) -> Result<(), Self::Error>;

    /// Should return the latest saved config entry if any.
    fn latest_config(&self) -> Result<Option<ConsensusConfig>, Self::Error>;

    /// Should return the index of latest saved config entry if any.
    fn latest_config_index(&self) -> Result<Option<LogIndex>, Self::Error>;

    /// Expected to flush all the volatile entities on disk, if any.
    ///
    /// Sync is not called by consensus and only used when log is tested. It also
    /// may be useful in other scenarios, calling it is not prohibited.
    /// The default implementation always succeds.
    fn sync(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// The record to be added into log
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct LogEntry {
    pub term: Term,
    pub data: LogEntryData,
}

/// Kinds of a log entry without internal data
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum LogEntryMeta {
    Empty,
    Proposal(ClientGuarantee),
    Config(ConsensusConfig),
}

/// Kinds of a log entry to be processed
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum LogEntryData {
    Empty,
    Proposal(Bytes, ClientGuarantee),
    Config(ConsensusConfig),
}

impl LogEntry {
    pub fn meta(&self) -> LogEntryMeta {
        match &self.data {
            LogEntryData::Empty => LogEntryMeta::Empty,
            LogEntryData::Proposal(_, guarantee) => LogEntryMeta::Proposal(guarantee.clone()),
            LogEntryData::Config(config) => LogEntryMeta::Config(config.clone()),
        }
    }

    /// returns data stored in proposal if the entry matches type
    pub fn try_proposal_data(&self) -> Option<Bytes> {
        if let LogEntryData::Proposal(ref data, _) = self.data {
            Some(data.clone())
        } else {
            None
        }
    }
}

impl Default for LogEntry {
    fn default() -> Self {
        Self {
            term: Term(0),
            data: LogEntryData::Empty,
        }
    }
}

impl From<LogEntry> for Entry {
    fn from(e: LogEntry) -> Self {
        Entry {
            term: e.term,
            data: match e.data {
                LogEntryData::Empty => EntryData::Noop,
                LogEntryData::Proposal(proposal, guarantee) => {
                    EntryData::Proposal(proposal, guarantee)
                }
                LogEntryData::Config(c) => EntryData::Config(c, false),
            },
        }
    }
}

impl LogEntry {
    pub fn new_empty(term: Term, data: Vec<u8>) -> Self {
        Self {
            term,
            data: LogEntryData::Empty,
        }
    }

    pub fn new_proposal(term: Term, data: Bytes, guarantee: ClientGuarantee) -> Self {
        Self {
            term,
            data: LogEntryData::Proposal(data, guarantee),
        }
    }

    pub fn new_config(term: Term, config: ConsensusConfig) -> Self {
        Self {
            term,
            data: LogEntryData::Config(config),
        }
    }
}

///// The reference to a record to be added into log
//#[derive(Debug, Clone, PartialEq, Eq)]
//pub struct LogEntryRef<'a> {
//pub term: Term,
//pub data: LogEntryDataRef<'a>,
//}

///// Kinds of a log entry to be processed
//#[derive(Debug, Clone, PartialEq, Eq)]
//pub enum LogEntryDataRef<'a> {
//Empty,
//Proposal(&'a [u8], ClientId, ClientGuarantee),
//Config(&'a ConsensusConfig, AdminId),
//}

//impl<'a> From<LogEntryRef<'a>> for LogEntry {
//fn from(e: LogEntryRef<'a>) -> Self {
//LogEntry {
//term: e.term,
//data: match e.data {
//LogEntryDataRef::Empty => LogEntryData::Empty,
//LogEntryDataRef::Proposal(proposal, client, guarantee) => {
//LogEntryData::Proposal(proposal.to_vec(), client, guarantee)
//}
//LogEntryDataRef::Config(c, admin) => LogEntryData::Config(c.clone(), admin),
//},
//}
//}
//}

//impl<'a> From<&'a LogEntryRef<'a>> for LogEntry {
//fn from(e: &'a LogEntryRef<'a>) -> Self {
//LogEntry {
//term: e.term,
//data: match e.data {
//LogEntryDataRef::Empty => LogEntryData::Empty,
//LogEntryDataRef::Proposal(proposal, client, guarantee) => {
//LogEntryData::Proposal(proposal.to_vec(), client, guarantee)
//}
//LogEntryDataRef::Config(c, admin) => LogEntryData::Config(c.clone(), admin),
//},
//}
//}
//}

//impl<'a> From<LogEntryRef<'a>> for Entry {
//fn from(e: LogEntryRef<'a>) -> Self {
//Entry {
//term: e.term,
//data: match e.data {
//LogEntryDataRef::Empty => EntryData::Noop,
//LogEntryDataRef::Proposal(proposal, client, guarantee) => {
//EntryData::Proposal(proposal.to_vec(), client, guarantee)
//}
//LogEntryDataRef::Config(c, admin) => EntryData::Config(c.clone(), false, admin),
//},
//}
//}
//}

//impl<'a> From<&'a LogEntryRef<'a>> for Entry {
//fn from(e: &'a LogEntryRef<'a>) -> Self {
//Entry {
//term: e.term,
//data: match e.data {
//LogEntryDataRef::Empty => EntryData::Noop,
//LogEntryDataRef::Proposal(proposal, client, guarantee) => {
//EntryData::Proposal(proposal.to_vec(), client, guarantee)
//}
//LogEntryDataRef::Config(c, admin) => EntryData::Config(c.clone(), false, admin),
//},
//}
//}
//}

/// A helper type for errors in persistent log. Contains typical errors, that could happen,
/// but no code depends on it, so it only provided to make implementor's life easier.
#[derive(ThisError, Debug)]
#[error("persistent log error")]
pub enum LogError {
    Version(u64, u64),
    BadIndex(LogIndex),
    NoConfig,
    Io(#[from] ::std::io::Error),
}

//#[cfg(test)]

// TODO: Test logs:
// * persists everything as expected
// * if log is non-volatile, persistence does not break on restarts
// * no gaps, keeps ordering
// * discards as requested
// * appended config change entries are not actualized in latest_config_*
