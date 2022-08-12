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

use crate::message::Urgency;
// FIXME:
//pub use persistent_log::fs::FsLog;
pub use crate::persistent_log::mem::MemLog;

// this module imports
use std::fmt::Debug;

use crate::message::peer::{Entry, EntryData};
use crate::{LogIndex, Peer, ServerId, Term};

use bytes::Bytes;
#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

use thiserror::Error as ThisError;

/// Log is a layer of persistence to store Raft entries in an ordered queue.
/// Implementation sof this trait should handle a log-like storage for appending entries
/// along with storing several specific values, like voted_for, current cluster configuration, etc.
///
/// The log is allowed have some entries pending to be persisted (for example not yet synced on disk)
/// but the corresponding indexes must only point to the start and the end of already persisted part
/// without gaps.
///
/// The latter automatically means, that the tradeoff between cluster stability
/// and probable performance benefits lays on the implementor.
///
/// Since Raft always counts on entries persisted to the log, it does not manage the
/// indexes of the tail (i.e. just appended, but not yet persisted) entries. If implementation wants
/// to guarantee the persistence of these entries, it should not return from
/// the function until the entries are synced properly, or should not shift its
/// latest index towards entries being persisted yet. To avoid resending of these
/// entries, the latest_volatile_index is used. The index returned by this function
/// will be sent to leader, and leader will try to put only entries following
/// this index.
///
///  To maintain all log states correctly, the implementation should keep track of:
///     * latest_log_index - the index of the last persisted entry
///     * zero_log_index - the index of entry, preceding the first existing entry in the log
///     * so called "zero term" - the term of the entry preceding the first existing entry in the log
///
/// With these two indexes and additional metadata it is possible to maintain any log state, including the empty one:
/// * for non-empty log the latest_log_index always point to the index of last persisted entry
/// * for empty log the latest_log_index points to zero_entry
/// * zero_entry_index is always pointing to non existing(or marked so) entry
/// * zero term must be kept separately
///
/// Note, that this also fits into the original whitepapers's requirements
/// on starting log indexes from LogIndex(1). In that case LogIndex(0) will be a zero entry
/// with zero term equaling to Term(0).
///
/// The consensus gives guarantee to do all other function calls or arguments
/// only on existing values. This means the implementation is free to return an error
/// if it couldn't found the entry, which, in turn will mean either a bug or data corruption
/// in the implementaion itself or a bug in the consensus implementation.
pub trait Log {
    type Error: std::error::Error + Sized + 'static;

    /// Should return the actual log state in form of current indexes:
    /// (
    ///   zero_index - the index before the first persisted entry,
    ///   latest_index - the index of last persisted entry,
    ///   latest_volatile_index - the index of last appended, but not persisted entry
    /// )
    ///
    /// In a freshly created log all indexes must be LogIndex(0)
    /// In the log, where nothing is persisted, latest_index must be equal to zero_index
    fn current_view(&self) -> Result<(LogIndex, LogIndex, LogIndex), Self::Error>;

    /// Should return term corresponding to the entry at `index`.
    /// The consensus guarantees to always request a term of the existing
    /// entry or the term of zero entry(aka zero term).
    fn term_of(&self, index: LogIndex) -> Result<Term, Self::Error>;

    /// Should consider all entries up to and including the specified `index`
    /// as discarded. It is up to the implementation to really delete these entries or
    /// just mark them, but the new zero and last indexes must become consistent with `index`
    /// at the moment when function returns.
    ///
    /// It is possible that the `index` will point beyond the log's latest_index or latest_volatile_index,
    /// or, in some rare cases to index earlier than zero_index.
    /// The implementation must support these cases by discarding the whole log, persisting the
    /// zero term as `term` (because there may be no knowlege or an incorrect knowlege about this term in the log) and
    /// setting its indexes accordingly. That is zero_index and latest_index must be set to `index`,
    /// while latest_volatile_index may or may not be moved to `index` as well.
    fn discard_until(&mut self, index: LogIndex, zero_term: Term) -> Result<(), Self::Error>;

    /// The implementation must consider all entries(including volatiles) after and including the specified index
    /// as discarded. It is up to the implementation to really delete these entries or
    /// just mark them, but both latest indexes(persistent and volatile) must point to the
    /// entry preceeding the entry at `start` (this includes zero entry) at the moment
    /// when functon returns.
    /// The implementation is allowed to discard more entries than requested, up to the earlier
    /// index, updating the last indexes correspondingly.
    fn discard_since(&mut self, start: LogIndex) -> Result<(), Self::Error>;

    /// Should read the entry at the provided log index into an entry provided by reference.
    /// Must return the success of entry being filled.
    ///
    /// Consensus guarantees that, given the indices returned by `current_view` no attempts to read
    /// the following will be done:
    /// * entries after volatile(!) index
    /// * entries before or equal to zero entry
    ///
    /// The function MUST return true or an error if a persisted entry is requested,
    /// i.e. the index range of (zero_entry..latest_index]
    /// The function MAY return false and avoid reading the not-yet-persisted entry, i.e
    /// the index range (latest_index, latest_volatile_index]
    fn read_entry(&self, index: LogIndex, dest: &mut LogEntry) -> Result<bool, Self::Error>;

    /// Must append an entry to the log. The moment of persisting entries may be decided by an implementation,
    /// but the indexes must change only after the entries are persisted leaving no gaps and keeping the incoming ordering.
    /// The consensus guarantees that no overwrites of existing entries will be requested unless
    /// the preliminary call to discard is made. Also, consensus guarantees there will be no
    /// gaps between entries, i.e. `start` will always point to the latest_volatile_index + 1.
    ///
    /// Implementations are recommended to NOT rely on any content of entries, including
    /// entry's term and type. This means configuration change entries should only be persisted,
    /// but not marked as latest affecting further calls to `latest_config*` functions. Such marking
    /// will be done later, when configuration is committed by calling set_latest_config function explicitly.
    fn append_entries(&mut self, start: LogIndex, entries: &[LogEntry]) -> Result<(), Self::Error>;

    /// Should return the latest known term or Term(0) when initialized.
    fn current_term(&self) -> Result<Term, Self::Error>;

    /// Should set the current term to the provided value.
    /// The implementations is NOT recommended to rely on any term change patterns or regularity.
    /// The function must be synchronous, delays or failures in persisting after returning
    /// from this function may break the consensus.
    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error>;

    /// Returns the id of the candidate node voted for in the current term, if set.
    fn voted_for(&self) -> Result<Option<ServerId>, Self::Error>;

    /// Should save a provided candidate id the node voted for the current term.
    /// server = None means voted_for should be unset(i.e. cleared)
    ///
    /// The function must be synchronous, delays or failures in persisting after returning
    /// from this function may break the consensus.
    fn set_voted_for(&mut self, server: Option<ServerId>) -> Result<(), Self::Error>;

    /// Should return the latest saved config indices, in the same order as they were set
    /// by set_latest_config_view. With empty log or until set_latest_config_view is called,
    /// should return `(LogIndex(0), LogIndex(0))`.
    fn latest_config_view(&self) -> Result<(LogIndex, LogIndex), Self::Error>;

    /// Should persist the provided data about config indices.
    ///
    /// The function must be synchronous, delays or failures in persisting after returning
    /// from this function may break the consensus.
    fn set_latest_config_view(
        &mut self,
        stable: LogIndex,
        new: LogIndex,
    ) -> Result<(), Self::Error>;

    /// Should return the index of latest saved config entry if any.
    // fn latest_config_index(&self) -> Result<Option<LogIndex>, Self::Error>;

    /// Expected to flush all the volatile entities on disk, if any.
    ///
    /// Sync is not called by consensus and only used when log is tested. It also
    /// may be useful in other scenarios, calling it is not prohibited.
    /// The default implementation always succeeds.
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

/// Kinds of a log entry to be processed
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum LogEntryData {
    Empty,
    Proposal(Bytes, Urgency),
    Config(Vec<Peer>),
}

impl LogEntry {
    /// returns data stored in proposal if the entry matches type
    pub fn try_proposal_data(&self) -> Option<Bytes> {
        if let LogEntryData::Proposal(ref data, _) = self.data {
            Some(data.clone())
        } else {
            None
        }
    }

    pub fn try_config_data(&self) -> Option<Vec<Peer>> {
        if let LogEntryData::Config(ref config) = self.data {
            Some(config.clone())
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
                LogEntryData::Proposal(proposal, urgency) => EntryData::Proposal(proposal, urgency),
                LogEntryData::Config(c) => EntryData::Config(c, false),
            },
        }
    }
}

impl LogEntry {
    pub fn new_empty(term: Term) -> Self {
        Self {
            term,
            data: LogEntryData::Empty,
        }
    }

    pub fn new_proposal(term: Term, data: Bytes, urgency: Urgency) -> Self {
        Self {
            term,
            data: LogEntryData::Proposal(data, urgency),
        }
    }

    pub fn new_config(term: Term, config: Vec<Peer>) -> Self {
        Self {
            term,
            data: LogEntryData::Config(config),
        }
    }
}

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
