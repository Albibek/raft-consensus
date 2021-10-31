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
use crate::error::Error;
use crate::message::peer::{Entry, EntryData};
use crate::{AdminId, ClientId, LogIndex, ServerId, Term};

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
/// indexes of the "tail"(i.e. just appended) entries. If implementation wants
/// to guarantee the persistence of these entries, it should not return from
/// the function until the entries are synced properly.
///
/// Log indexes in Raft start with 1 (0 is reserved as special).
pub trait Log {
    type Error: std::error::Error + Sized + 'static;

    /// Should return the index of the latest log entry ever appended to log. To avoid data losses
    /// this index should point the data that is guaranteed to be persisted.
    /// Should return LogIndex(0) if the log is empty.
    fn latest_index(&self) -> Result<LogIndex, Self::Error>;
    fn latest_persisted_index(&self) -> Result<LogIndex, Self::Error>;

    /// Should return the index of the first log entry (0 if the log is empty).
    fn first_log_index(&self) -> Result<LogIndex, Self::Error>;

    /// Should return term corresponding to log index if such term and index exists in log
    /// Shoult NOT return an error on non-existence.
    fn term_of(&self, index: LogIndex) -> Result<Option<Term>, Self::Error>;

    /// Delete or mark as invalid all entries since specified log index
    /// return the new index. It is allowed to return any index lower than requested.
    fn discard_since(&mut self, index: LogIndex) -> Result<LogIndex, Self::Error>;

    /// Should discard all entries until specified index (including the entry at `index`).
    /// Should return the new first index, index should be less or equal to the requested
    fn discard_until(&self, index: LogIndex) -> Result<LogIndex, Self::Error>;

    /// Should reads the entry at the provided log index into an entry provided by reference.
    /// Consensus guarantees that no entries at indexes higher than latest_log_index will be read.
    /// The entry metadata should also be persisted, but it is recommended to store it in the memory because
    /// it is highly likely to be requested soon.
    fn read_entry(&self, index: LogIndex, dest: &mut LogEntry) -> Result<(), Self::Error>;

    /// Should return the metadata of the entry at the specified index. No indexes higher
    /// than latest_log_index will be requested.
    fn entry_meta_at(&self, index: LogIndex) -> Result<LogEntryMeta, Self::Error>;

    /// Must append an entry to the log. The sync and index shifting should be decided by implementation, but
    /// kept with no gaps and incoming ordering.
    /// This function should persist configuration change entries, but MUST not instantly be used from
    /// last_config_* functions. This will be done separately only when configuration is committed.
    fn append_entries(&mut self, start: LogIndex, entries: &[LogEntry]) -> Result<(), Self::Error>;

    /// Should return the latest known term.
    fn current_term(&self) -> Result<Term, Self::Error>;

    /// Should sets the current term to the provided value. The provided term must be greater than
    /// the current term. Recommended to be synchronous, delays in persisting term may break the
    /// consensus.
    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error>;

    /// Returns the id of the candidate node voted for in the current term, if any.
    fn voted_for(&self) -> Result<Option<ServerId>, Self::Error>;

    /// Should save a provided candidate id the node voted for the current term.
    /// None means voted_for should be unset(i.e. cleared)
    fn set_voted_for(&mut self, server: Option<ServerId>) -> Result<(), Self::Error>;

    /// Should persist the provided configuration and it's index for later retrieval.
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
    Proposal(ClientId, ClientGuarantee),
    Config(ConsensusConfig, AdminId),
}

/// Kinds of a log entry to be processed
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum LogEntryData {
    Empty,
    Proposal(Vec<u8>, ClientId, ClientGuarantee),
    Config(ConsensusConfig, AdminId),
}

impl LogEntry {
    pub fn as_entry_ref<'a>(&'a self) -> LogEntryRef<'a> {
        LogEntryRef {
            term: self.term,
            data: match self.data {
                LogEntryData::Empty => LogEntryDataRef::Empty,
                LogEntryData::Proposal(ref v, client, guarantee) => {
                    LogEntryDataRef::Proposal(v.as_slice(), client, guarantee)
                }
                LogEntryData::Config(ref c, admin) => LogEntryDataRef::Config(c, admin),
            },
        }
    }

    pub fn meta(&self) -> LogEntryMeta {
        match &self.data {
            LogEntryData::Empty => LogEntryMeta::Empty,
            LogEntryData::Proposal(_, client, guarantee) => {
                LogEntryMeta::Proposal(*client, *guarantee)
            }
            LogEntryData::Config(config, admin) => LogEntryMeta::Config(config.clone(), *admin),
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
                LogEntryData::Proposal(proposal, client, _) => {
                    // no need to replicate a guarantee
                    EntryData::Proposal(proposal, client)
                }
                LogEntryData::Config(c, admin) => EntryData::Config(c, false, admin),
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

    pub fn new_proposal(
        term: Term,
        data: Vec<u8>,
        client: ClientId,
        guarantee: ClientGuarantee,
    ) -> Self {
        Self {
            term,
            data: LogEntryData::Proposal(data, client, guarantee),
        }
    }

    pub fn new_config(term: Term, config: ConsensusConfig, admin: AdminId) -> Self {
        Self {
            term,
            data: LogEntryData::Config(config, admin),
        }
    }
}

/// The reference to a record to be added into log
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntryRef<'a> {
    pub term: Term,
    pub data: LogEntryDataRef<'a>,
}

/// Kinds of a log entry to be processed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEntryDataRef<'a> {
    Empty,
    Proposal(&'a [u8], ClientId, ClientGuarantee),
    Config(&'a ConsensusConfig, AdminId),
}

impl<'a> From<LogEntryRef<'a>> for LogEntry {
    fn from(e: LogEntryRef<'a>) -> Self {
        LogEntry {
            term: e.term,
            data: match e.data {
                LogEntryDataRef::Empty => LogEntryData::Empty,
                LogEntryDataRef::Proposal(proposal, client, guarantee) => {
                    LogEntryData::Proposal(proposal.to_vec(), client, guarantee)
                }
                LogEntryDataRef::Config(c, admin) => LogEntryData::Config(c.clone(), admin),
            },
        }
    }
}

impl<'a> From<&'a LogEntryRef<'a>> for LogEntry {
    fn from(e: &'a LogEntryRef<'a>) -> Self {
        LogEntry {
            term: e.term,
            data: match e.data {
                LogEntryDataRef::Empty => LogEntryData::Empty,
                LogEntryDataRef::Proposal(proposal, client, guarantee) => {
                    LogEntryData::Proposal(proposal.to_vec(), client, guarantee)
                }
                LogEntryDataRef::Config(c, admin) => LogEntryData::Config(c.clone(), admin),
            },
        }
    }
}

impl<'a> From<LogEntryRef<'a>> for Entry {
    fn from(e: LogEntryRef<'a>) -> Self {
        Entry {
            term: e.term,
            data: match e.data {
                LogEntryDataRef::Empty => EntryData::Noop,
                LogEntryDataRef::Proposal(proposal, client, _) => {
                    EntryData::Proposal(proposal.to_vec(), client)
                }
                LogEntryDataRef::Config(c, admin) => EntryData::Config(c.clone(), false, admin),
            },
        }
    }
}

impl<'a> From<&'a LogEntryRef<'a>> for Entry {
    fn from(e: &'a LogEntryRef<'a>) -> Self {
        Entry {
            term: e.term,
            data: match e.data {
                LogEntryDataRef::Empty => EntryData::Noop,
                LogEntryDataRef::Proposal(proposal, client, _) => {
                    EntryData::Proposal(proposal.to_vec(), client)
                }
                LogEntryDataRef::Config(c, admin) => EntryData::Config(c.clone(), false, admin),
            },
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
    BadLogIndex,
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
