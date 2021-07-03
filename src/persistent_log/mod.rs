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

// FIXME:
//pub use persistent_log::fs::FsLog;
pub use crate::persistent_log::mem::MemLog;

// this module imports
use std::fmt::Debug;

use crate::config::ConsensusConfig;
use crate::error::Error;
use crate::message::peer::{Entry, EntryData};
use crate::{LogIndex, ServerId, Term};

#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

use thiserror::Error as ThisError;

/// A layer of persistence to store Raft log and state data.
/// Should implement general-purpose log storage for appending raft log entries
/// along with storing iseveral specific values, like voted_for, current cluster configuration
// TODO store or a list of registered clients?
pub trait Log {
    type Error: std::error::Error + Sized + 'static;

    //  Logging related persistent functions

    /// Should return the index of the latest persisted log entry (0 if the log is empty).
    fn latest_log_index(&self) -> Result<LogIndex, Self::Error>;

    /// Should return the index of the first persisted log entry (0 if the log is empty).
    fn first_log_index(&self) -> Result<LogIndex, Self::Error>;

    /// Should return term corresponding to log index if such term and index exists in log
    /// Shoult NOT return an error on non-existence
    fn term_of(&self, index: LogIndex) -> Result<Option<Term>, Self::Error>;

    /// Delete or mark invalid all entries since specified log index
    /// return the new index. It is allowed to return any index lower than requested
    fn discard_since(&mut self, index: LogIndex) -> Result<LogIndex, Self::Error>;

    /// Should discard all entries until specified index (including the entry at `index`).
    /// Should return the new first index, index should be less or equal to the requested
    fn discard_until(&self, index: LogIndex) -> Result<LogIndex, Self::Error>;

    /// Reads the entry at the provided log index into an entry provided by reference
    fn read_entry(&self, index: LogIndex, dest: &mut LogEntry) -> Result<(), Self::Error>;

    /// Must append an entry to the log. If the index in the log is earlier, than requested,
    /// the log must be truncated so the added entry always being the last one, with no gaps.
    /// Is not required for this function to save a config entries, if any pass by. This will be
    /// done separately to make sure only committed configuration is persisted.
    /// The caller(i.e. the consensus) will check for the gaps, so they should not actually happen.
    /// NOTE: log indexes in Raft start with 1 (0 is reserved as special), this function should consider this
    fn append_entry<'a>(
        &mut self,
        at: LogIndex,
        entry: &LogEntryRef<'a>,
    ) -> Result<(), Self::Error>;

    /// Returns the latest known term.
    fn current_term(&self) -> Result<Term, Self::Error>;

    /// Sets the current term to the provided value. The provided term must be greater than
    /// the current term.
    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error>;

    // Voting related persistence
    /// Returns the candidate id of the candidate voted for in the current term (or none).
    fn voted_for(&self) -> Result<Option<ServerId>, Self::Error>;

    /// Sets the candidate id the node voted for in the current term.
    /// None means voted_for should be unset
    fn set_voted_for(&mut self, server: Option<ServerId>) -> Result<(), Self::Error>;

    // Config pesistence
    /// Should persist the provided configuration and it's index for later retrieval.
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

/// Kinds of a log entry to be processed
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum LogEntryData {
    Empty,
    Proposal(Vec<u8>),
    Config(ConsensusConfig),
}

impl LogEntry {
    pub fn as_entry_ref<'a>(&'a self) -> LogEntryRef<'a> {
        LogEntryRef {
            term: self.term,
            data: match self.data {
                LogEntryData::Empty => LogEntryDataRef::Empty,
                LogEntryData::Proposal(ref v) => LogEntryDataRef::Proposal(v.as_slice()),
                LogEntryData::Config(ref c) => LogEntryDataRef::Config(c),
            },
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
                LogEntryData::Proposal(proposal) => EntryData::Proposal(proposal),
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

    pub fn new_proposal(term: Term, data: Vec<u8>) -> Self {
        Self {
            term,
            data: LogEntryData::Proposal(data),
        }
    }

    pub fn new_config(term: Term, config: ConsensusConfig) -> Self {
        Self {
            term,
            data: LogEntryData::Config(config),
        }
    }
}

/// The record to be added into log
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntryRef<'a> {
    pub term: Term,
    pub data: LogEntryDataRef<'a>,
}

/// Kinds of a log entry to be processed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEntryDataRef<'a> {
    Empty,
    Proposal(&'a [u8]),
    Config(&'a ConsensusConfig),
}

impl<'a> From<LogEntryRef<'a>> for LogEntry {
    fn from(e: LogEntryRef<'a>) -> Self {
        LogEntry {
            term: e.term,
            data: match e.data {
                LogEntryDataRef::Empty => LogEntryData::Empty,
                LogEntryDataRef::Proposal(proposal) => LogEntryData::Proposal(proposal.to_vec()),
                LogEntryDataRef::Config(c) => LogEntryData::Config(c.clone()),
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
                LogEntryDataRef::Proposal(proposal) => LogEntryData::Proposal(proposal.to_vec()),
                LogEntryDataRef::Config(c) => LogEntryData::Config(c.clone()),
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
                LogEntryDataRef::Proposal(proposal) => EntryData::Proposal(proposal.to_vec()),
                LogEntryDataRef::Config(c) => EntryData::Config(c.clone(), false),
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
                LogEntryDataRef::Proposal(proposal) => EntryData::Proposal(proposal.to_vec()),
                LogEntryDataRef::Config(c) => EntryData::Config(c.clone(), false),
            },
        }
    }
}

/// A helper type for errors in persistent log. Contains typical errors, that could happen,
/// but no code depends on it, so it only provided to make implementor's life easier.
#[error("persistent log error")]
#[derive(ThisError, Debug)]
pub enum LogError {
    Version(u64, u64),
    BadIndex(LogIndex),
    BadLogIndex,
    NoConfig,
    Io(#[from] ::std::io::Error),
}

//#[cfg(test)]
//use std::io::Cursor;

//#[cfg(test)]
//// helper for easier test migration
//pub(crate) fn append_entries<L: Log>(
//store: &mut L,
//from: LogIndex,
//entries: &[(Term, EntryKind, &[u8])],
//) -> Result<(), L::Error> {
//let entries = entries
//.iter()
//.map(|&(term, kind, entry)| (term, kind, Cursor::new(entry)));
//store.append_entries(from, entries)?;
//Ok(())
//}

//#[cfg(test)]
//// helper for easier test migration
//pub(crate) fn get_entry<L: Log>(store: &L, log_index: LogIndex) -> (Term, EntryKind, Vec<u8>) {
//let mut data = Vec::new();
//let term = store.entry(log_index, Some(&mut data)).unwrap();
//(term, kind, data)
//}
