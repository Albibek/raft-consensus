//! The persistent storage of Raft state.
//!
//! In your consuming application you may want to implement this trait on one of your structures to
//! have your own facility for storing Raft log
//!
//! *Note:* This is not necessary in your consuming application. The `Log` is meant to be
//! internally used by the library, while letting this library authors no to be opinionated about
//! how data is stored.

//FIXME
//pub mod fs;
pub mod mem;

use std::error;
use std::fmt::{self, Debug};

// FIXME:
//pub use persistent_log::fs::FsLog;
pub use crate::persistent_log::mem::MemLog;

use crate::entry::ConsensusConfig;
use crate::{LogIndex, ServerId, Term};

/// The record to be added into log
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct LogEntry {
    term: Term,
    data: LogEntryData,
}

/// Kinds of a log entry to be processed
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum LogEntryData {
    Empty,
    Proposal(Vec<u8>),
    Config(ConsensusConfig),
}

//impl LogEntry {
//pub fn new_proposal(term: Term, data: Vec<u8>) -> Self {
//Self {
//term,
//data: LogEntryData::Proposal(data),
//}
//}

//pub fn new_config(term: Term, data: Vec<u8>) -> Self {
//Self {
//term,
//data: LogEntryData::Proposal(data),
//}
//}
//}

/// A layer of persistence to store Raft state data.
/// Should implement storing a few specific values, like current_term or voted_for
/// along with the general-purpose log storage for appending raft log entries.
pub trait Log {
    // FIXME: snapshot should probably be guided by leader(using snapshot flag for example), so all the nodes could have
    // the snapshot at same point in time (QUESTION: it may not be nesessary actually)

    type Error: error::Error + Debug + Sized + 'static + Send + Sync;

    // Terms and log indexes

    /// Returns the latest known term.
    fn current_term(&self) -> Result<Term, Self::Error>;

    /// Sets the current term to the provided value. The provided term must be greater than
    /// the current term.
    // Must also reset the `voted_for` value.
    // TODO: introduce reset_voted_for, to take the burden from the implementor
    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error>;

    /// Returns the index of the latest persisted log entry (0 if the log is empty).
    fn latest_log_index(&self) -> Result<LogIndex, Self::Error>;

    /// Should return term corresponding to log index if such term and index exists in log
    /// Shoult NOT return an error on non-existence
    fn term_at(&self, index: LogIndex) -> Result<Option<Term>, Self::Error>;

    /// Delete or mark invalid all entries since specified log index
    /// return the new index. It is allowed to return any index lower than requested
    fn discard_log_since(&self, index: LogIndex) -> Result<LogIndex, Self::Error>;

    // Voted-for persistence
    /// Returns the candidate id of the candidate voted for in the current term (or none).
    fn voted_for(&self) -> Result<Option<ServerId>, Self::Error>;

    /// Sets the candidate id the node voted for in the current term.
    /// None means voted_for should be unset
    fn set_voted_for(&mut self, server: Option<ServerId>) -> Result<(), Self::Error>;

    // Configuration related functions
    /// Must put the latest (actual for the current term) config into config provided rerefence
    fn read_latest_config(&self, config: &mut ConsensusConfig) -> Result<LogIndex, Self::Error>;

    /// Reads the entry at the provided log index into entry provided by reference
    fn entry(&self, index: LogIndex, dest: &mut LogEntry) -> Result<(), Self::Error>;

    /// Must append an entry to the log
    fn append_entry(
        &mut self,
        at: LogIndex,
        term: Term,
        entry: &LogEntry,
    ) -> Result<(), Self::Error>;
}

//fn inc_current_term<L: Log>(log: &mut L) -> Result<Term, L::Error> {
//let next_term = if let Some(term) = log.term()? {
//term
//} else {

//}
//}

///// Returns the term of the latest persisted log entry (0 if the log is empty).
//fn latest_log_term(&self) -> Result<Term, Self::Error>;

//    /// Must put the latest (actual for the current term) config into config provided rerefence
//fn read_latest_config(&self, config: &mut ConsensusConfig) -> Result<LogIndex, Self::Error>;

///// Appends the provided entries to the log beginning at the given index.
///// Note that for configuration change entries, the previous one should also be stored to be
///// retrievable via previous_config_index
//fn append_entries<'a, I: Iterator<Item = &'a Entry>>(
//&mut self,
//from: LogIndex,
//entries: I,
//) -> Result<(), Self::Error>;

#[derive(Debug)]
pub enum Error {
    Version(u64, u64),
    BadIndex,
    BadLogIndex,
    NoConfig,
    Io(::std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "An error occurred")
    }
}

impl From<::std::io::Error> for Error {
    fn from(e: ::std::io::Error) -> Error {
        Error::Io(e)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        "An error occurred"
    }
}

#[cfg(test)]
use std::io::Cursor;

#[cfg(test)]
// helper for easier test migration
pub(crate) fn append_entries<L: Log>(
    store: &mut L,
    from: LogIndex,
    entries: &[(Term, EntryKind, &[u8])],
) -> Result<(), L::Error> {
    let entries = entries
        .iter()
        .map(|&(term, kind, entry)| (term, kind, Cursor::new(entry)));
    store.append_entries(from, entries)?;
    Ok(())
}

#[cfg(test)]
// helper for easier test migration
pub(crate) fn get_entry<L: Log>(store: &L, log_index: LogIndex) -> (Term, EntryKind, Vec<u8>) {
    let mut data = Vec::new();
    let term = store.entry(log_index, Some(&mut data)).unwrap();
    (term, kind, data)
}
