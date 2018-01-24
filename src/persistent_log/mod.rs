//! The persistent storage of Raft state.
//!
//! In your consuming application you may want to implement this trait on one of your structures to
//! have your own facility for storing Raft log
//!
//! *Note:* This is not necessary in your consuming application. The `Log` is meant to be
//! internally used by the library, while letting this library authors no to be opinionated about
//! how data is stored.

pub mod fs;
pub mod mem;

use std::error;
use std::fmt::{self, Debug};
use std::result;
use std::io::{Read, Write};

pub use persistent_log::fs::FsLog;
pub use persistent_log::mem::MemLog;

use {LogIndex, ServerId, Term};

/// A store of persistent Raft state.
pub trait Log: Clone + Debug + 'static {
    type Error: error::Error + Debug + Sized + 'static + Send + Sync;

    /// Returns the latest known term.
    fn current_term(&self) -> result::Result<Term, Self::Error>;

    /// Sets the current term to the provided value. The provided term must be greater than
    /// the current term. The `voted_for` value will be reset`.
    fn set_current_term(&mut self, term: Term) -> result::Result<(), Self::Error>;

    /// Increment the current term. The `voted_for` value will be reset.
    fn inc_current_term(&mut self) -> result::Result<Term, Self::Error>;

    /// Returns the candidate id of the candidate voted for in the current term (or none).
    fn voted_for(&self) -> result::Result<Option<ServerId>, Self::Error>;

    /// Sets the candidate id voted for in the current term.
    fn set_voted_for(&mut self, server: ServerId) -> result::Result<(), Self::Error>;

    /// Returns the index of the latest persisted log entry (0 if the log is empty).
    fn latest_log_index(&self) -> result::Result<LogIndex, Self::Error>;

    /// Returns the term of the latest persisted log entry (0 if the log is empty).
    fn latest_log_term(&self) -> result::Result<Term, Self::Error>;

    /// Returns the term for the entry at the provided log index writing entry itself to writer if
    /// requested
    fn entry<W: Write>(&self, index: LogIndex, buf: Option<W>)
        -> result::Result<Term, Self::Error>;

    // fn entry(&self, index: LogIndex) -> Result<(Term, &[u8]), Self::Error>;

    ///// Returns the given range of entries (excluding the right endpoint).
    fn entries(
        &self,
        lo: LogIndex,
        hi: LogIndex,
    ) -> result::Result<Vec<(Term, &[u8])>, Self::Error> {
        unimplemented!()
    }
    //// TODO: can make LogIndex compatible for use in ranges.
    //(lo.as_u64()..hi.as_u64())
    //.map(|index| {
    ////                let mut v = Vec::new();
    ////let term = self.entry(LogIndex::from(index), &mut v);
    ////(term, v.as_slice())

    //self.entry(LogIndex::from(index))
    //})
    //.collect::<Result<_, _>>()
    //}

    /// Appends the provided entries to the log beginning at the given index.
    fn append_entries<R: Read, I: Iterator<Item = (Term, R)>>(
        &mut self,
        from: LogIndex,
        entries: I,
    ) -> result::Result<(), Self::Error>;
}


///// Error type for `FsLog` and `MemLog`
//#[derive(Fail, Debug)]
//#[fail(display = "Consensus error")]
//pub enum Error {
//#[fail(display = "Log file version mismatch {} instead of {}", _0, _1)] Version(u64, u64),
//#[fail(display = "Bad index requested")] BadIndex,
//#[fail(display = "Log index out of bounds")] BadLogIndex,
//#[fail(display = "Log I/O error")] Io(#[cause] ::std::io::Error),
//}

#[derive(Debug)]
pub enum Error {
    Version(u64, u64),
    BadIndex,
    BadLogIndex,
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
    entries: &[(Term, &[u8])],
) -> Result<(), L::Error> {
    let entries = entries
        .iter()
        .map(|&(term, entry)| (term, Cursor::new(entry)));
    store.append_entries(from, entries)?;
    Ok(())
}


#[cfg(test)]
// helper for easier test migration
pub(crate) fn get_entry<L: Log>(store: &L, log_index: LogIndex) -> (Term, Vec<u8>) {
    let mut data = Vec::new();
    let term = store.entry(log_index, Some(&mut data)).unwrap();
    (term, data)
}
