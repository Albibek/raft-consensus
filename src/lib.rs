extern crate byteorder;
extern crate failure;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate log;
#[cfg(test)]
extern crate pretty_env_logger;
extern crate uuid;

pub mod error;
pub mod state_machine;
pub mod persistent_log;

pub(crate) mod state;
/// Implementation of Raft consensus API
pub mod consensus;
/// Messages that are passed during consensus work
pub mod message;
/// Handlers for consensus callbacks
pub mod handler;

use std::{fmt, ops};

use error::Error;
use uuid::Uuid;

/// The term of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Term(pub u64);
impl Term {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for Term {
    fn from(val: u64) -> Term {
        Term(val)
    }
}

impl Into<u64> for Term {
    fn into(self) -> u64 {
        self.0
    }
}

impl ops::Add<u64> for Term {
    type Output = Term;
    fn add(self, rhs: u64) -> Term {
        Term(
            self.0
                .checked_add(rhs)
                .expect("overflow while incrementing Term"),
        )
    }
}

impl ops::Sub<u64> for Term {
    type Output = Term;
    fn sub(self, rhs: u64) -> Term {
        Term(
            self.0
                .checked_sub(rhs)
                .expect("underflow while decrementing Term"),
        )
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The index of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogIndex(pub u64);
impl LogIndex {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for LogIndex {
    fn from(val: u64) -> LogIndex {
        LogIndex(val)
    }
}

impl Into<u64> for LogIndex {
    fn into(self) -> u64 {
        self.0
    }
}

impl ops::Add<u64> for LogIndex {
    type Output = LogIndex;
    fn add(self, rhs: u64) -> LogIndex {
        LogIndex(
            self.0
                .checked_add(rhs)
                .expect("overflow while incrementing LogIndex"),
        )
    }
}

impl ops::Sub<u64> for LogIndex {
    type Output = LogIndex;
    fn sub(self, rhs: u64) -> LogIndex {
        LogIndex(
            self.0
                .checked_sub(rhs)
                .expect("underflow while decrementing LogIndex"),
        )
    }
}

/// Find the offset between two log indices.
impl ops::Sub for LogIndex {
    type Output = u64;
    fn sub(self, rhs: LogIndex) -> u64 {
        self.0
            .checked_sub(rhs.0)
            .expect("underflow while subtracting LogIndex")
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The ID of a Raft server. Must be unique among the participants in a
/// consensus group.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub struct ServerId(pub u64);

impl ServerId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for ServerId {
    fn from(val: u64) -> ServerId {
        ServerId(val)
    }
}

impl Into<u64> for ServerId {
    fn into(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The ID of a Raft client.
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub struct ClientId(pub Uuid);
impl ClientId {
    pub fn new() -> ClientId {
        ClientId(Uuid::new_v4())
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<ClientId, Error> {
        Uuid::from_bytes(bytes)
            .map(ClientId)
            .map_err(|e| Error::Uuid(e))
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Type representing a log entry
#[derive(Debug, Clone)]
pub struct Entry {
    pub term: Term,
    pub data: Vec<u8>,
}

impl Entry {
    pub fn new(term: Term, data: Vec<u8>) -> Self {
        Self { term, data }
    }
}

impl From<Entry> for (Term, Vec<u8>) {
    fn from(e: Entry) -> (Term, Vec<u8>) {
        (e.term, e.data)
    }
}
