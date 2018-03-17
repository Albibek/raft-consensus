//! # Raft
//!
//! This is a crate containing a Raft consensus protocol implementation and encoding/decoding
//! helpers. This is a logic-only crate without any network implementations.
//!
//! To use this crate in implementation, one should do the following:
//!
//! * determine and implement(or take ready ones) state machine and persistent log implementations
//!
//! * pass all raft-related messages from receivers/transmitters to `Consensus` structure
//!
//! * define a CollectionHandle that which functions are called at different stages of consensus
//! defining actions depending on receive/transmit environment

extern crate byteorder;
extern crate failure;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate log;
#[cfg(test)]
extern crate pretty_env_logger;
extern crate uuid;

#[cfg(feature = "use_serde")]
extern crate serde;
#[cfg(feature = "use_serde")]
#[cfg_attr(feature = "use_serde", macro_use)]
extern crate serde_derive;

#[cfg(feature = "use_capnp")]
extern crate capnp;

pub mod error;
pub mod state_machine;
pub mod persistent_log;

/// Implementation of Raft consensus API
pub mod consensus;

/// Messages that are passed during consensus work
#[cfg_attr(feature = "use_capnp", macro_use)]
pub mod message;

pub(crate) mod state;
/// Handlers for consensus callbacks
pub mod handler;

/// Handle consensus from many threads
pub mod shared;
#[cfg(feature = "use_capnp")]
pub mod messages_capnp {
    //    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/schema/messages_capnp.rs"));
}

use std::{fmt, ops};

use error::Error;
use uuid::Uuid;

pub use consensus::{Consensus, ConsensusHandler};
pub use shared::SharedConsensus;
pub use persistent_log::Log;
pub use state_machine::StateMachine;

#[cfg(feature = "use_capnp")]
use messages_capnp::entry;

#[cfg(feature = "use_capnp")]
use capnp::message::{Allocator, Builder, HeapAllocator, Reader, ReaderSegments};

/// The term of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
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
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
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
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
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
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
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
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
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

#[cfg(feature = "use_capnp")]
impl Entry {
    pub fn from_capnp<'a>(reader: entry::Reader<'a>) -> Result<Self, Error> {
        Ok(Entry {
            term: reader.get_term().into(),
            data: reader.get_data().map_err(Error::Capnp)?.to_vec(),
        })
    }

    pub fn fill_capnp<'a>(&self, builder: &mut entry::Builder<'a>) {
        builder.set_term(self.term.as_u64());
        builder.set_data(&self.data);
    }

    common_capnp!(entry::Builder, entry::Reader);
}
