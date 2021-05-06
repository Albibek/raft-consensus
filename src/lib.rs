//! # Raft
//!
//! This is a crate containing a Raft consensus protocol implementation and encoding/decoding
//! helpers. This is a logic-only crate without any networking part.
//!
//! To use Raft in it's full strength using this crate, one should do the following:
//!
//! * determine and implement(or take ready ones) state machine and persistent log implementations
//!
//! * find or make a part responsible for passing peer and client messages over the wire and pass
//! all these messages from to one of `...Consensus` structures
//!
//! * define a ConsensusHandler with callbacks doing the job for passing messages generated by
//! consensus to other nodes

pub mod error;
pub mod persistent_log;
pub mod state_machine;

/// Implementation of Raft consensus API
pub mod raft;

/// Messages that are passed during consensus work
#[cfg_attr(feature = "use_capnp", macro_use)]
pub mod message;

// Data and functions for all states
mod state;

// Trait and helpers for internal state handling API
mod state_impl;

/// Definition of a persistent log entry
pub mod entry;

/// Module with all functions required in follower state
mod candidate;
/// Module with all functions required in candidate state
mod follower;
/// Module with all functions required in leader state
mod leader;

/// Handlers for consensus callbacks
pub mod handler;

#[cfg(feature = "use_capnp")]
pub mod messages_capnp {
    //    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/schema/messages_capnp.rs"));
}

use std::{fmt, ops};

#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Error;

pub use crate::handler::Handler;
pub use crate::persistent_log::Log;
pub use crate::raft::Raft;
pub use crate::state_machine::StateMachine;

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
    pub fn as_usize(self) -> usize {
        self.0 as usize
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

#[derive(Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd, Debug)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct Peer {
    id: ServerId,
    //    status: PeerStatus,
}

//#[derive(Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd, Debug)]
//#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
//pub enum PeerStatus {
//Member,
//NonVoter,
//}

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
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug, Default)]
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
        Uuid::from_slice(bytes).map(ClientId).map_err(Error::Uuid)
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
