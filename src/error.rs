//! Error type with all possible errors
use crate::{LogIndex, ServerId, Term};
use std::error::Error as StdError;
use thiserror::Error as ThisError;
use uuid::Error as UuidError;

#[cfg(feature = "use_capnp")]
use capnp::Error as CapnpError;
#[cfg(feature = "use_capnp")]
use capnp::NotInSchema;

#[derive(ThisError, Debug)]
#[error("raft consensus error")]
pub enum Error {
    #[error("consensus have reached unrecoverable error: {}", _0)]
    Critical(#[from] CriticalError),

    #[error("consensus state was not leader while it had to be")]
    MustLeader,

    #[error("consensus state was Leader while it had NOT to be")]
    MustNotLeader,

    #[error("consensus state was not follower while it had to be")]
    MustFollower,

    #[error("follower responded with inconsistent index.")]
    BadFollowerIndex,

    #[error("unknown peer")]
    UnknownPeer(ServerId),

    #[error("unexpected message")]
    UnexpectedMessage,

    #[error("trying to removing last node makes cluster unable to being restored")]
    LastNodeRemoval,

    #[error("UUID conversion")]
    Uuid(#[from] UuidError),

    #[error("decoding capnp")]
    #[cfg(feature = "use_capnp")]
    Capnp(#[from] CapnpError),

    #[error("detecting capnp schema")]
    #[cfg(feature = "use_capnp")]
    CapnpSchema(#[from] NotInSchema),

    #[error("error reading persistent log: {}", _0)]
    PersistentLogRead(Box<dyn StdError + 'static>),

    #[error("follower node must be in list of peers at start")]
    MustBootstrap,

    #[error("unable to support {} cluster members", _0)]
    BadClusterSize(usize),

    #[error("state machine didn't have snapshot where it expected to exist")]
    SnapshotExpected,
}

impl Error {
    pub fn is_critical(&self) -> bool {
        if let Error::Critical(_) = self {
            true
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn unreachable(at: &'static str) -> Error {
        Error::Critical(CriticalError::Unreachable(at))
    }

    #[inline]
    pub(crate) fn log_broken(index: LogIndex, at: &'static str) -> Error {
        Error::Critical(CriticalError::LeaderLogBroken(index, at))
    }
}

#[derive(ThisError, Debug)]
#[error("Critical error in consensus")]
pub enum CriticalError {
    #[error("BUG: unreachable condition at {}", _0)]
    Unreachable(&'static str),

    #[error("BUG: leader's log could not find term for index {} at {}", _0, _1)]
    LeaderLogBroken(LogIndex, &'static str),

    #[error("BUG: entry kind does not match entry data at at {}", _0)]
    LogInconsistentEntry(LogIndex),

    #[error("BUG: log returned existing voted_for in a solitary consensus: {}", _0)]
    LogInconsistentVoted(ServerId),

    #[error("Consensus state become unrecoverable after last error, consensus cannot proceed")]
    Unrecoverable,

    #[error("BUG: peer leader with matching term detected")]
    AnotherLeader(ServerId, Term),

    #[error("error writing to persistent log: {}", _0)]
    PersistentLogWrite(Box<dyn StdError + 'static>),

    #[error("state machine snapshot is at later index than commit index")]
    SnapshotCorrupted,

    #[error("error interacting with the state machine {}", _0)]
    StateMachine(Box<dyn StdError + 'static>),
}
