//! Error type with all possible errors
use crate::{LogIndex, ServerId, Term};
use std::error::Error as StdError;
use thiserror::Error as ThisError;
use uuid::BytesError;

#[cfg(feature = "use_capnp")]
use capnp::Error as CapnpError;
#[cfg(feature = "use_capnp")]
use capnp::NotInSchema;

#[error("Consensus error")]
#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Consensus have reached unrecoverable error: {}", _0)]
    Critical(#[from] CriticalError),

    #[error("Consensus state was not Leader while it had to be.")]
    MustLeader,

    #[error("Consensus state was Leader while it had NOT to be.")]
    MustNotLeader,

    #[error("Follower responded with inconsistent index.")]
    BadFollowerIndex,

    #[error("Unknown peer")]
    UnknownPeer(ServerId),

    #[error("unexpected message")]
    UnexpectedMessage,

    #[error("trying to removing last node makes cluster unable to being restored")]
    LastNodeRemoval,

    #[error("UUID conversion")]
    Uuid(#[from] BytesError),

    #[error("Decoding capnp")]
    #[cfg(feature = "use_capnp")]
    Capnp(#[from] CapnpError),

    #[error("Detecting capnp schema")]
    #[cfg(feature = "use_capnp")]
    CapnpSchema(#[from] NotInSchema),

    #[error("Error in PersistentLog")]
    // TODO: Proper error conversions
    PersistentLog(Box<dyn StdError + 'static + Send + Sync>),

    #[error("Follower node must be in list of peers at start")]
    MustBootstrap,

    #[error("unable to support {} cluster members", _0)]
    BadClusterSize(usize),
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

#[error("Critical error in consensus")]
#[derive(ThisError, Debug)]
pub enum CriticalError {
    #[error("BUG: unreachable condition at {}", _0)]
    Unreachable(&'static str),

    #[error("BUG: leader's log could not find term for index {} at {}", _0, _1)]
    LeaderLogBroken(LogIndex, &'static str),

    #[error("Consensus state become unrecoverable after last error, consensus cannot proceed")]
    Unrecoverable,

    #[error("BUG: peer leader with matching term detected")]
    AnotherLeader(ServerId, Term),
}
