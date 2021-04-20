//! Error type with all possible errors
use crate::{ServerId, Term};
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

    #[error("catching up failed because of timeout or leader change")]
    CatchUpFailed,

    #[error("catching up failed because of bug")]
    CatchUpBug,

    #[error("BUG: peer leader with matching term detected")]
    AnotherLeader(ServerId, Term),

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
