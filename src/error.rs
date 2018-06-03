//! Error type with all possible errors
use std::error::Error as StdError;
use uuid::ParseError;
use {ServerId, Term};

#[cfg(feature = "use_capnp")]
use capnp::Error as CapnpError;
#[cfg(feature = "use_capnp")]
use capnp::NotInSchema;

#[fail(display = "Consensus error")]
#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "Consensus state was not Leader while it had to be.")]
    MustLeader,

    #[fail(display = "Consensus state was Leader while it had NOT to be.")]
    MustNotLeader,

    #[fail(display = "Follower responded with inconsistent index.")]
    BadFollowerIndex,

    #[fail(display = "BUG: peer leader with matching term detected")]
    AnotherLeader(ServerId, Term),

    #[fail(display = "UUID conversion")]
    Uuid(#[cause] ParseError),

    #[cfg(feature = "use_capnp")]
    #[fail(display = "Decoding capnp")]
    Capnp(#[cause] CapnpError),

    #[cfg(feature = "use_capnp")]
    #[fail(display = "Detecting capnp schema")]
    CapnpSchema(#[cause] NotInSchema),

    #[fail(display = "Error in PersistentLog")]
    // TODO: Proper error conversions
    PersistentLog(Box<StdError + 'static + Send + Sync>),
}
