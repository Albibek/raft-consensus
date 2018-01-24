use std::error::Error as StdError;
use uuid::ParseError;
use {ServerId, Term};

#[fail(display = "Consensus error")]
#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "Consensus state was not Leader while it had to be.")] MustLeader,
    #[fail(display = "Consensus state was Leader while it had NOT to be.")] MustNotLeader,
    #[fail(display = "Follower responded with inconsistent index.")] BadFollowerIndex,
    #[fail(display = "BUG: peer leader with matching term detected")] AnotherLeader(ServerId, Term),
    #[fail(display = "UUID conversion")] Uuid(#[cause] ParseError),
    #[fail(display = "Error in PersistentLog")]
    // TODO: Proper error conversions
    PersistentLog(Box<StdError + 'static + Send + Sync>),
}
