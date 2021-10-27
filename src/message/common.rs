#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "use_capnp")]
use crate::error::Error;

#[cfg(feature = "use_capnp")]
use crate::messages_capnp::*;

#[cfg(feature = "use_capnp")]
use capnp::message::{Allocator, Builder, HeapAllocator, Reader, ReaderSegments};

/// Module contains message types used in consensus
/// Any network message have to be converted to theese enums to be processed
use crate::ServerId;

#[cfg(feature = "use_capnp")]
#[cfg_attr(feature = "use_capnp", macro_export)]
macro_rules! common_capnp {
    ($b:ty, $r:ty) => {
        pub fn as_capnp<A: Allocator>(&self, allocator: A) -> Builder<A> {
            let mut builder = Builder::new(allocator);
            {
                let mut root = builder.init_root::<$b>();
                self.fill_capnp(&mut root);
            }
            builder
        }

        pub fn as_capnp_heap(&self) -> Builder<HeapAllocator> {
            self.as_capnp(HeapAllocator::new())
        }

        pub fn from_capnp_untyped<S: ReaderSegments>(reader: Reader<S>) -> Result<Self, Error> {
            let message = reader.get_root::<$r>().map_err(Error::Capnp)?;
            Self::from_capnp(message)
        }
    };
}

/// Consensus timeout types. Usually not sent over the network, but may still be, so
/// they are treated as the ususl messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum Timeout {
    /// An election timeout. Randomized value.
    Election,
    /// A heartbeat timeout. Stable value, set by leader separately for each peer.
    Heartbeat(ServerId),
    /// A client timeout, if time based client message batching is enabled
    Client,
}

// TODO
#[cfg(test)]
mod test {

    //#[cfg(feature = "use_capnp")]
    //use message::*;

    //#[cfg(feature = "use_capnp")]
    //macro_rules! test_message_capnp {
    //($i:ident, $t:ty) => {
    //fn $i(message: $t) {
    //let builder = message.as_capnp_heap();
    //let mut encoded = Vec::new();

    //::capnp::serialize::write_message(&mut encoded, &builder).unwrap();
    //let mut encoded = ::std::io::Cursor::new(encoded);
    //let decoded = ::capnp::serialize::read_message(
    //&mut encoded,
    //::capnp::message::DEFAULT_READER_OPTIONS,
    //)
    //.unwrap();
    //let decoded = <$t>::from_capnp_untyped(decoded).unwrap();
    //assert_eq!(message, decoded);
    //}
    //};
    //}

    //#[cfg(feature = "use_capnp")]
    //test_message_capnp!(test_peer_message_capnp, PeerMessage);

    //#[test]
    //#[cfg(feature = "use_capnp")]
    //fn test_append_entries_request_capnp() {
    //let message = AppendEntriesRequest {
    //// The values are pretty random here, maybe not matching raft conditions
    //term: 5.into(),
    //prev_log_index: 3.into(),
    //prev_log_term: 2.into(),
    //leader_commit: 4.into(),
    //entries: vec![Entry {
    //term: 9.into(),
    //data: "qwer".to_string().into_bytes(),
    //}],
    //};

    //test_peer_message_capnp(message.into());
    //}

    //#[test]
    //#[cfg(feature = "use_capnp")]
    //fn test_append_entries_response_capnp() {
    //let message = AppendEntriesResponse::Success(1.into(), 2.into());
    //test_peer_message_capnp(message.into());
    //let message = AppendEntriesResponse::StaleTerm(3.into());
    //test_peer_message_capnp(message.into());
    //let message = AppendEntriesResponse::InconsistentPrevEntry(4.into(), 5.into());
    //test_peer_message_capnp(message.into());
    //let message = AppendEntriesResponse::StaleEntry;
    //test_peer_message_capnp(message.into());
    //}

    //#[test]
    //#[cfg(feature = "use_capnp")]
    //fn test_request_vote_request_capnp() {
    //let message = RequestVoteRequest {
    //term: 1.into(),
    //last_log_index: 2.into(),
    //last_log_term: 3.into(),
    //};
    //test_peer_message_capnp(message.into());
    //}

    //#[test]
    //#[cfg(feature = "use_capnp")]
    //fn test_request_vote_response_capnp() {
    //let message = RequestVoteResponse::StaleTerm(1.into());
    //test_peer_message_capnp(message.into());
    //let message = RequestVoteResponse::Granted(2.into());
    //test_peer_message_capnp(message.into());
    //let message = RequestVoteResponse::InconsistentLog(3.into());
    //test_peer_message_capnp(message.into());
    //let message = RequestVoteResponse::AlreadyVoted(4.into());
    //test_peer_message_capnp(message.into());
    //}

    //#[test]
    //#[cfg(feature = "use_capnp")]
    //fn test_client_request_capnp() {
    //test_message_capnp!(test_message, ClientRequest);

    //let message = ClientRequest::Proposal("proposal".to_string().into_bytes());
    //test_message(message);
    //let message = ClientRequest::Query("query".to_string().into_bytes());
    //test_message(message);
    //let message = ClientRequest::Ping;
    //test_message(message);
    //}

    //#[test]
    //#[cfg(feature = "use_capnp")]
    //fn test_client_response_capnp() {
    //test_message_capnp!(test_message, ClientResponse);

    //let message = ClientResponse::Ping(PingResponse {
    //term: 10000.into(),
    //index: 2000.into(),
    //state: ConsensusState::Leader,
    //});
    //test_message(message);
    //}
}
