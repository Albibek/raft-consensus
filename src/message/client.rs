#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "use_capnp")]
use crate::error::Error;

#[cfg(feature = "use_capnp")]
use crate::messages_capnp::*;

#[cfg(feature = "use_capnp")]
use capnp::message::{Allocator, Builder, HeapAllocator, Reader, ReaderSegments};

use super::common::ConsensusState;

use crate::entry::Entry;
use crate::{LogIndex, ServerId, Term};

/// The module contains all messages related to client API of a consensus
/// Since, the structures for proposal request and responses are totally same,
/// they are only differentiated at the level of `ClientMessage` enum, being same internally

#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq)]
/// Any message related to client requests and responses
pub enum ClientMessage {
    ClientProposalRequest(Vec<u8>),
    ClientProposalResponse(ClientResponse),
    ClientQueryRequest(Vec<u8>),
    ClientQueryResponse(ClientResponse),
}

#[cfg(feature = "use_capnp")]
impl ClientMessage {
    //    pub fn from_capnp<'a>(reader: client_request::Reader<'a>) -> Result<Self, Error> {
    //let message = match reader.which().map_err(Error::CapnpSchema)? {
    //client_request::Which::Proposal(data) => {
    //let data = data.map_err(Error::Capnp)?;
    //ClientCommandRequest::Proposal(data.into())
    //}
    //client_request::Which::Query(data) => {
    //let data = data.map_err(Error::Capnp)?;
    //ClientCommandRequest::Query(data.into())
    //}
    //};
    //Ok(message)
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut client_request::Builder<'a>) {
    //match self {
    //&ClientCommandRequest::Proposal(ref data) => builder.set_proposal(&data),
    //&ClientCommandRequest::Query(ref data) => builder.set_query(&data),
    //}
    //}

    //common_capnp!(client_request::Builder, client_request::Reader);
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Response to client command
pub enum ClientResponse {
    Success(Vec<u8>),

    /// The proposal has been queued on the leader and waiting the majority
    /// of nodes to commit it
    Queued,

    /// The request failed because the Raft node is not the leader, and does
    /// not know who the leader is.
    UnknownLeader,

    /// The client request failed because the Raft node is not the leader.
    /// The value returned may be the address of the current leader.
    NotLeader(ServerId),
}

#[cfg(feature = "use_capnp")]
impl ClientResponse {
    //pub fn from_capnp<'a>(reader: command_response::Reader<'a>) -> Result<Self, Error> {
    //let message = match reader.which().map_err(Error::CapnpSchema)? {
    //command_response::Which::Success(data) => {
    //let data = data.map_err(Error::Capnp)?.to_vec();
    //ClientResponse::Success(data)
    //}
    //command_response::Which::Queued(()) => ClientResponse::Queued,
    //command_response::Which::UnknownLeader(()) => ClientResponse::UnknownLeader,
    //command_response::Which::NotLeader(id) => ClientResponse::NotLeader(id.into()),
    //};
    //Ok(message)
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut command_response::Builder<'a>) {
    //match self {
    //&ClientResponse::Success(ref data) => builder.set_success(&data),
    //&ClientResponse::Queued => builder.set_queued(()),
    //&ClientResponse::UnknownLeader => builder.set_unknown_leader(()),
    //&ClientResponse::NotLeader(id) => builder.set_not_leader(id.into()),
    //}
    //}

    //common_capnp!(command_response::Builder, command_response::Reader);
}
