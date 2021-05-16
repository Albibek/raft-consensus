#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "use_capnp")]
use crate::error::Error;

#[cfg(feature = "use_capnp")]
use crate::messages_capnp::*;

#[cfg(feature = "use_capnp")]
use capnp::message::{Allocator, Builder, HeapAllocator, Reader, ReaderSegments};

use crate::{LogIndex, ServerId, Term};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Any message related to client requests and responses
pub enum AdminMessage {
    AddServerRequest(AddServerRequest),
    AddServerResponse(ConfigurationChangeResponse),
    RemoveServerRequest(RemoveServerRequest),
    RemoveServerResponse(ConfigurationChangeResponse),

    StepDownRequest(Option<ServerId>),
    StepDownResponse(ConfigurationChangeResponse),

    PingRequest,
    PingResponse(PingResponse),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Request for adding new server to cluster
pub struct AddServerRequest {
    /// The id of new server being added
    pub id: ServerId,
    pub info: Vec<u8>,
}

#[cfg(feature = "use_capnp")]
impl AddServerRequest {
    //    pub fn from_capnp<'a>(reader: add_server_request::Reader<'a>) -> Result<Self, Error> {
    //Ok(Self {
    //id: reader.get_id().into(),
    //info: reader.get_info()?.to_vec(),
    //})
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut add_server_request::Builder<'a>) {
    //builder.set_id(self.id.into());
    //builder.set_info(&self.info);
    //}

    //common_capnp!(add_server_request::Builder, add_server_request::Reader);
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Request for adding new server to cluster
pub struct RemoveServerRequest {
    /// The id of new server being added
    pub id: ServerId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Response when adding new server to cluster
pub enum ConfigurationChangeResponse {
    Success,
    Started,
    BadPeer,
    LeaderJustChanged,
    AlreadyPending,
    UnknownLeader,
    NotLeader(ServerId),
}

#[cfg(feature = "use_capnp")]
impl ConfigurationChangeResponse {
    //    pub fn from_capnp<'a>(reader: add_server_response::Reader<'a>) -> Result<Self, Error> {
    //let message = match reader.which().map_err(Error::CapnpSchema)? {
    //server_command_response::Which::Success(()) => ConfigurationChangeResponse::Success,
    //server_command_response::Which::BadPeer(()) => ConfigurationChangeResponse::BadPeer,
    //server_command_response::Which::LeaderJustChanged(()) => {
    //ConfigurationChangeResponse::LeaderJustChanged
    //}
    //server_command_response::Which::AlreadyPending(()) => {
    //ConfigurationChangeResponse::AlreadyPending
    //}
    //server_command_response::Which::UnknownLeader(()) => {
    //ConfigurationChangeResponse::UnknownLeader
    //}
    //server_command_response::Which::NotLeader(id) => {
    //ConfigurationChangeResponse::NotLeader(id.into())
    //}
    //};
    //Ok(message)
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut server_command_response::Builder<'a>) {
    //match self {
    //&ConfigurationChangeResponse::Success => builder.set_success(()),
    //&ConfigurationChangeResponse::BadPeer => builder.set_bad_peer(()),
    //&ConfigurationChangeResponse::LeaderJustChanged => builder.set_leader_just_changed(()),
    //&ConfigurationChangeResponse::AlreadyPending => builder.set_already_pending(()),
    //&ConfigurationChangeResponse::UnknownLeader => builder.set_unknown_leader(()),
    //&ConfigurationChangeResponse::NotLeader(id) => builder.set_not_leader(id.into()),
    //}
    //}

    //common_capnp!(add_server_response::Builder, add_server_response::Reader);
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Part of client message.
pub struct PingResponse {
    /// The server's current term
    pub term: Term,

    /// The server's current index
    pub index: LogIndex,

    /// The server's current state
    pub state: ConsensusState,
}

#[cfg(feature = "use_capnp")]
impl PingResponse {
    //pub fn from_capnp<'a>(reader: ping_response::Reader<'a>) -> Result<Self, Error> {
    //Ok(Self {
    //term: reader.get_term().into(),
    //index: reader.get_index().into(),
    //state: {
    //let reader = reader.get_state().map_err(Error::Capnp)?;
    //ConsensusState::from_capnp(reader)?
    //},
    //})
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut ping_response::Builder<'a>) {
    //builder.set_term(self.term.into());
    //builder.set_index(self.index.into());
    //{
    //let mut builder = builder.reborrow().init_state();
    //self.state.fill_capnp(&mut builder);
    //}
    //}

    //common_capnp!(ping_response::Builder, ping_response::Reader);
}

/// A state of the node. Used in ping responses.
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Any message that cluster peers can exchange
pub enum ConsensusState {
    Follower,
    Candidate,
    Leader,
}

#[cfg(feature = "use_capnp")]
impl ConsensusState {
    //    pub fn from_capnp<'a>(reader: consensus_state::Reader<'a>) -> Result<Self, Error> {
    //match reader.which().map_err(Error::CapnpSchema)? {
    //consensus_state::Which::Follower(()) => Ok(ConsensusState::Follower),
    //consensus_state::Which::Candidate(()) => Ok(ConsensusState::Candidate),
    //consensus_state::Which::Leader(()) => Ok(ConsensusState::Leader),
    //}
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut consensus_state::Builder<'a>) {
    //match self {
    //&ConsensusState::Follower => builder.reborrow().set_follower(()),
    //&ConsensusState::Candidate => builder.reborrow().set_candidate(()),
    //&ConsensusState::Leader => builder.reborrow().set_leader(()),
    //};
    //}

    //common_capnp!(consensus_state::Builder, consensus_state::Reader);
}
