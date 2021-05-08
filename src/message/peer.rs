#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "use_capnp")]
use crate::error::Error;

#[cfg(feature = "use_capnp")]
use crate::messages_capnp::*;

#[cfg(feature = "use_capnp")]
use capnp::message::{Allocator, Builder, HeapAllocator, Reader, ReaderSegments};

use crate::persistent_log::LogEntry;

use crate::{LogIndex, Term};

/// Module contains all messages required for consensus' peer message API

#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq)]
/// Any message that cluster peers can exchange
pub enum PeerMessage {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
}

#[cfg(feature = "use_capnp")]
impl PeerMessage {
    //    pub fn from_capnp<'a>(reader: peer_message::Reader<'a>) -> Result<Self, Error> {
    //match reader.which().map_err(Error::CapnpSchema)? {
    //peer_message::Which::AppendEntriesRequest(message) => {
    //Ok(AppendEntriesRequest::from_capnp(message?)?.into())
    //}
    //peer_message::Which::AppendEntriesResponse(message) => {
    //Ok(AppendEntriesResponse::from_capnp(message?)?.into())
    //}
    //peer_message::Which::RequestVoteRequest(message) => {
    //Ok(RequestVoteRequest::from_capnp(message?)?.into())
    //}
    //peer_message::Which::RequestVoteResponse(message) => {
    //Ok(RequestVoteResponse::from_capnp(message?)?.into())
    //}
    //}
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut peer_message::Builder<'a>) {
    //match self {
    //&PeerMessage::AppendEntriesRequest(ref message) => {
    //let mut builder = builder.reborrow().init_append_entries_request();
    //message.fill_capnp(&mut builder);
    //}
    //&PeerMessage::AppendEntriesResponse(ref message) => {
    //let mut builder = builder.reborrow().init_append_entries_response();
    //message.fill_capnp(&mut builder);
    //}
    //&PeerMessage::RequestVoteRequest(ref message) => {
    //let mut builder = builder.reborrow().init_request_vote_request();
    //message.fill_capnp(&mut builder);
    //}
    //&PeerMessage::RequestVoteResponse(ref message) => {
    //let mut builder = builder.reborrow().init_request_vote_response();
    //message.fill_capnp(&mut builder);
    //}
    //};
    //}

    //common_capnp!(peer_message::Builder, peer_message::Reader);
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Request for Raft AppendEntriesRPC
pub struct AppendEntriesRequest {
    /// The leader's term.
    pub term: Term,

    /// Index of log entry immediately preceding new ones
    pub prev_log_index: LogIndex,

    /// Term of prevLogIndex entry
    pub prev_log_term: Term,

    /// The Leader’s commit log index.
    pub leader_commit: LogIndex,

    /// Log entries to store (empty for heartbeat; may send more than one for efficiency)
    pub entries: Vec<Entry>,
}

impl From<AppendEntriesRequest> for PeerMessage {
    fn from(msg: AppendEntriesRequest) -> PeerMessage {
        PeerMessage::AppendEntriesRequest(msg)
    }
}

#[cfg(feature = "use_capnp")]
impl AppendEntriesRequest {
    //    pub fn from_capnp<'a>(reader: append_entries_request::Reader<'a>) -> Result<Self, Error> {
    //Ok(Self {
    //term: reader.get_term().into(),
    //prev_log_index: reader.get_prev_log_index().into(),
    //prev_log_term: reader.get_prev_log_term().into(),
    //leader_commit: reader.get_leader_commit().into(),
    //entries: if reader.has_entries() {
    //let entries = reader.get_entries().map_err(Error::Capnp)?;
    //let mut v = Vec::with_capacity(entries.len() as usize);
    //for e in entries.iter() {
    //v.push(Entry::from_capnp(e)?);
    //}
    //v
    //} else {
    //Vec::new()
    //},
    //})
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut append_entries_request::Builder<'a>) {
    //builder.set_term(self.term.into());
    //builder.set_prev_log_term(self.prev_log_term.into());
    //builder.set_prev_log_index(self.prev_log_index.into());
    //builder.set_leader_commit(self.leader_commit.into());
    //if self.entries.len() > 0 {
    //// TODO: guarantee entries length fits u32
    //let mut entries = builder.reborrow().init_entries(self.entries.len() as u32);

    //for (n, entry) in self.entries.iter().enumerate() {
    //let mut slot = entries.reborrow().get(n as u32);
    //entry.fill_capnp(&mut slot);
    //}
    //}
    //}

    //common_capnp!(
    //append_entries_request::Builder,
    //append_entries_request::Reader
    //);
}

/// Type representing a part of the AppendEntriesRequest message
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum Entry {
    /// Means entry is e heartbeat. Unlike the original paper recommendation we can
    /// differentiate empty entry not to be added to the log and one to be. So we do it for the
    /// sake of less coding errors.
    Heartbeat,
    /// An empty entry, which is added to every node's log at the beginning of each term
    Empty(LogEntry),
    /// A client proposal that should be provided to the state machine
    Proposal(LogEntry),
    /// A configuration change
    Config(LogEntry, bool),
}

#[cfg(feature = "use_capnp")]
impl Entry {
    //pub fn from_capnp<'a>(reader: entry_capnp::Reader<'a>) -> Result<Self, Error> {
    //        let data = match reader.get_data()?.which()? {
    //entry_data::Which::Client(reader) => EntryData::Client(reader?.to_vec()),
    //entry_data::Which::AddServer(reader) => {
    //let add_server_entry = reader?;
    //EntryData::AddServer(
    //add_server_entry.get_id().into(),
    //add_server_entry.get_info()?.to_vec(),
    //)
    //}
    //entry_data::Which::RemoveServer(id) => EntryData::RemoveServer(id.into()),
    //};

    //Ok(Entry {
    //term: reader.get_term().into(),
    //data,
    //})
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut entry_capnp::Builder<'a>) {
    //builder.set_term(self.term.as_u64());
    //let mut data_builder = builder.reborrow().init_data();
    //match self.data {
    //EntryData::Client(ref data) => data_builder.set_client(data),
    //EntryData::AddServer(id, ref info) => {
    //let mut add_server_builder = data_builder.init_add_server();
    //add_server_builder.set_id(id.into());
    //add_server_builder.set_info(info);
    //}
    //EntryData::RemoveServer(id) => data_builder.set_remove_server(id.into()),
    //}
    //}

    //common_capnp!(entry_capnp::Builder, entry_capnp::Reader);
}

/// Type representing all possible data types that can be sent over network
/// inside AppendEntriesRequest
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum EntryData {
    Client(Vec<u8>),
    /// The new config when a configuration change happens.
    /// A flag shows if a config is the latest one or coming
    /// from some previous config change because the older config should probably not be
    /// considered by node, but sill have to be added to log for consistency.
    Config(ConsensusConfig, bool),
    // TODO:
    // RegisterClient
    // RemoveClient
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Response for Raft AppendEntriesRPC
pub enum AppendEntriesResponse {
    Success(Term, LogIndex),
    StaleTerm(Term),
    InconsistentPrevEntry(Term, LogIndex),
    StaleEntry,
}

impl From<AppendEntriesResponse> for PeerMessage {
    fn from(msg: AppendEntriesResponse) -> PeerMessage {
        PeerMessage::AppendEntriesResponse(msg)
    }
}

#[cfg(feature = "use_capnp")]
impl AppendEntriesResponse {
    //    pub fn from_capnp<'a>(reader: append_entries_response::Reader<'a>) -> Result<Self, Error> {
    //let message = match reader.which().map_err(Error::CapnpSchema)? {
    //append_entries_response::Success(m) => {
    //let m = m.map_err(Error::Capnp)?;
    //AppendEntriesResponse::Success(m.get_term().into(), m.get_log_index().into())
    //}
    //append_entries_response::StaleTerm(m) => AppendEntriesResponse::StaleTerm(m.into()),
    //append_entries_response::InconsistentPrevEntry(m) => {
    //let m = m.map_err(Error::Capnp)?;
    //AppendEntriesResponse::InconsistentPrevEntry(
    //m.get_term().into(),
    //m.get_log_index().into(),
    //)
    //}
    //append_entries_response::StaleEntry(()) => AppendEntriesResponse::StaleEntry,
    //};
    //Ok(message)
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut append_entries_response::Builder<'a>) {
    //match self {
    //&AppendEntriesResponse::Success(term, log_index) => {
    //let mut message = builder.reborrow().init_success();
    //message.set_term(term.into());
    //message.set_log_index(log_index.into());
    //}
    //&AppendEntriesResponse::StaleTerm(term) => builder.set_stale_term(term.into()),
    //&AppendEntriesResponse::InconsistentPrevEntry(term, log_index) => {
    //let mut message = builder.reborrow().init_inconsistent_prev_entry();
    //message.set_term(term.into());
    //message.set_log_index(log_index.into());
    //}
    //&AppendEntriesResponse::StaleEntry => builder.set_stale_entry(()),
    //}
    //}

    //common_capnp!(
    //append_entries_response::Builder,
    //append_entries_response::Reader
    //);
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Request for Raft voting RPC
pub struct RequestVoteRequest {
    /// The candidate's term.
    pub term: Term,

    /// The index of the candidate's last log entry.
    pub last_log_index: LogIndex,

    /// The term of the candidate's last log entry.
    pub last_log_term: Term,

    /// If this request was done due to leadership given away by leader
    /// by it's own will
    pub is_voluntary_step_down: bool,
}

impl From<RequestVoteRequest> for PeerMessage {
    fn from(msg: RequestVoteRequest) -> PeerMessage {
        PeerMessage::RequestVoteRequest(msg)
    }
}

#[cfg(feature = "use_capnp")]
impl RequestVoteRequest {
    //    pub fn from_capnp<'a>(reader: request_vote_request::Reader<'a>) -> Result<Self, Error> {
    //Ok(Self {
    //term: reader.get_term().into(),
    //last_log_index: reader.get_last_log_index().into(),
    //last_log_term: reader.get_last_log_term().into(),
    //is_voluntary_step_down: reader.get_is_voluntary_step_down(),
    //})
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut request_vote_request::Builder<'a>) {
    //builder.set_term(self.term.into());
    //builder.set_last_log_term(self.last_log_term.into());
    //builder.set_last_log_index(self.last_log_index.into());
    //builder.set_is_voluntary_step_down(self.is_voluntary_step_down);
    //}

    //common_capnp!(request_vote_request::Builder, request_vote_request::Reader);
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Response for Raft voting RPC
pub enum RequestVoteResponse {
    StaleTerm(Term),
    InconsistentLog(Term),
    Granted(Term),
    AlreadyVoted(Term),
}

impl From<RequestVoteResponse> for PeerMessage {
    fn from(msg: RequestVoteResponse) -> PeerMessage {
        PeerMessage::RequestVoteResponse(msg)
    }
}

impl RequestVoteResponse {
    pub fn voter_term(&self) -> Term {
        match *self {
            RequestVoteResponse::StaleTerm(t)
            | RequestVoteResponse::InconsistentLog(t)
            | RequestVoteResponse::Granted(t)
            | RequestVoteResponse::AlreadyVoted(t) => t,
        }
    }
}

#[cfg(feature = "use_capnp")]
impl RequestVoteResponse {
    // pub fn from_capnp<'a>(reader: request_vote_response::Reader<'a>) -> Result<Self, Error> {
    //let message = match reader.which().map_err(Error::CapnpSchema)? {
    //request_vote_response::Which::StaleTerm(m) => RequestVoteResponse::StaleTerm(m.into()),
    //request_vote_response::Which::InconsistentLog(m) => {
    //RequestVoteResponse::InconsistentLog(m.into())
    //}
    //request_vote_response::Which::Granted(m) => RequestVoteResponse::Granted(m.into()),
    //request_vote_response::Which::AlreadyVoted(m) => {
    //RequestVoteResponse::AlreadyVoted(m.into())
    //}
    //};
    //Ok(message)
    //}

    //pub fn fill_capnp<'a>(&self, builder: &mut request_vote_response::Builder<'a>) {
    //match self {
    //&RequestVoteResponse::StaleTerm(term) => builder.set_stale_term(term.into()),
    //&RequestVoteResponse::InconsistentLog(term) => {
    //builder.set_inconsistent_log(term.into())
    //}
    //&RequestVoteResponse::Granted(term) => builder.set_granted(term.into()),
    //&RequestVoteResponse::AlreadyVoted(term) => builder.set_already_voted(term.into()),
    //}
    //}

    //common_capnp!(
    //request_vote_response::Builder,
    //request_vote_response::Reader
    //);
}
