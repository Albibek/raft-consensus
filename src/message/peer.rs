use bytes::Bytes;
#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "use_capnp")]
use crate::error::Error;

#[cfg(feature = "use_capnp")]
use crate::messages_capnp::*;

#[cfg(feature = "use_capnp")]
use crate::messages_capnp::peer_message::*;
use crate::persistent_log::{LogEntry, LogEntryData};

#[cfg(feature = "use_capnp")]
use capnp::message::{Allocator, Builder, HeapAllocator, Reader, ReaderSegments};

use crate::{config::ConsensusConfig, message::client::ClientGuarantee};

use crate::{LogIndex, Peer, ServerId, Term};

/// Module contains all messages required for consensus' peer message API

#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Any message that cluster peers can exchange
pub enum PeerMessage {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    TimeoutNow,
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

#[cfg(feature = "use_capnp")]
impl PeerMessage {
    pub fn from_capnp<'a>(reader: peer_message::Reader<'a>) -> Result<Self, Error> {
        match reader.which().map_err(Error::CapnpSchema)? {
            peer_message::Which::AppendEntriesRequest(message) => {
                Ok(AppendEntriesRequest::from_capnp(message?)?.into())
            }
            peer_message::Which::AppendEntriesResponse(message) => {
                Ok(AppendEntriesResponse::from_capnp(message?)?.into())
            }
            peer_message::Which::RequestVoteRequest(message) => {
                Ok(RequestVoteRequest::from_capnp(message?)?.into())
            }
            peer_message::Which::RequestVoteResponse(message) => {
                Ok(RequestVoteResponse::from_capnp(message?)?.into())
            }
            peer_message::Which::TimeoutNow(()) => Ok(PeerMessage::TimeoutNow),
            peer_message::Which::InstallSnapshotRequest(message) => {
                Ok(InstallSnapshotRequest::from_capnp(message?)?.into())
            }
            peer_message::Which::InstallSnapshotResponse(message) => {
                Ok(InstallSnapshotResponse::from_capnp(message?)?.into())
            }
        }
    }

    pub fn fill_capnp<'a>(&self, builder: &mut peer_message::Builder<'a>) {
        match self {
            &PeerMessage::AppendEntriesRequest(ref message) => {
                let mut builder = builder.reborrow().init_append_entries_request();
                message.fill_capnp(&mut builder);
            }
            &PeerMessage::AppendEntriesResponse(ref message) => {
                let mut builder = builder.reborrow().init_append_entries_response();
                message.fill_capnp(&mut builder);
            }
            &PeerMessage::RequestVoteRequest(ref message) => {
                let mut builder = builder.reborrow().init_request_vote_request();
                message.fill_capnp(&mut builder);
            }
            &PeerMessage::RequestVoteResponse(ref message) => {
                let mut builder = builder.reborrow().init_request_vote_response();
                message.fill_capnp(&mut builder);
            }
            &PeerMessage::TimeoutNow => {
                builder.reborrow().set_timeout_now(());
            }
            PeerMessage::InstallSnapshotRequest(ref message) => {
                let mut builder = builder.reborrow().init_install_snapshot_request();
                message.fill_capnp(&mut builder);
            }
            PeerMessage::InstallSnapshotResponse(ref message) => {
                let mut builder = builder.reborrow().init_install_snapshot_response();
                message.fill_capnp(&mut builder);
            }
        };
    }

    common_capnp!(peer_message::Builder, peer_message::Reader);
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

    /// Log entries to store (empty for heartbeat)
    pub entries: Vec<Entry>,
}

impl From<AppendEntriesRequest> for PeerMessage {
    fn from(msg: AppendEntriesRequest) -> PeerMessage {
        PeerMessage::AppendEntriesRequest(msg)
    }
}

impl AppendEntriesRequest {
    pub fn new(num_entries: usize) -> Self {
        Self {
            term: Term(0),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            leader_commit: LogIndex(0),
            entries: Vec::with_capacity(num_entries),
        }
    }
}

#[cfg(feature = "use_capnp")]
impl AppendEntriesRequest {
    pub fn from_capnp<'a>(reader: append_entries_request::Reader<'a>) -> Result<Self, Error> {
        Ok(Self {
            term: reader.get_term().into(),
            prev_log_index: reader.get_prev_log_index().into(),
            prev_log_term: reader.get_prev_log_term().into(),
            leader_commit: reader.get_leader_commit().into(),
            entries: if reader.has_entries() {
                let entries = reader.get_entries().map_err(Error::Capnp)?;
                let mut v = Vec::with_capacity(entries.len() as usize);
                for e in entries.iter() {
                    v.push(Entry::from_capnp(e)?);
                }
                v
            } else {
                Vec::new()
            },
        })
    }

    pub fn fill_capnp<'a>(&self, builder: &mut append_entries_request::Builder<'a>) {
        builder.set_term(self.term.into());
        builder.set_prev_log_term(self.prev_log_term.into());
        builder.set_prev_log_index(self.prev_log_index.into());
        builder.set_leader_commit(self.leader_commit.into());
        if self.entries.len() > 0 {
            // TODO: guarantee entries length fits u32
            let mut entries = builder.reborrow().init_entries(self.entries.len() as u32);

            for (n, entry) in self.entries.iter().enumerate() {
                let mut slot = entries.reborrow().get(n as u32);
                entry.fill_capnp(&mut slot);
            }
        }
    }

    common_capnp!(
        append_entries_request::Builder,
        append_entries_request::Reader
    );
}

/// Type representing a part of the AppendEntriesRequest message.
/// It not the same type as a LogEntry, because of additional fields (like config activeness flag)
/// and may become more different in future.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct Entry {
    pub term: Term,
    pub data: EntryData,
}

impl Entry {
    pub(crate) fn set_config_active(&mut self, is_active: bool) {
        if let EntryData::Config(_, ref mut active) = self.data {
            *active = is_active
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum EntryData {
    /// An empty entry, which is added to every node's log at the beginning of each term
    Noop,
    /// A client proposal that should be provided to the state machine
    Proposal(Bytes, ClientGuarantee),
    /// A configuration change with an flag showing if the change is active now
    Config(ConsensusConfig, bool),
}

impl From<Entry> for LogEntry {
    fn from(e: Entry) -> Self {
        LogEntry {
            term: e.term,
            data: match e.data {
                EntryData::Noop => LogEntryData::Empty,
                EntryData::Proposal(proposal, guarantee) => {
                    LogEntryData::Proposal(proposal, guarantee)
                }
                EntryData::Config(c, _) => LogEntryData::Config(c),
            },
        }
    }
}
//impl Entry {
//pub fn as_entry_ref<'a>(&'a self) -> LogEntryRef<'a> {
//LogEntryRef {
//term: self.term,
//data: match self.data {
//EntryData::Noop => LogEntryDataRef::Empty,
//EntryData::Proposal(ref v, client, guarantee) => {
//LogEntryDataRef::Proposal(v.as_slice(), client, guarantee)
//}
//EntryData::Config(ref c, _, admin) => LogEntryDataRef::Config(c, admin),
//},
//}
//}
//}

#[cfg(feature = "use_capnp")]
impl Entry {
    pub fn from_capnp<'a>(reader: entry::Reader<'a>) -> Result<Self, Error> {
        todo!("from_capnp for proposal");
        // let data = match reader.which()? {
        //entry::Which::Noop(()) => EntryData::Noop,
        //entry::Which::Proposal(reader) => EntryData::Proposal(reader?.to_vec()),
        //entry::Which::Config(reader) => {
        //let reader = reader?;
        //let peers_reader = reader.get_peers()?;

        //let mut config = ConsensusConfig {
        //peers: Vec::with_capacity(peers_reader.len() as usize),
        //};
        //for e in peers_reader.iter() {
        //let peer = Peer {
        //id: ServerId(e.get_id()),
        //metadata: e.get_metadata().map(|s| s.to_vec()).unwrap_or(Vec::new()),
        //};
        //config.peers.push(peer);
        //}
        //EntryData::Config(config, reader.get_is_actual())
        //     }
        //};

        //Ok(Entry {
        //term: reader.get_term().into(),
        //data,
        //})
    }

    pub fn fill_capnp<'a>(&self, builder: &mut entry::Builder<'a>) {
        todo!()
        //        builder.set_term(self.term.as_u64());
        //match self.data {
        //EntryData::Noop => builder.set_noop(()),
        //EntryData::Proposal(ref data, client_id, guarantee) => {
        //todo!("capnpn for proposal struct");
        ////    builder.set_proposal(data)
        //}
        //EntryData::Config(ref config, ref is_actual, admin_id) => {
        //let mut config_builder = builder.reborrow().init_config();
        //let mut peers_builder = config_builder
        //.reborrow()
        //.init_peers(config.peers.len() as u32);

        //for (n, peer) in config.peers.iter().enumerate() {
        //let mut peer_slot = peers_builder.reborrow().get(n as u32);
        //peer_slot.set_id(peer.id.as_u64());
        //peer_slot.set_metadata(&peer.metadata);
        //}
        //config_builder.set_is_actual(*is_actual);
        //todo!("set admin id");
        ////config_builder.set_admin_id(admin_id);
        //}
        //        }
    }

    common_capnp!(entry::Builder, entry::Reader);
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Response for Raft AppendEntriesRPC
pub enum AppendEntriesResponse {
    Success(Term, LogIndex, LogIndex),
    StaleTerm(Term),
    InconsistentPrevEntry(Term, LogIndex, LogIndex),
    StaleEntry,
}

impl From<AppendEntriesResponse> for PeerMessage {
    fn from(msg: AppendEntriesResponse) -> PeerMessage {
        PeerMessage::AppendEntriesResponse(msg)
    }
}

#[cfg(feature = "use_capnp")]
impl AppendEntriesResponse {
    pub fn from_capnp<'a>(reader: append_entries_response::Reader<'a>) -> Result<Self, Error> {
        let message = match reader.which().map_err(Error::CapnpSchema)? {
            append_entries_response::Success(m) => {
                let m = m.map_err(Error::Capnp)?;
                AppendEntriesResponse::Success(
                    m.get_term().into(),
                    m.get_latest_log_index().into(),
                    m.get_volatile_log_index().into(),
                )
            }
            append_entries_response::StaleTerm(m) => AppendEntriesResponse::StaleTerm(m.into()),
            append_entries_response::InconsistentPrevEntry(m) => {
                let m = m.map_err(Error::Capnp)?;
                AppendEntriesResponse::InconsistentPrevEntry(
                    m.get_term().into(),
                    m.get_latest_log_index().into(),
                    m.get_volatile_log_index().into(),
                )
            }
            append_entries_response::StaleEntry(()) => AppendEntriesResponse::StaleEntry,
        };
        Ok(message)
    }

    pub fn fill_capnp<'a>(&self, builder: &mut append_entries_response::Builder<'a>) {
        match self {
            &AppendEntriesResponse::Success(term, log_index, volatile_index) => {
                let mut message = builder.reborrow().init_success();
                message.set_term(term.into());
                message.set_latest_log_index(log_index.into());
                message.set_volatile_log_index(log_index.into());
            }
            &AppendEntriesResponse::StaleTerm(term) => builder.set_stale_term(term.into()),
            &AppendEntriesResponse::InconsistentPrevEntry(term, log_index, volatile_index) => {
                let mut message = builder.reborrow().init_inconsistent_prev_entry();
                message.set_term(term.into());
                message.set_latest_log_index(log_index.into());
                message.set_volatile_log_index(log_index.into());
            }
            &AppendEntriesResponse::StaleEntry => builder.set_stale_entry(()),
        }
    }

    common_capnp!(
        append_entries_response::Builder,
        append_entries_response::Reader
    );
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub fn from_capnp<'a>(reader: request_vote_request::Reader<'a>) -> Result<Self, Error> {
        Ok(Self {
            term: reader.get_term().into(),
            last_log_index: reader.get_last_log_index().into(),
            last_log_term: reader.get_last_log_term().into(),
            is_voluntary_step_down: reader.get_is_voluntary_step_down(),
        })
    }

    pub fn fill_capnp<'a>(&self, builder: &mut request_vote_request::Builder<'a>) {
        builder.set_term(self.term.into());
        builder.set_last_log_term(self.last_log_term.into());
        builder.set_last_log_index(self.last_log_index.into());
        builder.set_is_voluntary_step_down(self.is_voluntary_step_down);
    }

    common_capnp!(request_vote_request::Builder, request_vote_request::Reader);
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub fn from_capnp<'a>(reader: request_vote_response::Reader<'a>) -> Result<Self, Error> {
        let message = match reader.which().map_err(Error::CapnpSchema)? {
            request_vote_response::Which::StaleTerm(m) => RequestVoteResponse::StaleTerm(m.into()),
            request_vote_response::Which::InconsistentLog(m) => {
                RequestVoteResponse::InconsistentLog(m.into())
            }
            request_vote_response::Which::Granted(m) => RequestVoteResponse::Granted(m.into()),
            request_vote_response::Which::AlreadyVoted(m) => {
                RequestVoteResponse::AlreadyVoted(m.into())
            }
        };
        Ok(message)
    }

    pub fn fill_capnp<'a>(&self, builder: &mut request_vote_response::Builder<'a>) {
        match self {
            &RequestVoteResponse::StaleTerm(term) => builder.set_stale_term(term.into()),
            &RequestVoteResponse::InconsistentLog(term) => {
                builder.set_inconsistent_log(term.into())
            }
            &RequestVoteResponse::Granted(term) => builder.set_granted(term.into()),
            &RequestVoteResponse::AlreadyVoted(term) => builder.set_already_voted(term.into()),
        }
    }

    common_capnp!(
        request_vote_response::Builder,
        request_vote_response::Reader
    );
}

/////////////////////  Snapshots
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Request for Raft InstallSnapshot RPC
pub struct InstallSnapshotRequest {
    /// The leader's term.
    pub term: Term,

    /// For the first chunk: current cluster config
    pub last_config: Option<ConsensusConfig>,

    /// Index of the last entry included in the snapshot
    pub snapshot_index: LogIndex,

    /// Term of the last entry included in the snapshot
    pub snapshot_term: Term,

    /// Snapshot bytes to pass into consensus state machine
    pub chunk_data: Vec<u8>,
}

impl From<InstallSnapshotRequest> for PeerMessage {
    fn from(msg: InstallSnapshotRequest) -> PeerMessage {
        PeerMessage::InstallSnapshotRequest(msg)
    }
}

#[cfg(feature = "use_capnp")]
impl InstallSnapshotRequest {
    pub fn from_capnp<'a>(reader: install_snapshot_request::Reader<'a>) -> Result<Self, Error> {
        todo!("installsnapshotRequest::from_capnp");
        //        Ok(Self {
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
    }

    pub fn fill_capnp<'a>(&self, builder: &mut install_snapshot_request::Builder<'a>) {
        todo!("installsnapshotRequest::fill_capnp");
        //        builder.set_term(self.term.into());
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
    }

    //    common_capnp!(
    //install_snapshot_request::Builder,
    //install_snapshot_request::Reader
    //    );
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Response for Raft InstallSnapshotRPC
pub enum InstallSnapshotResponse {
    Success(Term, LogIndex, Option<Vec<u8>>),
    StaleTerm(Term),
}

impl From<InstallSnapshotResponse> for PeerMessage {
    fn from(msg: InstallSnapshotResponse) -> PeerMessage {
        PeerMessage::InstallSnapshotResponse(msg)
    }
}

#[cfg(feature = "use_capnp")]
impl InstallSnapshotResponse {
    pub fn from_capnp<'a>(reader: install_snapshot_response::Reader<'a>) -> Result<Self, Error> {
        todo!("installsnapshotResponse::from_capnp");
        //let message = match reader.which().map_err(Error::CapnpSchema)? {
        //install_snapshot_response::Success(m) => {
        //let m = m.map_err(Error::Capnp)?;
        //InstallSnapshotResponse::Success(m.get_term().into(), m.get_log_index().into())
        //}
        //install_snapshot_response::StaleTerm(m) => InstallSnapshotResponse::StaleTerm(m.into()),
        //};
        //Ok(message)
    }

    pub fn fill_capnp<'a>(&self, builder: &mut install_snapshot_response::Builder<'a>) {
        todo!("installsnapshotResponse::fill_capnp");
        //match self {
        //&InstallSnapshotResponse::Success(term, log_index) => {
        //let mut message = builder.reborrow().init_success();
        //message.set_term(term.into());
        //message.set_log_index(log_index.into());
        //}
        //&InstallSnapshotResponse::StaleTerm(term) => builder.set_stale_term(term.into()),
        //}
    }

    common_capnp!(
        install_snapshot_response::Builder,
        install_snapshot_response::Reader
    );
}

// TODO
#[cfg(test)]
mod test {

    //#[cfg(feature = "use_capnp")]
    //use message::*;
    use super::*;

    #[cfg(feature = "use_capnp")]
    macro_rules! test_message_capnp {
        ($i:ident, $t:ty) => {
            fn $i(message: $t) {
                let builder = message.as_capnp_heap();
                let mut encoded = Vec::new();

                ::capnp::serialize::write_message(&mut encoded, &builder).unwrap();
                let mut encoded = ::std::io::Cursor::new(encoded);
                let decoded = ::capnp::serialize::read_message(
                    &mut encoded,
                    ::capnp::message::DEFAULT_READER_OPTIONS,
                )
                .unwrap();
                let decoded = <$t>::from_capnp_untyped(decoded).unwrap();
                assert_eq!(message, decoded);
            }
        };
    }

    #[cfg(feature = "use_capnp")]
    test_message_capnp!(test_peer_message_capnp, PeerMessage);

    #[test]
    #[cfg(feature = "use_capnp")]
    fn test_append_entries_request_capnp() {
        // TODO
        // let message = AppendEntriesRequest {
        //// The values are pretty random here, maybe not matching raft conditions
        //term: 5.into(),
        //prev_log_index: 3.into(),
        //prev_log_term: 2.into(),
        //leader_commit: 4.into(),
        //entries: vec![
        //Entry {
        //term: 9.into(),
        //data: EntryData::Noop,
        //},
        //Entry {
        //term: 9.into(),
        //data: EntryData::Proposal(
        //"qwer".to_string().into_bytes(),
        //ClientGuarantee::Log,
        //),
        //},
        //Entry {
        //term: 9.into(),
        //data: EntryData::Config(
        //ConsensusConfig {
        //peers: vec![Peer {
        //id: ServerId(42),
        //metadata: b"127.0.0.1:8080"[..].to_vec(),
        //}],
        //},
        //true,
        //AdminId::from_bytes(&(911u64.to_le_bytes())[..]).unwrap(),
        //),
        //},
        //],
        //};

        //test_peer_message_capnp(message.into());
    }

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
