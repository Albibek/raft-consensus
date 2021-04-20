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
use crate::{Entry, LogIndex, ServerId, Term};

#[cfg(feature = "use_capnp")]
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

//================= Consensus state types without internals
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq)]
/// Any message that cluster peers can exchange
pub enum ConsensusStateKind {
    Follower,
    Candidate,
    Leader,
    //    CatchingUp,
}

#[cfg(feature = "use_capnp")]
impl ConsensusStateKind {
    pub fn from_capnp<'a>(reader: consensus_state::Reader<'a>) -> Result<Self, Error> {
        match reader.which().map_err(Error::CapnpSchema)? {
            consensus_state::Which::Follower(()) => Ok(ConsensusStateKind::Follower),
            consensus_state::Which::Candidate(()) => Ok(ConsensusStateKind::Candidate),
            consensus_state::Which::Leader(()) => Ok(ConsensusStateKind::Leader),
            //consensus_state::Which::CatchingUp(()) => Ok(ConsensusStateKind::CatchingUp),
        }
    }

    pub fn fill_capnp<'a>(&self, builder: &mut consensus_state::Builder<'a>) {
        match self {
            &ConsensusStateKind::Follower => builder.reborrow().set_follower(()),
            &ConsensusStateKind::Candidate => builder.reborrow().set_candidate(()),
            &ConsensusStateKind::Leader => builder.reborrow().set_leader(()),
            //&ConsensusStateKind::CatchingUp => builder.reborrow().set_catchingup(()),
        };
    }

    common_capnp!(consensus_state::Builder, consensus_state::Reader);
}

//================= Peer messages
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
        };
    }

    common_capnp!(peer_message::Builder, peer_message::Reader);
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
    pub fn from_capnp<'a>(reader: append_entries_response::Reader<'a>) -> Result<Self, Error> {
        let message = match reader.which().map_err(Error::CapnpSchema)? {
            append_entries_response::Success(m) => {
                let m = m.map_err(Error::Capnp)?;
                AppendEntriesResponse::Success(m.get_term().into(), m.get_log_index().into())
            }
            append_entries_response::StaleTerm(m) => AppendEntriesResponse::StaleTerm(m.into()),
            append_entries_response::InconsistentPrevEntry(m) => {
                let m = m.map_err(Error::Capnp)?;
                AppendEntriesResponse::InconsistentPrevEntry(
                    m.get_term().into(),
                    m.get_log_index().into(),
                )
            }
            append_entries_response::StaleEntry(()) => AppendEntriesResponse::StaleEntry,
        };
        Ok(message)
    }

    pub fn fill_capnp<'a>(&self, builder: &mut append_entries_response::Builder<'a>) {
        match self {
            &AppendEntriesResponse::Success(term, log_index) => {
                let mut message = builder.reborrow().init_success();
                message.set_term(term.into());
                message.set_log_index(log_index.into());
            }
            &AppendEntriesResponse::StaleTerm(term) => builder.set_stale_term(term.into()),
            &AppendEntriesResponse::InconsistentPrevEntry(term, log_index) => {
                let mut message = builder.reborrow().init_inconsistent_prev_entry();
                message.set_term(term.into());
                message.set_log_index(log_index.into());
            }
            &AppendEntriesResponse::StaleEntry => builder.set_stale_entry(()),
        }
    }

    common_capnp!(
        append_entries_response::Builder,
        append_entries_response::Reader
    );
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
    pub fn from_capnp<'a>(reader: request_vote_request::Reader<'a>) -> Result<Self, Error> {
        Ok(Self {
            term: reader.get_term().into(),
            last_log_index: reader.get_last_log_index().into(),
            last_log_term: reader.get_last_log_term().into(),
            is_voluntary_step_down: reader.get_is_voluntaery_step_down(),
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

//================= Cluster membership change messages
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Request for adding new server to cluster
pub struct AddServerRequest {
    /// The id of new server being added
    pub id: ServerId,
    pub info: Vec<u8>,
}

#[cfg(feature = "use_capnp")]
impl AddServerRequest {
    pub fn from_capnp<'a>(reader: add_server_request::Reader<'a>) -> Result<Self, Error> {
        Ok(Self {
            id: reader.get_id().into(),
            info: reader.get_info()?.to_vec(),
        })
    }

    pub fn fill_capnp<'a>(&self, builder: &mut add_server_request::Builder<'a>) {
        builder.set_id(self.id.into());
        builder.set_info(&self.info);
    }

    common_capnp!(add_server_request::Builder, add_server_request::Reader);
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Response when adding new server to cluster
pub enum ServerCommandResponse {
    Success,
    BadPeer,
    LeaderJustChanged,
    AlreadyPending,
    UnknownLeader,
    NotLeader(ServerId),
}

#[cfg(feature = "use_capnp")]
impl ServerCommandResponse {
    pub fn from_capnp<'a>(reader: add_server_response::Reader<'a>) -> Result<Self, Error> {
        let message = match reader.which().map_err(Error::CapnpSchema)? {
            server_command_response::Which::Success(()) => ServerCommandResponse::Success,
            server_command_response::Which::BadPeer(()) => ServerCommandResponse::BadPeer,
            server_command_response::Which::LeaderJustChanged(()) => {
                ServerCommandResponse::LeaderJustChanged
            }
            server_command_response::Which::AlreadyPending(()) => {
                ServerCommandResponse::AlreadyPending
            }
            server_command_response::Which::UnknownLeader(()) => {
                ServerCommandResponse::UnknownLeader
            }
            server_command_response::Which::NotLeader(id) => {
                ServerCommandResponse::NotLeader(id.into())
            }
        };
        Ok(message)
    }

    pub fn fill_capnp<'a>(&self, builder: &mut server_command_response::Builder<'a>) {
        match self {
            &ServerCommandResponse::Success => builder.set_success(()),
            &ServerCommandResponse::BadPeer => builder.set_bad_peer(()),
            &ServerCommandResponse::LeaderJustChanged => builder.set_leader_just_changed(()),
            &ServerCommandResponse::AlreadyPending => builder.set_already_pending(()),
            &ServerCommandResponse::UnknownLeader => builder.set_unknown_leader(()),
            &ServerCommandResponse::NotLeader(id) => builder.set_not_leader(id.into()),
        }
    }

    common_capnp!(add_server_response::Builder, add_server_response::Reader);
}

//================= Client messages
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Request from client.
pub enum ClientRequest {
    Ping,
    Proposal(Vec<u8>),
    Query(Vec<u8>),
}

#[cfg(feature = "use_capnp")]
impl ClientRequest {
    pub fn from_capnp<'a>(reader: client_request::Reader<'a>) -> Result<Self, Error> {
        let message = match reader.which().map_err(Error::CapnpSchema)? {
            client_request::Which::Ping(()) => ClientRequest::Ping,
            client_request::Which::Proposal(data) => {
                let data = data.map_err(Error::Capnp)?;
                ClientRequest::Proposal(data.into())
            }
            client_request::Which::Query(data) => {
                let data = data.map_err(Error::Capnp)?;
                ClientRequest::Query(data.into())
            }
        };
        Ok(message)
    }

    pub fn fill_capnp<'a>(&self, builder: &mut client_request::Builder<'a>) {
        match self {
            &ClientRequest::Ping => builder.set_ping(()),
            &ClientRequest::Proposal(ref data) => builder.set_proposal(&data),
            &ClientRequest::Query(ref data) => builder.set_query(&data),
        }
    }

    common_capnp!(client_request::Builder, client_request::Reader);
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Response to clienti request.
pub enum ClientResponse {
    Ping(PingResponse),
    Proposal(CommandResponse),
    Query(CommandResponse),
}

#[cfg(feature = "use_capnp")]
impl ClientResponse {
    pub fn from_capnp<'a>(reader: client_response::Reader<'a>) -> Result<Self, Error> {
        let message = match reader.which().map_err(Error::CapnpSchema)? {
            client_response::Which::Ping(reader) => {
                let reader = reader.map_err(Error::Capnp)?;
                let data = PingResponse::from_capnp(reader)?;
                ClientResponse::Ping(data)
            }
            client_response::Which::Proposal(reader) => {
                let reader = reader.map_err(Error::Capnp)?;
                let data = CommandResponse::from_capnp(reader)?;
                ClientResponse::Proposal(data)
            }
            client_response::Which::Query(reader) => {
                let reader = reader.map_err(Error::Capnp)?;
                let data = CommandResponse::from_capnp(reader)?;
                ClientResponse::Query(data)
            }
        };
        Ok(message)
    }

    pub fn fill_capnp<'a>(&self, builder: &mut client_response::Builder<'a>) {
        match self {
            &ClientResponse::Ping(ref data) => {
                let mut builder = builder.reborrow().init_ping();
                data.fill_capnp(&mut builder);
            }
            &ClientResponse::Proposal(ref data) => {
                let mut builder = builder.reborrow().init_proposal();
                data.fill_capnp(&mut builder);
            }
            &ClientResponse::Query(ref data) => {
                let mut builder = builder.reborrow().init_query();
                data.fill_capnp(&mut builder);
            }
        }
    }

    common_capnp!(client_response::Builder, client_response::Reader);
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Part of client message.
pub struct PingResponse {
    /// The server's current term
    pub(crate) term: Term,

    /// The server's current index
    pub(crate) index: LogIndex,

    /// The server's current state
    pub(crate) state: ConsensusStateKind,
}

#[cfg(feature = "use_capnp")]
impl PingResponse {
    pub fn from_capnp<'a>(reader: ping_response::Reader<'a>) -> Result<Self, Error> {
        Ok(Self {
            term: reader.get_term().into(),
            index: reader.get_index().into(),
            state: {
                let reader = reader.get_state().map_err(Error::Capnp)?;
                ConsensusStateKind::from_capnp(reader)?
            },
        })
    }

    pub fn fill_capnp<'a>(&self, builder: &mut ping_response::Builder<'a>) {
        builder.set_term(self.term.into());
        builder.set_index(self.index.into());
        {
            let mut builder = builder.reborrow().init_state();
            self.state.fill_capnp(&mut builder);
        }
    }

    common_capnp!(ping_response::Builder, ping_response::Reader);
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
/// Response to client command
pub enum CommandResponse {
    Success(Vec<u8>),

    // The proposal has been queued on the leader and waiting the majority
    // of nodes to commit it
    Queued,

    // The proposal failed because the Raft node is not the leader, and does
    // not know who the leader is.
    UnknownLeader,

    // The client request failed because the Raft node is not the leader.
    // The value returned may be the address of the current leader.
    NotLeader(ServerId),
}

#[cfg(feature = "use_capnp")]
impl CommandResponse {
    pub fn from_capnp<'a>(reader: command_response::Reader<'a>) -> Result<Self, Error> {
        let message = match reader.which().map_err(Error::CapnpSchema)? {
            command_response::Which::Success(data) => {
                let data = data.map_err(Error::Capnp)?.to_vec();
                CommandResponse::Success(data)
            }
            command_response::Which::Queued(()) => CommandResponse::Queued,
            command_response::Which::UnknownLeader(()) => CommandResponse::UnknownLeader,
            command_response::Which::NotLeader(id) => CommandResponse::NotLeader(id.into()),
        };
        Ok(message)
    }

    pub fn fill_capnp<'a>(&self, builder: &mut command_response::Builder<'a>) {
        match self {
            &CommandResponse::Success(ref data) => builder.set_success(&data),
            &CommandResponse::Queued => builder.set_queued(()),
            &CommandResponse::UnknownLeader => builder.set_unknown_leader(()),
            &CommandResponse::NotLeader(id) => builder.set_not_leader(id.into()),
        }
    }

    common_capnp!(command_response::Builder, command_response::Reader);
}
//================= other messages

/// Consensus timeout types.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub enum ConsensusTimeout {
    // An election timeout. Randomized value.
    Election,
    // A heartbeat timeout. Stable value.
    Heartbeat(ServerId),
}

#[cfg(test)]
mod test {

    #[cfg(feature = "use_capnp")]
    use message::*;

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
        let message = AppendEntriesRequest {
            // The values are pretty random here, maybe not matching raft conditions
            term: 5.into(),
            prev_log_index: 3.into(),
            prev_log_term: 2.into(),
            leader_commit: 4.into(),
            entries: vec![Entry {
                term: 9.into(),
                data: "qwer".to_string().into_bytes(),
            }],
        };

        test_peer_message_capnp(message.into());
    }

    #[test]
    #[cfg(feature = "use_capnp")]
    fn test_append_entries_response_capnp() {
        let message = AppendEntriesResponse::Success(1.into(), 2.into());
        test_peer_message_capnp(message.into());
        let message = AppendEntriesResponse::StaleTerm(3.into());
        test_peer_message_capnp(message.into());
        let message = AppendEntriesResponse::InconsistentPrevEntry(4.into(), 5.into());
        test_peer_message_capnp(message.into());
        let message = AppendEntriesResponse::StaleEntry;
        test_peer_message_capnp(message.into());
    }

    #[test]
    #[cfg(feature = "use_capnp")]
    fn test_request_vote_request_capnp() {
        let message = RequestVoteRequest {
            term: 1.into(),
            last_log_index: 2.into(),
            last_log_term: 3.into(),
        };
        test_peer_message_capnp(message.into());
    }

    #[test]
    #[cfg(feature = "use_capnp")]
    fn test_request_vote_response_capnp() {
        let message = RequestVoteResponse::StaleTerm(1.into());
        test_peer_message_capnp(message.into());
        let message = RequestVoteResponse::Granted(2.into());
        test_peer_message_capnp(message.into());
        let message = RequestVoteResponse::InconsistentLog(3.into());
        test_peer_message_capnp(message.into());
        let message = RequestVoteResponse::AlreadyVoted(4.into());
        test_peer_message_capnp(message.into());
    }

    #[test]
    #[cfg(feature = "use_capnp")]
    fn test_client_request_capnp() {
        test_message_capnp!(test_message, ClientRequest);

        let message = ClientRequest::Proposal("proposal".to_string().into_bytes());
        test_message(message);
        let message = ClientRequest::Query("query".to_string().into_bytes());
        test_message(message);
        let message = ClientRequest::Ping;
        test_message(message);
    }

    #[test]
    #[cfg(feature = "use_capnp")]
    fn test_client_response_capnp() {
        test_message_capnp!(test_message, ClientResponse);

        let message = ClientResponse::Ping(PingResponse {
            term: 10000.into(),
            index: 2000.into(),
            state: ConsensusState::Leader,
        });
        test_message(message);
    }
}
