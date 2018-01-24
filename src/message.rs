/// Module contains message types used in consensus
/// Any network message have to be converted to theese enums to be processed
use {LogIndex, ServerId, Term};
use state::ConsensusState;

#[derive(Debug, Clone)]
pub struct Entry {
    pub term: Term,
    pub data: Vec<u8>,
}

impl Entry {
    pub fn new(term: Term, data: Vec<u8>) -> Self {
        Self { term, data }
    }
}

//================= Peer messages

#[derive(Debug, Clone)]
/// Any message that cluster peers can exchange
pub enum PeerMessage {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
}

#[derive(Clone, Debug)]
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
    // TODO: custom Clone or Cow to avoid cloning vector
}

#[derive(Debug, Clone)]
/// Response for Raft AppendEntriesRPC
pub enum AppendEntriesResponse {
    Success(Term, LogIndex),
    StaleTerm(Term),
    InconsistentPrevEntry(Term, LogIndex),
    StaleEntry,
}

#[derive(Clone, Debug)]
/// Request for Raft voting RPC
pub struct RequestVoteRequest {
    /// The candidate's term.
    pub term: Term,

    /// The index of the candidate's last log entry.
    pub last_log_index: LogIndex,

    /// The term of the candidate's last log entry.
    pub last_log_term: Term,
}

#[derive(Debug, Clone)]
/// Response for Raft voting RPC
pub enum RequestVoteResponse {
    StaleTerm(Term),
    InconsistentLog(Term),
    Granted(Term),
    AlreadyVoted(Term),
}

impl RequestVoteResponse {
    pub fn voter_term(&self) -> Term {
        match self {
            &RequestVoteResponse::StaleTerm(t)
            | &RequestVoteResponse::InconsistentLog(t)
            | &RequestVoteResponse::Granted(t)
            | &RequestVoteResponse::AlreadyVoted(t) => t,
        }
    }
}

//================= Client messages
#[derive(Debug, Clone)]
pub enum ClientRequest {
    Ping,
    Proposal(Vec<u8>),
    Query(Vec<u8>),
}

#[derive(Debug, Clone)]
pub enum ClientResponse {
    Ping(PingResponse),
    Proposal(CommandResponse),
    Query(CommandResponse),
}

#[derive(Debug, Clone)]
pub struct PingResponse {
    /// The server's current term
    pub(crate) term: Term,

    /// The server's current index
    pub(crate) index: LogIndex,

    /// The server's current state
    pub(crate) state: ConsensusState,
}

#[derive(Debug, Clone)]
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

//================= other messages

/// Consensus timeout types.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ConsensusTimeout {
    // An election timeout. Randomized value.
    Election,
    // A heartbeat timeout. Stable value.
    Heartbeat(ServerId),
}
