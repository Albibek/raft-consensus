@0x99bbdf91931a3304;

# Peer messages

struct PeerMessage {
   union {
   appendEntriesRequest @0 :AppendEntriesRequest;
   appendEntriesResponse @1 :AppendEntriesResponse;

   requestVoteRequest @2 :RequestVoteRequest;
   requestVoteResponse @3 :RequestVoteResponse;

   addServerRequest @4 :AddServerRequest;
   addServerResponse @5 :AddServerResponse;
   }
}

struct AppendEntriesRequest {
    term @0 :UInt64;
    prevLogIndex @1 :UInt64;
    prevLogTerm @2 :UInt64;
    leaderCommit @3 :UInt64;

    entries @4 :List(Entry);
}

struct AppendEntriesResponse {
    term @0 :UInt64;

  union {
    success @1 :TermAndIndex;
    staleTerm @2 :UInt64;
    inconsistentPrevEntry @3 :TermAndIndex;
    staleEntry @4 :Void;
  }
}

struct RequestVoteRequest {
    term @0 :UInt64;
    lastLogIndex @1 :UInt64;
    lastLogTerm @2 :UInt64;
}

struct RequestVoteResponse {
  union {
    staleTerm @0 :UInt64;
    inconsistentLog @1 :UInt64;
    granted @2 :UInt64;
    alreadyVoted @3 :UInt64;
  }
}

# Client messages
struct ClientRequest {
    union {
        ping @0 :Void;
        proposal @1 :Data;
        query @2 :Data;
    }
}

struct ClientResponse {
    union {
        ping @0 :PingResponse;
        proposal @1 :CommandResponse;
        query @2 :CommandResponse;
    }
}

# Cluster membership change messages
struct AddServerRequest {
    id @0 :UInt64;
    info @1 :Data;
}

struct ServerCommandResponse {
    union {
        success @0 :Void;
        badPeer @1 :Void;
        alreadyPending @2 :Void;
        unknownLeader @3 :Void;
        notLeader @4 :UInt64;
    }
}

# Other messages
struct CommandResponse {
    union {
        success @0 :Data;
        queued @1 :Void;
        unknownLeader @2 :Void;
        notLeader @3 :UInt64;
    }
}

struct PingResponse {
    term @0 :UInt64;
    index @1 :UInt64;
    state @2 :ConsensusState;
}

# Other structures
struct ConsensusState {
    union {
        leader @0 :Void;
        candidate @1 :Void;
        follower @2 :Void;
    }
}

struct Entry {
    term @0 :UInt64;
    data @1 :EntryData;
}

struct EntryData {
    union {
        client @0 :Data;
        addServer @1 :AddServerEntry;
        removeServer @2 :UInt64;
    }
}

struct AddServerEntry {
    id @0 :UInt64;
    info @1 :Data;
}

struct TermAndIndex {
    term @0 :UInt64;
    logIndex @1 :UInt64;
}
