@0x99bbdf91931a3304;

struct Peer {
    id @0 :UInt64;
    metadata @1: Data;
}

struct Entry {
    term @0 :UInt64;
    union {
        noop @1 :Void;
        proposal @2 :Data;
        config @3 :ConfigChangeEntry;
    }

    struct ConfigChangeEntry {
        peers @0 :List(Peer);
        isActual @1 :Bool;
    }
}

struct Timeout {
    union {
        election @0 :Void;
        heartbeat @1 :UInt64;
    }
}

struct PeerMessage {
    union {
        appendEntriesRequest @0 :AppendEntriesRequest;
        appendEntriesResponse @1 :AppendEntriesResponse;

        requestVoteRequest @2 :RequestVoteRequest;
        requestVoteResponse @3 :RequestVoteResponse;
        timeoutNow @4 :Void;

        installSnapshotRequest @5 :InstallSnapshotRequest;
        installSnapshotResponse @6 :InstallSnapshotResponse;
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

  struct TermAndIndex {
        term @0 :UInt64;
        logIndex @1 :UInt64;
    }

    struct RequestVoteRequest {
        term @0 :UInt64;
        lastLogIndex @1 :UInt64;
        lastLogTerm @2 :UInt64;
        isVoluntaryStepDown @3 :Bool;
    }

    struct RequestVoteResponse {
      union {
        staleTerm @0 :UInt64;
        inconsistentLog @1 :UInt64;
        granted @2 :UInt64;
        alreadyVoted @3 :UInt64;
      }
    }


    struct InstallSnapshotRequest {
        term @0 :UInt64;
        lastIndex @1 :UInt64;
        lastTerm @2 :UInt64;
        leaderCommit @3 :UInt64;

        lastConfig @4 :List(Peer);

        snapshotIndex @5 :UInt64;
        chunkData @6 :Data;
    }

    struct InstallSnapshotResponse {
      union {
        success @0 :SnapshotChunkSuccess;
        staleTerm @1 :UInt64;
      }
    }

    struct SnapshotChunkSuccess {
        term @0 :UInt64;
        logIndex @1 :UInt64;
        nextChunkRequest @2 :Data;
    }
}

# Client messages
struct ClientMessage {
    union {
        clientProposalRequest @0 :Data;
        clientProposalResponse @1 :ClientResponse;

        clientQueryRequest @2 :Data;
        clientQueryResponse @3 :ClientResponse;
    }

    struct ClientResponse {
        union {
            success @0 :Data;
            queued @1 :Void;
            unknownLeader @2 :Void;
            notLeader @3 :UInt64;
        }
    }
}

# Administration messages

struct AdminMessage {
    union {
        addServerRequest @0 :AddServerRequest;
        addServerResponse @1 :ConfigurationChangeResponse;

        removeServerRequest @2 :RemoveServerRequest;
        removeServerResponse @3 :ConfigurationChangeResponse;

        stepDownRequest @4 :UInt64;
        stepDownResponse @5 :ConfigurationChangeResponse;

        pingRequest @6 :Void;
        pingResponse @7 :PingResponse;
    }

    struct AddServerRequest {
        id @0 :UInt64;
        metadata @1 :Data;
    }

    struct RemoveServerRequest {
        id @0 :UInt64;
    }

    struct ConfigurationChangeResponse {
        union {
            success @0 :Void;
            badPeer @1 :Void;
            alreadyPending @2 :Void;
            leaderJustChanged @3 :Void;
            unknownLeader @4 :Void;
            notLeader @5 :UInt64;
        }
    }


    struct PingResponse {
        term @0 :UInt64;
        index @1 :UInt64;
        state @2 :ConsensusState;
        leader @3 :Peer;
    }

    struct ConsensusState {
        union {
            leader @0 :Void;
            candidate @1 :Void;
            follower @2 :Void;
        }
    }

}
