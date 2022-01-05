use bytes::Bytes;
use raft_consensus::persistent_log::{Log, LogEntry, LogEntryData};
use raft_consensus::state_machine::{SnapshotInfo, StateMachine};
use raft_consensus::{LogIndex, Term};
use std::hash::Hasher;

use std::{collections::hash_map::DefaultHasher, fmt::Debug};

use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
#[error("Hash machine error")]
pub enum Error {
    HashMachineError(&'static str),
    LogError(Box<dyn std::error::Error>),
}

/// A state machine which hashes an incoming request with it's current state on apply.
/// On query it hashes it's own state then adds the query above withtout modifying state itself
/// Snapshot is a copy of a hash at the specific index split into 8 1-byte chunks with chunked flag
/// of just a 8-byte arrays/vectors otherwise
///
/// In chunked mode the simpliest scheme is used for chunk/request:
/// chunk: first byte is the number of chunk being sent, second byte is the chunk value
/// requess: single byte requesting the next chunk
#[derive(Clone)]
pub struct HashMachine<L: Log> {
    pub hash: u64,
    pub current_snapshot: [u8; 8],
    pub pending_snapshot: [u8; 8],
    hasher: DefaultHasher,
    pub index: LogIndex,
    pub term: Term,
    last_applied: LogIndex,
    pub chunked: bool,
    log: L,
}

impl<L: Log> HashMachine<L> {
    pub fn new(log: L, chunked: bool) -> Self {
        Self {
            hash: 0,
            current_snapshot: [0u8; 8],
            pending_snapshot: [0u8; 8],
            hasher: DefaultHasher::new(),
            index: LogIndex(0),
            term: Term(0),
            last_applied: LogIndex(0),
            chunked,
            log,
        }
    }
}

impl<L: Log> Debug for HashMachine<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.hash, f)
    }
}

impl<L: Log> StateMachine for HashMachine<L> {
    type Error = Error;
    type Log = L;

    fn apply(&mut self, index: LogIndex, results_required: bool) -> Result<Option<Bytes>, Error> {
        let mut entry = LogEntry::default();
        self.last_applied = index;
        self.log
            .read_entry(index, &mut entry)
            .map_err(|e| Error::LogError(Box::new(e)))?;
        if let LogEntryData::Proposal(command, _) = entry.data {
            for byte in command {
                self.hasher.write_u8(byte);
            }

            self.hash = self.hasher.finish();
            if results_required {
                Ok(Some(Bytes::copy_from_slice(&self.hash.to_le_bytes()[..])))
            } else {
                Ok(Some(Bytes::new()))
            }
        } else {
            // don't apply other types of entries, only last_applied should change the hash
            Ok(None)
        }
    }

    fn query(&self, query: Bytes) -> Result<Bytes, Error> {
        let mut hasher = self.hasher.clone();
        for byte in &query {
            hasher.write_u8(*byte);
        }
        Ok(Bytes::copy_from_slice(&hasher.finish().to_le_bytes()[..]))
    }

    fn snapshot_info(&self) -> Result<Option<SnapshotInfo>, Self::Error> {
        if self.current_snapshot == [0; 8] {
            Ok(None)
        } else {
            Ok(Some(SnapshotInfo {
                index: self.index,
                term: self.term,
                size: 64,
            }))
        }
    }

    fn take_snapshot(&mut self, index: LogIndex, term: Term) -> Result<(), Self::Error> {
        self.index = index;
        self.term = term;
        self.current_snapshot = self.hash.to_le_bytes();
        Ok(())
    }

    fn read_snapshot_chunk(&self, query: Option<&[u8]>) -> Result<Vec<u8>, Self::Error> {
        if self.index == LogIndex(0) {
            return Err(Error::HashMachineError("snapshot expected"));
        };
        if self.chunked {
            if let Some(query) = query {
                let chunk = query[0] as usize;
                return Ok(vec![chunk as u8, self.current_snapshot[chunk]]);
            } else {
                return Ok(vec![0, self.current_snapshot[0]]);
            }
        } else {
            if query == None {
                return Ok(self.current_snapshot[..].to_vec());
            } else {
                Err(Error::HashMachineError("unexpected chunk request"))
            }
        }
    }

    fn write_snapshot_chunk(
        &mut self,
        index: LogIndex,
        chunk_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        // in this test implementation we consider all hash_machines initiated
        // using the same `chunked` flag, but real impls could add it to return values
        if self.chunked {
            let chunk = chunk_bytes[0] as usize;
            self.pending_snapshot[chunk] = chunk_bytes[1];
            if chunk == 7 {
                // 7th chunk is the last one
                self.current_snapshot = self.pending_snapshot;
                Ok(None)
            } else {
                Ok(Some(vec![chunk as u8 + 1u8]))
            }
        } else {
            for i in 0..7 {
                self.current_snapshot[i] = chunk_bytes[i];
            }
            self.hash = u64::from_le_bytes(self.current_snapshot);
            Ok(None)
        }
    }

    fn log(&self) -> &Self::Log {
        &self.log
    }

    fn log_mut(&mut self) -> &mut Self::Log {
        &mut self.log
    }

    fn last_applied(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.last_applied)
    }
}
