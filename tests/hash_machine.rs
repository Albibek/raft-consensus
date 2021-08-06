use raft_consensus::state_machine::{self, SnapshotInfo, StateMachine};
use raft_consensus::LogIndex;
use std::hash::Hasher;
use std::{collections::hash_map::DefaultHasher, fmt::Debug};

use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
#[error("Hash machine error")]
pub enum Error {
    HashMachineError,
}

/// A state machine which hashes an incoming request with it's current state on apply.
/// On query it hashes it's own state then adds the query above withtout modifying state itself
/// Snapshot is a copy of a hash at the specific index split into 8 1-byte chunks
#[derive(Clone)]
pub struct HashMachine {
    hash: u64,
    current_snapshot: [u8; 8],
    pending_snapshot: [u8; 8],
    hasher: DefaultHasher,
    index: LogIndex,
    chunked: bool,
}

impl PartialEq for HashMachine {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for HashMachine {}

impl HashMachine {
    pub fn new(chunked: bool) -> Self {
        Self {
            hash: 0,
            current_snapshot: [0u8; 8],
            pending_snapshot: [0u8; 8],
            hasher: DefaultHasher::new(),
            index: LogIndex(0),
            chunked,
        }
    }
}

impl Debug for HashMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.hash, f)
    }
}

impl StateMachine for HashMachine {
    type Error = Error;

    fn apply(&mut self, command: &[u8], results_required: bool) -> Result<Vec<u8>, Error> {
        for byte in command {
            self.hasher.write_u8(*byte);
        }

        self.hash = self.hasher.finish();
        if results_required {
            Ok((&self.hash.to_le_bytes()[..]).to_vec())
        } else {
            Ok(Vec::new())
        }
    }

    fn query(&self, query: &[u8]) -> Result<Vec<u8>, Error> {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(self.hash);
        for byte in query {
            hasher.write_u8(*byte);
        }
        (&hasher.finish().to_le_bytes()[..]).to_vec()
    }

    fn snapshot_info(&self) -> Result<Option<SnapshotInfo>, Self::Error> {
        if self.hash == 0 {
            Ok(None)
        } else {
            Ok(Some(SnapshotInfo {
                index: self.index,
                size: 64,
            }))
        }
    }

    fn take_snapshot(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        self.index = index;
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(self.hash);
        self.snapshot = hasher.finish();
        Ok(())
    }

    fn read_snapshot_chunk(&self, query: Option<&[u8]>) -> Result<Option<Vec<u8>>, Self::Error> {
        if self.index == LogIndex(0) {
            Ok(None)
        } else if query == None {
            Ok(Some((self.hash.to_le_bytes()[..]).to_vec()))
        } else {
            Ok(None)
        }
    }

    fn write_snapshot_chunk(
        &mut self,
        index: raft_consensus::LogIndex,
        chunk_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        todo!()
    }
}
