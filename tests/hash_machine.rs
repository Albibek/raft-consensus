use raft_consensus::state_machine::StateMachine;
use std::hash::Hasher;
use std::{collections::hash_map::DefaultHasher, fmt::Debug};

/// A state machine which hashes an incoming request with it's current state on apply.
/// On query it hashes it's own state then adds the query above withtout modifying state itself
#[derive(Clone)]
pub struct HashMachine {
    hash: u64,
    hasher: DefaultHasher,
}

impl PartialEq for HashMachine {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for HashMachine {}

impl HashMachine {
    pub fn new() -> Self {
        Self {
            hash: 0,
            hasher: DefaultHasher::new(),
        }
    }
}

impl Debug for HashMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.hash, f)
    }
}

impl StateMachine for HashMachine {
    fn apply(&mut self, command: &[u8], results_required: bool) -> Vec<u8> {
        for byte in command {
            self.hasher.write_u8(*byte);
        }

        self.hash = self.hasher.finish();
        if results_required {
            (&self.hash.to_le_bytes()[..]).to_vec()
        } else {
            Vec::new()
        }
    }

    fn query(&self, query: &[u8]) -> Vec<u8> {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(self.hash);
        for byte in query {
            hasher.write_u8(*byte);
        }
        (&hasher.finish().to_le_bytes()[..]).to_vec()
    }

    fn snapshot(&self) -> Vec<u8> {
        // HASH itself with some special value, like zero
        todo!()
    }

    fn restore_snapshot(&mut self, _snapshot: &[u8]) {
        todo!()
    }
}
