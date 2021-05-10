use raft_consensus::state_machine::StateMachine;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

/// A state machine which hashes an incoming request with it's current state on apply.
/// On query it hashes it's own state then adds the query above withtout modifying state itself
#[derive(Debug, Clone)]
pub struct HashMachine(DefaultHasher);

impl HashMachine {
    pub fn new() -> Self {
        Self(DefaultHasher::new())
    }
}

impl StateMachine for HashMachine {
    fn apply(&mut self, command: &[u8], results_required: bool) -> Vec<u8> {
        for byte in command {
            self.0.write_u8(*byte);
        }
        if results_required {
            (&self.0.finish().to_le_bytes()[..]).to_vec()
        } else {
            Vec::new()
        }
    }

    fn query(&self, query: &[u8]) -> Vec<u8> {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(self.0.finish());
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
