use thiserror::Error as ThisError;

use crate::{state_machine::StateMachine, LogIndex};

use super::SnapshotInfo;

/// A state machine with no states.
#[derive(Debug)]
pub struct NullStateMachine {
    snapshot_index: LogIndex,
}

#[derive(ThisError, Debug)]
#[error("null machine error")]
pub enum Error {
    NullMachineError,
}

impl NullStateMachine {
    pub fn new() -> Self {
        Self {
            snapshot_index: LogIndex(0),
        }
    }
}

impl StateMachine for NullStateMachine {
    type Error = Error;

    fn apply(&mut self, _command: &[u8], _: bool) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }

    fn query(&self, _query: &[u8]) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }

    fn snapshot_info(&self) -> Result<Option<SnapshotInfo>, Self::Error> {
        if self.snapshot_index != LogIndex(0) {
            Ok(Some(SnapshotInfo {
                index: self.snapshot_index,
                size: 0,
            }))
        } else {
            Ok(None)
        }
    }

    fn take_snapshot(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        self.snapshot_index = index;
        Ok(())
    }

    fn read_snapshot_chunk(&self, _chunk: Option<&[u8]>) -> Result<Option<Vec<u8>>, Error> {
        Ok(None)
    }

    fn write_snapshot_chunk(
        &mut self,
        index: LogIndex,
        _chunk_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.snapshot_index = index;
        Ok(None)
    }
}
