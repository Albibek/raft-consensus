use thiserror::Error as ThisError;

use crate::{state_machine::StateMachine, LogIndex};

use super::SnapshotInfo;

/// A state machine with no states.
#[derive(Debug)]
pub struct NullStateMachine {
    snapshot_index: LogIndex,
}

#[error("null machine error")]
#[derive(ThisError, Debug)]
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

    fn snapshot_info(&self, _meta_reuqired: bool) -> Result<Option<SnapshotInfo>, Self::Error> {
        if self.snapshot_index != LogIndex(0) {
            Ok(Some(SnapshotInfo {
                index: self.snapshot_index,
                size: 0,
                chunks: 1,
                finished: true,
                metadata: None,
            }))
        } else {
            Ok(None)
        }
    }

    fn take_snapshot(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        self.snapshot_index = index;
        Ok(())
    }

    fn read_snapshot_chunk(&self, chunk: usize) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }

    fn init_new_snapshot(&mut self, info: SnapshotInfo) -> Result<(), Self::Error> {
        self.snapshot_index = info.index;
        Ok(())
    }

    fn write_snapshot_chunk(
        &mut self,
        chunk: usize,
        chunk_bytes: &[u8],
    ) -> Result<(), Self::Error> {
        if chunk != 1 || chunk_bytes.len() > 0 {
            Err(Error::NullMachineError)
        } else {
            Ok(())
        }
    }
}
