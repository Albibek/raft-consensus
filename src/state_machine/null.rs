use std::marker::PhantomData;

use thiserror::Error as ThisError;

use crate::persistent_log::Log;
use crate::{state_machine::StateMachine, LogIndex};

use super::SnapshotInfo;

/// A state machine with no states.
/// Always replies to queries with existing, but empty answer.
#[derive(Debug)]
pub struct NullStateMachine<L: Log> {
    index: LogIndex,
    _pd: PhantomData<L>,
}

#[derive(ThisError, Debug)]
#[error("null machine error")]
pub enum Error {
    NullMachineError,
}

impl<L: Log> NullStateMachine<L> {
    pub fn new() -> Self {
        Self {
            index: LogIndex(0),
            _pd: PhantomData,
        }
    }
}

impl<L: Log> StateMachine for NullStateMachine<L> {
    type Log = L;
    type Error = Error;

    fn apply(&mut self, index: LogIndex, _: bool) -> Result<Option<Vec<u8>>, Self::Error> {
        self.index = index;
        Ok(Some(Vec::new()))
    }

    fn query(&self, _query: &[u8]) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }

    fn snapshot_info(&self) -> Result<Option<SnapshotInfo>, Self::Error> {
        if self.index != LogIndex(0) {
            Ok(Some(SnapshotInfo {
                index: self.index,
                size: 0,
            }))
        } else {
            Ok(None)
        }
    }

    fn take_snapshot(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        self.index = index;
        Ok(())
    }

    fn read_snapshot_chunk(&self, _chunk: Option<&[u8]>) -> Result<Vec<u8>, Error> {
        Ok(Vec::new())
    }

    fn write_snapshot_chunk(
        &mut self,
        index: LogIndex,
        _chunk_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.index = index;
        Ok(None)
    }

    fn last_applied(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.index)
    }

    fn log(&self) -> &Self::Log {
        todo!()
    }

    fn log_mut(&mut self) -> &mut Self::Log {
        todo!()
    }
}
