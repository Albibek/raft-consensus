use bytes::Bytes;
use thiserror::Error as ThisError;

use crate::persistent_log::Log;
use crate::Term;
use crate::{state_machine::StateMachine, LogIndex};

use super::SnapshotInfo;

/// A state machine with no states.
/// Always replies to queries with existing, but empty answer.
#[derive(Debug)]
pub struct NullStateMachine<L: Log> {
    index: LogIndex,
    term: Term,
    state: Bytes,
    log: L,
}

#[derive(ThisError, Debug)]
#[error("null machine error")]
pub enum Error {
    NullMachineError,
}

impl<L: Log> NullStateMachine<L> {
    pub fn new(log: L) -> Self {
        Self {
            index: LogIndex(0),
            term: Term(0),
            state: Bytes::new(),
            log,
        }
    }
}

impl<L: Log> StateMachine for NullStateMachine<L> {
    type Log = L;
    type Error = Error;

    fn log(&self) -> &Self::Log {
        &self.log
    }

    fn log_mut(&mut self) -> &mut Self::Log {
        &mut self.log
    }

    fn apply(&mut self, index: LogIndex, _: bool) -> Result<Option<Bytes>, Error> {
        self.index = index;
        Ok(Some(self.state.clone()))
    }

    fn query(&mut self, _query: Bytes) -> Result<Bytes, Error> {
        Ok(self.state.clone())
    }

    fn last_applied(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.index)
    }

    fn snapshot_info(&self) -> Result<Option<SnapshotInfo>, Self::Error> {
        if self.index != LogIndex(0) {
            Ok(Some(SnapshotInfo {
                index: self.index,
                term: self.term,
                size: 0,
            }))
        } else {
            Ok(None)
        }
    }

    fn take_snapshot(&mut self, index: LogIndex, term: Term) -> Result<(), Self::Error> {
        self.index = index;
        self.term = term;
        Ok(())
    }

    fn read_snapshot_chunk(&self, _chunk: Option<&[u8]>) -> Result<Vec<u8>, Error> {
        Ok(Vec::new())
    }

    fn write_snapshot_chunk(
        &mut self,
        index: LogIndex,
        term: Term,
        _chunk_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.index = index;
        self.term = term;
        Ok(None)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::persistent_log::MemLog;
    use crate::testing::*;

    #[test]
    fn test_null_machine() {
        let mut tester = MachineTester::new(|| {
            let log = MemLog::new();
            NullStateMachine::new(log)
        });
        tester.test_all();
    }
}
