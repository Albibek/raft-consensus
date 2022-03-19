use crate::persistent_log::{Log, LogEntry, LogEntryMeta};
use crate::{LogIndex, ServerId, Term};

use thiserror::Error as ThisError;

/// This is a `Log` implementation that stores entries in a simple in-memory vector. Other data
/// is stored in a struct. It is chiefly intended for testing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemLog {
    current_term: Term,
    voted_for: Option<ServerId>,
    entries: Vec<LogEntry>,
    latest_config_view: (LogIndex, LogIndex),
    zero_index: u64,
    zero_term: Term,
}

impl MemLog {
    pub fn new() -> MemLog {
        MemLog {
            current_term: Term(0),
            voted_for: None,
            entries: Vec::new(),
            latest_config_view: (LogIndex(0), LogIndex(0)),
            zero_index: 0,
            zero_term: Term(0),
        }
    }

    fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        let index = index.as_u64();
        let entry_index = index - self.zero_index - 1;
        self.entries.get(entry_index as usize)
    }

    fn latest_index(&self) -> LogIndex {
        if self.entries.len() == 0 {
            // edge case: entry does not exist, but index must be there
            self.zero_index.into()
        } else {
            LogIndex(self.zero_index + self.entries.len() as u64)
        }
    }
}

impl Log for MemLog {
    type Error = MemLogError;

    fn current_view(&self) -> Result<(LogIndex, LogIndex, LogIndex), Self::Error> {
        let latest_index = self.latest_index();
        Ok((LogIndex(self.zero_index), latest_index, latest_index))
    }

    fn term_of(&self, index: LogIndex) -> Result<Term, MemLogError> {
        let index: u64 = index.as_u64();
        if index == self.zero_index {
            return Ok(self.zero_term);
        }

        if index < self.zero_index || index > self.latest_index().as_u64() {
            return Err(MemLogError::ConsensusGuarantee(
                "consensus should not request a term for non-existent entry except zero entry"
                    .into(),
            ));
        }
        self.entry_at(LogIndex(index))
            .map(|entry| entry.term)
            .ok_or(MemLogError::ConsensusGuarantee(
                "consensus should not request a term for non-existent entry except zero entry"
                    .into(),
            ))
    }

    fn discard_until(&mut self, index: LogIndex, zero_term: Term) -> Result<(), MemLogError> {
        let index = index.as_u64();
        if index > self.zero_index && index <= self.latest_index().as_u64() {
            let until = (index - self.zero_index) as usize;
            self.entries.drain(..until);
            self.zero_index += until as u64;
        } else {
            // index is beyond log indices: discard the whole log
            // and save zero index and term
            self.entries.clear();
            self.zero_index = index;
            self.zero_term = zero_term;
        }

        Ok(())
    }

    fn discard_since(&mut self, index: LogIndex) -> Result<(), Self::Error> {
        if index > self.latest_index() {
            return Ok(());
        } else if index.as_u64() <= self.zero_index {
            return Err(MemLogError::ConsensusGuarantee(format!(
                "discarding since non existent index {}, only {} exists",
                index,
                self.zero_index + 1
            )));
        } else if index.as_u64() == self.zero_index + 1 {
            self.zero_index = index.as_u64() - 1;
            self.entries.drain(..).last();
        }

        let trunc = index.as_u64() - self.zero_index - 1;
        self.entries.truncate(trunc as usize);
        Ok(())
    }

    fn read_entry(&self, index: LogIndex, dest: &mut LogEntry) -> Result<bool, Self::Error> {
        let index: u64 = index.as_u64();

        if index <= self.zero_index {
            return Err(MemLogError::ConsensusGuarantee(format!(
                "reading entry at {} which is earlier than first index {}",
                index, self.zero_index
            )));
        }
        self.entry_at(LogIndex(index))
            .map(|entry| *dest = entry.clone())
            .ok_or(MemLogError::BadIndex(index.into()))?;
        Ok(true)
    }

    fn entry_meta_at(&self, index: LogIndex) -> Result<LogEntryMeta, Self::Error> {
        let index: u64 = index.as_u64();
        if index <= self.zero_index {
            return Err(MemLogError::ConsensusGuarantee(format!(
                "reading entry meta at {} which is earlier than first index {}",
                index, self.zero_index
            )));
        }
        self.entries
            .get((index - 1) as usize)
            .map(|entry| entry.meta())
            .ok_or(MemLogError::BadIndex(index.into()))
    }

    fn append_entries(&mut self, start: LogIndex, entries: &[LogEntry]) -> Result<(), Self::Error> {
        if start.as_u64() <= self.zero_index {
            return Err(MemLogError::ConsensusGuarantee(format!(
                "appending at {} is beyond log boundaries ({}, {})",
                start,
                self.zero_index + 1,
                start
            )));
        }

        let latest_index = self.latest_index().as_u64();

        if start.as_u64() > latest_index + 1 {
            // while we can easily overcome rewriting, consensus
            // should not do it, so we intentionally return error here co catch
            // this
            return Err(MemLogError::ConsensusGuarantee(format!(
                "appending rewrites entries without truncation: requested {} while index must be {}",
                start,
                latest_index + 1
            )));
        }

        self.entries.extend_from_slice(entries);
        Ok(())
    }

    fn current_term(&self) -> Result<Term, Self::Error> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, term: Term) -> Result<(), Self::Error> {
        self.voted_for = None;
        self.current_term = term;
        Ok(())
    }

    fn voted_for(&self) -> Result<Option<ServerId>, Self::Error> {
        Ok(self.voted_for)
    }

    fn set_voted_for(&mut self, address: Option<ServerId>) -> Result<(), Self::Error> {
        self.voted_for = address;
        Ok(())
    }

    fn set_latest_config_view(&mut self, prev: LogIndex, new: LogIndex) -> Result<(), Self::Error> {
        self.latest_config_view = (prev, new);
        Ok(())
    }

    fn latest_config_view(&self) -> Result<(LogIndex, LogIndex), MemLogError> {
        Ok(self.latest_config_view)
    }
}

impl Default for MemLog {
    fn default() -> Self {
        MemLog::new()
    }
}

#[derive(ThisError, Debug)]
#[error("memory log error")]
pub enum MemLogError {
    BadIndex(LogIndex),
    #[error("consensus guarantee violation: {}", _0)]
    ConsensusGuarantee(String),
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::testing::*;

    #[test]
    fn test_mem_log() {
        let mut tester = LogTester::new(MemLog::new);
        tester.test_all();
    }
}
