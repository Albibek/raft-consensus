use crate::config::ConsensusConfig;
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
    latest_config: Option<(ConsensusConfig, LogIndex)>,
    first_index: u64,
}

impl MemLog {
    pub fn new() -> MemLog {
        MemLog {
            current_term: Term(0),
            voted_for: None,
            entries: Vec::new(),
            latest_config: None,
            first_index: 0,
        }
    }

    fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        let index = index.as_u64();
        let entry_index = index - self.first_index;
        self.entries.get(entry_index as usize)
    }
}

impl Log for MemLog {
    type Error = MemLogError;

    fn latest_index(&self) -> Result<LogIndex, Self::Error> {
        if self.first_index == 0 {
            Ok(LogIndex(0))
        } else if self.entries.len() == 0 {
            // edge case: entry does not exist, but index must be there
            Ok(self.first_index.into())
        } else {
            Ok(LogIndex((self.first_index - 1) + self.entries.len() as u64))
        }
    }

    #[inline]
    fn latest_volatile_index(&self) -> Result<LogIndex, Self::Error> {
        self.latest_index()
    }

    fn first_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(LogIndex(self.first_index))
    }

    fn term_of(&self, index: LogIndex) -> Result<Term, MemLogError> {
        let index: u64 = index.as_u64();
        if index == 0 {
            return Err(MemLogError::ConsensusGuarantee(
                "consensus should not request a term for LogIndex(0)".into(),
            ));
        }
        if self.entries.len() == 0 {
            return Err(MemLogError::ConsensusGuarantee(
                "consensus should not request a term when log is empty".into(),
            ));
        }
        if index < self.first_index || index > self.latest_index().unwrap().as_u64() {
            return Err(MemLogError::ConsensusGuarantee(
                "consensus should not request a term for non-existent entry".into(),
            ));
        }
        self.entry_at(LogIndex(index))
            .map(|entry| entry.term)
            .ok_or(MemLogError::ConsensusGuarantee(
                "consensus should not request a term for non-existent entry".into(),
            ))
    }

    fn discard_since(&mut self, index: LogIndex) -> Result<LogIndex, Self::Error> {
        if index > self.latest_index()? {
            return self.latest_index();
        } else if index.as_u64() < self.first_index {
            return Err(MemLogError::ConsensusGuarantee(format!(
                "discarding non existent index {}, only {} exists",
                index, self.first_index
            )));
        } else if index.as_u64() == self.first_index {
            self.first_index = index.as_u64();
            self.entries.drain(..).last();
        }

        let trunc = index.as_u64() - self.first_index;
        self.entries.truncate(trunc as usize);
        Ok(index - 1)
    }

    fn discard_until(&mut self, index: LogIndex) -> Result<(), MemLogError> {
        let index = index.as_u64();
        if index <= self.first_index {
            // do nothing
        } else if index <= self.latest_index()?.as_u64() {
            let until = (index - self.first_index) as usize;
            self.entries.drain(..until);
            self.first_index += until as u64;
        } else {
            // index is beyond last_index: discard the whole log
            self.entries.clear();
            self.first_index = index + 1;
        }

        Ok(())
    }

    fn read_entry(&self, index: LogIndex, dest: &mut LogEntry) -> Result<(), Self::Error> {
        let index: u64 = index.as_u64();

        if index < self.first_index {
            return Err(MemLogError::ConsensusGuarantee(format!(
                "reading entry  at {} which is before first index {}",
                index, self.first_index
            )));
        }
        self.entry_at(LogIndex(index))
            .map(|entry| *dest = entry.clone())
            .ok_or(MemLogError::BadIndex(index.into()))
    }

    fn entry_meta_at(&self, index: LogIndex) -> Result<LogEntryMeta, Self::Error> {
        self.entries
            .get((index - 1).as_u64() as usize)
            .map(|entry| entry.meta())
            .ok_or(MemLogError::BadIndex(index))
    }

    fn append_entries(&mut self, start: LogIndex, entries: &[LogEntry]) -> Result<(), Self::Error> {
        // this check can be skipped in implementations and only checks consensus logic
        if start == LogIndex(0) {
            return Err(MemLogError::ConsensusGuarantee(
                "appending to LogIndex(0)".into(),
            ));
        }
        if self.first_index > start.as_u64() {
            return Err(MemLogError::ConsensusGuarantee(format!(
                "appending at older index: {} > {}",
                self.first_index, start
            )));
            //return Err(MemLogError::BadIndex(start));
        }

        let latest_index = self.latest_index()?.as_u64();
        //let index = (start.as_u64() - self.first_index) as usize;

        if start.as_u64() > latest_index + 1 {
            // while we can easily overcome rewriting, consensus
            // should not do it, so we intentionally return error here co catch
            // this
            return Err(MemLogError::ConsensusGuarantee(format!(
                "appending rewrites entries without truncation: requested {} while index must be {}",
                start,
                latest_index
            )));
        }

        self.entries.extend_from_slice(entries);
        if self.first_index == 0 {
            self.first_index = 1
        }

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

    fn set_latest_config(
        &mut self,
        config: &ConsensusConfig,
        index: LogIndex,
    ) -> Result<(), Self::Error> {
        self.latest_config = Some((config.clone(), index));
        Ok(())
    }

    fn latest_config(&self) -> Result<Option<ConsensusConfig>, Self::Error> {
        Ok(self.latest_config.as_ref().map(|(c, _)| c.clone()))
    }

    fn latest_config_index(&self) -> Result<Option<LogIndex>, Self::Error> {
        Ok(self.latest_config.as_ref().map(|(_, i)| i.clone()))
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
