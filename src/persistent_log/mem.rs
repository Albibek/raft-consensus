use crate::config::ConsensusConfig;
use crate::persistent_log::{
    Error, Log, LogEntry, LogEntryData, LogEntryDataRef, LogEntryRef, LogError,
};
use crate::{LogIndex, ServerId, Term};
use log::trace;

/// This is a `Log` implementation that stores entries in a simple in-memory vector. Other data
/// is stored in a struct. It is chiefly intended for testing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemLog {
    current_term: Term,
    voted_for: Option<ServerId>,
    entries: Vec<LogEntry>,
    latest_config_index: Option<u64>,
}

impl MemLog {
    pub fn new() -> MemLog {
        MemLog {
            current_term: Term(0),
            voted_for: None,
            entries: Vec::new(),
            latest_config_index: None,
        }
    }
}

impl Log for MemLog {
    type Error = LogError;

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

    fn latest_log_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(LogIndex(self.entries.len() as u64))
    }

    fn discard_log_since(&mut self, index: LogIndex) -> Result<LogIndex, Self::Error> {
        if index.as_usize() < self.entries.len() {
            self.entries.truncate(index.as_usize())
        }
        Ok(index)
    }

    fn latest_config_index(&self) -> Result<Option<LogIndex>, Self::Error> {
        Ok(self.latest_config_index.map(|i| i.into()))
    }

    fn term_of(&self, index: LogIndex) -> Result<Option<Term>, Self::Error> {
        if index == LogIndex(0) {
            return Ok(Some(Term(0)));
        }
        self.entries
            .get((index - 1).as_u64() as usize)
            .map(|entry| Some(entry.term))
            .ok_or(LogError::BadIndex(index))
    }

    fn read_entry(&self, index: LogIndex, dest: &mut LogEntry) -> Result<(), Self::Error> {
        self.entries
            .get((index - 1).as_u64() as usize)
            .map(|entry| *dest = entry.clone())
            .ok_or(LogError::BadIndex(index))
    }

    /// Must append an entry to the log
    fn append_entry<'a>(
        &mut self,
        at: LogIndex,
        entry: &LogEntryRef<'a>,
    ) -> Result<(), Self::Error> {
        if self.latest_log_index()? + 1 < at {
            return Err(LogError::BadLogIndex);
        }

        let start = (at - 1).as_u64() as usize;
        if self.entries.len() < start {
            return Err(LogError::BadLogIndex);
        } else {
            self.entries.truncate(start);
        }

        self.entries.push(entry.into());
        Ok(())
    }
}

impl Default for MemLog {
    fn default() -> Self {
        MemLog::new()
    }
}

#[cfg(test)]
mod test {

    use super::*;

    // TODO test not filling logindex(0) and term(0) because log indexes start from 1
    /*
           use crate::persistent_log::{append_entries, get_entry, Log};

           use {LogIndex, ServerId, Term};

           #[test]
           fn test_current_term() {
           let mut store = MemLog::new();
           assert_eq!(Term(0), store.current_term().unwrap());
           store.set_voted_for(ServerId::from(0)).unwrap();
           store.set_current_term(Term(42)).unwrap();
           assert_eq!(None, store.voted_for().unwrap());
           assert_eq!(Term(42), store.current_term().unwrap());
           store.inc_current_term().unwrap();
           assert_eq!(Term(43), store.current_term().unwrap());
           }

           #[test]
           fn test_voted_for() {
           let mut store = MemLog::new();
           assert_eq!(None, store.voted_for().unwrap());
           let id = ServerId::from(0);
           store.set_voted_for(id).unwrap();
           assert_eq!(Some(id), store.voted_for().unwrap());
           }

           #[test]
           fn test_append_entries() {
           let mut store = MemLog::new();
           assert_eq!(LogIndex::from(0), store.latest_log_index().unwrap());
           assert_eq!(Term::from(0), store.latest_log_term().unwrap());

        // [0.1, 0.2, 0.3, 1.4]
        append_entries(
        &mut store,
        LogIndex(1),
        &[
        (Term::from(0), &[1]),
        (Term::from(0), &[2]),
        (Term::from(0), &[3]),
        (Term::from(1), &[4]),
        ],
        )
        .unwrap();
        assert_eq!(LogIndex::from(4), store.latest_log_index().unwrap());
        assert_eq!(Term::from(1), store.latest_log_term().unwrap());

        assert_eq!(
        (Term::from(0), vec![1u8]),
        get_entry(&store, LogIndex::from(1))
        );
        assert_eq!(
        (Term::from(0), vec![2u8]),
        get_entry(&store, LogIndex::from(2))
        );
        assert_eq!(
        (Term::from(0), vec![3u8]),
        get_entry(&store, LogIndex::from(3))
        );
        assert_eq!(
        (Term::from(1), vec![4u8]),
        get_entry(&store, LogIndex::from(4))
        );

        // [0.1, 0.2, 0.3]
        append_entries(&mut store, LogIndex::from(4), &[]).unwrap();
        assert_eq!(LogIndex(3), store.latest_log_index().unwrap());
        assert_eq!(Term::from(0), store.latest_log_term().unwrap());
        assert_eq!(
        (Term::from(0), vec![1u8]),
        get_entry(&store, LogIndex::from(1))
        );
        assert_eq!(
            (Term::from(0), vec![2u8]),
            get_entry(&store, LogIndex::from(2))
        );
        assert_eq!(
            (Term::from(0), vec![3u8]),
            get_entry(&store, LogIndex::from(3))
        );

        // [0.1, 0.2, 2.3, 3.4]
        append_entries(
            &mut store,
            LogIndex::from(3),
            &[(Term(2), &[3]), (Term(3), &[4])],
        )
            .unwrap();
        assert_eq!(LogIndex(4), store.latest_log_index().unwrap());
        assert_eq!(Term::from(3), store.latest_log_term().unwrap());
        assert_eq!(
            (Term::from(0), vec![1u8]),
            get_entry(&store, LogIndex::from(1))
        );
        assert_eq!(
            (Term::from(0), vec![2u8]),
            get_entry(&store, LogIndex::from(2))
        );
        assert_eq!(
            (Term::from(2), vec![3u8]),
            get_entry(&store, LogIndex::from(3))
        );
        assert_eq!(
            (Term::from(3), vec![4u8]),
            get_entry(&store, LogIndex::from(4))
        );
    }

    */
}
