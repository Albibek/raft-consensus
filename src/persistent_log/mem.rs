use crate::entry::{ConsensusConfig, Entry, EntryData};
use crate::persistent_log::{Error, Log};
use crate::{LogIndex, ServerId, Term};

/// This is a `Log` implementation that stores entries in a simple in-memory vector. Other data
/// is stored in a struct. It is chiefly intended for testing.
#[derive(Clone, Debug)]
pub struct MemLog {
    current_term: Term,
    voted_for: Option<ServerId>,
    entries: Vec<Entry>,
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
    type Error = Error;

    fn current_term(&self) -> Result<Term, Error> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, term: Term) -> Result<(), Error> {
        self.voted_for = None;
        self.current_term = term;
        Ok(())
    }

    fn inc_current_term(&mut self) -> Result<Term, Error> {
        self.voted_for = None;
        self.current_term = self.current_term + 1;
        self.current_term()
    }

    fn voted_for(&self) -> Result<Option<ServerId>, Error> {
        Ok(self.voted_for)
    }

    fn set_voted_for(&mut self, address: ServerId) -> Result<(), Error> {
        self.voted_for = Some(address);
        Ok(())
    }

    fn latest_log_index(&self) -> Result<LogIndex, Error> {
        Ok(LogIndex(self.entries.len() as u64))
    }

    fn latest_log_term(&self) -> Result<Term, Error> {
        let len = self.entries.len();
        if len == 0 {
            Ok(Term::from(0))
        } else {
            Ok(self.entries[len - 1].term)
        }
    }

    //    fn set_latest_config_index(&mut self, index: LogIndex) -> Result<(), Self::Error> {
    //self.latest_config_index = Some(index.0);
    //Ok(())
    //}

    fn discard_since(&self, index: LogIndex) -> Result<(), Self::Error> {
        todo!()
    }

    fn latest_config_index(&self) -> Result<LogIndex, Self::Error> {
        self.latest_config_index
            .map(|i| i.into())
            .ok_or(Error::NoConfig)
    }

    fn read_latest_config(&self, config: &mut ConsensusConfig) -> Result<LogIndex, Self::Error> {
        let index = self.latest_config_index()?;
        match self.entries.get(index.as_usize()) {
            Some(Entry {
                data: EntryData::Config(latest_config, _),
                ..
            }) => {
                *config = latest_config.clone();
                Ok(index)
            }
            _ => Err(Error::BadIndex),
        }
    }

    fn term(&self, index: LogIndex) -> Result<Option<Term>, Error> {
        todo!();
        self.entries
            .get((index - 1).as_u64() as usize)
            .map(|entry| Some(entry.term))
            .ok_or(Error::BadIndex)
    }

    fn entry(&self, index: LogIndex, dest: &mut Entry) -> Result<(), Error> {
        self.entries
            .get((index - 1).as_u64() as usize)
            .map(|entry| *dest = entry.clone())
            .ok_or(Error::BadIndex)
    }

    fn append_entries<'a, I: Iterator<Item = &'a Entry>>(
        &mut self,
        from: LogIndex,
        entries: I,
    ) -> Result<(), Self::Error> {
        if self.latest_log_index()? + 1 < from {
            return Err(Error::BadLogIndex);
        }

        let start = (from - 1).as_u64() as usize;
        if self.entries.len() < start + 1 {
            return Err(Error::BadLogIndex);
        } else {
            self.entries.truncate(start);
        }

        self.entries.extend(entries.cloned());
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::*;

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
}

impl Default for MemLog {
    fn default() -> Self {
        MemLog::new()
    }
}
