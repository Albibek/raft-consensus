use std::io::{Read, Write};
use std::result;

use persistent_log::{Error, Log};
use {LogIndex, ServerId, Term};

/// This is a `Log` implementation that stores entries in a simple in-memory vector. Other data
/// is stored in a struct. It is chiefly intended for testing.
///
/// # Panic
///
/// No bounds checking is performed and attempted access to non-existing log
/// indexes will panic.
#[derive(Clone, Debug)]
pub struct MemLog {
    current_term: Term,
    voted_for: Option<ServerId>,
    entries: Vec<(Term, Vec<u8>)>,
}

impl MemLog {
    pub fn new() -> MemLog {
        MemLog {
            current_term: Term(0),
            voted_for: None,
            entries: Vec::new(),
        }
    }
}

impl Log for MemLog {
    type Error = Error;

    fn current_term(&self) -> result::Result<Term, Error> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, term: Term) -> result::Result<(), Error> {
        self.voted_for = None;
        self.current_term = term;
        Ok(())
    }

    fn inc_current_term(&mut self) -> result::Result<Term, Error> {
        self.voted_for = None;
        self.current_term = self.current_term + 1;
        self.current_term()
    }

    fn voted_for(&self) -> result::Result<Option<ServerId>, Error> {
        Ok(self.voted_for)
    }

    fn set_voted_for(&mut self, address: ServerId) -> result::Result<(), Error> {
        self.voted_for = Some(address);
        Ok(())
    }

    fn latest_log_index(&self) -> result::Result<LogIndex, Error> {
        Ok(LogIndex(self.entries.len() as u64))
    }

    fn latest_log_term(&self) -> result::Result<Term, Error> {
        let len = self.entries.len();
        if len == 0 {
            Ok(Term::from(0))
        } else {
            Ok(self.entries[len - 1].0)
        }
    }

    fn entry<W: Write>(&self, index: LogIndex, buf: Option<W>) -> Result<Term, Error> {
        match self.entries.get((index - 1).as_u64() as usize) {
            Some(&(term, ref bytes)) => {
                if let Some(mut buf) = buf {
                    buf.write_all(&bytes)?;
                };
                Ok(term)
            }
            None => Err(Error::BadIndex),
        }
    }

    fn append_entries<R: Read, I: Iterator<Item = (Term, R)>>(
        &mut self,
        from: LogIndex,
        entries: I,
    ) -> result::Result<(), Self::Error> {
        if self.latest_log_index()? + 1 < from {
            return Err(Error::BadLogIndex);
        }

        // TODO remove vector hack
        let mut entries_vec = Vec::new();
        for (term, mut reader) in entries {
            let mut v = Vec::new();
            reader.read_to_end(&mut v)?;
            entries_vec.push((term, v));
        }
        self.entries.truncate((from - 1).as_u64() as usize);
        self.entries.extend(entries_vec.into_iter());
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::*;

    use persistent_log::{append_entries, get_entry, Log};
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
        ).unwrap();
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
        ).unwrap();
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
