use bytes::Bytes;

use crate::message::Urgency;
use crate::persistent_log::{Log, LogEntry, LogEntryData, LogEntryMeta};
use crate::{ConsensusConfig, LogIndex, Peer, ServerId, Term};

use std::marker::PhantomData;

/// Runs a test suite for the log. Will panic if anything goes wrong.
/// Function F should return the intialized empty log each run, with no 3rd party effects.
///
/// All tests are run sequentially, so each log should clean up after itself in Drop,
/// before returning new log or whatever other way it wants.
pub struct LogTester<L: Log, F: Fn() -> L> {
    new_log: F,
    _l: PhantomData<L>,
}

impl<L, F> LogTester<L, F>
where
    L: Log,
    F: Fn() -> L,
{
    /// Creates a new test suite.
    pub fn new(new_log: F) -> Self {
        Self {
            new_log,
            _l: PhantomData,
        }
    }

    pub fn test_all(&mut self) {
        self.test_append_entries_basic();
        self.test_store_values();
        self.test_log_truncations();
        self.test_config_stored_separately();
    }

    pub fn create_log(&self) -> L {
        let log = (self.new_log)();
        let (zero_index, latest_index, latest_volatile_index) = log.current_view().unwrap();
        // ensure log conforms default requirements
        assert_eq!(
            latest_index,
            LogIndex(0),
            "latest_index should return LogIndex(0) for empty log"
        );
        assert_eq!(
            zero_index,
            LogIndex(0),
            "zero_index should return LogIndex(0) for empty log"
        );
        assert_eq!(
            latest_volatile_index,
            LogIndex(0),
            "latest_volatile_index should return LogIndex(0) for empty log"
        );

        /*assert_eq!(*/
        /*log.term_of(LogIndex(0)).unwrap(),*/
        /*None,*/
        /*"Term of LogIndex(0) must always be Some(Term(0))"*/
        /*);*/
        assert_eq!(
            log.voted_for().unwrap(),
            None,
            "voted_for should be unset in empty log"
        );

        assert_eq!(
            log.current_term().unwrap(),
            Term(0),
            "Empty log must always start with Term(0)"
        );
        todo!("fix log tests");
        /*
         *assert_eq!(
         *    log.latest_config_index().unwrap(),
         *    None,
         *    "empty log should have no config index set"
         *);
         *assert_eq!(
         *    log.latest_config().unwrap(),
         *    None,
         *    "empty log should have no config set"
         *);
         */
        log
    }

    /// Ensure entries of all types appended in order and read
    pub fn test_append_entries_basic(&mut self) {
        let mut log = self.create_log();
        let empty_entry = LogEntry {
            term: Term(0),
            data: LogEntryData::Empty,
        };
        let proposal_entry = LogEntry {
            term: Term(0),
            data: LogEntryData::Proposal(Bytes::from("hi"), Urgency::Fast),
        };

        todo!("fix log tests");
        /*
         *        let config =
         *            ConsensusConfig::new(vec![Peer::new(42.into()), Peer::new(69.into())].into_iter());
         *        let config_entry = LogEntry {
         *            term: Term(1),
         *            data: LogEntryData::Config(config.clone()),
         *        };
         *        log.append_entries(
         *            LogIndex(1),
         *            &[
         *                empty_entry.clone(),
         *                proposal_entry.clone(),
         *                config_entry.clone(),
         *            ],
         *        )
         *        .unwrap();
         *        log.sync().unwrap();
         *
         *        let mut entry = LogEntry::default();
         *        log.read_entry(1.into(), &mut entry).unwrap();
         *        assert_eq!(&entry, &empty_entry);
         *        log.read_entry(2.into(), &mut entry).unwrap();
         *        assert_eq!(&entry, &proposal_entry);
         *        log.read_entry(3.into(), &mut entry).unwrap();
         *        assert_eq!(&entry, &config_entry);
         *
         *        assert_eq!(log.entry_meta_at(LogIndex(1)).unwrap(), LogEntryMeta::Empty,);
         *        assert_eq!(
         *            log.entry_meta_at(LogIndex(2)).unwrap(),
         *            LogEntryMeta::Proposal(Urgency::Fast)
         *        );
         *
         *        assert_eq!(
         *            log.entry_meta_at(LogIndex(3)).unwrap(),
         *            LogEntryMeta::Config(config)
         *        );
         *
         *        // ensure log indexes conform the requirements
         *
         *        let (zero_index, latest_index, latest_volatile_index) = log.current_view().unwrap();
         *        assert_eq!(latest_index, LogIndex(3));
         *        assert_eq!(latest_volatile_index, LogIndex(3));
         *        assert_eq!(zero_index, LogIndex(0));
         *
         *        assert_eq!(log.term_of(LogIndex(0)).unwrap(), Term(0));
         *        assert_eq!(log.term_of(LogIndex(1)).unwrap(), Term(0));
         *        assert_eq!(log.term_of(LogIndex(2)).unwrap(), Term(0));
         *        assert_eq!(log.term_of(LogIndex(3)).unwrap(), Term(1));
         */
    }

    /// Ensure values like term, voted_for, config, etc. are stored.
    pub fn test_store_values(&mut self) {
        let mut log = self.create_log();

        log.set_current_term(Term(2)).unwrap();
        log.sync().unwrap();
        assert_eq!(log.current_term().unwrap(), (Term(2)));

        todo!("fix log tests");
        /*
         *        let mut config =
         *            ConsensusConfig::new(vec![Peer::new(42.into()), Peer::new(69.into())].into_iter());
         *        let mut config_entry = LogEntry {
         *            term: Term(2),
         *            data: LogEntryData::Config(config.clone()),
         *        };
         *
         *        log.append_entries(LogIndex(1), &[config_entry.clone()])
         *            .unwrap();
         *        log.sync().unwrap();
         *
         *        // make sure appending entry did not affect latest_config
         *        assert_eq!(log.latest_config().unwrap(), None);
         *
         *        log.set_latest_config(&config, LogIndex(1)).unwrap();
         *        assert_eq!(log.latest_config().unwrap(), Some(config.clone()));
         *        assert_eq!(log.latest_config_index().unwrap(), Some(LogIndex(1)));
         *
         *        config.peers.pop();
         *        config_entry.data = LogEntryData::Config(config.clone());
         *
         *        log.append_entries(LogIndex(2), &[config_entry.clone()])
         *            .unwrap();
         *        log.sync().unwrap();
         *
         *        log.set_latest_config(&config, LogIndex(2)).unwrap();
         *        assert_eq!(log.latest_config().unwrap(), Some(config.clone()));
         *        assert_eq!(log.latest_config_index().unwrap(), Some(LogIndex(2)));
         *
         *        // in case the implementation is using ServerId from the latest config for it's own
         *        // reasons, we intentionally use the ID from the config provided in previous step
         *
         *        log.set_voted_for(Some(ServerId(42))).unwrap();
         *        log.sync().unwrap();
         *        assert_eq!(log.voted_for().unwrap(), Some(ServerId(42)));
         *
         *        log.set_voted_for(None).unwrap();
         *        log.sync().unwrap();
         *        assert_eq!(log.voted_for().unwrap(), None);
         */
    }

    /// Ensure latest config is not stored in log, and returned even if log is truncated
    pub fn test_log_truncations(&mut self) {
        let mut log = self.create_log();

        let mut entries = Vec::with_capacity(8);
        for i in 11..19 {
            // 1 to 8 is to easier matching of log index
            // + 10 is to mot match the index completely
            entries.push(LogEntry {
                term: Term(0),
                data: LogEntryData::Proposal(Bytes::from(format!("{}", i)), Urgency::Fast),
            });
        }
        log.append_entries(LogIndex(1), &entries).unwrap();
        log.sync().unwrap();

        // At this point we should have 8 entries: 11 @ LogIndex(1) to 18 @ LogIndex(8)

        // ensure ordering
        for i in 1..9 {
            let mut entry = LogEntry::default();
            log.read_entry(LogIndex(i), &mut entry).unwrap();
            assert_eq!(
                entry.try_proposal_data().unwrap(),
                Bytes::from(format!("{}", i + 10))
            );
        }

        // Now, try to discard log starting from LogIndex(5) appending some more entries
        let mut new_entries = Vec::with_capacity(8);
        for i in 105..114 {
            // 5 to 13 is the same purpose as before - to match the log index
            new_entries.push(LogEntry {
                term: Term(0),
                data: LogEntryData::Proposal(Bytes::from(format!("{}", i)), Urgency::Fast),
            });
        }
        log.discard_since(LogIndex(5)).unwrap();
        log.sync().unwrap();

        let (zero_index, new_latest_index, latest_volatile_index) = log.current_view().unwrap();
        // log should be cut properly
        assert!(new_latest_index < LogIndex(5));
        assert!(latest_volatile_index < LogIndex(5));

        // if log is cut to some earlier value, fill it with the old entries first
        if new_latest_index == LogIndex(0) {
            // if log was emptied, reappend all entries
            log.append_entries(LogIndex(1), &entries).unwrap();
        } else if new_latest_index < LogIndex(4) {
            // if some older part of the log was discarded - restore it
            log.append_entries(new_latest_index, &entries[new_latest_index.as_usize()..4])
                .unwrap();
        }
        log.append_entries(LogIndex(5), &new_entries).unwrap();
        log.sync().unwrap();

        let (zero_index, latest_index, latest_volatile_index) = log.current_view().unwrap();
        assert_eq!(zero_index, LogIndex(0));
        assert_eq!(latest_index, LogIndex(13));
        assert_eq!(latest_volatile_index, LogIndex(13));

        // ensure log looks like
        // [11, ..., 14, 105, ..., 113]
        for i in 1u64..13 {
            let mut entry = LogEntry::default();
            log.read_entry(LogIndex(i), &mut entry).unwrap();
            if i < 5 {
                assert_eq!(
                    entry.try_proposal_data().unwrap(),
                    Bytes::from(format!("{}", i + 10))
                );
            } else {
                assert_eq!(
                    entry.try_proposal_data().unwrap(),
                    Bytes::from(format!("{}", i + 100))
                );
            }
        }

        // while log exists, take zero term from the log
        let zero_term = log.term_of(LogIndex(3)).unwrap();

        // Now truncate the head of log up until 103
        log.discard_until(LogIndex(3), zero_term).unwrap();
        log.sync().unwrap();

        let (zero_index, latest_index, latest_volatile_index) = log.current_view().unwrap();
        // log must be cut until exactly requested index
        assert_eq!(zero_index, LogIndex(3));
        // log must save the provided term
        assert_eq!(log.term_of(LogIndex(3)).unwrap(), zero_term);
        assert_eq!(latest_index, LogIndex(13));
        assert_eq!(latest_volatile_index, LogIndex(13));

        // fully discarded log should store it's indexes index intact, even having no
        // entries, must point to zero index and be able to read zero term
        log.discard_since(zero_index + 1).unwrap();
        log.sync().unwrap();

        let (zero_index, latest_index, latest_volatile_index) = log.current_view().unwrap();
        assert_eq!(zero_index, LogIndex(3));
        assert_eq!(log.term_of(LogIndex(3)).unwrap(), zero_term);
        assert_eq!(latest_index, zero_index);
        assert_eq!(latest_volatile_index, zero_index);

        // Append a single entry to the emptied log
        let replaced_entry = LogEntry {
            term: Term(5),
            data: LogEntryData::Proposal(Bytes::from(format!("{}", 5)), Urgency::Fast),
        };
        log.append_entries(zero_index + 1, &[replaced_entry.clone()])
            .unwrap();
        log.sync().unwrap();

        let (zero_index, latest_index, latest_volatile_index) = log.current_view().unwrap();
        // make sure entry is placed at LogIndex(4)
        assert_eq!(zero_index, LogIndex(3));
        assert_eq!(log.term_of(LogIndex(4)).unwrap(), Term(5));
        assert_eq!(latest_index, LogIndex(4));
        assert_eq!(latest_volatile_index, LogIndex(4));

        let mut entry = LogEntry::default();
        log.read_entry(LogIndex(4), &mut entry).unwrap();
        assert_eq!(entry, replaced_entry);

        // Now, imagine some snapshot happening at LogIndex(42) with Term(128) (why not)
        log.discard_until(LogIndex(42), Term(128)).unwrap();
        log.sync().unwrap();

        let (zero_index, latest_index, latest_volatile_index) = log.current_view().unwrap();
        // ensure log is emptied again and zero term is retrievable
        assert_eq!(zero_index, LogIndex(42));
        assert_eq!(log.term_of(LogIndex(42)).unwrap(), Term(128));
        assert_eq!(latest_index, LogIndex(42));
        assert_eq!(latest_volatile_index, LogIndex(42));

        // Now, imagine some snapshot required to be reset to LogIndex(4) with Term(2) (why not)
        log.discard_until(LogIndex(4), Term(2)).unwrap();
        log.sync().unwrap();

        let (zero_index, latest_index, latest_volatile_index) = log.current_view().unwrap();
        // ensure log is emptied again and zero term is retrievable
        assert_eq!(zero_index, LogIndex(4));
        assert_eq!(log.term_of(LogIndex(4)).unwrap(), Term(2));
        assert_eq!(latest_index, LogIndex(4));
        assert_eq!(latest_volatile_index, LogIndex(4));
    }

    /// Ensure latest config is not stored in log, and returned even if log is truncated
    pub fn test_config_stored_separately(&mut self) {
        let mut log = self.create_log();

        log.set_current_term(Term(2)).unwrap();
        log.sync().unwrap();
        assert_eq!(log.current_term().unwrap(), (Term(2)));

        todo!("fix log tests");
        /*
         *        let config =
         *            ConsensusConfig::new(vec![Peer::new(42.into()), Peer::new(69.into())].into_iter());
         *
         *        let config_entry = LogEntry {
         *            term: Term(2),
         *            data: LogEntryData::Config(config.clone()),
         *        };
         *
         *        log.append_entries(LogIndex(1), &[config_entry.clone()])
         *            .unwrap();
         *        log.sync().unwrap();
         *
         *        log.set_latest_config(&config, LogIndex(1)).unwrap();
         *
         *        // discard the whole log
         *        log.discard_until(LogIndex(1), Term(2)).unwrap();
         *        log.sync().unwrap();
         *
         *        // make sure config and its data is readable, meaning it is not taken from the entry data
         *        assert_eq!(log.latest_config().unwrap(), Some(config.clone()));
         *        assert_eq!(log.latest_config_index().unwrap(), Some(LogIndex(1)));
         */
    }
}
