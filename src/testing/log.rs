use bytes::Bytes;

use crate::message::ClientGuarantee;
use crate::persistent_log::{Log, LogEntry, LogEntryData};
use crate::{ConsensusConfig, LogIndex, Peer, Term};

use std::marker::PhantomData;

/// Runs a test suite for the log. Will panic if anything goes wrong.
/// Function F should return the clean log, fully initialized and ready for work and will be run
/// more than one time.
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

    pub fn test_all(self) {
        self.test_append_entries_simple()
    }

    pub fn create_log(self) -> L {
        let log = (self.new_log)();
        // ensure log conforms default requirements
        if log.latest_index().unwrap() != LogIndex(0) {
            panic!("latest_index should return LogIndex(0) for empty log");
        }
        if log.first_index().unwrap() != LogIndex(0) {
            panic!("first_index should return LogIndex(0) for empty log");
        }
        if log.latest_volatile_index().unwrap() != LogIndex(0) {
            panic!("latest_volatile_index should return LogIndex(0) for empty log");
        }
        log
    }

    /// Ensure entries of all types appended in order and read
    pub fn test_append_entries_simple(self) {
        let mut log = self.create_log();
        let empty_entry = LogEntry {
            term: Term(0),
            data: LogEntryData::Empty,
        };
        let proposal_entry = LogEntry {
            term: Term(0),
            data: LogEntryData::Proposal(Bytes::from("hi"), ClientGuarantee::default()),
        };
        let config_entry = LogEntry {
            term: Term(0),
            data: LogEntryData::Config(ConsensusConfig {
                peers: vec![Peer::new(42.into()), Peer::new(69.into())],
            }),
        };
        log.append_entries(
            LogIndex(1),
            &[
                empty_entry.clone(),
                proposal_entry.clone(),
                config_entry.clone(),
            ],
        )
        .unwrap();
        let mut entry = LogEntry::default();
        log.read_entry(1.into(), &mut entry).unwrap();
        assert_eq!(&entry, &empty_entry);
        log.read_entry(2.into(), &mut entry).unwrap();
        assert_eq!(&entry, &proposal_entry);
        log.read_entry(3.into(), &mut entry).unwrap();
        assert_eq!(&entry, &config_entry);
    }
}
