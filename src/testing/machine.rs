use bytes::Bytes;

use crate::message::ClientGuarantee;
use crate::persistent_log::{LogEntry, LogEntryData};
use crate::{ConsensusConfig, Log, LogIndex, Peer, StateMachine, Term};

use std::marker::PhantomData;

/// Runs a test suite for the state maching. Will panic if anything goes wrong.
/// Function F should return the intialized a fresh machine each run, with no (notable)
/// 3rd party effects.
///
/// All tests are run sequentially, so each machine should clean up after itself
/// before returning new log or whatever other way it wants.
pub struct MachineTester<M: StateMachine, F: Fn() -> M> {
    new_machine: F,
    _l: PhantomData<M>,
}

impl<M, F> MachineTester<M, F>
where
    M: StateMachine,
    F: Fn() -> M,
{
    /// Creates a new test suite.
    pub fn new(new_machine: F) -> Self {
        Self {
            new_machine,
            _l: PhantomData,
        }
    }

    /// Run all unit tests on the machine
    pub fn test_all(&mut self) {
        self.test_all_entries_applied();
        self.test_snapshot_index_and_term_correct();
    }

    pub fn test_all_entries_applied(&mut self) {
        let mut machine = (self.new_machine)();
        let log = machine.log_mut();
        let empty_entry = LogEntry {
            term: Term(0),
            data: LogEntryData::Empty,
        };
        let proposal_entry = LogEntry {
            term: Term(0),
            data: LogEntryData::Proposal(Bytes::from("hi"), ClientGuarantee::Fast),
        };
        let config = ConsensusConfig {
            peers: vec![Peer::new(42.into()), Peer::new(69.into())],
        };
        let config_entry = LogEntry {
            term: Term(1),
            data: LogEntryData::Config(config.clone()),
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
        log.sync().unwrap();
        machine.apply(LogIndex(1), false).unwrap();
        machine.apply(LogIndex(2), false).unwrap();
        machine.apply(LogIndex(3), false).unwrap();
        machine.sync().unwrap();
        assert_eq!(machine.last_applied().unwrap(), LogIndex(3));
    }

    pub fn test_snapshot_index_and_term_correct(&mut self) {
        let mut machine = (self.new_machine)();
        let log = machine.log_mut();
        let mut entries = Vec::with_capacity(8);
        for i in 1..9 {
            entries.push(LogEntry {
                term: Term(i),
                data: LogEntryData::Proposal(Bytes::from(format!("{}", i)), ClientGuarantee::Log),
            });
        }
        log.append_entries(LogIndex(1), &entries).unwrap();
        log.sync().unwrap();

        for i in 1..9 {
            machine.apply(LogIndex(i), false).unwrap();
        }

        // take snapshot at each index, make sure the index and term is correct
        for i in 1..9 {
            machine.take_snapshot(LogIndex(i), Term(i)).unwrap();
            machine.sync().unwrap();
            let info = machine.snapshot_info().unwrap();
            if let Some(info) = info {
                assert!(info.index <= LogIndex(i));
                assert_eq!(LogIndex(info.term.as_u64()), LogIndex(i));
            }
        }
    }
}
