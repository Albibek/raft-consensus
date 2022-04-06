use std::marker::PhantomData;

use tracing::{info, trace};

use crate::persistent_log::LogEntry;
use crate::state_machine::StateMachine;
use crate::{error::CriticalError, persistent_log::Log};

use crate::config::{ConsensusConfig, ConsensusOptions};
//use crate::entry::{ConsensusConfig, Entry, EntryData};
use crate::error::Error;
use crate::handler::Handler;
use crate::raft::CurrentState;
use crate::state_impl::StateImpl;
use crate::{debug_where, message::*, Peer};
use crate::{LogIndex, ServerId, Term};

use crate::follower::Follower;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct State<M, H, S>
where
    M: StateMachine,
    H: Handler,
{
    // The ID of this consensus instance.
    pub(crate) id: ServerId,

    // The IDs of peers in the consensus group.
    pub(crate) config: ConsensusConfig,

    // The client state machine to which client commands are applied.
    pub(crate) state_machine: M,

    // Index of the latest entry known to be committed.
    pub(crate) commit_index: LogIndex,

    // The minimal index at which entries can be appended. This bit of state
    // allows avoiding overwriting of possibly committed parts of the log
    // when messages arrive out of order.
    // It is set when follower becomes leader and otherwise left untouched.
    // See see ktoso/akka-raft#66.
    pub(crate) min_index: LogIndex,

    // Index of the latest entry applied to the state machine.
    //    pub(crate) last_applied: LogIndex,

    // State-specific data
    pub(crate) state_data: S,

    pub(crate) options: ConsensusOptions,

    pub(crate) _h: PhantomData<H>,

    pub(crate) zero_log_index: LogIndex,
    pub(crate) latest_log_index: LogIndex,
    pub(crate) latest_volatile_log_index: LogIndex,
    pub(crate) latest_config_index: LogIndex,
}

/// Common functions for any state
impl<M, H, S> State<M, H, S>
where
    M: StateMachine,
    H: Handler,
{
    /// Returns the current term.
    pub(crate) fn current_term(&self) -> Result<Term, Error> {
        Ok(self
            .log()
            .current_term()
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?)
    }

    /// Increases the current term and resets the voted_for value.
    pub(crate) fn inc_current_term(&mut self) -> Result<(), Error> {
        let current = self
            .log()
            .current_term()
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
        self.log_mut()
            .set_voted_for(None)
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;
        self.log_mut()
            .set_current_term(current + 1)
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))
    }

    /// Helper for returning the term of the latest applied log entry.
    /// Hints are for cases when values were already read from log.
    #[inline]
    pub(crate) fn latest_log_term(&self) -> Result<Term, Error> {
        if self.latest_log_index == LogIndex(0) {
            return Ok(Term(0));
        }

        self.log()
            .term_of(self.latest_log_index)
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))
    }

    /// Helper for self.state_machine.log()
    #[inline]
    pub(crate) fn log(&self) -> &M::Log {
        self.state_machine.log()
    }

    /// Helper for self.state_machine.log_mut()
    #[inline]
    pub(crate) fn log_mut(&mut self) -> &mut M::Log {
        self.state_machine.log_mut()
    }
}

// State change helpers
impl<M, H, S> State<M, H, S>
where
    M: StateMachine,
    H: Handler,
    Self: StateImpl<M, H>,
{
    pub(crate) fn into_follower(
        self,
        handler: &mut H,
        from: ConsensusState,
        leader_term: Term,
    ) -> Result<State<M, H, Follower>, Error> {
        trace!("transitioning to follower");
        handler.state_changed(from, ConsensusState::Follower);

        let mut follower_state = State {
            id: self.id,
            config: self.config,
            state_machine: self.state_machine,
            commit_index: self.commit_index,
            min_index: self.min_index,
            _h: PhantomData,
            state_data: Follower::new(),
            options: self.options,
            zero_log_index: self.zero_log_index,
            latest_log_index: self.latest_log_index,
            latest_volatile_log_index: self.latest_volatile_log_index,
            latest_config_index: self.latest_config_index,
        };

        // Apply all changes via the new state
        follower_state
            .log_mut()
            .set_current_term(leader_term)
            .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;

        follower_state.config.clear_heartbeats(handler);
        handler.set_timeout(Timeout::Election);

        Ok(follower_state)
    }

    // At some point all nodes behave in the same way regardless of their state
    pub(crate) fn common_request_vote_request(
        mut self,
        handler: &mut H,
        from: ServerId,
        request: RequestVoteRequest,
        from_state: ConsensusState,
    ) -> Result<(RequestVoteResponse, CurrentState<M, H>), Error> {
        let candidate_term = request.term;
        let candidate_last_term = request.last_log_term;
        let candidate_log_index = request.last_log_index;
        trace!(
        %candidate_term,
               remote_log_index = %candidate_log_index,
               remote_last_term = %candidate_last_term,
               prev_state = ?from_state,
               "received vote request",
           );

        let current_term = self.current_term()?;
        let (new_local_term, change_to_follower) = if candidate_term > current_term {
            info!(
                 remote_term = ?candidate_term,
                "request has newer term, than local transitioning to follower"
            );

            (candidate_term, true)
        } else {
            (current_term, false)
        };

        let message = if candidate_term < current_term {
            RequestVoteResponse::StaleTerm(new_local_term)
        } else if candidate_last_term < self.latest_log_term()?
            || candidate_log_index < self.latest_log_index
        {
            RequestVoteResponse::InconsistentLog(new_local_term)
        } else {
            // candidate_term == current_term
            match self
                .log()
                .voted_for()
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?
            {
                None => {
                    self.log_mut().set_voted_for(Some(from)).map_err(|e| {
                        Error::Critical(CriticalError::PersistentLogWrite(Box::new(e)))
                    })?;
                    trace!("granted vote to peer");
                    RequestVoteResponse::Granted(new_local_term)
                }
                Some(voted_for) => {
                    trace!("already voted for {}", voted_for);
                    if voted_for == from {
                        RequestVoteResponse::Granted(new_local_term)
                    } else {
                        RequestVoteResponse::AlreadyVoted(new_local_term)
                    }
                }
            }
        };

        if change_to_follower {
            let new_state = self.into_follower(handler, from_state, candidate_term)?;
            Ok((message, new_state.into()))
        } else {
            Ok((message, self.into()))
        }
    }

    pub(crate) fn common_client_ping_request(
        &self,
        kind: ConsensusState,
    ) -> Result<PingResponse, Error> {
        Ok(PingResponse {
            term: self.current_term()?,
            index: self.latest_log_index,
            state: kind,
        })
    }

    // checks snapshot only if forced,
    pub(crate) fn common_check_compaction(&mut self, force: bool) -> Result<bool, Error> {
        let info = self
            .state_machine
            .snapshot_info()
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

        if let Some(info) = info {
            if info.index == self.commit_index || self.commit_index == LogIndex(0) {
                trace!("not taking snapshot because log index did not change or commit index is not known yet");
                return Ok(false);
            }

            if self.commit_index < info.index {
                // snapshot is from later index, meaning it is corrupted
                return Err(Error::Critical(CriticalError::SnapshotCorrupted));
            } else if self.commit_index == info.index {
                // taking snapshot is not required
                return Ok(false);
            } else {
                if !force {
                    return Ok(false);
                }
            }
        }

        // we should get here if:
        // * commit_index > info.index
        // * snapshot_info is None
        // both meaning there is a point to make the snapshot
        let commit_term = self
            .log()
            .term_of(self.commit_index)
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
        self.state_machine
            .take_snapshot(self.commit_index, commit_term)
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;

        // request new info in case the snapshot was taken synchronously
        if let Some(new_info) = self
            .state_machine
            .snapshot_info()
            .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?
        {
            trace!(
            snapshot_index = %new_info.index,
            snapshot_term = %new_info.term,
            "took snapshot"
            );
            self.log_mut()
                .discard_until(new_info.index, new_info.term)
                .map_err(|e| Error::Critical(CriticalError::PersistentLogWrite(Box::new(e))))?;
        }
        return Ok(true);
    }

    pub(crate) fn update_latest_config(
        &mut self,
        snapshot_hint: Option<&[Peer]>,
    ) -> Result<(), Error> {
        if self.latest_config_index > self.zero_log_index {
            if let Some(snapshot_config) = snapshot_hint {
                self.config = ConsensusConfig::new(snapshot_config.iter().cloned());
                Ok(())
            } else {
                let info = self
                    .state_machine
                    .snapshot_info()
                    .map_err(|e| Error::Critical(CriticalError::StateMachine(Box::new(e))))?;
                if let Some(info) = info {
                    self.config = ConsensusConfig::new(info.config.iter().cloned());
                }
                Ok(())
            }
        } else if self.latest_config_index <= self.latest_log_index {
            let mut config_entry = LogEntry::default();
            self.log()
                .read_entry(self.latest_config_index, &mut config_entry)
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
            if let Some(peers) = config_entry.try_config_data() {
                self.config.update_from_new(&peers)?;
                Ok(())
            } else {
                Err(Error::Critical(CriticalError::LogInconsistentEntry(
                    self.latest_config_index,
                )))
            }
        } else {
            Err(Error::unreachable(debug_where!()))
        }
    }

    pub(crate) fn update_log_view(&mut self) -> Result<(), Error> {
        // read actual log indexes from the log
        let log_view = self
            .log()
            .current_view()
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
        self.zero_log_index = log_view.0;
        self.latest_log_index = log_view.1;
        self.latest_volatile_log_index = log_view.2;
        Ok(())
    }

    pub(crate) fn common_on_any_event(&mut self) -> Result<(), Error> {
        // read actual log indexes from the log
        let log_view = self
            .log()
            .current_view()
            .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
        self.zero_log_index = log_view.0;
        self.latest_log_index = log_view.1;
        self.latest_volatile_log_index = log_view.2;

        // if new config index is in view and we didn't already read it into memory:
        // read the entry from log and update self.config
        if !self.config.has_changes() && self.latest_config_index <= self.latest_log_index {
            let mut config_entry = LogEntry::default();
            self.log()
                .read_entry(self.latest_config_index, &mut config_entry)
                .map_err(|e| Error::PersistentLogRead(Box::new(e)))?;
            if let Some(peers) = config_entry.try_config_data() {
                self.config.update_from_new(&peers)?;
            } else {
                return Err(Error::Critical(CriticalError::LogInconsistentEntry(
                    self.latest_config_index,
                )));
            }
        }

        Ok(())

        // on append_entries:
        // scan entries, if active config entry found:
        // DONE: * persist entry index in log as new
        //
        // on follower: if new config index is to be discarded (persistent or volatile):
        // * cancel pending in memory
        // * persist updated config to log
        //
        // on snapshot: update prev config index to one in snapshot
        // when new_config_index is committed
        // * commit pending in memory
        // * save updated config to log
        //
        // DONE: on restart: read lastest config, remove pending if index is beyond volatile
    }
}
