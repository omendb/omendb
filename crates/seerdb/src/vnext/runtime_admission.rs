//! Runtime admission, checkpoint drain, and failure fencing for storage-kernel vNext.
//!
//! A structurally complete checkpoint needs a precise boundary: stop admitting
//! new commits, let every already-admitted commit finish installation and
//! visibility publication, then capture while commit-side mutation stays
//! quiescent. This module owns that process-local boundary.
//!
//! Reads/snapshots are not drained by checkpoint capture because they do not
//! mutate the captured page graph. They are, however, refused after the runtime
//! enters recovery-required state. Existing long-running readers must recheck
//! admission at their normal cancellation/operation boundaries once the server
//! runtime is wired to this gate.
//!
//! There is intentionally no in-process "unfence" operation. Recovery creates a
//! new runtime after durable state has been revalidated.

use std::sync::{Condvar, Mutex};

/// Observable process-local admission state.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RuntimeAdmissionState {
    /// Ordinary reads and commits may begin.
    Open,
    /// New commits are blocked while already-admitted commits drain.
    Checkpointing,
    /// Reads and writes fail closed until recovery/reopen.
    RecoveryRequired,
}

#[derive(Debug)]
struct AdmissionState {
    mode: RuntimeAdmissionState,
    active_commits: usize,
}

/// Process-local authority for commit admission, checkpoint drain and failure fencing.
pub struct RuntimeAdmission {
    state: Mutex<AdmissionState>,
    changed: Condvar,
}

impl Default for RuntimeAdmission {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeAdmission {
    /// Construct an open runtime with no admitted commits.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: Mutex::new(AdmissionState {
                mode: RuntimeAdmissionState::Open,
                active_commits: 0,
            }),
            changed: Condvar::new(),
        }
    }

    /// Admit one commit/install/publication attempt.
    ///
    /// The returned guard must live through the entire commit path, including
    /// authoritative page installation and contiguous visibility completion.
    /// Checkpoint drain waits for all such guards to leave.
    pub fn admit_commit(&self) -> Result<CommitAdmission<'_>, RuntimeAdmissionError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| RuntimeAdmissionError::Poisoned)?;
        match state.mode {
            RuntimeAdmissionState::Open => {
                state.active_commits = state
                    .active_commits
                    .checked_add(1)
                    .ok_or(RuntimeAdmissionError::CommitCountExhausted)?;
                Ok(CommitAdmission {
                    admission: self,
                    active: true,
                })
            }
            RuntimeAdmissionState::Checkpointing => {
                Err(RuntimeAdmissionError::CheckpointInProgress)
            }
            RuntimeAdmissionState::RecoveryRequired => Err(RuntimeAdmissionError::RecoveryRequired),
        }
    }

    /// Check whether a new read/snapshot operation may proceed.
    ///
    /// Reads are allowed during checkpoint capture because capture quiesces
    /// mutation, not immutable snapshot traversal. Recovery-required state
    /// fences them until reopen.
    pub fn ensure_read_admission(&self) -> Result<(), RuntimeAdmissionError> {
        match self.state()? {
            RuntimeAdmissionState::Open | RuntimeAdmissionState::Checkpointing => Ok(()),
            RuntimeAdmissionState::RecoveryRequired => Err(RuntimeAdmissionError::RecoveryRequired),
        }
    }

    /// Start a checkpoint drain and wait until every already-admitted commit has
    /// completed or the runtime becomes recovery-required.
    ///
    /// Holding the returned guard keeps new commits blocked. Dropping it reopens
    /// commit admission only if no failure fenced the runtime in the meantime.
    pub fn begin_checkpoint(&self) -> Result<CheckpointAdmission<'_>, RuntimeAdmissionError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| RuntimeAdmissionError::Poisoned)?;
        match state.mode {
            RuntimeAdmissionState::Open => {
                state.mode = RuntimeAdmissionState::Checkpointing;
            }
            RuntimeAdmissionState::Checkpointing => {
                return Err(RuntimeAdmissionError::CheckpointInProgress);
            }
            RuntimeAdmissionState::RecoveryRequired => {
                return Err(RuntimeAdmissionError::RecoveryRequired);
            }
        }

        while state.active_commits != 0 {
            state = self
                .changed
                .wait(state)
                .map_err(|_| RuntimeAdmissionError::Poisoned)?;
            if state.mode == RuntimeAdmissionState::RecoveryRequired {
                return Err(RuntimeAdmissionError::RecoveryRequired);
            }
        }

        Ok(CheckpointAdmission {
            admission: self,
            active: true,
        })
    }

    /// Fence the runtime after an unresolved failure.
    ///
    /// This is monotonic for the process lifetime. Waiters are woken so a
    /// checkpoint drain cannot block forever behind a commit that now requires
    /// recovery.
    pub fn require_recovery(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.mode = RuntimeAdmissionState::RecoveryRequired;
        drop(state);
        self.changed.notify_all();
    }

    /// Current admission state.
    pub fn state(&self) -> Result<RuntimeAdmissionState, RuntimeAdmissionError> {
        self.state
            .lock()
            .map(|state| state.mode)
            .map_err(|_| RuntimeAdmissionError::Poisoned)
    }

    /// Number of commits currently inside the admitted commit/install path.
    pub fn active_commits(&self) -> Result<usize, RuntimeAdmissionError> {
        self.state
            .lock()
            .map(|state| state.active_commits)
            .map_err(|_| RuntimeAdmissionError::Poisoned)
    }

    fn release_commit(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        debug_assert!(state.active_commits > 0);
        if state.active_commits != 0 {
            state.active_commits -= 1;
        }
        let drained = state.active_commits == 0;
        drop(state);
        if drained {
            self.changed.notify_all();
        }
    }

    fn finish_checkpoint(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.mode == RuntimeAdmissionState::Checkpointing {
            state.mode = RuntimeAdmissionState::Open;
        }
        drop(state);
        self.changed.notify_all();
    }
}

/// RAII membership in the set a checkpoint must drain.
pub struct CommitAdmission<'a> {
    admission: &'a RuntimeAdmission,
    active: bool,
}

impl Drop for CommitAdmission<'_> {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.admission.release_commit();
        }
    }
}

/// Exclusive process-local checkpoint capture interval.
pub struct CheckpointAdmission<'a> {
    admission: &'a RuntimeAdmission,
    active: bool,
}

impl Drop for CheckpointAdmission<'_> {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.admission.finish_checkpoint();
        }
    }
}

/// Admission or drain failure.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum RuntimeAdmissionError {
    #[error("checkpoint capture is already draining or active")]
    CheckpointInProgress,
    #[error("runtime requires recovery before more database work")]
    RecoveryRequired,
    #[error("runtime admission lock is poisoned")]
    Poisoned,
    #[error("active commit admission count is exhausted")]
    CommitCountExhausted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn checkpoint_blocks_new_commits_until_guard_drops() {
        let admission = RuntimeAdmission::new();
        let checkpoint = admission.begin_checkpoint().expect("checkpoint starts");
        assert_eq!(
            admission.state().expect("state"),
            RuntimeAdmissionState::Checkpointing
        );
        assert!(matches!(
            admission.admit_commit(),
            Err(RuntimeAdmissionError::CheckpointInProgress)
        ));
        assert!(admission.ensure_read_admission().is_ok());

        drop(checkpoint);
        assert_eq!(
            admission.state().expect("state"),
            RuntimeAdmissionState::Open
        );
        let permit = admission.admit_commit().expect("commit admitted");
        assert_eq!(admission.active_commits().expect("count"), 1);
        drop(permit);
        assert_eq!(admission.active_commits().expect("count"), 0);
    }

    #[test]
    fn checkpoint_waits_for_already_admitted_commit() {
        let admission = Arc::new(RuntimeAdmission::new());
        let permit = admission.admit_commit().expect("commit admitted");
        let (sender, receiver) = mpsc::channel();
        let worker = Arc::clone(&admission);
        let thread = thread::spawn(move || {
            let checkpoint = worker.begin_checkpoint().expect("checkpoint drains");
            sender.send(()).expect("signal");
            drop(checkpoint);
        });

        thread::sleep(Duration::from_millis(20));
        assert!(receiver.try_recv().is_err());
        assert_eq!(
            admission.state().expect("state"),
            RuntimeAdmissionState::Checkpointing
        );

        drop(permit);
        receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("checkpoint completes drain");
        thread.join().expect("thread");
        assert_eq!(
            admission.state().expect("state"),
            RuntimeAdmissionState::Open
        );
    }

    #[test]
    fn recovery_fence_wakes_checkpoint_drain_and_never_reopens() {
        let admission = Arc::new(RuntimeAdmission::new());
        let _permit = admission.admit_commit().expect("commit admitted");
        let worker = Arc::clone(&admission);
        let fence = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            worker.require_recovery();
        });

        assert!(matches!(
            admission.begin_checkpoint(),
            Err(RuntimeAdmissionError::RecoveryRequired)
        ));
        fence.join().expect("fence thread");
        assert_eq!(
            admission.state().expect("state"),
            RuntimeAdmissionState::RecoveryRequired
        );
        assert!(matches!(
            admission.admit_commit(),
            Err(RuntimeAdmissionError::RecoveryRequired)
        ));
        assert!(matches!(
            admission.ensure_read_admission(),
            Err(RuntimeAdmissionError::RecoveryRequired)
        ));
    }

    #[test]
    fn failure_during_checkpoint_guard_does_not_reopen_runtime() {
        let admission = RuntimeAdmission::new();
        let checkpoint = admission.begin_checkpoint().expect("checkpoint starts");
        admission.require_recovery();
        drop(checkpoint);
        assert_eq!(
            admission.state().expect("state"),
            RuntimeAdmissionState::RecoveryRequired
        );
    }
}
