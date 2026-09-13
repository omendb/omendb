//! Contiguous logical visibility frontier for vNext MVCC snapshots.
//!
//! Transaction status is published before a CSN can enter the visible frontier.
//! Commits may finish durability/status work out of order, but new snapshots only
//! observe the highest contiguous committed CSN. Existing older snapshots remain
//! correct because a newly committed owner resolves as `NewerCommit` and follows
//! its undo chain.

use super::{CommitSeq, StatusTableError, TransactionStatusTable, TxnId};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};

/// Process-local visibility frontier rebuilt from durable commit decisions.
pub struct VisibilityFrontier {
    visible: AtomicU64,
    state: Mutex<FrontierState>,
    advanced: Condvar,
}

#[derive(Default)]
struct FrontierState {
    ready: BTreeSet<CommitSeq>,
    recovery_required: bool,
}

impl Default for VisibilityFrontier {
    fn default() -> Self {
        Self::new(CommitSeq::new(0))
    }
}

impl VisibilityFrontier {
    #[must_use]
    pub const fn new(initial: CommitSeq) -> Self {
        Self {
            visible: AtomicU64::new(initial.get()),
            state: Mutex::new(FrontierState {
                ready: BTreeSet::new(),
                recovery_required: false,
            }),
            advanced: Condvar::new(),
        }
    }

    /// Snapshot frontier safe for a newly beginning transaction.
    #[must_use]
    pub fn snapshot(&self) -> CommitSeq {
        CommitSeq::new(self.visible.load(Ordering::Acquire))
    }

    /// Publish one durable active transaction into status and then mark its CSN
    /// ready for snapshots. The frontier advances only through contiguous ready
    /// CSNs, so a slower earlier commit cannot be skipped by a later one.
    ///
    /// This is readiness publication, not completion: the returned frontier may
    /// still be below `csn`. Callers that owe a synchronous acknowledgement use
    /// [`Self::complete_commit`].
    pub fn publish_commit(
        &self,
        statuses: &TransactionStatusTable,
        txn: TxnId,
        csn: CommitSeq,
    ) -> Result<CommitSeq, VisibilityError> {
        statuses.commit(txn, csn)?;
        self.mark_ready(csn)
    }

    /// Complete one synchronous commit: publish its status and CSN as ready, then
    /// block until the contiguous frontier covers it.
    ///
    /// A durable decision outranks physical publication, so success must mean
    /// the transaction is observable by any snapshot taken afterwards. An
    /// unresolved earlier decision cannot be waited through; it surfaces as
    /// [`VisibilityError::RecoveryRequired`] once the runtime reports it.
    pub fn complete_commit(
        &self,
        statuses: &TransactionStatusTable,
        txn: TxnId,
        csn: CommitSeq,
    ) -> Result<CommitSeq, VisibilityError> {
        self.publish_commit(statuses, txn, csn)?;
        self.wait_visible(csn)
    }

    /// Block until `csn` is covered by the contiguous visible frontier.
    pub fn wait_visible(&self, csn: CommitSeq) -> Result<CommitSeq, VisibilityError> {
        let mut state = self.state.lock().map_err(|_| VisibilityError::Poisoned)?;
        loop {
            let visible = self.snapshot();
            if visible >= csn {
                return Ok(visible);
            }
            if state.recovery_required {
                return Err(VisibilityError::RecoveryRequired { visible, csn });
            }
            state = self
                .advanced
                .wait(state)
                .map_err(|_| VisibilityError::Poisoned)?;
        }
    }

    /// Refuse further visibility progress after an unresolved durable decision.
    ///
    /// Waiters must neither block indefinitely nor acknowledge a commit that no
    /// snapshot can yet observe. Recovery re-establishes the frontier.
    pub fn require_recovery(&self) {
        let Ok(mut state) = self.state.lock() else {
            // A poisoned frontier already fails waiters instead of blocking them.
            return;
        };
        state.recovery_required = true;
        drop(state);
        self.advanced.notify_all();
    }

    /// Rebuild a committed status/frontier from validated WAL recovery.
    pub fn publish_recovered(
        &self,
        statuses: &TransactionStatusTable,
        txn: TxnId,
        csn: CommitSeq,
    ) -> Result<CommitSeq, VisibilityError> {
        statuses.recover_committed(txn, csn)?;
        self.mark_ready(csn)
    }

    fn mark_ready(&self, csn: CommitSeq) -> Result<CommitSeq, VisibilityError> {
        let visible = self.snapshot();
        if csn <= visible {
            return Err(VisibilityError::AlreadyVisible {
                visible,
                requested: csn,
            });
        }

        let mut state = self.state.lock().map_err(|_| VisibilityError::Poisoned)?;
        state.ready.insert(csn);
        let frontier = self.advance_locked(&mut state)?;
        drop(state);
        self.advanced.notify_all();
        Ok(CommitSeq::new(frontier))
    }

    /// Consume contiguous ready CSNs. Callers hold the frontier state lock so a
    /// waiter cannot observe an advanced frontier without a following wakeup.
    fn advance_locked(&self, state: &mut FrontierState) -> Result<u64, VisibilityError> {
        let mut frontier = self.visible.load(Ordering::Acquire);
        loop {
            let next = frontier
                .checked_add(1)
                .ok_or(VisibilityError::CommitSeqExhausted)?;
            if !state.ready.remove(&CommitSeq::new(next)) {
                break;
            }
            frontier = next;
        }
        self.visible.store(frontier, Ordering::Release);
        Ok(frontier)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VisibilityError {
    #[error(transparent)]
    Status(#[from] StatusTableError),
    #[error("visibility frontier lock is poisoned")]
    Poisoned,
    #[error("commit sequence space is exhausted")]
    CommitSeqExhausted,
    #[error("commit {requested:?} is already at or below visible frontier {visible:?}")]
    AlreadyVisible {
        visible: CommitSeq,
        requested: CommitSeq,
    },
    #[error("commit {csn:?} cannot become visible at frontier {visible:?} without recovery")]
    RecoveryRequired { visible: CommitSeq, csn: CommitSeq },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{RecordOwner, RecordVisibility, TransactionStatus};

    #[test]
    fn out_of_order_status_publication_cannot_advance_snapshot_across_a_gap() {
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let first = TxnId::new(1);
        let second = TxnId::new(2);
        statuses.begin(first).expect("first begins");
        statuses.begin(second).expect("second begins");

        assert_eq!(
            frontier
                .publish_commit(&statuses, second, CommitSeq::new(2))
                .expect("second status publishes"),
            CommitSeq::new(0)
        );
        assert_eq!(frontier.snapshot(), CommitSeq::new(0));
        assert_eq!(
            statuses.status(second).expect("status"),
            Some(TransactionStatus::Committed(CommitSeq::new(2)))
        );
        assert_eq!(
            statuses
                .visibility(RecordOwner::Transaction(second), None, frontier.snapshot())
                .expect("visibility"),
            RecordVisibility::NewerCommit(CommitSeq::new(2))
        );

        assert_eq!(
            frontier
                .publish_commit(&statuses, first, CommitSeq::new(1))
                .expect("first status publishes"),
            CommitSeq::new(2)
        );
        assert_eq!(frontier.snapshot(), CommitSeq::new(2));
    }

    #[test]
    fn older_snapshots_keep_following_undo_after_status_publication() {
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let txn = TxnId::new(7);
        statuses.begin(txn).expect("transaction begins");
        let old_snapshot = frontier.snapshot();

        frontier
            .publish_commit(&statuses, txn, CommitSeq::new(1))
            .expect("commit publishes");
        assert_eq!(frontier.snapshot(), CommitSeq::new(1));
        assert_eq!(
            statuses
                .visibility(RecordOwner::Transaction(txn), None, old_snapshot)
                .expect("old visibility"),
            RecordVisibility::NewerCommit(CommitSeq::new(1))
        );
        assert_eq!(
            statuses
                .visibility(RecordOwner::Transaction(txn), None, frontier.snapshot(),)
                .expect("new visibility"),
            RecordVisibility::Visible
        );
    }

    #[test]
    fn recovered_commits_rebuild_the_same_contiguous_frontier() {
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        frontier
            .publish_recovered(&statuses, TxnId::new(12), CommitSeq::new(2))
            .expect("second recovers");
        assert_eq!(frontier.snapshot(), CommitSeq::new(0));
        frontier
            .publish_recovered(&statuses, TxnId::new(11), CommitSeq::new(1))
            .expect("first recovers");
        assert_eq!(frontier.snapshot(), CommitSeq::new(2));
    }

    #[test]
    fn readiness_publication_alone_does_not_complete_a_later_commit() {
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let first = TxnId::new(21);
        let second = TxnId::new(22);
        statuses.begin(first).expect("first begins");
        statuses.begin(second).expect("second begins");

        // The later transaction is published ready, but the earlier admitted
        // CSN has not reached publication, so completion must be withheld.
        assert_eq!(
            frontier
                .publish_commit(&statuses, second, CommitSeq::new(2))
                .expect("second becomes ready"),
            CommitSeq::new(0)
        );
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| frontier.wait_visible(CommitSeq::new(2)));
            assert_eq!(
                frontier
                    .publish_commit(&statuses, first, CommitSeq::new(1))
                    .expect("first becomes ready"),
                CommitSeq::new(2)
            );
            assert_eq!(
                waiter
                    .join()
                    .expect("waiter joins")
                    .expect("closing the gap must release the waiter"),
                CommitSeq::new(2)
            );
        });
    }

    #[test]
    fn require_recovery_wakes_waiters_instead_of_blocking_forever() {
        let frontier = VisibilityFrontier::default();
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| frontier.wait_visible(CommitSeq::new(4)));
            frontier.require_recovery();
            assert!(matches!(
                waiter.join().expect("waiter joins"),
                Err(VisibilityError::RecoveryRequired { visible, csn })
                    if visible == CommitSeq::new(0) && csn == CommitSeq::new(4)
            ));
        });
    }
}
