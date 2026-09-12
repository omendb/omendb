//! Contiguous logical visibility frontier for vNext MVCC snapshots.
//!
//! Transaction status is published before a CSN can enter the visible frontier.
//! Commits may finish durability/status work out of order, but new snapshots only
//! observe the highest contiguous committed CSN. Existing older snapshots remain
//! correct because a newly committed owner resolves as `NewerCommit` and follows
//! its undo chain.

use super::{CommitSeq, StatusTableError, TransactionStatus, TransactionStatusTable, TxnId};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Process-local visibility frontier rebuilt from durable commit decisions.
pub struct VisibilityFrontier {
    visible: AtomicU64,
    ready: Mutex<BTreeSet<CommitSeq>>,
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
            ready: Mutex::new(BTreeSet::new()),
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
    pub fn publish_commit(
        &self,
        statuses: &TransactionStatusTable,
        txn: TxnId,
        csn: CommitSeq,
    ) -> Result<CommitSeq, VisibilityError> {
        statuses.commit(txn, csn)?;
        self.mark_ready(csn)
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

        let mut ready = self.ready.lock().map_err(|_| VisibilityError::Poisoned)?;
        ready.insert(csn);

        let mut frontier = self.visible.load(Ordering::Acquire);
        loop {
            let next = frontier
                .checked_add(1)
                .ok_or(VisibilityError::CommitSeqExhausted)?;
            if !ready.remove(&CommitSeq::new(next)) {
                break;
            }
            frontier = next;
        }
        self.visible.store(frontier, Ordering::Release);
        Ok(CommitSeq::new(frontier))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{RecordOwner, RecordVisibility};

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
                .visibility(RecordOwner::Transaction(txn), None, frontier.snapshot())
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
}
