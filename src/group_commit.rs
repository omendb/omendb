//! Group commit pipeline over the coalesced Seer publication boundary.
//!
//! Concurrent workers queue prepared transactions; the batch leader drains the
//! queue and publishes all same-snapshot, write-disjoint transactions through
//! one durable kernel publication ([`RelationalDatabase::commit_coalesced`]).
//! The kernel performs one WAL append and one sync for the whole batch.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{
    CommitId, DbError, OperationControl, RelationalDatabase, RelationalDatabaseTransaction, Result,
};

/// Configuration for the group commit pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupCommitConfig {
    /// Maximum number of transactions coalesced into one durable publication.
    pub max_batch_size: usize,
    /// How long the leader waits for a forming wave to assemble before
    /// draining. Short delays raise batch sizes and align same-snapshot
    /// workers; the delay runs once per batch, not per transaction.
    pub max_batch_delay_micros: u64,
    /// How long a submitter waits for its outcome before giving up. A
    /// still-queued request is removed and the caller receives a
    /// definitive `DeadlineExceeded`; a selected request is always awaited
    /// to its durable outcome.
    pub wait_timeout: Duration,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 128,
            max_batch_delay_micros: 1_000,
            wait_timeout: Duration::from_secs(30),
        }
    }
}

/// Statistics observed by the group commit pipeline.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GroupCommitMetrics {
    /// Transactions published successfully through the pipeline.
    pub total_transactions_committed: u64,
    /// Durable publications issued (one publication may carry many transactions).
    pub total_publications: u64,
    /// Largest number of transactions observed in one publication.
    pub max_observed_batch_size: usize,
    /// Transactions rejected by publication validation or write conflicts.
    pub total_rejected: u64,
}

struct PreparedCommitRequest {
    id: u64,
    transaction: RelationalDatabaseTransaction,
    sender: std::sync::mpsc::SyncSender<Result<CommitId>>,
}

/// Coordinator that coalesces concurrently prepared transactions into one
/// durable publication per batch.
pub struct GroupCommitPipeline {
    config: GroupCommitConfig,
    queue: Arc<Mutex<Vec<PreparedCommitRequest>>>,
    next_request_id: Arc<AtomicU64>,
    is_flushing: Arc<AtomicBool>,
    total_committed: Arc<AtomicU64>,
    total_publications: Arc<AtomicU64>,
    max_observed_batch_size: Arc<AtomicUsize>,
    total_rejected: Arc<AtomicU64>,
}

impl GroupCommitPipeline {
    /// Create a new group commit pipeline with the given configuration.
    #[must_use]
    pub fn new(config: GroupCommitConfig) -> Self {
        Self {
            config,
            queue: Arc::new(Mutex::new(Vec::new())),
            next_request_id: Arc::new(AtomicU64::new(0)),
            is_flushing: Arc::new(AtomicBool::new(false)),
            total_committed: Arc::new(AtomicU64::new(0)),
            total_publications: Arc::new(AtomicU64::new(0)),
            max_observed_batch_size: Arc::new(AtomicUsize::new(0)),
            total_rejected: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Return current group commit metrics.
    #[must_use]
    pub fn metrics(&self) -> GroupCommitMetrics {
        GroupCommitMetrics {
            total_transactions_committed: self.total_committed.load(Ordering::Relaxed),
            total_publications: self.total_publications.load(Ordering::Relaxed),
            max_observed_batch_size: self.max_observed_batch_size.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
        }
    }

    /// Number of transactions queued or publishing right now.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.queue
            .lock()
            .map(|queue| queue.len() + usize::from(self.is_flushing.load(Ordering::SeqCst)))
            .unwrap_or(usize::MAX)
    }

    /// Submit a prepared transaction and wait for its durable outcome.
    ///
    /// The first submitter becomes the batch leader: it drains the queue and
    /// publishes the coalesced batch while other submitters wait on their
    /// per-request channel.
    ///
    /// # Errors
    ///
    /// Returns the transaction's own publication error. If the request is
    /// still queued when the wait bound expires, it is removed from the
    /// queue and [`DbError::DeadlineExceeded`] is returned: the transaction
    /// was never selected for publication and nothing durable was
    /// attempted, so the outcome is definitive. Once the leader has selected
    /// a request, the caller waits until that publication completes — the
    /// outcome is never ambiguous.
    pub fn submit_and_await(
        &self,
        database: &std::sync::RwLock<Option<RelationalDatabase>>,
        control: &OperationControl,
        transaction: RelationalDatabaseTransaction,
    ) -> Result<CommitId> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let is_leader = {
            let mut queue = self
                .queue
                .lock()
                .map_err(|_| DbError::InvalidState("group commit queue poisoned".to_owned()))?;
            queue.push(PreparedCommitRequest {
                id: request_id,
                transaction,
                sender,
            });
            if !self.is_flushing.load(Ordering::SeqCst) {
                self.is_flushing.store(true, Ordering::SeqCst);
                true
            } else {
                false
            }
        };

        if is_leader {
            // Let the current wave assemble: workers released by a drain
            // wait begin together, and one same-snapshot batch is worth
            // more than several single-transaction publications.
            if self.config.max_batch_delay_micros > 0 {
                std::thread::sleep(Duration::from_micros(self.config.max_batch_delay_micros));
            }
            self.flush_loop(database, control);
        }

        loop {
            match receiver.recv_timeout(self.config.wait_timeout) {
                Ok(result) => return result,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(DbError::SessionClosed);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    let mut queue = self.queue.lock().map_err(|_| {
                        DbError::InvalidState("group commit queue poisoned".to_owned())
                    })?;
                    if let Some(position) =
                        queue.iter().position(|request| request.id == request_id)
                    {
                        // Still queued: the leader never selected this
                        // request, so removing it is definitive — nothing
                        // durable was attempted and dropping the transaction
                        // aborts it.
                        queue.remove(position);
                        drop(queue);
                        return Err(DbError::DeadlineExceeded);
                    }
                    // Selected and publishing: the outcome is bounded by the
                    // in-flight publication, so keep waiting for it.
                }
            }
        }
    }

    fn flush_loop(
        &self,
        database_lock: &std::sync::RwLock<Option<RelationalDatabase>>,
        control: &OperationControl,
    ) {
        loop {
            let batch: Vec<PreparedCommitRequest> = {
                let mut queue = match self.queue.lock() {
                    Ok(queue) => queue,
                    Err(_) => {
                        self.is_flushing.store(false, Ordering::SeqCst);
                        break;
                    }
                };
                if queue.is_empty() {
                    self.is_flushing.store(false, Ordering::SeqCst);
                    break;
                }
                let count = queue.len().min(self.config.max_batch_size.max(1));
                queue.drain(..count).collect()
            };

            let (transactions, senders): (Vec<_>, Vec<_>) = batch
                .into_iter()
                .map(|request| (request.transaction, request.sender))
                .unzip();

            // Shared guard: publication serializes inside the kernel's DB
            // mutex, so holding the write guard here would only block
            // preparation behind the publisher's syncs — the exact
            // serialization the lane split removes.
            let database_guard = match database_lock.read() {
                Ok(guard) => guard,
                Err(_) => {
                    for sender in senders {
                        let _ = sender.send(Err(DbError::SessionClosed));
                    }
                    self.is_flushing.store(false, Ordering::SeqCst);
                    break;
                }
            };
            let outcomes = database_guard
                .as_ref()
                .map(|database| database.commit_coalesced(transactions));
            match outcomes {
                Some(outcomes) => {
                    let senders_len = senders.len();
                    let committed = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
                    for (sender, outcome) in senders.into_iter().zip(outcomes) {
                        let _ = sender.send(outcome);
                    }
                    self.total_committed
                        .fetch_add(committed as u64, Ordering::Relaxed);
                    self.total_rejected
                        .fetch_add((senders_len - committed) as u64, Ordering::Relaxed);
                    if committed > 0 {
                        // Only a batch that published at least one durable
                        // transaction counts as a publication.
                        self.total_publications.fetch_add(1, Ordering::Relaxed);
                        self.max_observed_batch_size
                            .fetch_max(committed, Ordering::Relaxed);
                    }
                }
                None => {
                    for sender in senders {
                        let _ = sender.send(Err(DbError::SessionClosed));
                    }
                }
            }
            drop(database_guard);

            if control.check().is_err() {
                self.is_flushing.store(false, Ordering::SeqCst);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatabaseConfig, RelationalBackendConfig};
    use tempfile::tempdir;

    #[test]
    fn group_commit_queued_timeout_is_definitive() {
        use crate::{
            ColumnDefinition, ColumnId, ColumnType, RelationalBackendConfig, TableDefinition,
        };
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let db = RelationalDatabase::open(RelationalBackendConfig::Seer(
            crate::SeerKernelConfig::new(dir.path().join("seerdb")),
        ))
        .unwrap();
        let db_lock = std::sync::Arc::new(std::sync::RwLock::new(Some(db)));
        {
            let mut guard = db_lock.write().unwrap();
            guard
                .as_mut()
                .unwrap()
                .create_table(TableDefinition {
                    id: crate::TableId(901),
                    name: "t".to_owned(),
                    columns: vec![ColumnDefinition {
                        id: ColumnId(1),
                        name: "v".to_owned(),
                        data_type: ColumnType::U64,
                        nullable: false,
                    }],
                })
                .unwrap();
        }

        let config = GroupCommitConfig {
            wait_timeout: Duration::from_millis(100),
            ..GroupCommitConfig::default()
        };
        let pipeline = std::sync::Arc::new(GroupCommitPipeline::new(config));
        let control = OperationControl::new();

        // Begin both transactions before taking the exclusive guard; the
        // submit path itself does not need database access.
        let leader_transaction = {
            let guard = db_lock.read().unwrap();
            guard
                .as_ref()
                .unwrap()
                .begin_with_control(&control)
                .unwrap()
        };
        let follower_transaction = {
            let guard = db_lock.read().unwrap();
            guard
                .as_ref()
                .unwrap()
                .begin_with_control(&control)
                .unwrap()
        };

        // Hold the exclusive guard so the leader blocks after selecting its
        // request but before publishing it.
        let held_guard = db_lock.write().unwrap();

        let leader = {
            let db_lock = std::sync::Arc::clone(&db_lock);
            let control = control.clone();
            let pipeline = std::sync::Arc::clone(&pipeline);
            std::thread::spawn(move || {
                pipeline.submit_and_await(&db_lock, &control, leader_transaction)
            })
        };
        // Let the leader drain its request and block on the held guard.
        std::thread::sleep(Duration::from_millis(20));

        let follower = {
            let control = control.clone();
            let pipeline = std::sync::Arc::clone(&pipeline);
            let db_lock = std::sync::Arc::clone(&db_lock);
            std::thread::spawn(move || {
                pipeline.submit_and_await(&db_lock, &control, follower_transaction)
            })
        };
        // Follower queues behind the blocked leader, then hits the 100ms
        // wait bound while still queued.
        std::thread::sleep(Duration::from_millis(200));

        drop(held_guard);
        let leader_result = leader.join().unwrap();
        assert!(
            leader_result.is_ok(),
            "leader must publish: {leader_result:?}"
        );
        let follower_result = follower.join().unwrap();
        assert!(
            matches!(follower_result, Err(DbError::DeadlineExceeded)),
            "queued timeout must be definitive, got {follower_result:?}"
        );
        let metrics = pipeline.metrics();
        assert_eq!(metrics.total_transactions_committed, 1);
    }

    #[test]
    fn group_commit_pipeline_publishes_read_only_transaction() {
        let dir = tempdir().unwrap();
        let config = RelationalBackendConfig::Temporary(DatabaseConfig {
            directory: dir.path().to_owned(),
        });
        let db = RelationalDatabase::open(config).unwrap();
        let db_lock = std::sync::RwLock::new(Some(db));

        let pipeline = GroupCommitPipeline::new(GroupCommitConfig::default());
        let control = OperationControl::new();

        let transaction = {
            let guard = db_lock.read().unwrap();
            guard
                .as_ref()
                .unwrap()
                .begin_with_control(&control)
                .unwrap()
        };

        let result = pipeline.submit_and_await(&db_lock, &control, transaction);
        assert!(result.is_ok());
    }
}
