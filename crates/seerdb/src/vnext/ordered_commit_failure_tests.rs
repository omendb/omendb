//! Continuing-process unwind qualification through the real ordered coordinator.

use super::*;
use crate::vnext::{
    CommitSeq, DurableLog, LogDevice, Lsn, ObjectAuthority, OrderedMvccReader, PageIo, PageKey,
    RuntimeAdmissionState, StorageObjectDescriptor, StoreDirectory,
};
use durable_fs::SyncClass;
use std::collections::HashMap;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, mpsc};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq)]
enum FailurePoint {
    Append,
    Sync,
    Install,
    PauseSync,
}

#[derive(Default)]
struct PanicBoundary {
    entered: Mutex<bool>,
    changed: Condvar,
    released: Mutex<bool>,
    release_changed: Condvar,
}

impl PanicBoundary {
    fn wait_here(&self) {
        *self.entered.lock().expect("entered lock") = true;
        self.changed.notify_all();
        let mut released = self.released.lock().expect("release lock");
        while !*released {
            released = self.release_changed.wait(released).expect("release wait");
        }
    }

    fn panic_here(&self) -> ! {
        self.wait_here();
        panic!("injected commit unwind");
    }

    fn wait_until_entered(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut entered = self.entered.lock().expect("entered lock");
        while !*entered {
            let now = Instant::now();
            assert!(now < deadline, "commit never reached panic boundary");
            entered = self
                .changed
                .wait_timeout(entered, deadline - now)
                .expect("entered wait")
                .0;
        }
    }

    fn release(&self) {
        *self.released.lock().expect("release lock") = true;
        self.release_changed.notify_all();
    }
}

struct PanicPageIo {
    pages: RwLock<HashMap<PageKey, Vec<u8>>>,
    panic_on_write: AtomicBool,
    boundary: Arc<PanicBoundary>,
}

impl PageIo for PanicPageIo {
    fn read_page(&self, key: PageKey, destination: &mut [u8]) -> io::Result<()> {
        let pages = self.pages.read().expect("page map");
        let page = pages
            .get(&key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing page"))?;
        destination.copy_from_slice(page);
        Ok(())
    }

    fn write_page(&self, key: PageKey, bytes: &[u8]) -> io::Result<()> {
        if self.panic_on_write.load(Ordering::Acquire) {
            self.boundary.panic_here();
        }
        self.pages
            .write()
            .expect("page map")
            .insert(key, bytes.to_vec());
        Ok(())
    }
}

struct PanicLogDevice {
    bytes: Mutex<Vec<u8>>,
    failure: FailurePoint,
    boundary: Arc<PanicBoundary>,
    pages: Arc<PanicPageIo>,
}

impl LogDevice for PanicLogDevice {
    fn append(&self, bytes: &[u8]) -> io::Result<Lsn> {
        let end = {
            let mut log = self.bytes.lock().expect("log bytes");
            log.extend_from_slice(bytes);
            log.len() as u64
        };
        if self.failure == FailurePoint::Append {
            // Bytes may survive, but the caller never receives an append ticket.
            self.boundary.panic_here();
        }
        Ok(Lsn::from_wal_position(0, end).expect("LSN"))
    }

    fn sync_through(&self, _lsn: Lsn) -> io::Result<()> {
        match self.failure {
            FailurePoint::Sync => self.boundary.panic_here(),
            FailurePoint::Install => self.pages.panic_on_write.store(true, Ordering::Release),
            FailurePoint::PauseSync => self.boundary.wait_here(),
            FailurePoint::Append => unreachable!("append panics first"),
        }
        Ok(())
    }
}

/// Scoped threads must be released even if a qualification assertion fails.
struct UnblockOnDrop<'a> {
    boundary: &'a PanicBoundary,
    frontier: &'a VisibilityFrontier,
    admission: &'a RuntimeAdmission,
}

impl Drop for UnblockOnDrop<'_> {
    fn drop(&mut self) {
        self.boundary.release();
        self.admission.require_recovery();
        self.frontier.require_recovery();
    }
}

#[test]
fn clean_append_refusal_disarms_responsibility_without_fencing() {
    let boundary = Arc::new(PanicBoundary::default());
    boundary.release();
    let pages = Arc::new(PanicPageIo {
        pages: RwLock::new(HashMap::new()),
        panic_on_write: AtomicBool::new(false),
        boundary: Arc::clone(&boundary),
    });
    let buffer = BufferPool::new(4, 512, pages.clone()).expect("buffer");
    let directory = tempfile::tempdir().expect("tempdir");
    let store = StoreDirectory::create(directory.path()).expect("store");
    let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
    let device = Arc::new(PanicLogDevice {
        bytes: Mutex::new(Vec::new()),
        failure: FailurePoint::Append,
        boundary,
        pages,
    });
    // Exhaustion is known before device entry. Any accidental append panics.
    let appender = CommitAppender::new(
        Arc::new(DurableLog::new(device.clone())),
        CommitSeq::new(u64::MAX),
    );
    let statuses = TransactionStatusTable::new();
    let frontier = VisibilityFrontier::default();
    let intents = WriteIntentTable::new();
    let admission = RuntimeAdmission::new();
    let coordinator =
        OrderedCommitCoordinator::new(&admission, &appender, &statuses, &frontier, &intents, &undo);
    let mut transaction = coordinator.begin_write(TxnId::new(1)).expect("begin");
    assert!(matches!(
        coordinator.commit(&mut transaction, &buffer, &[]),
        Err(OrderedCommitError::Append(
            CommitAppendError::CommitSeqExhausted
        ))
    ));
    assert_eq!(transaction.phase(), TransactionPhase::Aborted);
    assert_eq!(
        statuses.status(transaction.id()).expect("status"),
        Some(TransactionStatus::Aborted)
    );
    assert!(device.bytes.lock().expect("bytes").is_empty());
    assert_eq!(admission.active_commits().expect("active"), 0);
    assert!(!coordinator.is_fenced());
    assert!(admission.ensure_read_admission().is_ok());
    assert!(admission.begin_checkpoint().is_ok());
}

#[test]
fn admitted_commit_cannot_ack_between_runtime_and_frontier_fences() {
    let boundary = Arc::new(PanicBoundary::default());
    let pages = Arc::new(PanicPageIo {
        pages: RwLock::new(HashMap::new()),
        panic_on_write: AtomicBool::new(false),
        boundary: Arc::clone(&boundary),
    });
    let buffer = BufferPool::new(4, 512, pages.clone()).expect("buffer");
    let descriptor =
        StorageObjectDescriptor::new(StorageObjectId::new(1), ObjectAuthority::Authoritative);
    let tree = BTreeObject::create(descriptor, &buffer).expect("tree");
    let directory = tempfile::tempdir().expect("tempdir");
    let store = StoreDirectory::create(directory.path()).expect("store");
    let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
    let device = Arc::new(PanicLogDevice {
        bytes: Mutex::new(Vec::new()),
        failure: FailurePoint::PauseSync,
        boundary: Arc::clone(&boundary),
        pages,
    });
    let appender = CommitAppender::new(Arc::new(DurableLog::new(device)), CommitSeq::new(0));
    let statuses = TransactionStatusTable::new();
    let frontier = VisibilityFrontier::default();
    let intents = WriteIntentTable::new();
    let admission = RuntimeAdmission::new();
    let coordinator =
        OrderedCommitCoordinator::new(&admission, &appender, &statuses, &frontier, &intents, &undo);
    let mut transaction = coordinator.begin_write(TxnId::new(1)).expect("begin");
    transaction
        .stage_ordered_put(descriptor, b"key".to_vec(), b"value".to_vec())
        .expect("stage");
    let (commit_tx, commit_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let _cleanup = UnblockOnDrop {
            boundary: &boundary,
            frontier: &frontier,
            admission: &admission,
        };
        scope.spawn(|| {
            commit_tx
                .send(coordinator.commit(&mut transaction, &buffer, &[&tree]))
                .expect("result");
        });
        boundary.wait_until_entered();
        // Model a concurrent fencer paused between closing shared read/write
        // admission and notifying the frontier. Completion must order against
        // the shared gate, not depend on notification reaching the frontier.
        admission.require_recovery();
        boundary.release();
        assert!(matches!(
            commit_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("commit returns"),
            Err(OrderedCommitError::Admission(
                RuntimeAdmissionError::RecoveryRequired
            ))
        ));
    });
    assert_eq!(transaction.phase(), TransactionPhase::RecoveryRequired);
    assert_eq!(frontier.snapshot(), CommitSeq::new(0));
}

#[test]
fn caught_commit_unwind_fences_before_checkpoint_membership_is_released() {
    for failure in [
        FailurePoint::Append,
        FailurePoint::Sync,
        FailurePoint::Install,
    ] {
        let boundary = Arc::new(PanicBoundary::default());
        let pages = Arc::new(PanicPageIo {
            pages: RwLock::new(HashMap::new()),
            panic_on_write: AtomicBool::new(false),
            boundary: Arc::clone(&boundary),
        });
        let buffer = BufferPool::new(4, 512, pages.clone()).expect("buffer");
        let descriptor =
            StorageObjectDescriptor::new(StorageObjectId::new(1), ObjectAuthority::Authoritative);
        let tree = BTreeObject::create(descriptor, &buffer).expect("tree");
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let undo = UndoStore::create(&store, SyncClass::KernelBarrier).expect("undo");
        let device = Arc::new(PanicLogDevice {
            bytes: Mutex::new(Vec::new()),
            failure,
            boundary: Arc::clone(&boundary),
            pages,
        });
        let appender = CommitAppender::new(Arc::new(DurableLog::new(device)), CommitSeq::new(0));
        let statuses = TransactionStatusTable::new();
        let frontier = VisibilityFrontier::default();
        let intents = WriteIntentTable::new();
        let admission = RuntimeAdmission::new();
        let coordinator = OrderedCommitCoordinator::new(
            &admission, &appender, &statuses, &frontier, &intents, &undo,
        );
        let mut transaction = coordinator.begin_write(TxnId::new(1)).expect("begin");
        // Enough leaves to force dirty-page eviction during structural installation,
        // after WAL sync has armed the physical I/O panic. Preparation fits in RAM.
        for key in 0u32..64 {
            transaction
                .stage_ordered_put(descriptor, key.to_be_bytes().to_vec(), vec![7; 24])
                .expect("stage");
        }
        let (commit_tx, commit_rx) = mpsc::channel();
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel();
        let (visible_tx, visible_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let _cleanup = UnblockOnDrop {
                boundary: &boundary,
                frontier: &frontier,
                admission: &admission,
            };
            scope.spawn(|| {
                commit_tx
                    .send(catch_unwind(AssertUnwindSafe(|| {
                        coordinator.commit(&mut transaction, &buffer, &[&tree])
                    })))
                    .expect("commit result");
            });
            boundary.wait_until_entered();
            assert_eq!(admission.active_commits().expect("active"), 1);
            scope.spawn(|| {
                checkpoint_tx
                    .send(admission.begin_checkpoint().map(|_| ()))
                    .expect("checkpoint result");
            });
            scope.spawn(|| {
                visible_tx
                    .send(frontier.wait_visible(CommitSeq::new(1)))
                    .expect("visibility result");
            });
            let deadline = Instant::now() + Duration::from_secs(10);
            while admission.state().expect("state") != RuntimeAdmissionState::Checkpointing {
                assert!(
                    Instant::now() < deadline,
                    "checkpoint never started draining"
                );
                std::thread::yield_now();
            }
            boundary.release();
            assert!(
                commit_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("unwind returns")
                    .is_err()
            );
            let checkpoint_result = checkpoint_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("checkpoint wakes");
            let visibility_result = visible_rx.recv_timeout(Duration::from_secs(10));
            assert!(
                matches!(
                    checkpoint_result,
                    Err(RuntimeAdmissionError::RecoveryRequired)
                ),
                "{failure:?}: unsafe checkpoint capture was admitted"
            );
            assert!(
                matches!(
                    visibility_result,
                    Ok(Err(VisibilityError::RecoveryRequired { .. }))
                ),
                "{failure:?}: visibility waiter was not woken by unwind"
            );
        });
        assert_eq!(transaction.phase(), TransactionPhase::RecoveryRequired);
        assert_eq!(admission.active_commits().expect("active"), 0);
        assert!(coordinator.is_fenced());
        assert!(admission.ensure_read_admission().is_err());
        assert!(coordinator.begin_write(TxnId::new(2)).is_err());
        assert!(coordinator.abort(&mut transaction).is_err());
        // Intents are released, but only after the shared failure gate is terminal.
        let mut contender = Transaction::new(TxnId::new(2), CommitSeq::new(0));
        contender
            .stage_ordered_put(descriptor, 0u32.to_be_bytes().to_vec(), vec![8])
            .expect("contender stages");
        intents
            .try_acquire(contender.id(), &contender.final_effects().expect("effects"))
            .expect("intents released");
        let reader = OrderedMvccReader::new(&admission, &statuses, &undo);
        assert!(matches!(
            reader.lookup(&tree, &buffer, b"absent", None, frontier.snapshot()),
            Err(super::super::OrderedMvccReadError::Admission(
                RuntimeAdmissionError::RecoveryRequired
            ))
        ));
        let sibling = OrderedCommitCoordinator::new(
            &admission, &appender, &statuses, &frontier, &intents, &undo,
        );
        assert!(sibling.begin_write(TxnId::new(3)).is_err());
    }
}
