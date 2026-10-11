use super::*;
use crate::vnext::{PageId, StorageObjectId};
use std::collections::HashMap;
use std::sync::{Barrier, Condvar, Mutex};

const TEST_PAGE_SIZE: usize = 64;

fn key(page: u64) -> PageKey {
    PageKey::new(StorageObjectId::new(1), PageId::new(page))
}

#[derive(Default)]
struct MemoryPageIo {
    pages: RwLock<HashMap<PageKey, Vec<u8>>>,
    reads: AtomicU64,
    writes: AtomicU64,
}

impl MemoryPageIo {
    fn insert(&self, key: PageKey, value: u8) {
        self.pages
            .write()
            .expect("memory page map writable")
            .insert(key, vec![value; TEST_PAGE_SIZE]);
    }

    fn first_byte(&self, key: PageKey) -> u8 {
        self.pages
            .read()
            .expect("memory page map readable")
            .get(&key)
            .expect("page exists")[0]
    }
}

impl PageIo for MemoryPageIo {
    fn read_page(&self, key: PageKey, destination: &mut [u8]) -> io::Result<()> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let pages = self
            .pages
            .read()
            .map_err(|_| io::Error::other("memory page map poisoned"))?;
        let page = pages
            .get(&key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing page"))?;
        if page.len() != destination.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "page size mismatch",
            ));
        }
        destination.copy_from_slice(page);
        Ok(())
    }

    fn write_page(&self, key: PageKey, source: &[u8]) -> io::Result<()> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.pages
            .write()
            .map_err(|_| io::Error::other("memory page map poisoned"))?
            .insert(key, source.to_vec());
        Ok(())
    }
}

#[test]
fn cache_hit_does_not_repeat_io() {
    let device = Arc::new(MemoryPageIo::default());
    device.insert(key(1), 7);
    let pool = BufferPool::new(2, TEST_PAGE_SIZE, device.clone()).expect("pool creates");

    {
        let guard = pool.pin(key(1)).expect("first pin loads");
        assert_eq!(guard.read().expect("read latch")[0], 7);
    }
    {
        let guard = pool.pin(key(1)).expect("second pin hits");
        assert_eq!(guard.read().expect("read latch")[0], 7);
    }

    assert_eq!(device.reads.load(Ordering::Relaxed), 1);
    let stats = pool.stats().expect("stats");
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.translation_entries, 1);
}

#[test]
fn concurrent_page_writers_wait_inside_guard() {
    let device = Arc::new(MemoryPageIo::default());
    device.insert(key(1), 0);
    let pool = Arc::new(BufferPool::new(2, TEST_PAGE_SIZE, device).expect("pool creates"));
    {
        let guard = pool.pin(key(1)).expect("page loads");
        assert_eq!(guard.read().expect("read latch")[0], 0);
    }
    let start = Arc::new(Barrier::new(8));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let pool = Arc::clone(&pool);
        let start = Arc::clone(&start);
        workers.push(std::thread::spawn(move || {
            let guard = pool.pin(key(1)).expect("page pins");
            start.wait();
            let mut bytes = guard.write().expect("writer waits internally");
            bytes[0] = bytes[0].checked_add(1).expect("test counter fits");
        }));
    }
    for worker in workers {
        worker.join().expect("writer completes");
    }
    let guard = pool.pin(key(1)).expect("page remains resident");
    assert_eq!(guard.read().expect("read latch")[0], 8);
}

#[test]
fn newly_created_page_is_dirty_without_a_device_read() {
    let device = Arc::new(MemoryPageIo::default());
    let pool = BufferPool::new(2, TEST_PAGE_SIZE, device.clone()).expect("pool creates");
    let image = vec![13u8; TEST_PAGE_SIZE];

    let guard = pool.create_page(key(7), &image).expect("new page installs");
    assert_eq!(guard.read().expect("read latch")[0], 13);
    drop(guard);

    let stats = pool.stats().expect("stats");
    assert_eq!(stats.new_pages, 1);
    assert_eq!(stats.loads, 0);
    assert_eq!(stats.bytes_read, 0);
    assert_eq!(stats.dirty_frames, 1);
    assert_eq!(device.reads.load(Ordering::Relaxed), 0);
    assert!(pool.flush_page(key(7)).expect("new page flushes"));
    assert_eq!(device.first_byte(key(7)), 13);
}

#[test]
fn dirty_eviction_writes_before_reusing_single_frame() {
    let device = Arc::new(MemoryPageIo::default());
    device.insert(key(1), 1);
    device.insert(key(2), 2);
    let pool = BufferPool::new(1, TEST_PAGE_SIZE, device.clone()).expect("pool creates");

    {
        let guard = pool.pin(key(1)).expect("page one loads");
        let mut bytes = guard.write().expect("page is writable");
        bytes[0] = 9;
    }
    {
        let guard = pool.pin(key(2)).expect("page two evicts page one");
        assert_eq!(guard.read().expect("read latch")[0], 2);
    }

    assert_eq!(device.first_byte(key(1)), 9);
    assert_eq!(device.writes.load(Ordering::Relaxed), 1);
    let stats = pool.stats().expect("stats");
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.writebacks, 1);
    assert_eq!(stats.dirty_frames, 0);
}

#[derive(Clone, Copy)]
enum PausedReadOutcome {
    Success,
    Error,
    Panic,
}

struct PausedReadPageIo {
    inner: MemoryPageIo,
    key: PageKey,
    pause_next: AtomicBool,
    gate: Mutex<(bool, bool)>,
    changed: Condvar,
    outcome: PausedReadOutcome,
}

impl PausedReadPageIo {
    fn new(key: PageKey) -> Self {
        Self {
            inner: MemoryPageIo::default(),
            key,
            pause_next: AtomicBool::new(true),
            gate: Mutex::new((false, false)),
            changed: Condvar::new(),
            outcome: PausedReadOutcome::Success,
        }
    }

    fn wait_until_read(&self) {
        let gate = self.gate.lock().expect("read gate");
        let (gate, timeout) = self
            .changed
            .wait_timeout_while(gate, std::time::Duration::from_secs(10), |gate| !gate.0)
            .expect("read starts");
        assert!(gate.0 && !timeout.timed_out(), "read never started");
    }

    fn release(&self) {
        self.gate.lock().expect("read gate").1 = true;
        self.changed.notify_all();
    }
}

impl PageIo for PausedReadPageIo {
    fn read_page(&self, key: PageKey, destination: &mut [u8]) -> io::Result<()> {
        self.inner.read_page(key, destination)?;
        if key == self.key && self.pause_next.swap(false, Ordering::AcqRel) {
            let mut gate = self
                .gate
                .lock()
                .map_err(|_| io::Error::other("read gate"))?;
            gate.0 = true;
            self.changed.notify_all();
            let (_gate, timeout) = self
                .changed
                .wait_timeout_while(gate, std::time::Duration::from_secs(20), |gate| !gate.1)
                .map_err(|_| io::Error::other("read gate"))?;
            if timeout.timed_out() {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "read not released"));
            }
            drop(_gate);
            match self.outcome {
                PausedReadOutcome::Success => {}
                PausedReadOutcome::Error => {
                    return Err(io::Error::other("injected read error"));
                }
                PausedReadOutcome::Panic => panic!("injected read panic"),
            }
        }
        Ok(())
    }

    fn write_page(&self, key: PageKey, source: &[u8]) -> io::Result<()> {
        self.inner.write_page(key, source)
    }
}

#[test]
fn delayed_load_cannot_resurrect_an_acknowledged_version_after_eviction() {
    use crate::vnext::{
        BTreeObject, CommitAppender, CommitSeq, DependencyCheckedPageIo, DurableLog, LogDevice,
        Lsn, MvccLookup, ObjectAuthority, OrderedCommitCoordinator, OrderedMvccReader,
        PageDependencyTable, RuntimeAdmission, StorageObjectDescriptor, StoreDirectory,
        TransactionStatusTable, TxnId, UndoStore, VisibilityFrontier, WriteIntentTable,
    };

    #[derive(Default)]
    struct MemoryLog(Mutex<Vec<u8>>);
    impl LogDevice for MemoryLog {
        fn append(&self, bytes: &[u8]) -> io::Result<Lsn> {
            let mut log = self.0.lock().map_err(|_| io::Error::other("log"))?;
            log.extend_from_slice(bytes);
            Ok(Lsn::new(log.len() as u64))
        }
        fn sync_through(&self, _: Lsn) -> io::Result<()> {
            Ok(())
        }
    }

    const PAGE_SIZE: usize = 384;
    let directory = tempfile::tempdir().expect("tempdir");
    let store = StoreDirectory::create(directory.path()).expect("store");
    let undo = UndoStore::create(&store, durable_fs::SyncClass::DeviceBarrier).expect("undo");
    let appender = CommitAppender::new(
        Arc::new(DurableLog::new(Arc::new(MemoryLog::default()))),
        CommitSeq::new(0),
    );
    let statuses = TransactionStatusTable::new();
    let frontier = VisibilityFrontier::default();
    let intents = WriteIntentTable::new();
    let dependencies = Arc::new(PageDependencyTable::new());
    let admission = RuntimeAdmission::new();
    let coordinator = OrderedCommitCoordinator::with_page_dependencies(
        &appender,
        &statuses,
        &frontier,
        &intents,
        &undo,
        &dependencies,
    )
    .with_runtime_admission(&admission);
    let device = Arc::new(PausedReadPageIo::new(key(0)));
    let filler = |page| PageKey::new(StorageObjectId::new(2), PageId::new(page));
    for page in 0..3 {
        device
            .inner
            .pages
            .write()
            .expect("pages")
            .insert(filler(page), vec![0; PAGE_SIZE]);
    }
    let pool = BufferPool::new(
        2,
        PAGE_SIZE,
        Arc::new(DependencyCheckedPageIo::new(
            device.clone(),
            Arc::clone(&dependencies),
        )),
    )
    .expect("pool");
    let descriptor =
        StorageObjectDescriptor::new(StorageObjectId::new(1), ObjectAuthority::Authoritative);
    let tree = BTreeObject::create(descriptor, &pool).expect("tree");
    let mut first = coordinator.begin_write(TxnId::new(1)).expect("begin");
    first
        .stage_ordered_put(descriptor, b"a".to_vec(), vec![1])
        .expect("stage");
    coordinator
        .commit(&mut first, &pool, &[&tree])
        .expect("first commit");
    drop(pool.pin(filler(0)).expect("filler"));
    drop(pool.pin(filler(1)).expect("evict root"));
    let reader = OrderedMvccReader::new(&statuses, &undo);

    std::thread::scope(|scope| {
        let delayed = scope.spawn(|| reader.lookup(&tree, &pool, b"a", None, CommitSeq::new(1)));
        device.wait_until_read();
        let (completed, completion) = std::sync::mpsc::channel();
        let (writer_coordinator, writer_pool, writer_tree) = (&coordinator, &pool, &tree);
        let writer = scope.spawn(move || {
            let mut second = writer_coordinator
                .begin_write(TxnId::new(2))
                .expect("begin");
            second
                .stage_ordered_put(descriptor, b"a".to_vec(), vec![9])
                .expect("stage");
            let result = writer_coordinator.commit(&mut second, writer_pool, &[writer_tree]);
            completed.send(result).expect("commit result");
        });
        // The old implementation allows ACK while the first read is still
        // outstanding. Evict its updated winner before releasing that read.
        // Single-flight loading instead makes the writer join the load.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let early_ack = loop {
            if let Ok(result) = completion.try_recv() {
                break Some(result);
            }
            if pool.stats().expect("stats").load_waits > 0 || std::time::Instant::now() >= deadline
            {
                break None;
            }
            std::thread::yield_now();
        };
        let acknowledged_early = early_ack.is_some();
        if acknowledged_early {
            drop(pool.pin(filler(2)).expect("evict acknowledged root"));
        }
        device.release();
        assert_eq!(
            delayed.join().expect("delayed reader").expect("lookup"),
            MvccLookup::Found(vec![1])
        );
        let ack = early_ack.unwrap_or_else(|| {
            completion
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("commit completes")
        });
        assert_eq!(ack.expect("second commit").csn, CommitSeq::new(2));
        writer.join().expect("writer");
        if !acknowledged_early {
            drop(pool.pin(filler(0)).expect("filler"));
            drop(pool.pin(filler(1)).expect("evict committed root"));
        }
        assert_eq!(
            reader
                .lookup(&tree, &pool, b"a", None, frontier.snapshot())
                .expect("post-ACK lookup"),
            MvccLookup::Found(vec![9])
        );
        assert!(!coordinator.is_fenced());
    });
}

#[test]
fn concurrent_misses_join_one_load_even_with_a_single_frame() {
    let device = Arc::new(PausedReadPageIo::new(key(1)));
    device.inner.insert(key(1), 42);
    let pool = BufferPool::new(1, TEST_PAGE_SIZE, device.clone()).expect("pool");
    std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            let guard = pool.pin(key(1)).expect("first pins");
            assert_eq!(guard.read().expect("read latch")[0], 42);
        });
        device.wait_until_read();
        let second = scope.spawn(|| {
            let guard = pool.pin(key(1)).expect("second joins");
            assert_eq!(guard.read().expect("read latch")[0], 42);
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while pool.stats().expect("stats").load_waits == 0 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        let joined = pool.stats().expect("stats").load_waits > 0;
        device.release();
        first.join().expect("first completes");
        second.join().expect("second completes");
        assert!(joined, "second miss did not join the load");
    });
    assert_eq!(device.inner.reads.load(Ordering::Relaxed), 1);
    let stats = pool.stats().expect("stats");
    assert_eq!(stats.translation_entries, 1);
    assert_eq!(stats.resident_frames, 1);
}

#[test]
fn failed_or_unwinding_load_wakes_joiners_to_retry() {
    for outcome in [PausedReadOutcome::Error, PausedReadOutcome::Panic] {
        let mut device = PausedReadPageIo::new(key(1));
        device.outcome = outcome;
        device.inner.insert(key(1), 7);
        let device = Arc::new(device);
        // A normal I/O error permits immediate reuse of the same incarnation
        // slot. Unwinding poisons its byte latch, so use the next free frame.
        let frames = if matches!(outcome, PausedReadOutcome::Error) {
            1
        } else {
            2
        };
        let pool = BufferPool::new(frames, TEST_PAGE_SIZE, device.clone()).expect("pool");
        std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    pool.pin(key(1))
                        .map(|guard| guard.read().expect("bytes")[0])
                }))
            });
            device.wait_until_read();
            let (completed, completion) = std::sync::mpsc::channel();
            let waiter_pool = &pool;
            let waiter = scope.spawn(move || {
                let value = waiter_pool
                    .pin(key(1))
                    .map(|guard| guard.read().expect("bytes")[0]);
                completed.send(value).expect("waiter result");
            });
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while pool.stats().expect("stats").load_waits == 0
                && std::time::Instant::now() < deadline
            {
                std::thread::yield_now();
            }
            let joined = pool.stats().expect("stats").load_waits > 0;
            device.release();
            let first_result = first.join().expect("owner caught unwind");
            let waiter_result = completion.recv_timeout(std::time::Duration::from_secs(3));
            if waiter_result.is_err() {
                // Drain the scoped thread even if cleanup forgot its wakeup;
                // the timeout still fails the contract assertion below.
                for slot in &pool.frames {
                    slot.load_completion.1.notify_all();
                }
            }
            waiter.join().expect("waiter");
            assert!(joined, "waiter never joined");
            match outcome {
                PausedReadOutcome::Error => assert!(matches!(
                    first_result,
                    Ok(Err(BufferError::Io {
                        operation: PageIoOperation::Read,
                        ..
                    }))
                )),
                PausedReadOutcome::Panic => assert!(first_result.is_err()),
                PausedReadOutcome::Success => unreachable!(),
            }
            assert_eq!(
                waiter_result
                    .expect("cleanup wakes waiter")
                    .expect("retry succeeds"),
                7
            );
        });
        let stats = pool.stats().expect("stats");
        assert_eq!(stats.occupied_frames, 1);
        assert_eq!(stats.translation_entries, 1);
        assert_eq!(device.inner.reads.load(Ordering::Relaxed), 2);
    }
}

struct BlockingWritePageIo {
    inner: MemoryPageIo,
    entered: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
}

impl BlockingWritePageIo {
    fn wait_until_writeback(&self) {
        let (lock, cv) = &self.entered;
        let mut entered = lock.lock().expect("entered mutex");
        while !*entered {
            entered = cv.wait(entered).expect("entered wait");
        }
    }

    fn release_writeback(&self) {
        let (lock, cv) = &self.release;
        *lock.lock().expect("release mutex") = true;
        cv.notify_all();
    }
}

impl PageIo for BlockingWritePageIo {
    fn read_page(&self, key: PageKey, destination: &mut [u8]) -> io::Result<()> {
        self.inner.read_page(key, destination)
    }

    fn write_page(&self, key: PageKey, source: &[u8]) -> io::Result<()> {
        {
            let (lock, cv) = &self.entered;
            *lock.lock().map_err(|_| io::Error::other("entered mutex"))? = true;
            cv.notify_all();
        }
        {
            let (lock, cv) = &self.release;
            let mut released = lock.lock().map_err(|_| io::Error::other("release mutex"))?;
            while !*released {
                released = cv
                    .wait(released)
                    .map_err(|_| io::Error::other("release wait"))?;
            }
        }
        self.inner.write_page(key, source)
    }
}

#[test]
fn lookup_during_writeback_waits_instead_of_reloading_stale_device_bytes() {
    let inner = MemoryPageIo::default();
    inner.insert(key(1), 1);
    let device = Arc::new(BlockingWritePageIo {
        inner,
        entered: (Mutex::new(false), Condvar::new()),
        release: (Mutex::new(false), Condvar::new()),
    });
    let pool = Arc::new(BufferPool::new(2, TEST_PAGE_SIZE, device.clone()).expect("pool creates"));

    {
        let guard = pool.pin(key(1)).expect("page loads");
        guard.write().expect("write latch")[0] = 9;
    }

    let flush_pool = Arc::clone(&pool);
    let flusher = std::thread::spawn(move || {
        assert!(flush_pool.flush_page(key(1)).expect("flush succeeds"));
    });
    device.wait_until_writeback();

    let (started, reader_started) = std::sync::mpsc::channel();
    let read_pool = Arc::clone(&pool);
    let reader = std::thread::spawn(move || {
        started.send(()).expect("signal reader start");
        let guard = read_pool.pin(key(1)).expect("lookup survives writeback");
        guard.read().expect("read latch")[0]
    });
    reader_started.recv().expect("reader starts");

    // The reader must reach the in-flight writeback before it is released.
    // Bound the wait so a broken pool fails the assertion instead of hanging,
    // but do not cap it by a retry count: reader progress depends on
    // scheduling, not on how many times this thread yields.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while pool.stats().expect("stats").writeback_waits == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "reader never waited for the in-flight writeback"
        );
        std::thread::yield_now();
    }
    device.release_writeback();
    flusher.join().expect("flusher completes");
    assert_eq!(reader.join().expect("reader completes"), 9);
    assert_eq!(device.inner.reads.load(Ordering::Relaxed), 1);
    assert!(pool.stats().expect("stats").writeback_waits > 0);
}

#[test]
fn failed_load_returns_reserved_frame_to_free_state() {
    let device = Arc::new(MemoryPageIo::default());
    let pool = BufferPool::new(1, TEST_PAGE_SIZE, device.clone()).expect("pool creates");

    assert!(matches!(
        pool.pin(key(3)),
        Err(BufferError::Io {
            operation: PageIoOperation::Read,
            ..
        })
    ));
    let stats = pool.stats().expect("stats");
    assert_eq!(stats.occupied_frames, 0);
    assert_eq!(stats.translation_entries, 0);

    device.insert(key(3), 5);
    let guard = pool.pin(key(3)).expect("retry loads after failure");
    assert_eq!(guard.read().expect("read latch")[0], 5);
}
