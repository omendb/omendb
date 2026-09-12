//! Safe concurrent buffer-pool baseline for the storage-kernel replacement.
//!
//! The hot cache-hit path uses a sharded translation lookup, atomic frame pin,
//! and frame-local byte latch. There is no database-wide or buffer-wide mutex.
//! Misses reserve a frame before I/O; translation is published only after the
//! bytes and logical identity are initialized and the frame is resident.
//!
//! This is deliberately a correctness/measurement baseline. Page translation,
//! eviction, byte latching, and I/O policy remain replaceable seams.

use super::frame::{FrameMeta, FramePin, FrameState, FrameTransitionError, FrameWriteLatch};
use super::ids::{FrameId, FrameIncarnation, FrameRef, PageKey};
use super::translation::{PublishResult, TranslationError, TranslationTable};
use std::io;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Logical-page I/O seam below the buffer pool.
///
/// Implementations may resolve a `PageKey` through an out-of-place page map,
/// local file, page service, or test device. The buffer pool intentionally does
/// not equate a logical page ID with a physical byte offset. Future WAL/page-LSN
/// eligibility belongs below or alongside this seam, not in access methods.
pub trait PageIo: Send + Sync {
    fn read_page(&self, key: PageKey, destination: &mut [u8]) -> io::Result<()>;
    fn write_page(&self, key: PageKey, source: &[u8]) -> io::Result<()>;
}

/// Buffer-pool operation that encountered an I/O failure.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PageIoOperation {
    Read,
    Write,
}

/// Error from the vNext buffer baseline.
#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("buffer pool must contain at least one frame")]
    EmptyPool,
    #[error("buffer page size must be nonzero")]
    InvalidPageSize,
    #[error("buffer frame {0:?} is outside this pool")]
    FrameOutOfRange(FrameId),
    #[error("no evictable buffer frame is currently available")]
    NoVictim,
    #[error("buffer frame {frame:?} has no logical page identity")]
    MissingIdentity { frame: FrameId },
    #[error("buffer frame {frame:?} {component} lock is poisoned")]
    Poisoned {
        frame: FrameId,
        component: &'static str,
    },
    #[error("frame {frame:?} transition failed: {source}")]
    FrameTransition {
        frame: FrameId,
        #[source]
        source: FrameTransitionError,
    },
    #[error("page translation failed: {0}")]
    Translation(#[from] TranslationError),
    #[error("page I/O {operation:?} failed for {key:?}: {source}")]
    Io {
        key: PageKey,
        operation: PageIoOperation,
        #[source]
        source: io::Error,
    },
}

/// Point-in-time buffer diagnostics.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct BufferStats {
    pub hits: u64,
    pub misses: u64,
    pub translation_lookups: u64,
    pub translation_retries: u64,
    pub stale_translations: u64,
    pub loads: u64,
    pub load_failures: u64,
    pub duplicate_loads: u64,
    pub pins: u64,
    pub latch_retries: u64,
    pub eviction_attempts: u64,
    pub eviction_refusals: u64,
    pub evictions: u64,
    pub writeback_attempts: u64,
    pub writeback_failures: u64,
    pub writebacks: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub occupied_frames: usize,
    pub resident_frames: usize,
    pub pinned_frames: usize,
    pub dirty_frames: usize,
    pub translation_entries: usize,
}

#[derive(Default)]
struct BufferMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    translation_lookups: AtomicU64,
    translation_retries: AtomicU64,
    stale_translations: AtomicU64,
    loads: AtomicU64,
    load_failures: AtomicU64,
    duplicate_loads: AtomicU64,
    pins: AtomicU64,
    latch_retries: AtomicU64,
    eviction_attempts: AtomicU64,
    eviction_refusals: AtomicU64,
    evictions: AtomicU64,
    writeback_attempts: AtomicU64,
    writeback_failures: AtomicU64,
    writebacks: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

struct FrameSlot {
    id: FrameId,
    meta: FrameMeta,
    page: RwLock<Option<PageKey>>,
    bytes: RwLock<Box<[u8]>>,
    referenced: AtomicBool,
    // Serializes allocation/publication for this slot without participating in
    // ordinary cache hits. It closes the tiny Resident-but-not-yet-published
    // window after a load without introducing a global allocation mutex.
    installing: AtomicBool,
}

impl FrameSlot {
    fn new(id: FrameId, page_size: usize) -> Self {
        Self {
            id,
            meta: FrameMeta::new_free(),
            page: RwLock::new(None),
            bytes: RwLock::new(vec![0u8; page_size].into_boxed_slice()),
            referenced: AtomicBool::new(false),
            installing: AtomicBool::new(false),
        }
    }
}

/// Fixed-frame concurrent buffer pool.
pub struct BufferPool {
    frames: Box<[FrameSlot]>,
    translation: TranslationTable,
    io: Arc<dyn PageIo>,
    clock: AtomicUsize,
    page_size: usize,
    metrics: BufferMetrics,
}

impl BufferPool {
    /// Create a pool over a concrete logical-page I/O implementation.
    pub fn new(
        frame_count: usize,
        page_size: usize,
        io: Arc<dyn PageIo>,
    ) -> Result<Self, BufferError> {
        if frame_count == 0 {
            return Err(BufferError::EmptyPool);
        }
        if page_size == 0 {
            return Err(BufferError::InvalidPageSize);
        }
        let frames = (0..frame_count)
            .map(|index| FrameSlot::new(FrameId::new(index), page_size))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            frames,
            translation: TranslationTable::new(),
            io,
            clock: AtomicUsize::new(0),
            page_size,
            metrics: BufferMetrics::default(),
        })
    }

    /// Return the configured logical page size.
    #[must_use]
    pub const fn page_size(&self) -> usize {
        self.page_size
    }

    /// Pin one logical page, loading it on demand.
    pub fn pin(&self, key: PageKey) -> Result<PageGuard<'_>, BufferError> {
        let mut counted_miss = false;
        loop {
            self.metrics
                .translation_lookups
                .fetch_add(1, Ordering::Relaxed);
            if let Some(reference) = self.translation.get(key)? {
                if let Some(guard) = self.try_pin_reference(key, reference)? {
                    if !counted_miss {
                        self.metrics.hits.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(guard);
                }
                self.metrics
                    .stale_translations
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .translation_retries
                    .fetch_add(1, Ordering::Relaxed);
                let _ = self.translation.remove_if(key, reference)?;
                continue;
            }

            if !counted_miss {
                self.metrics.misses.fetch_add(1, Ordering::Relaxed);
                counted_miss = true;
            } else {
                self.metrics
                    .translation_retries
                    .fetch_add(1, Ordering::Relaxed);
            }

            if let Some(guard) = self.load_miss(key)? {
                return Ok(guard);
            }
        }
    }

    /// Materialize one dirty page if it is resident and currently unpinned.
    /// Returns `false` for an absent or already-clean page.
    pub fn flush_page(&self, key: PageKey) -> Result<bool, BufferError> {
        self.metrics
            .translation_lookups
            .fetch_add(1, Ordering::Relaxed);
        let Some(reference) = self.translation.get(key)? else {
            return Ok(false);
        };
        let slot = self.slot(reference.frame())?;
        if slot.meta.incarnation() != Some(reference.incarnation())
            || slot.meta.state() != FrameState::Resident
            || !slot.meta.is_dirty()
        {
            return Ok(false);
        }
        self.writeback(slot)
    }

    /// Snapshot counters and frame occupancy without a global pool lock.
    pub fn stats(&self) -> Result<BufferStats, BufferError> {
        let mut stats = BufferStats {
            hits: self.metrics.hits.load(Ordering::Relaxed),
            misses: self.metrics.misses.load(Ordering::Relaxed),
            translation_lookups: self.metrics.translation_lookups.load(Ordering::Relaxed),
            translation_retries: self.metrics.translation_retries.load(Ordering::Relaxed),
            stale_translations: self.metrics.stale_translations.load(Ordering::Relaxed),
            loads: self.metrics.loads.load(Ordering::Relaxed),
            load_failures: self.metrics.load_failures.load(Ordering::Relaxed),
            duplicate_loads: self.metrics.duplicate_loads.load(Ordering::Relaxed),
            pins: self.metrics.pins.load(Ordering::Relaxed),
            latch_retries: self.metrics.latch_retries.load(Ordering::Relaxed),
            eviction_attempts: self.metrics.eviction_attempts.load(Ordering::Relaxed),
            eviction_refusals: self.metrics.eviction_refusals.load(Ordering::Relaxed),
            evictions: self.metrics.evictions.load(Ordering::Relaxed),
            writeback_attempts: self.metrics.writeback_attempts.load(Ordering::Relaxed),
            writeback_failures: self.metrics.writeback_failures.load(Ordering::Relaxed),
            writebacks: self.metrics.writebacks.load(Ordering::Relaxed),
            bytes_read: self.metrics.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.metrics.bytes_written.load(Ordering::Relaxed),
            ..BufferStats::default()
        };

        for slot in &self.frames {
            let state = slot.meta.state();
            if state != FrameState::Free {
                stats.occupied_frames += 1;
            }
            if state == FrameState::Resident {
                stats.resident_frames += 1;
            }
            if slot.meta.pin_count() != 0 {
                stats.pinned_frames += 1;
            }
            if slot.meta.is_dirty() {
                stats.dirty_frames += 1;
            }
        }
        stats.translation_entries = self.translation.len()?;
        Ok(stats)
    }

    fn load_miss(&self, key: PageKey) -> Result<Option<PageGuard<'_>>, BufferError> {
        let (slot, incarnation) = self.reserve_for_load()?;

        let read_result = match slot.bytes.write() {
            Ok(mut bytes) => self.io.read_page(key, &mut bytes),
            Err(_) => {
                self.abort_loading(slot);
                return Err(BufferError::Poisoned {
                    frame: slot.id,
                    component: "bytes",
                });
            }
        };
        if let Err(source) = read_result {
            self.metrics.load_failures.fetch_add(1, Ordering::Relaxed);
            self.abort_loading(slot);
            return Err(BufferError::Io {
                key,
                operation: PageIoOperation::Read,
                source,
            });
        }
        self.metrics.loads.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes_read
            .fetch_add(self.page_size as u64, Ordering::Relaxed);

        match slot.page.write() {
            Ok(mut page) => *page = Some(key),
            Err(_) => {
                self.abort_loading(slot);
                return Err(BufferError::Poisoned {
                    frame: slot.id,
                    component: "identity",
                });
            }
        }
        if let Err(source) = slot.meta.finish_load() {
            slot.installing.store(false, Ordering::Release);
            return Err(self.frame_transition(slot.id, source));
        }

        let reference = FrameRef::new(slot.id, incarnation);
        match self.translation.publish_if_absent(key, reference)? {
            PublishResult::Published => {
                // `installing` prevents victim selection until our own pin is
                // established. Translation readers may also pin immediately.
                let guard = self.try_pin_reference(key, reference)?.ok_or_else(|| {
                    self.frame_transition(
                        slot.id,
                        FrameTransitionError::WrongState {
                            expected: FrameState::Resident,
                            actual: slot.meta.state(),
                        },
                    )
                })?;
                slot.installing.store(false, Ordering::Release);
                Ok(Some(guard))
            }
            PublishResult::Existing(_) => {
                self.metrics.duplicate_loads.fetch_add(1, Ordering::Relaxed);
                self.discard_unpublished(slot)?;
                Ok(None)
            }
        }
    }

    fn try_pin_reference(
        &self,
        key: PageKey,
        reference: FrameRef,
    ) -> Result<Option<PageGuard<'_>>, BufferError> {
        let slot = self.slot(reference.frame())?;
        let Some(pin) = slot.meta.try_pin() else {
            return Ok(None);
        };
        if pin.incarnation() != reference.incarnation() {
            drop(pin);
            return Ok(None);
        }
        slot.referenced.store(true, Ordering::Release);
        self.metrics.pins.fetch_add(1, Ordering::Relaxed);
        Ok(Some(PageGuard {
            pool: self,
            slot,
            pin,
            key,
            reference,
        }))
    }

    fn reserve_for_load(&self) -> Result<(&FrameSlot, FrameIncarnation), BufferError> {
        let attempts = self.frames.len().saturating_mul(2);
        for _ in 0..attempts {
            let index = self.clock.fetch_add(1, Ordering::Relaxed) % self.frames.len();
            let slot = &self.frames[index];
            if slot
                .installing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            match slot.meta.state() {
                FrameState::Free => match slot.meta.begin_load() {
                    Ok(incarnation) => return Ok((slot, incarnation)),
                    Err(_) => {
                        slot.installing.store(false, Ordering::Release);
                        continue;
                    }
                },
                FrameState::Resident => {
                    self.metrics
                        .eviction_attempts
                        .fetch_add(1, Ordering::Relaxed);
                    if slot.referenced.swap(false, Ordering::AcqRel) {
                        slot.installing.store(false, Ordering::Release);
                        continue;
                    }

                    if slot.meta.is_dirty() {
                        match self.writeback(slot) {
                            Ok(true) | Ok(false) => {}
                            Err(BufferError::FrameTransition {
                                source: FrameTransitionError::Pinned,
                                ..
                            }) => {
                                self.metrics
                                    .eviction_refusals
                                    .fetch_add(1, Ordering::Relaxed);
                                slot.installing.store(false, Ordering::Release);
                                continue;
                            }
                            Err(error) => {
                                slot.installing.store(false, Ordering::Release);
                                return Err(error);
                            }
                        }
                    }

                    match slot.meta.try_begin_evict() {
                        Ok(()) => {}
                        Err(
                            FrameTransitionError::Pinned | FrameTransitionError::WrongState { .. },
                        ) => {
                            self.metrics
                                .eviction_refusals
                                .fetch_add(1, Ordering::Relaxed);
                            slot.installing.store(false, Ordering::Release);
                            continue;
                        }
                        Err(source) => {
                            slot.installing.store(false, Ordering::Release);
                            return Err(self.frame_transition(slot.id, source));
                        }
                    }

                    let key = match slot.page.read() {
                        Ok(page) => page.ok_or(BufferError::MissingIdentity { frame: slot.id })?,
                        Err(_) => {
                            slot.installing.store(false, Ordering::Release);
                            return Err(BufferError::Poisoned {
                                frame: slot.id,
                                component: "identity",
                            });
                        }
                    };
                    let incarnation = slot
                        .meta
                        .incarnation()
                        .ok_or(BufferError::MissingIdentity { frame: slot.id })?;
                    let _ = self
                        .translation
                        .remove_if(key, FrameRef::new(slot.id, incarnation))?;
                    match slot.page.write() {
                        Ok(mut page) => *page = None,
                        Err(_) => {
                            slot.installing.store(false, Ordering::Release);
                            return Err(BufferError::Poisoned {
                                frame: slot.id,
                                component: "identity",
                            });
                        }
                    }
                    slot.meta
                        .finish_evict()
                        .map_err(|source| self.frame_transition(slot.id, source))?;
                    self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
                    match slot.meta.begin_load() {
                        Ok(incarnation) => return Ok((slot, incarnation)),
                        Err(source) => {
                            slot.installing.store(false, Ordering::Release);
                            return Err(self.frame_transition(slot.id, source));
                        }
                    }
                }
                FrameState::Loading | FrameState::Writeback | FrameState::Evicting => {
                    slot.installing.store(false, Ordering::Release);
                }
            }
        }
        self.metrics
            .eviction_refusals
            .fetch_add(1, Ordering::Relaxed);
        Err(BufferError::NoVictim)
    }

    fn writeback(&self, slot: &FrameSlot) -> Result<bool, BufferError> {
        if !slot.meta.is_dirty() {
            return Ok(false);
        }
        self.metrics
            .writeback_attempts
            .fetch_add(1, Ordering::Relaxed);
        slot.meta
            .try_begin_writeback()
            .map_err(|source| self.frame_transition(slot.id, source))?;

        let key = match slot.page.read() {
            Ok(page) => match *page {
                Some(key) => key,
                None => {
                    let _ = slot.meta.abort_writeback();
                    return Err(BufferError::MissingIdentity { frame: slot.id });
                }
            },
            Err(_) => {
                let _ = slot.meta.abort_writeback();
                return Err(BufferError::Poisoned {
                    frame: slot.id,
                    component: "identity",
                });
            }
        };

        let result = match slot.bytes.read() {
            Ok(bytes) => self.io.write_page(key, &bytes),
            Err(_) => {
                let _ = slot.meta.abort_writeback();
                return Err(BufferError::Poisoned {
                    frame: slot.id,
                    component: "bytes",
                });
            }
        };
        if let Err(source) = result {
            self.metrics
                .writeback_failures
                .fetch_add(1, Ordering::Relaxed);
            let _ = slot.meta.abort_writeback();
            return Err(BufferError::Io {
                key,
                operation: PageIoOperation::Write,
                source,
            });
        }

        slot.meta
            .finish_writeback()
            .map_err(|source| self.frame_transition(slot.id, source))?;
        self.metrics.writebacks.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes_written
            .fetch_add(self.page_size as u64, Ordering::Relaxed);
        Ok(true)
    }

    fn discard_unpublished(&self, slot: &FrameSlot) -> Result<(), BufferError> {
        slot.meta
            .try_begin_evict()
            .map_err(|source| self.frame_transition(slot.id, source))?;
        match slot.page.write() {
            Ok(mut page) => *page = None,
            Err(_) => {
                slot.installing.store(false, Ordering::Release);
                return Err(BufferError::Poisoned {
                    frame: slot.id,
                    component: "identity",
                });
            }
        }
        slot.meta
            .finish_evict()
            .map_err(|source| self.frame_transition(slot.id, source))?;
        slot.installing.store(false, Ordering::Release);
        Ok(())
    }

    fn abort_loading(&self, slot: &FrameSlot) {
        if let Ok(mut page) = slot.page.write() {
            *page = None;
        }
        let _ = slot.meta.abort_load();
        slot.installing.store(false, Ordering::Release);
    }

    fn slot(&self, frame: FrameId) -> Result<&FrameSlot, BufferError> {
        self.frames
            .get(frame.index())
            .ok_or(BufferError::FrameOutOfRange(frame))
    }

    fn frame_transition(&self, frame: FrameId, source: FrameTransitionError) -> BufferError {
        BufferError::FrameTransition { frame, source }
    }
}

/// RAII pin of one logical page and exact frame incarnation.
pub struct PageGuard<'a> {
    pool: &'a BufferPool,
    slot: &'a FrameSlot,
    pin: FramePin<'a>,
    key: PageKey,
    reference: FrameRef,
}

impl PageGuard<'_> {
    /// Logical page protected by this guard.
    #[must_use]
    pub const fn page_key(&self) -> PageKey {
        self.key
    }

    /// Process-local frame incarnation protected by this guard.
    #[must_use]
    pub const fn frame_ref(&self) -> FrameRef {
        self.reference
    }

    /// Acquire a shared frame-local byte latch.
    pub fn read(&self) -> Result<RwLockReadGuard<'_, Box<[u8]>>, BufferError> {
        self.slot.bytes.read().map_err(|_| BufferError::Poisoned {
            frame: self.slot.id,
            component: "bytes",
        })
    }

    /// Acquire exclusive byte access and mark the page dirty.
    pub fn write(&self) -> Result<PageWriteGuard<'_>, BufferError> {
        let data = self.slot.bytes.write().map_err(|_| BufferError::Poisoned {
            frame: self.slot.id,
            component: "bytes",
        })?;
        let latch = self.pin.try_write().map_err(|source| {
            self.pool
                .metrics
                .latch_retries
                .fetch_add(1, Ordering::Relaxed);
            self.pool.frame_transition(self.slot.id, source)
        })?;
        Ok(PageWriteGuard {
            _latch: latch,
            data,
        })
    }
}

/// Exclusive mutable view of one pinned page.
pub struct PageWriteGuard<'a> {
    // Dropping the optimistic latch first publishes an even version after all
    // caller mutations have completed; the byte lock is then released.
    _latch: FrameWriteLatch<'a>,
    data: RwLockWriteGuard<'a, Box<[u8]>>,
}

impl Deref for PageWriteGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.data.as_ref()
    }
}

impl DerefMut for PageWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{PageId, StorageObjectId};
    use std::collections::HashMap;
    use std::sync::Barrier;

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

    struct BarrierPageIo {
        inner: MemoryPageIo,
        reads_meet: Barrier,
    }

    impl PageIo for BarrierPageIo {
        fn read_page(&self, key: PageKey, destination: &mut [u8]) -> io::Result<()> {
            self.inner.read_page(key, destination)?;
            self.reads_meet.wait();
            Ok(())
        }

        fn write_page(&self, key: PageKey, source: &[u8]) -> io::Result<()> {
            self.inner.write_page(key, source)
        }
    }

    #[test]
    fn concurrent_duplicate_misses_publish_one_resident_mapping() {
        let inner = MemoryPageIo::default();
        inner.insert(key(1), 42);
        let device = Arc::new(BarrierPageIo {
            inner,
            reads_meet: Barrier::new(2),
        });
        let pool =
            Arc::new(BufferPool::new(4, TEST_PAGE_SIZE, device.clone()).expect("pool creates"));

        let mut workers = Vec::new();
        for _ in 0..2 {
            let pool = Arc::clone(&pool);
            workers.push(std::thread::spawn(move || {
                let guard = pool.pin(key(1)).expect("concurrent pin succeeds");
                assert_eq!(guard.read().expect("read latch")[0], 42);
            }));
        }
        for worker in workers {
            worker.join().expect("worker completes");
        }

        assert_eq!(device.inner.reads.load(Ordering::Relaxed), 2);
        let stats = pool.stats().expect("stats");
        assert_eq!(stats.duplicate_loads, 1);
        assert_eq!(stats.translation_entries, 1);
        assert_eq!(stats.resident_frames, 1);
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
        assert_eq!(pool.stats().expect("stats").occupied_frames, 0);

        device.insert(key(3), 5);
        let guard = pool.pin(key(3)).expect("retry loads after failure");
        assert_eq!(guard.read().expect("read latch")[0], 5);
    }
}
