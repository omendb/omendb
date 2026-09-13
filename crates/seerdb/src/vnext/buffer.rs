//! Safe concurrent buffer-pool baseline for the storage-kernel replacement.
//!
//! The hot cache-hit path uses a sharded translation lookup, atomic frame pin,
//! and frame-local byte latch. There is no database-wide or buffer-wide mutex.
//! Misses reserve a frame before I/O; translation is published only after the
//! bytes and logical identity are initialized and the frame is resident with an
//! installer pin.
//!
//! This is deliberately a correctness/measurement baseline. Page translation,
//! eviction policy, byte latching, wait policy, and I/O remain replaceable
//! seams. The lifecycle itself is strict: transient writeback is not mistaken
//! for a stale translation, dirty eviction materializes before reuse, and a
//! frame slot is always identified by both slot and incarnation.

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
    #[error("page image has {actual} bytes; buffer page size is {expected}")]
    PageSizeMismatch { expected: usize, actual: usize },
    #[error("logical page {0:?} is already resident")]
    PageAlreadyResident(PageKey),
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
    pub writeback_waits: u64,
    pub loads: u64,
    pub load_failures: u64,
    pub new_pages: u64,
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
    writeback_waits: AtomicU64,
    loads: AtomicU64,
    load_failures: AtomicU64,
    new_pages: AtomicU64,
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
}

impl FrameSlot {
    fn new(id: FrameId, page_size: usize) -> Self {
        Self {
            id,
            meta: FrameMeta::new_free(),
            page: RwLock::new(None),
            bytes: RwLock::new(vec![0u8; page_size].into_boxed_slice()),
            referenced: AtomicBool::new(false),
        }
    }
}

enum PinAttempt<'a> {
    Pinned(FramePin<'a>),
    Busy,
    Stale,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WritebackTarget {
    Resident,
    Evicting,
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
                match self.try_pin_reference(reference)? {
                    PinAttempt::Pinned(pin) => {
                        if !counted_miss {
                            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
                        }
                        return Ok(self.guard_from_pin(key, reference, pin));
                    }
                    PinAttempt::Busy => {
                        self.metrics
                            .translation_retries
                            .fetch_add(1, Ordering::Relaxed);
                        self.metrics.writeback_waits.fetch_add(1, Ordering::Relaxed);
                        std::thread::yield_now();
                        continue;
                    }
                    PinAttempt::Stale => {
                        self.metrics
                            .stale_translations
                            .fetch_add(1, Ordering::Relaxed);
                        self.metrics
                            .translation_retries
                            .fetch_add(1, Ordering::Relaxed);
                        let _ = self.translation.remove_if(key, reference)?;
                        continue;
                    }
                }
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

    /// Install a newly allocated logical page directly into the buffer.
    ///
    /// The image is marked dirty before translation publication. If a duplicate
    /// publisher wins, the losing unpublished dirty frame is discarded through
    /// an explicit lifecycle transition that cannot be used for normal pages.
    /// Logical page ID allocation remains an access-method/object-metadata
    /// concern.
    pub(crate) fn create_page(
        &self,
        key: PageKey,
        image: &[u8],
    ) -> Result<PageGuard<'_>, BufferError> {
        if image.len() != self.page_size {
            return Err(BufferError::PageSizeMismatch {
                expected: self.page_size,
                actual: image.len(),
            });
        }
        self.metrics
            .translation_lookups
            .fetch_add(1, Ordering::Relaxed);
        if self.translation.get(key)?.is_some() {
            return Err(BufferError::PageAlreadyResident(key));
        }

        let (slot, incarnation) = self.reserve_for_load()?;
        if let Err(error) = self.initialize_slot(slot, key, Some(image)) {
            self.abort_loading(slot);
            return Err(error);
        }
        let pin = match slot.meta.finish_load_pinned() {
            Ok(pin) => pin,
            Err(source) => {
                self.abort_loading(slot);
                return Err(self.frame_transition(slot.id, source));
            }
        };
        let reference = FrameRef::new(slot.id, incarnation);

        let dirty_error = {
            match pin.try_write() {
                Ok(latch) => {
                    drop(latch);
                    None
                }
                Err(source) => Some(source),
            }
        };
        if let Some(source) = dirty_error {
            drop(pin);
            let _ = self.discard_unpublished(slot);
            return Err(self.frame_transition(slot.id, source));
        }

        match self.translation.publish_if_absent(key, reference) {
            Ok(PublishResult::Published) => {
                self.metrics.new_pages.fetch_add(1, Ordering::Relaxed);
                Ok(self.guard_from_pin(key, reference, pin))
            }
            Ok(PublishResult::Existing(_)) => {
                drop(pin);
                self.discard_unpublished(slot)?;
                Err(BufferError::PageAlreadyResident(key))
            }
            Err(error) => {
                drop(pin);
                let _ = self.discard_unpublished(slot);
                Err(BufferError::Translation(error))
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
        self.writeback(slot, WritebackTarget::Resident)?;
        Ok(true)
    }

    /// Snapshot counters and frame occupancy without a global pool lock.
    pub fn stats(&self) -> Result<BufferStats, BufferError> {
        let mut stats = BufferStats {
            hits: self.metrics.hits.load(Ordering::Relaxed),
            misses: self.metrics.misses.load(Ordering::Relaxed),
            translation_lookups: self.metrics.translation_lookups.load(Ordering::Relaxed),
            translation_retries: self.metrics.translation_retries.load(Ordering::Relaxed),
            stale_translations: self.metrics.stale_translations.load(Ordering::Relaxed),
            writeback_waits: self.metrics.writeback_waits.load(Ordering::Relaxed),
            loads: self.metrics.loads.load(Ordering::Relaxed),
            load_failures: self.metrics.load_failures.load(Ordering::Relaxed),
            new_pages: self.metrics.new_pages.load(Ordering::Relaxed),
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

        if let Err(error) = self.set_page_identity(slot, key) {
            self.abort_loading(slot);
            return Err(error);
        }
        let pin = match slot.meta.finish_load_pinned() {
            Ok(pin) => pin,
            Err(source) => {
                self.abort_loading(slot);
                return Err(self.frame_transition(slot.id, source));
            }
        };
        let reference = FrameRef::new(slot.id, incarnation);

        match self.translation.publish_if_absent(key, reference) {
            Ok(PublishResult::Published) => Ok(Some(self.guard_from_pin(key, reference, pin))),
            Ok(PublishResult::Existing(_)) => {
                self.metrics.duplicate_loads.fetch_add(1, Ordering::Relaxed);
                drop(pin);
                self.discard_unpublished(slot)?;
                Ok(None)
            }
            Err(error) => {
                drop(pin);
                let _ = self.discard_unpublished(slot);
                Err(BufferError::Translation(error))
            }
        }
    }

    fn initialize_slot(
        &self,
        slot: &FrameSlot,
        key: PageKey,
        image: Option<&[u8]>,
    ) -> Result<(), BufferError> {
        if let Some(image) = image {
            let mut bytes = slot.bytes.write().map_err(|_| BufferError::Poisoned {
                frame: slot.id,
                component: "bytes",
            })?;
            bytes.copy_from_slice(image);
        }
        self.set_page_identity(slot, key)
    }

    fn set_page_identity(&self, slot: &FrameSlot, key: PageKey) -> Result<(), BufferError> {
        let mut page = slot.page.write().map_err(|_| BufferError::Poisoned {
            frame: slot.id,
            component: "identity",
        })?;
        *page = Some(key);
        Ok(())
    }

    fn guard_from_pin<'a>(
        &'a self,
        key: PageKey,
        reference: FrameRef,
        pin: FramePin<'a>,
    ) -> PageGuard<'a> {
        let slot = &self.frames[reference.frame().index()];
        slot.referenced.store(true, Ordering::Release);
        self.metrics.pins.fetch_add(1, Ordering::Relaxed);
        PageGuard {
            pool: self,
            slot,
            pin,
            key,
            reference,
        }
    }

    fn try_pin_reference<'a>(&'a self, reference: FrameRef) -> Result<PinAttempt<'a>, BufferError> {
        let slot = self.slot(reference.frame())?;
        if slot.meta.incarnation() != Some(reference.incarnation()) {
            return Ok(PinAttempt::Stale);
        }
        if let Some(pin) = slot.meta.try_pin() {
            if pin.incarnation() == reference.incarnation() {
                return Ok(PinAttempt::Pinned(pin));
            }
            drop(pin);
            return Ok(PinAttempt::Stale);
        }

        if slot.meta.incarnation() != Some(reference.incarnation()) {
            return Ok(PinAttempt::Stale);
        }
        match slot.meta.state() {
            FrameState::Writeback | FrameState::Resident => Ok(PinAttempt::Busy),
            FrameState::Free | FrameState::Loading | FrameState::Evicting => Ok(PinAttempt::Stale),
        }
    }

    fn reserve_for_load(&self) -> Result<(&FrameSlot, FrameIncarnation), BufferError> {
        let attempts = self.frames.len().saturating_mul(2);
        for _ in 0..attempts {
            let index = self.clock.fetch_add(1, Ordering::Relaxed) % self.frames.len();
            let slot = &self.frames[index];

            match slot.meta.state() {
                FrameState::Free => match slot.meta.begin_load() {
                    Ok(incarnation) => return Ok((slot, incarnation)),
                    Err(_) => continue,
                },
                FrameState::Resident => {
                    self.metrics
                        .eviction_attempts
                        .fetch_add(1, Ordering::Relaxed);
                    if slot.referenced.swap(false, Ordering::AcqRel) {
                        continue;
                    }

                    if slot.meta.is_dirty() {
                        match self.writeback(slot, WritebackTarget::Evicting) {
                            Ok(()) => {}
                            Err(BufferError::FrameTransition {
                                source: FrameTransitionError::Pinned,
                                ..
                            }) => {
                                self.metrics
                                    .eviction_refusals
                                    .fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            Err(BufferError::FrameTransition {
                                source: FrameTransitionError::WrongState { .. },
                                ..
                            }) => continue,
                            Err(error) => return Err(error),
                        }
                    } else {
                        match slot.meta.try_begin_evict() {
                            Ok(()) => {}
                            Err(
                                FrameTransitionError::Pinned
                                | FrameTransitionError::WrongState { .. },
                            ) => {
                                self.metrics
                                    .eviction_refusals
                                    .fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            Err(source) => return Err(self.frame_transition(slot.id, source)),
                        }
                    }

                    if let Err(error) = self.detach_evicted(slot) {
                        let _ = slot.meta.abort_evict();
                        return Err(error);
                    }
                    slot.meta
                        .finish_evict()
                        .map_err(|source| self.frame_transition(slot.id, source))?;
                    self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
                    match slot.meta.begin_load() {
                        Ok(incarnation) => return Ok((slot, incarnation)),
                        Err(FrameTransitionError::WrongState { .. }) => continue,
                        Err(source) => return Err(self.frame_transition(slot.id, source)),
                    }
                }
                FrameState::Loading | FrameState::Writeback | FrameState::Evicting => continue,
            }
        }
        self.metrics
            .eviction_refusals
            .fetch_add(1, Ordering::Relaxed);
        Err(BufferError::NoVictim)
    }

    fn detach_evicted(&self, slot: &FrameSlot) -> Result<(), BufferError> {
        let key = self.page_identity(slot)?;
        let incarnation = slot
            .meta
            .incarnation()
            .ok_or(BufferError::MissingIdentity { frame: slot.id })?;
        let _ = self
            .translation
            .remove_if(key, FrameRef::new(slot.id, incarnation))?;
        let mut page = slot.page.write().map_err(|_| BufferError::Poisoned {
            frame: slot.id,
            component: "identity",
        })?;
        *page = None;
        Ok(())
    }

    fn page_identity(&self, slot: &FrameSlot) -> Result<PageKey, BufferError> {
        let page = slot.page.read().map_err(|_| BufferError::Poisoned {
            frame: slot.id,
            component: "identity",
        })?;
        (*page).ok_or(BufferError::MissingIdentity { frame: slot.id })
    }

    fn writeback(&self, slot: &FrameSlot, target: WritebackTarget) -> Result<(), BufferError> {
        self.metrics
            .writeback_attempts
            .fetch_add(1, Ordering::Relaxed);
        slot.meta
            .try_begin_writeback()
            .map_err(|source| self.frame_transition(slot.id, source))?;

        let key = match self.page_identity(slot) {
            Ok(key) => key,
            Err(error) => {
                let _ = slot.meta.abort_writeback();
                return Err(error);
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

        let transition = match target {
            WritebackTarget::Resident => slot.meta.finish_writeback(),
            WritebackTarget::Evicting => slot.meta.finish_writeback_for_evict(),
        };
        transition.map_err(|source| self.frame_transition(slot.id, source))?;
        self.metrics.writebacks.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes_written
            .fetch_add(self.page_size as u64, Ordering::Relaxed);
        Ok(())
    }

    fn discard_unpublished(&self, slot: &FrameSlot) -> Result<(), BufferError> {
        slot.meta
            .try_begin_unpublished_discard()
            .map_err(|source| self.frame_transition(slot.id, source))?;
        let mut page = slot.page.write().map_err(|_| BufferError::Poisoned {
            frame: slot.id,
            component: "identity",
        })?;
        *page = None;
        drop(page);
        slot.meta
            .finish_evict()
            .map_err(|source| self.frame_transition(slot.id, source))
    }

    fn abort_loading(&self, slot: &FrameSlot) {
        if let Ok(mut page) = slot.page.write() {
            *page = None;
        }
        let _ = slot.meta.abort_load();
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
    ///
    /// Writer contention is resolved at the guard boundary rather than exposed
    /// to every access method. Claim the optimistic writer version before the
    /// byte lock so a second writer waits here; the resulting guard still drops
    /// the byte lock before publishing the next even version.
    pub fn write(&self) -> Result<PageWriteGuard<'_>, BufferError> {
        let latch = loop {
            match self.pin.try_write() {
                Ok(latch) => break latch,
                Err(FrameTransitionError::WriteBusy) => {
                    self.pool
                        .metrics
                        .latch_retries
                        .fetch_add(1, Ordering::Relaxed);
                    std::thread::yield_now();
                }
                Err(source) => {
                    self.pool
                        .metrics
                        .latch_retries
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(self.pool.frame_transition(self.slot.id, source));
                }
            }
        };
        let data = match self.slot.bytes.write() {
            Ok(data) => data,
            Err(_) => {
                drop(latch);
                return Err(BufferError::Poisoned {
                    frame: self.slot.id,
                    component: "bytes",
                });
            }
        };
        Ok(PageWriteGuard {
            data,
            _latch: latch,
        })
    }
}

/// Exclusive mutable view of one pinned page.
pub struct PageWriteGuard<'a> {
    // Release the byte lock before publishing the next stable even version.
    // This ordering matters once optimistic lock-free reads are introduced.
    data: RwLockWriteGuard<'a, Box<[u8]>>,
    _latch: FrameWriteLatch<'a>,
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
        let pool =
            Arc::new(BufferPool::new(2, TEST_PAGE_SIZE, device.clone()).expect("pool creates"));

        {
            let guard = pool.pin(key(1)).expect("page loads");
            guard.write().expect("write latch")[0] = 9;
        }

        let flush_pool = Arc::clone(&pool);
        let flusher = std::thread::spawn(move || {
            assert!(flush_pool.flush_page(key(1)).expect("flush succeeds"));
        });
        device.wait_until_writeback();

        let read_pool = Arc::clone(&pool);
        let reader = std::thread::spawn(move || {
            let guard = read_pool.pin(key(1)).expect("lookup survives writeback");
            guard.read().expect("read latch")[0]
        });

        for _ in 0..100 {
            if pool.stats().expect("stats").writeback_waits > 0 {
                break;
            }
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
        assert_eq!(pool.stats().expect("stats").occupied_frames, 0);

        device.insert(key(3), 5);
        let guard = pool.pin(key(3)).expect("retry loads after failure");
        assert_eq!(guard.read().expect("read latch")[0], 5);
    }
}
