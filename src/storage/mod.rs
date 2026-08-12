//! Storage engine coordination.
//!
//! This module coordinates the B-tree, buffer manager, PMT, allocator,
//! and device to provide persistent storage.

mod flush;
pub mod format;
mod invariants;
mod lazy_read;
mod manifest_store;
mod materialization;
mod page_cache;
mod read_path;
mod reclamation;
mod retention_format;
mod verification;

pub use manifest_store::ManifestStore;
pub(crate) use read_path::StorageReadView;

use self::page_cache::ParsedPageCache;
use crate::allocator::PageAllocator;
use crate::btree::BTree;
use crate::buffer::{BufferManager, BufferStats, PageCacheKey};
use crate::error::{Error, Result};
use crate::mvcc::PMT;
use crate::space::Device;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// Cumulative physical work performed by one storage-engine handle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageMetrics {
    /// Logical page reads requested through the PMT-backed page seam.
    pub logical_page_reads: u64,
    /// Lazy lookups served by the parsed immutable-node cache.
    pub parsed_page_cache_hits: u64,
    /// Physical page reads issued to the data device.
    pub physical_page_reads: u64,
    /// Physical page writes completed on the data device.
    pub physical_page_writes: u64,
    /// Bytes read from the data device for page operations.
    pub page_bytes_read: u64,
    /// Bytes written to the data device for page operations.
    pub page_bytes_written: u64,
    /// Published generation flushes completed by this handle.
    pub generation_flushes: u64,
    /// Successful data-device sync calls.
    pub syncs: u64,
    /// Physical pages made reusable after a publication barrier.
    pub reclaimed_pages: u64,
    /// Bytes made reusable after a publication barrier.
    pub reclaimed_bytes: u64,
    /// Deterministic capacity preflight failures.
    pub capacity_preflight_failures: u64,
}

#[derive(Debug, Default)]
struct StorageCounters {
    logical_page_reads: AtomicU64,
    parsed_page_cache_hits: AtomicU64,
    physical_page_reads: AtomicU64,
    physical_page_writes: AtomicU64,
    page_bytes_read: AtomicU64,
    page_bytes_written: AtomicU64,
    generation_flushes: AtomicU64,
    syncs: AtomicU64,
    reclaimed_pages: AtomicU64,
    reclaimed_bytes: AtomicU64,
    capacity_preflight_failures: AtomicU64,
}

impl StorageCounters {
    fn snapshot(&self) -> StorageMetrics {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        StorageMetrics {
            logical_page_reads: load(&self.logical_page_reads),
            parsed_page_cache_hits: load(&self.parsed_page_cache_hits),
            physical_page_reads: load(&self.physical_page_reads),
            physical_page_writes: load(&self.physical_page_writes),
            page_bytes_read: load(&self.page_bytes_read),
            page_bytes_written: load(&self.page_bytes_written),
            generation_flushes: load(&self.generation_flushes),
            syncs: load(&self.syncs),
            reclaimed_pages: load(&self.reclaimed_pages),
            reclaimed_bytes: load(&self.reclaimed_bytes),
            capacity_preflight_failures: load(&self.capacity_preflight_failures),
        }
    }
}

fn capacity_preflight_error(error: std::io::Error) -> Error {
    if matches!(
        error.kind(),
        std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
    ) {
        Error::CapacityPreflight
    } else {
        Error::from(error)
    }
}

/// Storage engine that coordinates all components.
///
/// Provides persistent storage by serializing B-tree nodes to pages
/// and storing them through the buffer manager and device.
pub struct StorageEngine {
    /// The B-tree (logical operations).
    btree: BTree,
    /// Buffer manager (page cache).
    buffer: Mutex<BufferManager>,
    /// Parsed immutable nodes paired with the raw buffer cache.
    parsed_page_cache: Mutex<ParsedPageCache>,
    /// Page mapping table (page locations).
    pmt: Arc<PMT>,
    /// Device (file I/O).
    device: Device,
    /// Next offset for page allocation.
    next_offset: u64,
    /// Physical page offsets that are not referenced by the active generation.
    free_offsets: Vec<u64>,
    /// Offsets retired by the last flush, pending manifest publication.
    pending_reclaimed_offsets: Vec<u64>,
    /// Cache identities retired by the last flush, pending manifest
    /// publication. They cannot be evicted before the old root is fenced off.
    pending_reclaimed_cache_keys: Vec<PageCacheKey>,
    /// Physical page offsets referenced by retained root generations. These
    /// pages remain unavailable for reuse until the corresponding retention
    /// lease is released and the allocator refreshes its reclamation view.
    protected_offsets: Arc<Mutex<HashSet<u64>>>,
    /// Immutable PMTs held by live read views. Keeping the PMT itself here
    /// avoids copying every page offset when a view begins; reclamation walks
    /// these roots only when it refreshes its reuse plan.
    protected_pmts: Arc<Mutex<Vec<Arc<PMT>>>>,
    /// Active-generation offsets held aside while a logical rebuild is being
    /// published. Resetting the PMT before the new root is authoritative must
    /// not make the old root's pages reusable after a crash.
    rebuild_reserved_offsets: HashSet<u64>,
    /// Root of the immutable generation when its B-tree pages are still
    /// available through the PMT-backed lazy read path.
    lazy_root: Option<u32>,
    /// Whether retention/read-view state changed since the free-slot view was
    /// last refreshed.
    reclamation_dirty: Arc<AtomicBool>,
    /// Cumulative physical work counters for diagnostics and benchmarks.
    metrics: StorageCounters,
}

impl StorageEngine {
    /// Create a new storage engine.
    pub fn new(
        btree: BTree,
        buffer: BufferManager,
        pmt: PMT,
        allocator: PageAllocator,
        device: Device,
    ) -> Self {
        Self::new_with_protected_offsets(
            btree,
            buffer,
            pmt,
            allocator,
            device,
            Arc::new(Mutex::new(HashSet::new())),
        )
    }

    /// Create a storage engine sharing a retention protection set with the
    /// database's durable root registry.
    pub fn new_with_protected_offsets(
        btree: BTree,
        buffer: BufferManager,
        pmt: PMT,
        allocator: PageAllocator,
        device: Device,
        protected_offsets: Arc<Mutex<HashSet<u64>>>,
    ) -> Self {
        let buffer_frames = buffer.stats().total_frames;
        Self {
            btree: btree.with_page_allocator(allocator),
            buffer: Mutex::new(buffer),
            parsed_page_cache: Mutex::new(ParsedPageCache::new(buffer_frames)),
            pmt: Arc::new(pmt),
            device,
            next_offset: 0,
            free_offsets: Vec::new(),
            pending_reclaimed_offsets: Vec::new(),
            pending_reclaimed_cache_keys: Vec::new(),
            protected_offsets,
            protected_pmts: Arc::new(Mutex::new(Vec::new())),
            rebuild_reserved_offsets: HashSet::new(),
            lazy_root: None,
            reclamation_dirty: Arc::new(AtomicBool::new(false)),
            metrics: StorageCounters::default(),
        }
    }

    /// Get a reference to the B-tree.
    pub fn btree(&self) -> &BTree {
        &self.btree
    }

    /// Get a mutable reference to the B-tree.
    pub fn btree_mut(&mut self) -> &mut BTree {
        &mut self.btree
    }

    /// Get a reference to the PMT.
    pub fn pmt(&self) -> &PMT {
        self.pmt.as_ref()
    }

    /// Get a reference to the allocator.
    pub fn allocator(&self) -> &PageAllocator {
        self.btree.page_allocator()
    }

    /// Get a mutable reference to the allocator.
    pub fn allocator_mut(&mut self) -> &mut PageAllocator {
        self.btree.page_allocator_mut()
    }

    /// Return current buffer-pool counters and derived occupancy metrics.
    pub fn buffer_stats(&self) -> BufferStats {
        match self.buffer.lock() {
            Ok(buffer) => buffer.stats(),
            Err(poisoned) => poisoned.into_inner().stats(),
        }
    }

    /// Return cumulative physical work counters for this storage handle.
    pub fn metrics(&self) -> StorageMetrics {
        self.metrics.snapshot()
    }

    fn buffer_lock(&self) -> Result<MutexGuard<'_, BufferManager>> {
        self.buffer
            .lock()
            .map_err(|_| Error::Buffer("buffer pool mutex is poisoned".into()))
    }

    /// Inject one device sync failure for publication-boundary tests.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_sync_failure(&self) {
        self.device.inject_sync_failure();
    }

    /// Inject one device page-write failure for publication-boundary tests.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_write_failure(&self) {
        self.device.inject_write_failure();
    }

    /// Inject one failure after a complete device page write.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_after_write_failure(&self) {
        self.device.inject_after_write_failure();
    }

    /// Inject one failure after the complete page generation is written but
    /// before its device durability sync.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_page_range_sync_failure(&self) {
        self.device.inject_page_range_sync_failure();
    }

    /// Inject one final-write ENOSPC after a page write may have completed.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_final_write_disk_full(&self) {
        self.device.inject_final_write_disk_full();
    }

    /// Inject one deterministic disk-full result for recovery tests.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_disk_full(&self) {
        self.device.inject_disk_full();
    }

    /// Set a persistent device capacity limit for recovery tests.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_capacity_limit(&self, capacity: u64) {
        self.device.inject_capacity_limit(capacity);
    }

    /// Reject an artifact image that exceeds the deterministic device budget.
    pub fn check_artifact_capacity(&self, length: u64) -> Result<()> {
        self.device.check_capacity(length).map_err(Error::from)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
