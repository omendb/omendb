//! Storage engine coordination.
//!
//! This module coordinates the B-tree, buffer manager, PMT, allocator,
//! and device to provide persistent storage.

#[cfg(any(test, feature = "fault-injection"))]
mod faults;
mod flush;
pub mod format;
mod invariants;
mod lazy_read;
mod materialization;
mod metrics;
mod page_cache;
mod read_path;
mod reclamation;
mod retention_format;
mod verification;

pub use metrics::StorageMetrics;
pub(crate) use metrics::{durability_syncs, record_durability_sync};
pub(crate) use read_path::StorageReadView;

use self::metrics::StorageCounters;
use self::page_cache::ParsedPageCache;
use crate::allocator::PageAllocator;
use crate::btree::BTree;
use crate::buffer::{BufferManager, BufferStats, PageCacheKey};
use crate::error::{Error, Result};
use crate::mvcc::PMT;
use crate::space::Device;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard};

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
    /// Generation stamped into page headers at the next flush.
    write_generation: u64,
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
            write_generation: 0,
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

    /// Swap the active PMT, returning the previous instance. The pipelined
    /// publication barrier uses this to encode per-envelope checkpoints
    /// without materializing intermediate page states on the write path.
    pub(crate) fn swap_pmt(&mut self, replacement: Arc<PMT>) -> Arc<PMT> {
        std::mem::replace(&mut self.pmt, replacement)
    }

    /// Clone the active PMT handle without deep-copying its contents.
    pub(crate) fn clone_pmt_arc(&self) -> Arc<PMT> {
        self.pmt.clone()
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

    /// Reclamation-state probe for tests and diagnostics: logical page
    /// count, allocation frontier, and free/pending list lengths.
    #[cfg(test)]
    pub(crate) fn reclamation_probe(&self) -> (usize, u64, usize, usize) {
        (
            self.btree.node_count(),
            self.next_offset,
            self.free_offsets.len(),
            self.pending_reclaimed_offsets.len(),
        )
    }

    /// Protection-set probe for tests: registered protected offsets and
    /// leased PMTs.
    #[cfg(test)]
    pub(crate) fn protection_probe(&self) -> (usize, usize) {
        (
            self.protected_offsets
                .lock()
                .map(|set| set.len())
                .unwrap_or(usize::MAX),
            self.protected_pmts
                .lock()
                .map(|set| set.len())
                .unwrap_or(usize::MAX),
        )
    }

    fn buffer_lock(&self) -> Result<MutexGuard<'_, BufferManager>> {
        self.buffer
            .lock()
            .map_err(|_| Error::Buffer("buffer pool mutex is poisoned".into()))
    }

    /// Reject an artifact image that exceeds the deterministic device budget.
    pub fn check_artifact_capacity(&self, length: u64) -> Result<()> {
        self.device.check_capacity(length).map_err(Error::from)
    }

    /// Admit an artifact image before a maintenance candidate is installed.
    ///
    /// A deterministic capacity refusal at this boundary has issued no
    /// storage mutation, so callers can retry after restoring capacity.
    pub fn preflight_artifact_capacity(&self, length: u64) -> Result<()> {
        self.device
            .check_capacity(length)
            .map_err(capacity_preflight_error)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "churn_probe_tests.rs"]
mod churn_probe_tests;
