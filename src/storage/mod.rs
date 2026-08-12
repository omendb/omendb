//! Storage engine coordination.
//!
//! This module coordinates the B-tree, buffer manager, PMT, allocator,
//! and device to provide persistent storage.

pub mod format;
mod lazy_read;
mod manifest_store;
mod page_cache;
mod read_path;
mod verification;

pub use manifest_store::ManifestStore;
pub(crate) use read_path::StorageReadView;

use self::page_cache::ParsedPageCache;
use crate::allocator::PageAllocator;
use crate::btree::{BTree, BTreeError, Node, PAGE_SIZE};
use crate::buffer::{BufferManager, BufferStats, GuardAccess, PageCacheKey};
use crate::error::{Error, Result};
use crate::mvcc::{PMT, PageMapping};
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

    /// Create a read-only handle pinned to the current PMT and root.
    pub(crate) fn read_view(&self, root_page_id: u64) -> Result<StorageReadView> {
        let buffer_capacity = self.buffer_stats().total_frames * PAGE_SIZE;
        let pmt = Arc::clone(&self.pmt);
        let device = self.device.clone_for_read()?;
        self.protected_pmts
            .lock()
            .map_err(|_| Error::Corruption("storage PMT lease mutex is poisoned".into()))?
            .push(Arc::clone(&pmt));
        self.reclamation_dirty.store(true, Ordering::Release);
        let engine = Self {
            btree: BTree::new().with_page_allocator(PageAllocator::new()),
            buffer: Mutex::new(BufferManager::new(buffer_capacity)),
            parsed_page_cache: Mutex::new(ParsedPageCache::new(buffer_capacity / PAGE_SIZE)),
            pmt,
            device,
            next_offset: 0,
            free_offsets: Vec::new(),
            pending_reclaimed_offsets: Vec::new(),
            pending_reclaimed_cache_keys: Vec::new(),
            protected_offsets: Arc::clone(&self.protected_offsets),
            protected_pmts: Arc::clone(&self.protected_pmts),
            rebuild_reserved_offsets: HashSet::new(),
            lazy_root: Some(root_page_id as u32),
            reclamation_dirty: Arc::clone(&self.reclamation_dirty),
            metrics: StorageCounters::default(),
        };
        Ok(StorageReadView::new(engine, root_page_id))
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

    /// Number of physical page slots available for safe reuse.
    pub fn reclaimable_page_count(&self) -> usize {
        self.free_offsets.len()
    }

    /// Return the physical slots selected by the next dirty-page flush.
    ///
    /// The caller persists these candidates before invoking `flush`, because
    /// a failed publication may have written a reused slot before its new
    /// manifest became authoritative.
    pub fn pending_reuse_offsets(&self) -> Vec<u64> {
        let dirty_pages = self
            .btree
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| self.btree.node(*page_id).is_some())
            .count();
        self.free_offsets
            .iter()
            .rev()
            .take(dirty_pages)
            .copied()
            .collect()
    }

    /// Install the physical offsets protected by retained root generations.
    ///
    /// Recomputing from the device extent is intentionally conservative: a
    /// newly retained root can protect pages that were previously considered
    /// free, while a released root becomes reusable at the next flush or
    /// explicit refresh boundary.
    pub fn set_protected_offsets(&mut self, protected_offsets: HashSet<u64>) -> Result<()> {
        *self
            .protected_offsets
            .lock()
            .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))? =
            protected_offsets;
        self.refresh_reclamation()
    }

    /// Refresh the physical reuse view after retention leases or device
    /// state changed without a normal mutation flush.
    pub fn refresh_reclamation(&mut self) -> Result<()> {
        self.refresh_reclaimable_offsets()
    }

    /// Return whether retention or read-view state invalidated the current
    /// free-slot view.
    pub fn reclamation_needs_refresh(&self) -> bool {
        self.reclamation_dirty.load(Ordering::Acquire)
    }

    pub(crate) fn reclamation_dirty_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.reclamation_dirty)
    }

    fn protected_offsets_snapshot(&self) -> Result<HashSet<u64>> {
        let mut protected_offsets = self
            .protected_offsets
            .lock()
            .map_err(|_| Error::Corruption("retention protection mutex is poisoned".into()))?
            .clone();
        let protected_pmts = self
            .protected_pmts
            .lock()
            .map_err(|_| Error::Corruption("storage PMT lease mutex is poisoned".into()))?
            .clone();
        for pmt in protected_pmts {
            protected_offsets.extend(pmt.iter().map(|(_, mapping)| mapping.offset));
        }
        Ok(protected_offsets)
    }

    fn refresh_reclaimable_offsets(&mut self) -> Result<()> {
        let device_size = self.device.size()?;
        let active_offsets: HashSet<_> =
            self.pmt.iter().map(|(_, mapping)| mapping.offset).collect();
        let protected_offsets = self.protected_offsets_snapshot()?;
        let rebuild_reserved_offsets = &self.rebuild_reserved_offsets;
        self.free_offsets = (0..device_size)
            .step_by(PAGE_SIZE)
            .filter(|offset| {
                !active_offsets.contains(offset)
                    && !protected_offsets.contains(offset)
                    && !rebuild_reserved_offsets.contains(offset)
            })
            .collect();
        self.next_offset = device_size;
        self.reclamation_dirty.store(false, Ordering::Release);
        Ok(())
    }

    /// Return the current data size and the size after trimming only trailing
    /// free physical page slots.
    pub fn reclaimable_tail_range(&self) -> Result<(u64, u64)> {
        let before = self.device.size()?;
        let page_size = PAGE_SIZE as u64;
        let mut after = before;
        let mut free_index = self.free_offsets.len();

        while after >= page_size && free_index > 0 {
            let offset = self.free_offsets[free_index - 1];
            if offset != after - page_size {
                break;
            }
            after -= page_size;
            free_index -= 1;
        }

        Ok((before, after))
    }

    /// Trim trailing free page slots after the manifest barrier is complete.
    ///
    /// The caller must first ensure both manifest slots name the active
    /// generation. This method never removes an active PMT mapping and does
    /// not move interior free slots.
    pub fn truncate_reclaimable_tail(&mut self) -> Result<(u64, u64)> {
        if !self.pending_reclaimed_offsets.is_empty() {
            return Err(Error::NeedsRecovery(
                "cannot truncate pages before generation publication".into(),
            ));
        }

        let (before, after) = self.reclaimable_tail_range()?;
        if after == before {
            return Ok((before, after));
        }

        self.device.truncate(after)?;
        let retained = self
            .free_offsets
            .len()
            .saturating_sub(((before - after) / PAGE_SIZE as u64) as usize);
        self.free_offsets.truncate(retained);
        self.next_offset = after;
        Ok((before, after))
    }

    /// Relocate active pages from high offsets into lower unprotected holes.
    ///
    /// This is the interior-compaction half of the maintenance protocol. It
    /// writes copies first and changes the in-memory PMT only after every
    /// copy has been synced. The caller must have mirrored the current
    /// manifest before invoking this method; if a write or sync fails, the
    /// old manifest and all of its source pages remain authoritative after
    /// reopen.
    pub fn relocate_interior_pages(&mut self) -> Result<usize> {
        self.relocate_interior_pages_with_limit(usize::MAX)
    }

    /// Report whether at least one active page can move into a lower hole.
    ///
    /// This planning-only check performs no page reads or writes. Maintenance
    /// uses it to avoid demanding publication-sidecar capacity when a compact
    /// call can only trim an already-free tail.
    pub fn has_relocatable_interior_page(&self) -> Result<bool> {
        if !self.pending_reclaimed_offsets.is_empty() {
            return Err(Error::NeedsRecovery(
                "cannot plan relocation before generation publication".into(),
            ));
        }

        let protected_offsets = self.protected_offsets_snapshot()?;
        let mut free_offsets = self.free_offsets.clone();
        free_offsets.sort_unstable();
        let mut active_pages: Vec<_> = self
            .pmt
            .iter()
            .filter(|(_, mapping)| !protected_offsets.contains(&mapping.offset))
            .map(|(_, mapping)| mapping.offset)
            .collect();
        active_pages.sort_unstable_by_key(|offset| std::cmp::Reverse(*offset));

        let mut free_index = 0;
        for source in active_pages {
            while free_index < free_offsets.len() && free_offsets[free_index] >= source {
                free_index += 1;
            }
            if free_index < free_offsets.len() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Relocate at most `max_pages` active pages into lower unprotected holes.
    ///
    /// The limit bounds the write set and temporary page-image memory for one
    /// maintenance generation. The caller may invoke this method repeatedly;
    /// each successful batch still requires the same manifest publication
    /// barrier before its source pages can be reused.
    pub fn relocate_interior_pages_with_limit(&mut self, max_pages: usize) -> Result<usize> {
        if !self.pending_reclaimed_offsets.is_empty() {
            return Err(Error::NeedsRecovery(
                "cannot relocate pages before generation publication".into(),
            ));
        }
        if max_pages == 0 {
            return Ok(0);
        }

        let protected_offsets = self.protected_offsets_snapshot()?;
        let mut free_offsets = self.free_offsets.clone();
        free_offsets.sort_unstable();

        let mut active_pages: Vec<(u64, PageMapping)> = self
            .pmt
            .iter()
            .filter(|(_, mapping)| !protected_offsets.contains(&mapping.offset))
            .map(|(page_id, mapping)| (page_id, *mapping))
            .collect();
        active_pages.sort_unstable_by_key(|(_, mapping)| std::cmp::Reverse(mapping.offset));

        let mut moves = Vec::new();
        let mut free_index = 0;
        for (page_id, mapping) in active_pages {
            if moves.len() >= max_pages {
                break;
            }
            while free_index < free_offsets.len() && free_offsets[free_index] >= mapping.offset {
                free_index += 1;
            }
            let Some(&target) = free_offsets.get(free_index) else {
                break;
            };
            free_index += 1;

            let node = self.read_node_from_pmt(&self.pmt, page_id)?;
            moves.push((page_id, mapping, target, *node.as_bytes()));
        }

        if moves.is_empty() {
            return Ok(0);
        }

        for (_, _, target, page) in &moves {
            self.device.write_page(*target, page)?;
            self.metrics
                .physical_page_writes
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .page_bytes_written
                .fetch_add(PAGE_SIZE as u64, Ordering::Relaxed);
        }
        self.device.sync()?;
        self.metrics.syncs.fetch_add(1, Ordering::Relaxed);

        let mut retired_offsets = Vec::with_capacity(moves.len());
        let mut retired_cache_keys = Vec::with_capacity(moves.len());
        let targets: HashSet<_> = moves.iter().map(|(_, _, target, _)| *target).collect();
        for (page_id, mapping, target, _) in moves {
            retired_offsets.push(mapping.offset);
            retired_cache_keys.push(PageCacheKey::new(page_id, mapping.version));
            Arc::make_mut(&mut self.pmt).insert(page_id, mapping.file_id, target);
        }
        self.free_offsets.retain(|offset| !targets.contains(offset));
        self.pending_reclaimed_offsets = retired_offsets;
        self.pending_reclaimed_cache_keys = retired_cache_keys;
        Ok(self.pending_reclaimed_offsets.len())
    }

    /// Make the previous generation's retired pages reusable.
    ///
    /// DB calls this only after the new manifest has been durably published.
    /// Before that point, the old generation may still be the authoritative
    /// root after a crash and its physical pages must remain untouched.
    pub fn complete_generation(&mut self) {
        let protected_offsets = self
            .protected_offsets_snapshot()
            .unwrap_or_else(|_| self.pending_reclaimed_offsets.iter().copied().collect());
        let reclaimed_pages = self
            .pending_reclaimed_offsets
            .iter()
            .filter(|offset| !protected_offsets.contains(offset))
            .count() as u64;
        self.metrics
            .reclaimed_pages
            .fetch_add(reclaimed_pages, Ordering::Relaxed);
        self.metrics.reclaimed_bytes.fetch_add(
            reclaimed_pages.saturating_mul(PAGE_SIZE as u64),
            Ordering::Relaxed,
        );
        self.free_offsets.extend(
            self.pending_reclaimed_offsets
                .drain(..)
                .filter(|offset| !protected_offsets.contains(offset)),
        );
        self.free_offsets.extend(
            self.rebuild_reserved_offsets
                .drain()
                .filter(|offset| !protected_offsets.contains(offset)),
        );
        self.free_offsets.sort_unstable();
        self.free_offsets.dedup();
        if let Ok(mut buffer) = self.buffer.lock() {
            for page_key in self.pending_reclaimed_cache_keys.drain(..) {
                let _ = buffer.evict_key(page_key);
            }
        } else {
            self.pending_reclaimed_cache_keys.clear();
        }
        self.lazy_root = (!self.pmt.is_empty()).then_some(self.btree.root_id());
        self.btree.clear_dirty();
    }

    /// Get mutable access to the page mapping table for recovery.
    pub fn pmt_mut(&mut self) -> &mut PMT {
        Arc::make_mut(&mut self.pmt)
    }

    /// Replace the active logical tree as part of a full mark-and-rebuild
    /// maintenance generation.
    ///
    /// The old PMT locations are reserved until the caller publishes the new
    /// manifest and invokes [`Self::complete_generation`]. If maintenance
    /// fails before that barrier, reopening selects the old PMT and the
    /// speculative pages remain unreachable rather than overwriting the old
    /// root.
    pub(crate) fn prepare_logical_rebuild(&mut self, btree: BTree) -> Result<()> {
        if !self.pending_reclaimed_offsets.is_empty()
            || !self.pending_reclaimed_cache_keys.is_empty()
        {
            return Err(Error::NeedsRecovery(
                "logical rebuild has an unpublished retired-page set".into(),
            ));
        }

        self.rebuild_reserved_offsets =
            self.pmt.iter().map(|(_, mapping)| mapping.offset).collect();
        Arc::make_mut(&mut self.pmt).clear();
        self.btree = btree;
        self.lazy_root = None;
        self.free_offsets.clear();
        self.next_offset = self.device.size()?;
        Ok(())
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

    /// Admit the new data extent required by a logical rebuild before the
    /// active PMT is replaced. The candidate contains only newly allocated
    /// pages, so its flush starts at the current data-file end.
    pub fn preflight_rebuild_capacity(&self, candidate: &BTree) -> Result<()> {
        let page_count = candidate
            .dirty_page_ids()
            .into_iter()
            .filter(|page_id| candidate.node(*page_id).is_some())
            .count() as u64;
        if page_count == 0 {
            return Ok(());
        }
        let start = self.device.size()?;
        let required_end = start
            .checked_add(page_count.saturating_mul(PAGE_SIZE as u64))
            .ok_or(Error::DiskFull)?;
        self.device
            .check_write_capacity(required_end - PAGE_SIZE as u64)
            .map_err(capacity_preflight_error)?;
        self.device
            .reserve(required_end)
            .map_err(capacity_preflight_error)?;
        Ok(())
    }

    /// Flush all dirty pages to disk.
    ///
    /// Small generations keep staging images resident until the device sync,
    /// preserving the strongest buffer retry diagnostics. Larger generations
    /// stream one image at a time through the fixed-size pool; the logical
    /// B-tree remains dirty until publication, so a later write or sync error
    /// is still retryable and cannot make an uncommitted generation visible.
    pub fn flush(&mut self) -> Result<()> {
        // A lease may have been released since the last publication. Refresh
        // the allocator view before choosing a reusable physical slot only
        // when retention/read-view state made the cached view stale.
        if self.reclamation_needs_refresh() {
            self.refresh_reclamation()?;
        }
        self.flush_after_reclamation_refresh()
    }

    /// Flush after the caller has already refreshed the reclamation view and
    /// made a reuse decision from it. Keeping this boundary explicit avoids a
    /// second full data-file scan in the durable publication path.
    pub(crate) fn flush_after_reclamation_refresh(&mut self) -> Result<()> {
        // The bootstrap path rewrites the complete logical tree into a new
        // physical generation. It is coarse, but preserves the out-of-place
        // invariant needed by manifest publication.
        let dirty_page_ids = self.btree.dirty_page_ids();
        self.preflight_flush_capacity(&dirty_page_ids)?;
        let stream_writebacks = {
            let buffer = self.buffer_lock()?;
            dirty_page_ids.len() > buffer.capacity()
        };
        let mut retired_offsets = Vec::new();
        let mut retired_cache_keys = Vec::new();
        let pending_capacity = usize::from(!stream_writebacks) * dirty_page_ids.len();
        let mut pending_writebacks = Vec::with_capacity(pending_capacity);
        let mut pending_rekeys = Vec::with_capacity(pending_capacity);

        for page_id in dirty_page_ids {
            let node = self.btree.node(page_id);
            if let Some(node) = node {
                // B-tree mutations do not maintain the persisted checksum in
                // place. Rebuild a temporary node so every page written has a
                // checksum that covers its final bytes.
                let page = *node.as_bytes();
                let mut persisted_node = Node::from_bytes(Box::new(page)).ok_or_else(|| {
                    Error::Corruption(format!("invalid node page {page_id} before flush"))
                })?;
                persisted_node.update_checksum();

                // Every flush creates a new physical version. DB publishes
                // the resulting PMT only after all data is durable.
                if let Some(mapping) = self.pmt.get(page_id as u64) {
                    retired_offsets.push(mapping.offset);
                    retired_cache_keys.push(PageCacheKey::new(page_id as u64, mapping.version));
                }
                let (offset, reuses_retired_slot) = match self.free_offsets.last() {
                    Some(&offset) => (offset, true),
                    None => (self.next_offset, false),
                };

                // Stage the page through the buffer manager, then write the
                // clean flushed image to the out-of-place device version.
                let page = *persisted_node.as_bytes();
                let pending_key = PageCacheKey::unversioned(page_id as u64);
                let writeback = {
                    let mut buffer = self.buffer_lock()?;
                    let guard = buffer.fetch_key(pending_key, &page, GuardAccess::Write)?;
                    buffer.frame_data_mut(&guard)?.copy_from_slice(&page);
                    drop(guard);
                    buffer.begin_writeback_key(pending_key)?.ok_or_else(|| {
                        Error::Buffer(format!("page {page_id} was not dirty after staging"))
                    })?
                };
                self.device.write_page(offset, writeback.data())?;
                if stream_writebacks {
                    let mut buffer = self.buffer_lock()?;
                    buffer.discard_writeback(writeback)?;
                } else {
                    pending_writebacks.push(writeback);
                }
                self.metrics
                    .physical_page_writes
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .page_bytes_written
                    .fetch_add(PAGE_SIZE as u64, Ordering::Relaxed);
                if reuses_retired_slot {
                    self.free_offsets.pop();
                } else {
                    self.next_offset += PAGE_SIZE as u64;
                }
                Arc::make_mut(&mut self.pmt).insert(page_id as u64, 0, offset);
                let version = self
                    .pmt
                    .get(page_id as u64)
                    .ok_or_else(|| Error::Corruption("PMT insertion was lost".into()))?
                    .version;
                if !stream_writebacks {
                    pending_rekeys.push((pending_key, PageCacheKey::new(page_id as u64, version)));
                }
            }
        }

        // Sync to ensure data is persisted.
        #[cfg(any(test, feature = "fault-injection"))]
        self.device.check_page_range_sync()?;
        self.device.sync()?;
        {
            let mut buffer = self.buffer_lock()?;
            for writeback in pending_writebacks {
                buffer.complete_writeback(writeback)?;
            }
            for (from, to) in pending_rekeys {
                buffer.rekey(from, to);
            }
        }
        self.metrics.syncs.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .generation_flushes
            .fetch_add(1, Ordering::Relaxed);
        self.pending_reclaimed_offsets = retired_offsets;
        self.pending_reclaimed_cache_keys = retired_cache_keys;

        Ok(())
    }

    /// Admit the full generation before beginning any physical page writes.
    ///
    /// This closes the deterministic capacity/ENOSPC boundary for the active
    /// page set. Reuse of retired slots is always admitted; only newly growing
    /// data-file slots can fail this preflight. Supported filesystems reserve
    /// new data extents with keep-size semantics before page I/O; a final
    /// filesystem ENOSPC during the write itself remains fenced/recoverable.
    fn preflight_flush_capacity(&self, dirty_page_ids: &[u32]) -> Result<()> {
        let mut free_index = self.free_offsets.len();
        let mut next_offset = self.next_offset;
        let mut required_data_end = None;
        for &page_id in dirty_page_ids {
            if self.btree.node(page_id).is_none() {
                continue;
            }
            let offset = if free_index > 0 {
                free_index -= 1;
                self.free_offsets[free_index]
            } else {
                let offset = next_offset;
                next_offset = next_offset
                    .checked_add(PAGE_SIZE as u64)
                    .ok_or(Error::DiskFull)?;
                required_data_end = Some(next_offset);
                offset
            };
            if let Err(error) = self.device.check_write_capacity(offset) {
                self.metrics
                    .capacity_preflight_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(capacity_preflight_error(error));
            }
        }
        if let Some(end) = required_data_end
            && let Err(error) = self.device.reserve(end)
        {
            self.metrics
                .capacity_preflight_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(capacity_preflight_error(error));
        }
        Ok(())
    }

    /// Ensure mutations operate on a complete logical tree.
    ///
    /// Materialize the immutable PMT-selected generation for WAL replay or
    /// other recovery work that may touch arbitrary keys. Normal foreground
    /// mutations use `prepare_mutation` and remain sparse.
    pub fn ensure_materialized(&mut self) -> Result<()> {
        let Some(root_page_id) = self.lazy_root else {
            return Ok(());
        };

        let max_page_id = self
            .pmt
            .iter()
            .map(|(page_id, _)| page_id)
            .max()
            .ok_or_else(|| Error::Corruption("lazy manifest PMT is unexpectedly empty".into()))?;
        let node_count = max_page_id
            .checked_add(1)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| Error::Corruption("manifest PMT page count overflow".into()))?;
        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            nodes.push(Node::new_leaf());
        }

        for page_id in 0..=max_page_id {
            nodes[page_id as usize] = self.read_node(page_id)?;
        }

        let allocator = self.btree.take_page_allocator();
        self.btree = BTree::from_nodes_with_allocator(nodes, root_page_id, allocator);
        self.lazy_root = None;
        Ok(())
    }

    /// Load only the root-to-leaf path required for a mutation.
    ///
    /// A clean reopen keeps all other logical pages in the PMT-backed
    /// immutable generation. The loaded path becomes the sparse B-tree
    /// mutation overlay; unchanged pages remain readable through the PMT.
    pub fn prepare_mutation(&mut self, key: &[u8]) -> Result<()> {
        if self.lazy_root.is_none() {
            return Ok(());
        }

        let mut current = self.btree.root_id();
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(current) {
                return Err(Error::Corruption(
                    "cycle detected while preparing mutation path".into(),
                ));
            }
            if self.btree.node(current).is_none() {
                let node = self.read_node(current as u64)?;
                self.btree.add_node(node, current);
            }

            let next = {
                let node = self
                    .btree
                    .node(current)
                    .ok_or(BTreeError::MissingPage(current))?;
                if node.is_leaf() {
                    return Ok(());
                }

                let child_id = node.child_for_key(key).ok_or_else(|| {
                    Error::Corruption("internal mutation routing is malformed".into())
                })?;
                u32::try_from(child_id).map_err(|_| {
                    Error::Corruption("internal mutation child exceeds logical ID width".into())
                })?
            };
            current = next;
        }
    }

    /// Load the page versions named by a durable manifest generation.
    pub fn load_from_manifest(&mut self, root_page_id: u64) -> Result<()> {
        if self.pmt.is_empty() {
            if root_page_id != 0 {
                return Err(Error::Corruption(
                    "empty manifest PMT names a non-zero root page".into(),
                ));
            }
            self.next_offset = self.device.size()?;
            self.free_offsets.clear();
            self.pending_reclaimed_offsets.clear();
            self.pending_reclaimed_cache_keys.clear();
            self.reclamation_dirty.store(false, Ordering::Release);
            return Ok(());
        }

        let device_size = self.device.size()?;
        let active_offsets: HashSet<_> =
            self.pmt.iter().map(|(_, mapping)| mapping.offset).collect();
        let protected_offsets = self.protected_offsets_snapshot()?;
        self.free_offsets = (0..device_size)
            .step_by(PAGE_SIZE)
            .filter(|offset| {
                !active_offsets.contains(offset) && !protected_offsets.contains(offset)
            })
            .collect();
        self.pending_reclaimed_offsets.clear();
        self.pending_reclaimed_cache_keys.clear();

        let max_page_id = self
            .pmt
            .iter()
            .map(|(page_id, _)| page_id)
            .max()
            .ok_or_else(|| Error::Corruption("manifest PMT is unexpectedly empty".into()))?;
        if root_page_id > u32::MAX as u64
            || root_page_id > max_page_id
            || !self.pmt.contains(root_page_id)
        {
            return Err(Error::Corruption(format!(
                "manifest root page {root_page_id} is outside PMT"
            )));
        }

        let allocator = self.btree.take_page_allocator();
        let page_count = usize::try_from(
            max_page_id
                .checked_add(1)
                .ok_or_else(|| Error::Corruption("manifest PMT page count overflow".into()))?,
        )
        .map_err(|_| Error::Corruption("manifest PMT page count overflow".into()))?;
        self.btree = BTree::from_sparse_with_allocator(page_count, root_page_id as u32, allocator);
        self.lazy_root = Some(root_page_id as u32);
        self.next_offset = device_size;
        self.reclamation_dirty.store(false, Ordering::Release);
        Ok(())
    }

    /// Load all pages from disk into the B-tree.
    ///
    /// This is a temporary implementation for bootstrapping.
    /// In the full implementation, pages would be loaded on demand.
    pub fn load_from_disk(&mut self) -> Result<()> {
        let device_size = self.device.size()?;

        if device_size == 0 {
            return Ok(());
        }

        // Read all pages from disk.
        let mut offset = 0u64;
        let mut page_id = 0u64;

        while offset + PAGE_SIZE as u64 <= device_size {
            let mut buf = [0u8; PAGE_SIZE];
            self.metrics
                .physical_page_reads
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .page_bytes_read
                .fetch_add(PAGE_SIZE as u64, Ordering::Relaxed);
            self.device.read_page(offset, &mut buf)?;

            let buffered_page = {
                let mut buffer = self.buffer_lock()?;
                let guard = buffer.fetch(page_id, &buf, GuardAccess::Read)?;
                let page = *buffer.frame_data(&guard);
                drop(guard);
                Box::new(page)
            };

            // Deserialize the node and fail closed on malformed legacy data.
            let node = Node::from_bytes(buffered_page)
                .ok_or_else(|| Error::Corruption(format!("invalid page at offset {offset}")))?;
            if !node.verify_checksum() {
                return Err(Error::Corruption(format!(
                    "page checksum mismatch at offset {offset}"
                )));
            }

            // Add to the B-tree.
            self.btree.add_node(node, page_id as u32);

            // Update PMT.
            Arc::make_mut(&mut self.pmt).insert(page_id, 0, offset);
            let version = self
                .pmt
                .get(page_id)
                .ok_or_else(|| Error::Corruption("PMT insertion was lost".into()))?
                .version;
            self.buffer_lock()?.rekey(
                PageCacheKey::unversioned(page_id),
                PageCacheKey::new(page_id, version),
            );

            offset += PAGE_SIZE as u64;
            page_id += 1;
        }

        // Update the allocator.
        self.next_offset = offset;
        let node_count = self.btree.node_count() as u64;
        self.btree.page_allocator_mut().advance_next_id(node_count);
        self.free_offsets.clear();
        self.pending_reclaimed_offsets.clear();
        self.pending_reclaimed_cache_keys.clear();
        self.reclamation_dirty.store(false, Ordering::Release);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::LookupResult;
    use crate::space::DeviceOptions;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn buffer_stages_versioned_writeback_without_aliasing_generations() {
        let dir = tempdir().unwrap();
        let device = Device::open(
            dir.path().join("data"),
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(PAGE_SIZE * 2),
            PMT::new(),
            PageAllocator::new(),
            device,
        );

        engine
            .btree_mut()
            .insert(b"key", b"value")
            .expect("initial insert should fit");
        engine.flush().unwrap();
        let first = engine.buffer_stats();
        assert_eq!(first.reads, 1);
        assert_eq!(first.hits, 0);
        assert_eq!(first.writes, 1);
        assert_eq!(first.dirty_frames, 0);

        engine
            .btree_mut()
            .insert(b"key2", b"updated")
            .expect("second insert should fit");
        engine.flush().unwrap();
        let second = engine.buffer_stats();
        // The new pending image must not alias the old published-version
        // frame. It is a second cache miss until publication can retire the
        // old generation safely.
        assert_eq!(second.reads, 2);
        assert_eq!(second.hits, 0);
        assert_eq!(second.writes, 2);
        assert_eq!(second.dirty_frames, 0);
    }

    #[test]
    fn large_generation_streams_through_small_buffer_pool() {
        let dir = tempdir().unwrap();
        let device = Device::open(
            dir.path().join("data"),
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(PAGE_SIZE * 2),
            PMT::new(),
            PageAllocator::new(),
            device,
        );

        for index in 0..200 {
            let key = format!("key-{index:04}");
            engine
                .btree_mut()
                .insert(key.as_bytes(), b"value")
                .expect("test key should fit");
        }
        assert!(engine.btree().dirty_page_ids().len() > 2);

        engine.flush().unwrap();
        engine.complete_generation();
        let stats = engine.buffer_stats();
        assert_eq!(stats.dirty_frames, 0);
        assert!(stats.writeback_discards > 0);
        assert_eq!(
            engine.lookup(b"key-0199").unwrap(),
            LookupResult::Found(b"value".to_vec())
        );
    }

    #[test]
    fn streamed_generation_sync_failure_remains_retryable() {
        let dir = tempdir().unwrap();
        let device = Device::open(
            dir.path().join("data"),
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(PAGE_SIZE * 2),
            PMT::new(),
            PageAllocator::new(),
            device,
        );

        for index in 0..200 {
            let key = format!("key-{index:04}");
            engine
                .btree_mut()
                .insert(key.as_bytes(), b"value")
                .expect("test key should fit");
        }
        engine.inject_sync_failure();

        assert!(matches!(engine.flush(), Err(Error::Io(_))));
        assert!(!engine.btree().dirty_page_ids().is_empty());
        engine.flush().unwrap();
        engine.complete_generation();
        assert_eq!(
            engine.lookup(b"key-0199").unwrap(),
            LookupResult::Found(b"value".to_vec())
        );
    }

    #[test]
    fn failed_device_write_leaves_buffer_image_dirty_for_retry() {
        let dir = tempdir().unwrap();
        let device = Device::open(
            dir.path().join("data"),
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(PAGE_SIZE * 2),
            PMT::new(),
            PageAllocator::new(),
            device,
        );

        engine.btree_mut().insert(b"key", b"value").unwrap();
        engine.inject_write_failure();
        assert!(matches!(engine.flush(), Err(Error::Io(_))));
        assert_eq!(engine.buffer_stats().dirty_frames, 1);

        engine.flush().unwrap();
        assert_eq!(engine.buffer_stats().dirty_frames, 0);
        assert_eq!(engine.device.size().unwrap(), PAGE_SIZE as u64);
    }

    #[test]
    fn failed_device_sync_leaves_buffer_image_dirty_for_recovery() {
        let dir = tempdir().unwrap();
        let device = Device::open(
            dir.path().join("data"),
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(PAGE_SIZE * 2),
            PMT::new(),
            PageAllocator::new(),
            device,
        );

        engine.btree_mut().insert(b"key", b"value").unwrap();
        engine.inject_sync_failure();
        assert!(matches!(engine.flush(), Err(Error::Io(_))));
        assert_eq!(engine.buffer_stats().dirty_frames, 1);
    }

    #[test]
    fn load_from_disk_rejects_malformed_page() {
        let dir = tempdir().unwrap();
        let data_path = dir.path().join("data");
        fs::write(&data_path, [0u8; PAGE_SIZE]).unwrap();
        let device = Device::open(
            &data_path,
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(PAGE_SIZE * 2),
            PMT::new(),
            PageAllocator::new(),
            device,
        );

        assert!(matches!(
            engine.load_from_disk(),
            Err(Error::Corruption(message)) if message.contains("invalid page")
        ));
    }

    #[test]
    fn flush_writes_only_logically_dirty_pages() {
        let dir = tempdir().unwrap();
        let device = Device::open(
            dir.path().join("data"),
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(PAGE_SIZE * 600),
            PMT::new(),
            PageAllocator::new(),
            device,
        );

        for index in 0..500 {
            let key = format!("key-{index:06}");
            engine.btree_mut().insert(key.as_bytes(), b"v").unwrap();
        }
        engine.flush().unwrap();
        let first = engine.buffer_stats();
        assert!(first.writes > 1);
        engine.complete_generation();

        engine.btree_mut().upsert(b"key-000250", b"x").unwrap();
        engine.flush().unwrap();
        let second = engine.buffer_stats();
        assert_eq!(second.writes - first.writes, 1);
    }

    #[test]
    fn read_node_uses_pmt_and_buffer_boundary() {
        let dir = tempdir().unwrap();
        let device = Device::open(
            dir.path().join("data"),
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(PAGE_SIZE * 2),
            PMT::new(),
            PageAllocator::new(),
            device,
        );

        engine.btree_mut().insert(b"key", b"value").unwrap();
        engine.flush().unwrap();
        engine.complete_generation();

        assert!(engine.read_node(0).unwrap().is_leaf());
        assert!(engine.read_node(0).unwrap().is_leaf());
        assert!(matches!(
            engine.read_node(1),
            Err(Error::Corruption(message)) if message.contains("missing page")
        ));
        assert!(engine.metrics().parsed_page_cache_hits >= 1);
    }

    #[test]
    fn reuses_retired_physical_pages_after_generation_completion() {
        let dir = tempdir().unwrap();
        let device = Device::open(
            dir.path().join("data"),
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(PAGE_SIZE * 2),
            PMT::new(),
            PageAllocator::new(),
            device,
        );

        engine.btree_mut().insert(b"key", b"value-1").unwrap();
        engine.flush().unwrap();
        engine.complete_generation();
        assert_eq!(engine.device.size().unwrap(), PAGE_SIZE as u64);

        engine.btree_mut().upsert(b"key", b"value-2").unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.device.size().unwrap(), (PAGE_SIZE * 2) as u64);
        engine.complete_generation();
        assert_eq!(engine.reclaimable_page_count(), 1);

        engine.btree_mut().upsert(b"key", b"value-3").unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.device.size().unwrap(), (PAGE_SIZE * 2) as u64);
        assert_eq!(engine.reclaimable_page_count(), 0);
    }

    #[test]
    fn empty_buffer_pool_returns_typed_error() {
        let dir = tempdir().unwrap();
        let device = Device::open(
            dir.path().join("data"),
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(0),
            PMT::new(),
            PageAllocator::new(),
            device,
        );
        engine
            .btree_mut()
            .insert(b"key", b"value")
            .expect("initial insert should fit");

        assert!(
            matches!(engine.flush(), Err(Error::Buffer(message)) if message.contains("no frames"))
        );
    }

    #[test]
    fn capacity_preflight_rejects_before_page_io() {
        let dir = tempdir().unwrap();
        let device = Device::open(
            dir.path().join("data"),
            &DeviceOptions {
                use_odirect: false,
                sync_writes: false,
                create: true,
            },
        )
        .unwrap();
        let mut engine = StorageEngine::new(
            BTree::new(),
            BufferManager::new(PAGE_SIZE),
            PMT::new(),
            PageAllocator::new(),
            device,
        );
        engine.btree_mut().insert(b"key", b"value").unwrap();
        engine.inject_capacity_limit(0);

        assert!(matches!(engine.flush(), Err(Error::CapacityPreflight)));
        assert_eq!(engine.device.size().unwrap(), 0);
        let stats = engine.buffer_stats();
        assert_eq!(stats.reads, 0);
        assert_eq!(stats.writes, 0);
    }
}
