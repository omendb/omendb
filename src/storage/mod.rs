//! Storage engine coordination.
//!
//! This module coordinates the B-tree, buffer manager, PMT, allocator,
//! and device to provide persistent storage.

pub mod format;

use crate::allocator::PageAllocator;
use crate::btree::{BTree, BTreeError, LookupResult, Node, ValueRef, PAGE_SIZE};
use crate::buffer::{BufferManager, BufferStats, GuardAccess, PageCacheKey};
use crate::error::{Error, Result};
use crate::mvcc::PMT;
use crate::space::Device;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Mutex, MutexGuard};
use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative physical work performed by one storage-engine handle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageMetrics {
    /// Logical page reads requested through the PMT-backed page seam.
    pub logical_page_reads: u64,
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

/// Storage engine that coordinates all components.
///
/// Provides persistent storage by serializing B-tree nodes to pages
/// and storing them through the buffer manager and device.
pub struct StorageEngine {
    /// The B-tree (logical operations).
    btree: BTree,
    /// Buffer manager (page cache).
    buffer: Mutex<BufferManager>,
    /// Page mapping table (page locations).
    pmt: PMT,
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
    /// Root of the immutable generation when its B-tree pages are still
    /// available through the PMT-backed lazy read path.
    lazy_root: Option<u32>,
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
        Self {
            btree: btree.with_page_allocator(allocator),
            buffer: Mutex::new(buffer),
            pmt,
            device,
            next_offset: 0,
            free_offsets: Vec::new(),
            pending_reclaimed_offsets: Vec::new(),
            pending_reclaimed_cache_keys: Vec::new(),
            lazy_root: None,
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
        &self.pmt
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

    /// Make the previous generation's retired pages reusable.
    ///
    /// DB calls this only after the new manifest has been durably published.
    /// Before that point, the old generation may still be the authoritative
    /// root after a crash and its physical pages must remain untouched.
    pub fn complete_generation(&mut self) {
        let reclaimed_pages = self.pending_reclaimed_offsets.len() as u64;
        self.metrics
            .reclaimed_pages
            .fetch_add(reclaimed_pages, Ordering::Relaxed);
        self.metrics.reclaimed_bytes.fetch_add(
            reclaimed_pages.saturating_mul(PAGE_SIZE as u64),
            Ordering::Relaxed,
        );
        self.free_offsets
            .append(&mut self.pending_reclaimed_offsets);
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

    /// Read and validate one logical page through the active PMT and buffer.
    ///
    /// This is the page-oriented seam used by manifest loading today and by
    /// the future on-demand B-tree path. It deliberately owns all physical
    /// location and checksum checks instead of allowing callers to bypass the
    /// PMT with raw device offsets.
    pub fn read_node(&self, page_id: u64) -> Result<Node> {
        self.metrics
            .logical_page_reads
            .fetch_add(1, Ordering::Relaxed);
        let mapping = *self
            .pmt
            .get(page_id)
            .ok_or_else(|| Error::Corruption(format!("PMT missing page {page_id}")))?;
        if mapping.file_id != 0 {
            return Err(Error::Corruption(format!(
                "page {page_id} references unsupported file {}",
                mapping.file_id
            )));
        }
        if !mapping.offset.is_multiple_of(PAGE_SIZE as u64) {
            return Err(Error::Corruption(format!(
                "page {page_id} has unaligned offset {}",
                mapping.offset
            )));
        }

        let device_size = self.device.size()?;
        let end = mapping
            .offset
            .checked_add(PAGE_SIZE as u64)
            .ok_or_else(|| Error::Corruption(format!("page {page_id} offset overflows")))?;
        if end > device_size {
            return Err(Error::Corruption(format!(
                "page {page_id} at offset {} exceeds data file size {device_size}",
                mapping.offset
            )));
        }

        let page_key = PageCacheKey::new(page_id, mapping.version);
        let mut buf = [0u8; PAGE_SIZE];
        if !self.buffer_lock()?.is_resident_key(page_key) {
            self.metrics
                .physical_page_reads
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .page_bytes_read
                .fetch_add(PAGE_SIZE as u64, Ordering::Relaxed);
            self.device.read_page(mapping.offset, &mut buf)?;
        }
        let buffered_page = {
            let mut buffer = self.buffer_lock()?;
            let guard = buffer.fetch_key(page_key, &buf, GuardAccess::Read)?;
            let page = *buffer.frame_data(&guard);
            drop(guard);
            Box::new(page)
        };

        let node = Node::from_bytes(buffered_page)
            .ok_or_else(|| Error::Corruption(format!("invalid page {page_id}")))?;
        if !node.verify_checksum() {
            return Err(Error::Corruption(format!(
                "page checksum mismatch for page {page_id}"
            )));
        }
        Ok(node)
    }

    /// Get mutable access to the page mapping table for recovery.
    pub fn pmt_mut(&mut self) -> &mut PMT {
        &mut self.pmt
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

    /// Flush all dirty pages to disk.
    pub fn flush(&mut self) -> Result<()> {
        // The bootstrap path rewrites the complete logical tree into a new
        // physical generation. It is coarse, but preserves the out-of-place
        // invariant needed by manifest publication.
        let dirty_page_ids = self.btree.dirty_page_ids();
        self.preflight_flush_capacity(&dirty_page_ids)?;
        let mut retired_offsets = Vec::new();
        let mut retired_cache_keys = Vec::new();

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
                    retired_cache_keys.push(PageCacheKey::new(
                        page_id as u64,
                        mapping.version,
                    ));
                }
                let (offset, reuses_retired_slot) = match self.free_offsets.last() {
                    Some(&offset) => (offset, true),
                    None => (self.next_offset, false),
                };

                // Stage the page through the buffer manager, then write the
                // clean flushed image to the out-of-place device version.
                let page = *persisted_node.as_bytes();
                let pending_key = PageCacheKey::unversioned(page_id as u64);
                let flushed_page = {
                    let mut buffer = self.buffer_lock()?;
                    let guard = buffer.fetch_key(pending_key, &page, GuardAccess::Write)?;
                    buffer.frame_data_mut(&guard).copy_from_slice(&page);
                    drop(guard);
                    buffer.flush_key(pending_key).ok_or_else(|| {
                        Error::Buffer(format!("page {page_id} was not dirty after staging"))
                    })?
                };
                self.device.write_page(offset, &flushed_page)?;
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
                self.pmt.insert(page_id as u64, 0, offset);
                let version = self
                    .pmt
                    .get(page_id as u64)
                    .ok_or_else(|| Error::Corruption("PMT insertion was lost".into()))?
                    .version;
                self.buffer_lock()?.rekey(
                    pending_key,
                    PageCacheKey::new(page_id as u64, version),
                );
            }
        }

        // Sync to ensure data is persisted.
        self.device.sync()?;
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
    /// data-file slots can fail this preflight. Real filesystem ENOSPC can
    /// still occur during the write itself and remains fenced/recoverable.
    fn preflight_flush_capacity(&self, dirty_page_ids: &[u32]) -> Result<()> {
        let mut free_index = self.free_offsets.len();
        let mut next_offset = self.next_offset;
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
                offset
            };
            if let Err(error) = self.device.check_write_capacity(offset) {
                self.metrics
                    .capacity_preflight_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(Error::from(error));
            }
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

                let mut child_id = node.leftmost_child();
                for index in 0..node.count() {
                    let separator = node.key(index).ok_or_else(|| {
                        Error::Corruption("internal mutation key is malformed".into())
                    })?;
                    if key < separator.as_slice() {
                        break;
                    }
                    child_id = node.child_id(index).ok_or_else(|| {
                        Error::Corruption("internal mutation child is malformed".into())
                    })?;
                }
                u32::try_from(child_id).map_err(|_| {
                    Error::Corruption("internal mutation child exceeds logical ID width".into())
                })?
            };
            current = next;
        }
    }

    /// Look up a key through either the resident mutation tree or the
    /// PMT-backed lazy generation selected at reopen.
    pub fn lookup(&self, key: &[u8]) -> Result<LookupResult> {
        if let Some(root_page_id) = self.lazy_root {
            if self.btree.dirty_page_ids().is_empty() {
                return self.lookup_lazy(root_page_id, key);
            }
            match self.btree.lookup(key) {
                Ok(result) => Ok(result),
                Err(BTreeError::MissingPage(_)) => self.lookup_lazy(root_page_id, key),
                Err(error) => Err(error.into()),
            }
        } else {
            self.btree.lookup(key).map_err(Error::from)
        }
    }

    /// Scan a key range through either the resident mutation tree or the
    /// PMT-backed lazy generation selected at reopen.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, LookupResult)>> {
        if let Some(root_page_id) = self.lazy_root {
            let base = self.range_lazy(root_page_id, start, end)?;
            if self.btree.dirty_page_ids().is_empty() {
                return Ok(base);
            }
            let mut merged: BTreeMap<Vec<u8>, LookupResult> = base.into_iter().collect();
            for (key, value) in self.btree.dirty_leaf_entries(start, end)? {
                match value {
                    LookupResult::Found(_) | LookupResult::Blob(_) => {
                        merged.insert(key, value);
                    }
                    LookupResult::Deleted | LookupResult::NotFound => {
                        merged.remove(&key);
                    }
                }
            }
            Ok(merged.into_iter().collect())
        } else {
            self.btree
                .range_scan(start, end)
                .map_err(Error::from)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Error::from)
        }
    }

    fn lookup_lazy(&self, root_page_id: u32, key: &[u8]) -> Result<LookupResult> {
        let leaf_id = self.find_leaf_page(root_page_id, key)?;
        let node = self.read_node(leaf_id as u64)?;
        Ok(match node.search(key) {
            Ok(index) => match node.value(index) {
                Some(ValueRef::Inline(value)) => LookupResult::Found(value.to_vec()),
                Some(ValueRef::Blob(pointer)) => LookupResult::Blob(pointer),
                Some(ValueRef::Tombstone) => LookupResult::Deleted,
                None => {
                    return Err(Error::Corruption(
                        "lazy leaf value payload is malformed".into(),
                    ));
                }
            },
            Err(_) => LookupResult::NotFound,
        })
    }

    fn range_lazy(
        &self,
        root_page_id: u32,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, LookupResult)>> {
        let mut results = Vec::new();
        let mut current = self.find_leaf_page(root_page_id, start)?;
        let mut first_leaf = true;
        let mut previous_key = None;

        loop {
            let node = self.read_node(current as u64)?;
            let mut index = if first_leaf {
                first_leaf = false;
                match node.search(start) {
                    Ok(index) | Err(index) => index,
                }
            } else {
                0
            };
            while index < node.count() {
                let key = node
                    .key(index)
                    .ok_or_else(|| Error::Corruption("lazy range key is malformed".into()))?;
                if key.as_slice() >= end {
                    return Ok(results);
                }
                index += 1;

                if key.as_slice() < start {
                    continue;
                }
                if previous_key.as_deref() == Some(key.as_slice()) {
                    continue;
                }
                previous_key = Some(key.clone());

                match node.value(index - 1) {
                    Some(ValueRef::Inline(value)) => {
                        results.push((key, LookupResult::Found(value.to_vec())))
                    }
                    Some(ValueRef::Blob(pointer)) => {
                        results.push((key, LookupResult::Blob(pointer)))
                    }
                    Some(ValueRef::Tombstone) => {}
                    None => {
                        return Err(Error::Corruption(
                            "lazy range value payload is malformed".into(),
                        ));
                    }
                }
            }

            let Some(next) = self.next_leaf_page(root_page_id, current)? else {
                return Ok(results);
            };
            current = next;
        }
    }

    fn find_leaf_page(&self, root_page_id: u32, key: &[u8]) -> Result<u32> {
        let mut current = root_page_id;
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(current) {
                return Err(Error::Corruption(
                    "cycle detected during lazy B-tree descent".into(),
                ));
            }
            let node = self.read_node(current as u64)?;
            if node.is_leaf() {
                return Ok(current);
            }
            let mut child_id = node.leftmost_child();
            for index in 0..node.count() {
                let separator = node
                    .key(index)
                    .ok_or_else(|| Error::Corruption("lazy internal key is malformed".into()))?;
                if key < separator.as_slice() {
                    break;
                }
                child_id = node.child_id(index).ok_or_else(|| {
                    Error::Corruption("lazy internal child is malformed".into())
                })?;
            }
            current = u32::try_from(child_id).map_err(|_| {
                Error::Corruption("lazy internal child ID exceeds logical width".into())
            })?;
        }
    }

    fn find_path_to_leaf_page(
        &self,
        current: u32,
        target: u32,
        path: &mut Vec<(u32, usize)>,
        active: &mut HashSet<u32>,
    ) -> Result<bool> {
        if !active.insert(current) {
            return Err(Error::Corruption(
                "cycle detected during lazy range traversal".into(),
            ));
        }

        let result = (|| {
            let node = self.read_node(current as u64)?;
            if node.is_leaf() {
                return Ok(current == target);
            }
            let mut children = Vec::with_capacity(node.count() + 1);
            children.push(u32::try_from(node.leftmost_child()).map_err(|_| {
                Error::Corruption("lazy internal child ID exceeds logical width".into())
            })?);
            for index in 0..node.count() {
                let child = node.child_id(index).ok_or_else(|| {
                    Error::Corruption("lazy internal child is malformed".into())
                })?;
                children.push(u32::try_from(child).map_err(|_| {
                    Error::Corruption("lazy internal child ID exceeds logical width".into())
                })?);
            }
            for (position, child) in children.into_iter().enumerate() {
                path.push((current, position));
                if self.find_path_to_leaf_page(child, target, path, active)? {
                    return Ok(true);
                }
                path.pop();
            }
            Ok(false)
        })();
        active.remove(&current);
        result
    }

    fn leftmost_leaf_page(&self, mut current: u32) -> Result<Option<u32>> {
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(current) {
                return Err(Error::Corruption(
                    "cycle detected during lazy next-leaf traversal".into(),
                ));
            }
            let node = self.read_node(current as u64)?;
            if node.is_leaf() {
                return Ok(Some(current));
            }
            current = u32::try_from(node.leftmost_child()).map_err(|_| {
                Error::Corruption("lazy internal child ID exceeds logical width".into())
            })?;
        }
    }

    fn next_leaf_page(&self, root_page_id: u32, target: u32) -> Result<Option<u32>> {
        let mut path = Vec::new();
        let mut active = HashSet::new();
        if !self.find_path_to_leaf_page(root_page_id, target, &mut path, &mut active)? {
            return Err(Error::Corruption(
                "lazy range leaf is not reachable from root".into(),
            ));
        }
        for (parent_id, child_position) in path.into_iter().rev() {
            let parent = self.read_node(parent_id as u64)?;
            if child_position < parent.count() {
                let child = parent.child_id(child_position).ok_or_else(|| {
                    Error::Corruption("lazy internal child is malformed".into())
                })?;
                return self.leftmost_leaf_page(u32::try_from(child).map_err(|_| {
                    Error::Corruption("lazy internal child ID exceeds logical width".into())
                })?);
            }
        }
        Ok(None)
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
            return Ok(());
        }

        let device_size = self.device.size()?;
        let active_offsets: HashSet<_> =
            self.pmt.iter().map(|(_, mapping)| mapping.offset).collect();
        self.free_offsets = (0..device_size)
            .step_by(PAGE_SIZE)
            .filter(|offset| !active_offsets.contains(offset))
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
        Ok(())
    }

    /// Verify every active PMT page and its checksum without changing the
    /// logical tree or publication state.
    pub fn verify_pages(&mut self, root_page_id: u64) -> Result<(u64, u64)> {
        let device_size = self.device.size()?;
        if self.pmt.is_empty() {
            if root_page_id != 0 {
                return Err(Error::Corruption(format!(
                    "empty PMT names non-zero root page {root_page_id}"
                )));
            }
            return Ok((0, device_size));
        }

        let max_page_id = self
            .pmt
            .iter()
            .map(|(page_id, _)| page_id)
            .max()
            .ok_or_else(|| Error::Corruption("PMT unexpectedly has no maximum page".into()))?;
        if root_page_id > u32::MAX as u64 || root_page_id > max_page_id {
            return Err(Error::Corruption(format!(
                "root page {root_page_id} is outside PMT"
            )));
        }

        let mut verified_pages = 0u64;
        for page_id in 0..=max_page_id {
            let mapping = self
                .pmt
                .get(page_id)
                .ok_or_else(|| Error::Corruption(format!("PMT missing page {page_id}")))?;
            if !mapping.offset.is_multiple_of(PAGE_SIZE as u64) {
                return Err(Error::Corruption(format!(
                    "page {page_id} has unaligned offset {}",
                    mapping.offset
                )));
            }
            let end = mapping
                .offset
                .checked_add(PAGE_SIZE as u64)
                .ok_or_else(|| Error::Corruption(format!("page {page_id} offset overflows")))?;
            if end > device_size {
                return Err(Error::Corruption(format!(
                    "page {page_id} at offset {} exceeds data file size {device_size}",
                    mapping.offset
                )));
            }

            let mut page = [0u8; PAGE_SIZE];
            self.metrics
                .physical_page_reads
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .page_bytes_read
                .fetch_add(PAGE_SIZE as u64, Ordering::Relaxed);
            self.device.read_page(mapping.offset, &mut page)?;
            let node = Node::from_bytes(Box::new(page)).ok_or_else(|| {
                Error::Corruption(format!(
                    "invalid page {page_id} at offset {}",
                    mapping.offset
                ))
            })?;
            if !node.verify_checksum() {
                return Err(Error::Corruption(format!(
                    "page checksum mismatch at offset {}",
                    mapping.offset
                )));
            }
            verified_pages += 1;
        }

        Ok((verified_pages, device_size))
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
            let node = Node::from_bytes(buffered_page).ok_or_else(|| {
                Error::Corruption(format!("invalid page at offset {offset}"))
            })?;
            if !node.verify_checksum() {
                return Err(Error::Corruption(format!(
                    "page checksum mismatch at offset {offset}"
                )));
            }

            // Add to the B-tree.
            self.btree.add_node(node, page_id as u32);

            // Update PMT.
            self.pmt.insert(page_id, 0, offset);
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert!(engine.buffer_stats().hits >= 2);
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

        assert!(matches!(engine.flush(), Err(Error::DiskFull)));
        assert_eq!(engine.device.size().unwrap(), 0);
        let stats = engine.buffer_stats();
        assert_eq!(stats.reads, 0);
        assert_eq!(stats.writes, 0);
    }
}
