//! PMT-backed immutable page reads and generation-bound read handles.
//!
//! This module owns the read-only resource boundary: decoding versioned
//! pages, serving the parsed immutable-node cache, and releasing the
//! generation pin held by `StorageReadView`. `StorageEngine` retains logical
//! lookup/range merging and mutation, publication, and reclamation
//! coordination.

use super::StorageEngine;
use super::page_cache::ParsedPageCache;
use crate::allocator::PageAllocator;
use crate::btree::{BTree, LookupResult, Node, PAGE_SIZE};
use crate::buffer::BufferManager;
use crate::buffer::{GuardAccess, PageCacheKey};
use crate::error::{Error, Result};
use crate::mvcc::PMT;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

/// An immutable, generation-bound read handle.
///
/// The PMT is shared copy-on-write with the writer and the device descriptor
/// is cloned for independent positioned reads. Creating this handle therefore
/// does not copy the PMT or acquire the writer's buffer-pool mutex during any
/// subsequent lookup.
pub(crate) struct StorageReadView {
    engine: StorageEngine,
    root_page_id: u64,
}

impl StorageReadView {
    pub(super) fn new(engine: StorageEngine, root_page_id: u64) -> Self {
        Self {
            engine,
            root_page_id,
        }
    }

    pub(crate) fn lookup(&self, key: &[u8]) -> Result<LookupResult> {
        self.engine
            .lookup_at(self.root_page_id, self.engine.pmt(), key)
    }

    pub(crate) fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, LookupResult)>> {
        self.engine
            .range_at(self.root_page_id, self.engine.pmt(), start, end)
    }
}

impl StorageEngine {
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
            metrics: super::StorageCounters::default(),
        };
        Ok(StorageReadView::new(engine, root_page_id))
    }
}

impl Drop for StorageReadView {
    fn drop(&mut self) {
        if let Ok(mut leases) = self.engine.protected_pmts.lock()
            && let Some(index) = leases
                .iter()
                .position(|pmt| Arc::ptr_eq(pmt, &self.engine.pmt))
        {
            leases.swap_remove(index);
            self.engine.reclamation_dirty.store(true, Ordering::Release);
        }
    }
}

impl StorageEngine {
    /// Read and validate one logical page through the active PMT and buffer.
    ///
    /// This page-oriented seam owns physical location and checksum checks so
    /// callers cannot bypass the PMT with raw device offsets.
    pub fn read_node(&self, page_id: u64) -> Result<Node> {
        self.read_node_from_pmt(&self.pmt, page_id)
    }

    /// Read and validate one logical page through an explicitly selected PMT.
    ///
    /// Retained generations use this same device and versioned buffer-cache
    /// boundary rather than reopening a second database directory. The PMT
    /// mapping version is part of the cache key, so historical and current
    /// page versions cannot alias in the buffer pool.
    pub(super) fn read_node_from_pmt(&self, pmt: &PMT, page_id: u64) -> Result<Node> {
        Ok((*self.read_node_arc_from_pmt(pmt, page_id)?).clone())
    }

    /// Read and validate one immutable logical page, reusing its parsed node
    /// while the PMT physical version remains unchanged.
    pub(super) fn read_node_arc_from_pmt(&self, pmt: &PMT, page_id: u64) -> Result<Arc<Node>> {
        self.metrics
            .logical_page_reads
            .fetch_add(1, Ordering::Relaxed);
        let mapping = *pmt
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

        let page_key = PageCacheKey::new(page_id, mapping.version);
        if let Some(node) = self
            .parsed_page_cache
            .lock()
            .map_err(|_| Error::Buffer("parsed page cache mutex is poisoned".into()))?
            .get(page_key)
        {
            self.metrics
                .parsed_page_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(node);
        }

        let mut buf = [0u8; PAGE_SIZE];
        if !self.buffer_lock()?.is_resident_key(page_key) {
            // An immutable PMT-versioned page can be served from the cache
            // without re-stat'ing the data file. Keep the extent check on the
            // physical-miss path so an uncached truncated page still fails
            // closed before Device::read_page.
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
        let node = Arc::new(node);
        self.parsed_page_cache
            .lock()
            .map_err(|_| Error::Buffer("parsed page cache mutex is poisoned".into()))?
            .insert(page_key, Arc::clone(&node));
        Ok(node)
    }
}
