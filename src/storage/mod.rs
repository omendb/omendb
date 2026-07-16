//! Storage engine coordination.
//!
//! This module coordinates the B-tree, buffer manager, PMT, allocator,
//! and device to provide persistent storage.

pub mod format;

use crate::allocator::PageAllocator;
use crate::btree::{BTree, Node, PAGE_SIZE};
use crate::buffer::{BufferManager, BufferStats, GuardAccess};
use crate::error::{Error, Result};
use crate::mvcc::PMT;
use crate::space::Device;
use std::collections::HashSet;

/// Storage engine that coordinates all components.
///
/// Provides persistent storage by serializing B-tree nodes to pages
/// and storing them through the buffer manager and device.
pub struct StorageEngine {
    /// The B-tree (logical operations).
    btree: BTree,
    /// Buffer manager (page cache).
    buffer: BufferManager,
    /// Page mapping table (page locations).
    pmt: PMT,
    /// Page allocator.
    allocator: PageAllocator,
    /// Device (file I/O).
    device: Device,
    /// Next offset for page allocation.
    next_offset: u64,
    /// Physical page offsets that are not referenced by the active generation.
    free_offsets: Vec<u64>,
    /// Offsets retired by the last flush, pending manifest publication.
    pending_reclaimed_offsets: Vec<u64>,
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
            btree,
            buffer,
            pmt,
            allocator,
            device,
            next_offset: 0,
            free_offsets: Vec::new(),
            pending_reclaimed_offsets: Vec::new(),
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
        &self.allocator
    }

    /// Get a mutable reference to the allocator.
    pub fn allocator_mut(&mut self) -> &mut PageAllocator {
        &mut self.allocator
    }

    /// Return current buffer-pool counters and derived occupancy metrics.
    pub fn buffer_stats(&self) -> BufferStats {
        self.buffer.stats()
    }

    /// Number of physical page slots available for safe reuse.
    pub fn reclaimable_page_count(&self) -> usize {
        self.free_offsets.len()
    }

    /// Make the previous generation's retired pages reusable.
    ///
    /// DB calls this only after the new manifest has been durably published.
    /// Before that point, the old generation may still be the authoritative
    /// root after a crash and its physical pages must remain untouched.
    pub fn complete_generation(&mut self) {
        self.free_offsets
            .append(&mut self.pending_reclaimed_offsets);
        self.free_offsets.sort_unstable();
        self.free_offsets.dedup();
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
        let node_count = self.btree.node_count();
        let mut retired_offsets = Vec::new();

        for page_id in 0..node_count {
            let node = self.btree.node(page_id as u32);
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
                }
                let (offset, reuses_retired_slot) = match self.free_offsets.last() {
                    Some(&offset) => (offset, true),
                    None => (self.next_offset, false),
                };

                // Stage the page through the buffer manager, then write the
                // clean flushed image to the out-of-place device version.
                let page = *persisted_node.as_bytes();
                let guard = self
                    .buffer
                    .fetch(page_id as u64, &page, GuardAccess::Write)?;
                self.buffer.frame_data_mut(&guard).copy_from_slice(&page);
                drop(guard);
                let flushed_page = self.buffer.flush(page_id as u64).ok_or_else(|| {
                    Error::Buffer(format!("page {page_id} was not dirty after staging"))
                })?;
                self.device.write_page(offset, &flushed_page)?;
                if reuses_retired_slot {
                    self.free_offsets.pop();
                } else {
                    self.next_offset += PAGE_SIZE as u64;
                }
                self.pmt.insert(page_id as u64, 0, offset);
            }
        }

        // Sync to ensure data is persisted.
        self.device.sync()?;
        self.pending_reclaimed_offsets = retired_offsets;

        Ok(())
    }

    /// Load the page versions named by a durable manifest generation.
    pub fn load_from_manifest(&mut self, root_page_id: u64) -> Result<()> {
        if self.pmt.is_empty() {
            self.next_offset = self.device.size()?;
            self.free_offsets.clear();
            self.pending_reclaimed_offsets.clear();
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

        let max_page_id = self
            .pmt
            .iter()
            .map(|(page_id, _)| page_id)
            .max()
            .ok_or_else(|| Error::Corruption("manifest PMT is unexpectedly empty".into()))?;
        if root_page_id > u32::MAX as u64 || root_page_id > max_page_id {
            return Err(Error::Corruption(format!(
                "manifest root page {root_page_id} is outside PMT"
            )));
        }

        let node_count = max_page_id
            .checked_add(1)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| Error::Corruption("manifest PMT page count overflow".into()))?;
        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            nodes.push(Node::new_leaf());
        }

        for page_id in 0..=max_page_id {
            let mapping = self
                .pmt
                .get(page_id)
                .ok_or_else(|| Error::Corruption(format!("PMT missing page {page_id}")))?;
            let mut buf = [0u8; PAGE_SIZE];
            self.device.read_page(mapping.offset, &mut buf)?;
            let guard = self.buffer.fetch(page_id, &buf, GuardAccess::Read)?;
            let buffered_page = Box::new(*self.buffer.frame_data(&guard));
            drop(guard);
            let node = Node::from_bytes(buffered_page).ok_or_else(|| {
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
            nodes[page_id as usize] = node;
        }

        self.btree = BTree::from_nodes(nodes, root_page_id as u32);
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
            self.device.read_page(offset, &mut buf)?;

            let guard = self.buffer.fetch(page_id, &buf, GuardAccess::Read)?;
            let buffered_page = Box::new(*self.buffer.frame_data(&guard));
            drop(guard);

            // Deserialize the node.
            if let Some(node) = Node::from_bytes(buffered_page) {
                if !node.verify_checksum() {
                    return Err(Error::Corruption(format!(
                        "page checksum mismatch at offset {offset}"
                    )));
                }

                // Add to the B-tree.
                self.btree.add_node(node, page_id as u32);
            }

            // Update PMT.
            self.pmt.insert(page_id, 0, offset);

            offset += PAGE_SIZE as u64;
            page_id += 1;
        }

        // Update the allocator.
        self.next_offset = offset;
        self.free_offsets.clear();
        self.pending_reclaimed_offsets.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::space::DeviceOptions;
    use tempfile::tempdir;

    #[test]
    fn buffer_stages_clean_writeback_and_reuses_clean_frame() {
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
        assert_eq!(second.reads, 1);
        assert_eq!(second.hits, 1);
        assert_eq!(second.writes, 2);
        assert_eq!(second.dirty_frames, 0);
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
}
