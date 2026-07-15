//! Storage engine coordination.
//!
//! This module coordinates the B-tree, buffer manager, PMT, allocator,
//! and device to provide persistent storage.

pub mod format;

use crate::allocator::PageAllocator;
use crate::btree::{BTree, Node, PAGE_SIZE};
use crate::buffer::BufferManager;
use crate::error::{Error, Result};
use crate::mvcc::PMT;
use crate::space::Device;

/// Storage engine that coordinates all components.
///
/// Provides persistent storage by serializing B-tree nodes to pages
/// and storing them through the buffer manager and device.
pub struct StorageEngine {
    /// The B-tree (logical operations).
    btree: BTree,
    /// Buffer manager (page cache).
    #[expect(dead_code)]
    buffer: BufferManager,
    /// Page mapping table (page locations).
    pmt: PMT,
    /// Page allocator.
    allocator: PageAllocator,
    /// Device (file I/O).
    device: Device,
    /// Next offset for page allocation.
    next_offset: u64,
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

    /// Get mutable access to the page mapping table for recovery.
    pub fn pmt_mut(&mut self) -> &mut PMT {
        &mut self.pmt
    }

    /// Inject one device sync failure for publication-boundary tests.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn inject_sync_failure(&self) {
        self.device.inject_sync_failure();
    }

    /// Flush all dirty pages to disk.
    pub fn flush(&mut self) -> Result<()> {
        // The bootstrap path rewrites the complete logical tree into a new
        // physical generation. It is coarse, but preserves the out-of-place
        // invariant needed by manifest publication.
        let node_count = self.btree.node_count();

        for page_id in 0..node_count {
            let node = self.btree.node(page_id as u32);
            if let Some(node) = node {
                // B-tree mutations do not maintain the persisted checksum in
                // place. Rebuild a temporary node so every page written has a
                // checksum that covers its final bytes.
                let page = Box::new(*node.as_bytes());
                let mut persisted_node = Node::from_bytes(page).ok_or_else(|| {
                    Error::Corruption(format!("invalid node page {page_id} before flush"))
                })?;
                persisted_node.update_checksum();

                // Every flush creates a new physical version. DB publishes
                // the resulting PMT only after all data is durable.
                let offset = self.next_offset;

                // Write the page to the device.
                self.device.write_page(offset, persisted_node.as_bytes())?;
                self.next_offset += PAGE_SIZE as u64;
                self.pmt.insert(page_id as u64, 0, offset);
            }
        }

        // Sync to ensure data is persisted.
        self.device.sync()?;

        Ok(())
    }

    /// Load the page versions named by a durable manifest generation.
    pub fn load_from_manifest(&mut self, root_page_id: u64) -> Result<()> {
        if self.pmt.is_empty() {
            self.next_offset = self.device.size()?;
            return Ok(());
        }

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
            let node = Node::from_bytes(Box::new(buf)).ok_or_else(|| {
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
        self.next_offset = self.device.size()?;
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
            self.device.read_page(offset, &mut buf)?;

            // Deserialize the node.
            if let Some(node) = Node::from_bytes(Box::new(buf)) {
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

        Ok(())
    }
}
