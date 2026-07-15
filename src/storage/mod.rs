//! Storage engine coordination.
//!
//! This module coordinates the B-tree, buffer manager, PMT, allocator,
//! and device to provide persistent storage.

use crate::btree::{BTree, Node, PAGE_SIZE};
use crate::buffer::BufferManager;
use crate::error::{Error, Result};
use crate::mvcc::PMT;
use crate::space::Device;
use crate::allocator::PageAllocator;

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

    /// Flush all dirty pages to disk.
    pub fn flush(&mut self) -> Result<()> {
        // For now, serialize all nodes to pages and write to disk.
        // In the full implementation, this would only flush dirty pages.
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

                // Allocate a page offset if not already mapped.
                let offset = if let Some(mapping) = self.pmt.get(page_id as u64) {
                    mapping.offset
                } else {
                    let offset = self.next_offset;
                    self.next_offset += PAGE_SIZE as u64;
                    self.pmt.insert(page_id as u64, 0, offset);
                    offset
                };

                // Write the page to the device.
                self.device.write_page(offset, persisted_node.as_bytes())?;
            }
        }

        // Sync to ensure data is persisted.
        self.device.sync()?;

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
