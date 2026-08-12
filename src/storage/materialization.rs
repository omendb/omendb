//! Storage bootstrap and lazy-to-resident materialization.
//!
//! This module owns the transitions between a PMT-selected immutable
//! generation, a sparse mutation overlay, and a fully materialized B-tree.
//! `StorageEngine` remains the authority for the B-tree, PMT, allocator,
//! device, buffer, and reclamation state that those transitions update.

use super::StorageEngine;
use crate::btree::{BTree, BTreeError, Node, PAGE_SIZE};
use crate::buffer::{GuardAccess, PageCacheKey};
use crate::error::{Error, Result};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl StorageEngine {
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
    /// This legacy bootstrap path is retained for stores without a manifest.
    /// Manifest-backed stores use `load_from_manifest` and stay lazy until a
    /// mutation or recovery operation requires materialization.
    pub fn load_from_disk(&mut self) -> Result<()> {
        let device_size = self.device.size()?;

        if device_size == 0 {
            return Ok(());
        }

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

            let node = Node::from_bytes(buffered_page)
                .ok_or_else(|| Error::Corruption(format!("invalid page at offset {offset}")))?;
            if !node.verify_checksum() {
                return Err(Error::Corruption(format!(
                    "page checksum mismatch at offset {offset}"
                )));
            }

            self.btree.add_node(node, page_id as u32);

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
