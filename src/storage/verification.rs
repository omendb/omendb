//! Integrity verification for PMT pages and the logical B-tree graph.
//!
//! Verification is a read-only/fail-closed boundary. It validates durable
//! page bytes, forward child edges, routing bounds, reachability, cycles, and
//! blob-pointer collection without changing publication state.

use super::StorageEngine;
use crate::btree::{BlobPointer, Node, PAGE_SIZE, ValueRef};
use crate::error::{Error, Result};
use crate::mvcc::PMT;
use std::collections::HashSet;
use std::sync::atomic::Ordering;

struct TreeVerification<'a> {
    pmt: &'a PMT,
    visited: &'a mut HashSet<u32>,
    blob_pointers: &'a mut Vec<BlobPointer>,
}

impl StorageEngine {
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

    /// Verify the logical B-tree graph rooted at the active manifest.
    ///
    /// This traverses PMT-backed pages without materializing the resident
    /// mutation tree. It validates forward child edges, routing bounds,
    /// cycles, and that every mapped logical page is reachable. Parent IDs in
    /// the page header are non-authoritative mutation hints and are therefore
    /// not part of the durable graph contract. Blob pointers are returned for
    /// validation by the owning database.
    pub fn verify_tree(&self, root_page_id: u64) -> Result<Vec<BlobPointer>> {
        self.verify_tree_with_pmt(&self.pmt, root_page_id)
    }

    /// Verify a B-tree graph rooted in an explicitly selected historical PMT.
    ///
    /// This is used before creating a late historical retention lease so a
    /// root whose physical pages have already been truncated or reused fails
    /// closed instead of becoming a durable lease to an invalid image.
    pub fn verify_tree_at(&self, root_page_id: u64, pmt: &PMT) -> Result<Vec<BlobPointer>> {
        self.verify_tree_with_pmt(pmt, root_page_id)
    }

    fn verify_tree_with_pmt(&self, pmt: &PMT, root_page_id: u64) -> Result<Vec<BlobPointer>> {
        let root = u32::try_from(root_page_id)
            .map_err(|_| Error::Corruption("root page exceeds logical ID width".into()))?;
        if pmt.is_empty() {
            if root != 0 {
                return Err(Error::Corruption(format!(
                    "empty PMT names non-zero root page {root}"
                )));
            }
            return Ok(Vec::new());
        }

        if !pmt.contains(root as u64) {
            return Err(Error::Corruption(format!(
                "root page {root} is absent from PMT"
            )));
        }
        if pmt.iter().any(|(page_id, _)| page_id > u32::MAX as u64) {
            return Err(Error::Corruption(
                "PMT contains a page ID outside the logical width".into(),
            ));
        }

        let mut visited = HashSet::new();
        let mut blob_pointers = Vec::new();
        let mut verification = TreeVerification {
            pmt,
            visited: &mut visited,
            blob_pointers: &mut blob_pointers,
        };
        self.verify_tree_node(&mut verification, root, None, None)?;

        if visited.len() != pmt.len() {
            let unreachable = pmt
                .iter()
                .map(|(page_id, _)| page_id)
                .find(|page_id| !visited.contains(&(*page_id as u32)));
            return Err(Error::Corruption(format!(
                "PMT contains unreachable page {}",
                unreachable.unwrap_or_default()
            )));
        }

        Ok(blob_pointers)
    }

    fn verify_tree_node(
        &self,
        verification: &mut TreeVerification<'_>,
        page_id: u32,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
    ) -> Result<()> {
        if !verification.visited.insert(page_id) {
            return Err(Error::Corruption(format!(
                "B-tree cycle reaches page {page_id}"
            )));
        }

        let node = self.read_node_from_pmt(verification.pmt, page_id as u64)?;
        let mut keys = Vec::with_capacity(node.count());
        for index in 0..node.count() {
            let key = node
                .key(index)
                .ok_or_else(|| Error::Corruption(format!("page {page_id} has malformed key")))?;
            if lower.as_deref().is_some_and(|bound| key.as_slice() < bound)
                || upper
                    .as_deref()
                    .is_some_and(|bound| key.as_slice() >= bound)
            {
                return Err(Error::Corruption(format!(
                    "page {page_id} key violates routing bounds"
                )));
            }
            if keys
                .last()
                .is_some_and(|previous: &Vec<u8>| previous > &key)
            {
                return Err(Error::Corruption(format!(
                    "page {page_id} keys are not ordered"
                )));
            }
            keys.push(key);
        }

        if node.is_leaf() {
            for index in 0..node.count() {
                match node.value(index) {
                    Some(ValueRef::Blob(pointer)) => verification.blob_pointers.push(pointer),
                    Some(ValueRef::Inline(_) | ValueRef::Tombstone) => {}
                    None => {
                        return Err(Error::Corruption(format!(
                            "leaf page {page_id} has malformed value"
                        )));
                    }
                }
            }
            return Ok(());
        }

        let mut children = Vec::with_capacity(node.count() + 1);
        children.push(node.leftmost_child());
        for index in 0..node.count() {
            children.push(node.child_id(index).ok_or_else(|| {
                Error::Corruption(format!("internal page {page_id} has malformed child"))
            })?);
        }

        for (index, child) in children.into_iter().enumerate() {
            let child = u32::try_from(child).map_err(|_| {
                Error::Corruption(format!("internal page {page_id} child exceeds ID width"))
            })?;
            if !verification.pmt.contains(child as u64) {
                return Err(Error::Corruption(format!(
                    "internal page {page_id} references missing child {child}"
                )));
            }
            let child_lower = if index == 0 {
                lower.clone()
            } else {
                Some(keys[index - 1].clone())
            };
            let child_upper = if index < keys.len() {
                Some(keys[index].clone())
            } else {
                upper.clone()
            };
            self.verify_tree_node(verification, child, child_lower, child_upper)?;
        }

        Ok(())
    }
}
