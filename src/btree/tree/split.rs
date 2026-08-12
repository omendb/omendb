//! B-tree split propagation and parent-hint maintenance.
//!
//! This module owns the structural mutation that turns a full leaf or
//! internal page into a wider tree: logical-page allocation, root creation,
//! parent discovery, and split propagation. `tree.rs` retains the public
//! mutation API and ordinary routing; this module owns the split lifecycle and
//! its rollback-sensitive helpers.

use super::{BTree, BTreeError, PageId};
use crate::btree::node::{BlobPointer, InsertError, Node};

impl BTree {
    /// Allocate a new node and return its page ID.
    fn alloc_node(&mut self, node: Node) -> Result<PageId, BTreeError> {
        let id = self.page_allocator.alloc();
        if id > u32::MAX as u64 {
            return Err(BTreeError::PageIdExhausted);
        }
        let id = id as PageId;
        if id as usize >= self.nodes.len() {
            self.nodes.resize_with(id as usize + 1, || None);
        }
        self.nodes[id as usize] = Some(std::sync::Arc::new(node));
        self.dirty_pages.insert(id);
        Ok(id)
    }

    /// Split a leaf node that's full and insert the key-value.
    pub(super) fn split_and_insert_leaf(
        &mut self,
        leaf_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), BTreeError> {
        let (median_key, right_node) = {
            let leaf = self.node_mut(leaf_id).expect("leaf_id should be valid");
            leaf.split().map_err(BTreeError::SplitFailed)?
        };

        let right_id = self.alloc_node(right_node)?;

        let target_id = if key >= median_key.as_slice() {
            right_id
        } else {
            leaf_id
        };

        self.node_mut(target_id)
            .expect("target_id should be valid")
            .insert(key, value)
            .map_err(BTreeError::InsertFailed)?;

        if leaf_id == self.root {
            self.create_new_root(leaf_id, &median_key, right_id)?;
        } else {
            let parent_id = self
                .parent_of(leaf_id)
                .expect("parent should exist for non-root node");
            self.node_mut(right_id)
                .expect("right node should be valid")
                .set_parent_id(parent_id);
            self.insert_into_internal(parent_id, &median_key, right_id)?;
        }

        Ok(())
    }

    /// Split a full leaf and insert a blob pointer, preserving the same
    /// parent/root propagation rules as inline-value insertion.
    pub(super) fn split_and_insert_blob_leaf(
        &mut self,
        leaf_id: PageId,
        key: &[u8],
        ptr: BlobPointer,
    ) -> Result<(), BTreeError> {
        let (median_key, right_node) = {
            let leaf = self.node_mut(leaf_id).expect("leaf_id should be valid");
            leaf.split().map_err(BTreeError::SplitFailed)?
        };

        let right_id = self.alloc_node(right_node)?;
        let target_id = if key >= median_key.as_slice() {
            right_id
        } else {
            leaf_id
        };

        self.node_mut(target_id)
            .expect("target_id should be valid")
            .insert_blob(key, ptr)
            .map_err(BTreeError::InsertFailed)?;

        if leaf_id == self.root {
            self.create_new_root(leaf_id, &median_key, right_id)?;
        } else {
            let parent_id = self
                .parent_of(leaf_id)
                .expect("parent should exist for non-root node");
            self.node_mut(right_id)
                .expect("right node should be valid")
                .set_parent_id(parent_id);
            self.insert_into_internal(parent_id, &median_key, right_id)?;
        }

        Ok(())
    }

    /// Create a new root with two children.
    fn create_new_root(
        &mut self,
        left_id: PageId,
        key: &[u8],
        right_id: PageId,
    ) -> Result<(), BTreeError> {
        let mut new_root = Node::new_internal();

        // For internal nodes, child_id(i) is the child AFTER key_i.
        // The leftmost child (before key 0) is stored in leftmost_child.
        new_root.set_leftmost_child(left_id as u64);
        new_root
            .insert_child(key, right_id as u64)
            .map_err(BTreeError::InsertFailed)?;

        let new_root_id = self.alloc_node(new_root)?;

        self.node_mut(left_id)
            .expect("left_id should be valid")
            .set_parent_id(new_root_id);
        self.node_mut(right_id)
            .expect("right_id should be valid")
            .set_parent_id(new_root_id);

        self.root = new_root_id;
        Ok(())
    }

    /// Find the parent of a given node (by DFS).
    fn find_parent(&self, current: PageId, target: PageId) -> Option<PageId> {
        if current == target {
            return None;
        }

        let node = self.node(current)?;
        if node.is_leaf() {
            return None;
        }

        let leftmost_child = node.leftmost_child() as u32;
        if leftmost_child == target {
            return Some(current);
        }
        if let Some(parent) = self.find_parent(leftmost_child, target) {
            return Some(parent);
        }

        for i in 0..node.count() {
            if let Some(child_id) = node.child_id(i) {
                if child_id as u32 == target {
                    return Some(current);
                }
                if let Some(parent) = self.find_parent(child_id as u32, target) {
                    return Some(parent);
                }
            }
        }

        None
    }

    /// Return a node's recorded parent when it points back to this child,
    /// otherwise find the structural parent.
    ///
    /// Parent IDs are a cache used for persistence and diagnostics, but split
    /// propagation must not trust a stale cache entry: an existing internal
    /// page is not necessarily the page that currently references `node_id`.
    /// Validating the edge preserves the O(1) common path while keeping the
    /// structural walk as a safe fallback for legacy or malformed metadata.
    fn parent_of(&self, node_id: PageId) -> Option<PageId> {
        if node_id == self.root {
            return None;
        }

        if let Some(recorded_parent) = self.node(node_id).map(Node::parent_id)
            && recorded_parent != 0
            && self
                .node(recorded_parent)
                .is_some_and(|parent| self.references_child(parent, node_id))
        {
            return Some(recorded_parent);
        }

        self.find_parent(self.root, node_id)
    }

    fn references_child(&self, parent: &Node, child_id: PageId) -> bool {
        parent.leftmost_child() == child_id as u64
            || (0..parent.count()).any(|index| parent.child_id(index) == Some(child_id as u64))
    }

    /// Insert a key and right child into an internal node.
    fn insert_into_internal(
        &mut self,
        parent_id: PageId,
        key: &[u8],
        right_child_id: PageId,
    ) -> Result<(), BTreeError> {
        let result = self
            .node_mut(parent_id)
            .expect("parent_id should be valid")
            .insert_child(key, right_child_id as u64);

        match result {
            Ok(()) => Ok(()),
            Err(InsertError::PageFull) => self.split_internal(parent_id, key, right_child_id),
            Err(e) => Err(BTreeError::InsertFailed(e)),
        }
    }

    /// Split an internal node and insert the key.
    fn split_internal(
        &mut self,
        node_id: PageId,
        key: &[u8],
        right_child_id: PageId,
    ) -> Result<(), BTreeError> {
        let (median_key, right_node) = {
            let node = self.node_mut(node_id).expect("node_id should be valid");
            node.split().map_err(BTreeError::SplitFailed)?
        };

        let right_id = self.alloc_node(right_node)?;
        self.set_direct_children_parent(right_id, right_id)?;

        let target_id = if key >= median_key.as_slice() {
            right_id
        } else {
            node_id
        };

        self.node_mut(target_id)
            .expect("target_id should be valid")
            .insert_child(key, right_child_id as u64)
            .map_err(BTreeError::InsertFailed)?;
        self.node_mut(right_child_id)
            .ok_or(BTreeError::MissingPage(right_child_id))?
            .set_parent_id(target_id);

        if node_id == self.root {
            self.create_new_root(node_id, &median_key, right_id)?;
        } else {
            let parent_id = self
                .parent_of(node_id)
                .expect("parent should exist for non-root node");
            self.node_mut(right_id)
                .expect("right node should be valid")
                .set_parent_id(parent_id);
            self.insert_into_internal(parent_id, &median_key, right_id)?;
        }

        Ok(())
    }

    fn set_direct_children_parent(
        &mut self,
        node_id: PageId,
        parent_id: PageId,
    ) -> Result<(), BTreeError> {
        let children =
            {
                let node = self.node(node_id).ok_or(BTreeError::MissingPage(node_id))?;
                if node.is_leaf() {
                    return Ok(());
                }
                let mut children = Vec::with_capacity(node.count() + 1);
                children.push(node.leftmost_child());
                for index in 0..node.count() {
                    children.push(node.child_id(index).ok_or_else(|| {
                        BTreeError::Corruption("internal child is malformed".into())
                    })?);
                }
                children
            };
        for child in children {
            let child = u32::try_from(child).map_err(|_| {
                BTreeError::Corruption("internal child page ID exceeds width".into())
            })?;
            // Parent IDs are non-authoritative hints. A sparse tree may not
            // have the moved child resident; its forward edge is persisted by
            // the new internal node and the hint can be repaired if/when the
            // child is loaded for a later mutation.
            if let Some(child_node) = self.node_mut(child) {
                child_node.set_parent_id(parent_id);
            }
        }
        Ok(())
    }

    /// Remove an existing entry and insert its replacement, splitting the
    /// leaf when the resized value no longer fits.
    pub(super) fn replace_full_entry_with_split(
        &mut self,
        leaf_id: PageId,
        index: usize,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), BTreeError> {
        let node = self.node_mut(leaf_id).expect("leaf_id should be valid");
        node.remove_entry(index).map_err(BTreeError::InsertFailed)?;
        while let Ok(duplicate_index) = node.search(key) {
            node.remove_entry(duplicate_index)
                .map_err(BTreeError::InsertFailed)?;
        }

        match self
            .node_mut(leaf_id)
            .expect("leaf_id should be valid")
            .insert(key, value)
        {
            Ok(()) => Ok(()),
            Err(InsertError::PageFull) => self.split_and_insert_leaf(leaf_id, key, value),
            Err(error) => Err(BTreeError::InsertFailed(error)),
        }
    }
}
