//! B-tree operations: insert, lookup, delete, range scan.
//!
//! This module implements a B-tree that operates on nodes. For now it uses
//! an in-memory store; later it will be integrated with the buffer manager.
//!
//! # Design
//!
//! - **Insert**: Find leaf, insert key-value, split if full, propagate splits up
//! - **Lookup**: Traverse from root to leaf, binary search at each level
//! - **Delete**: Find leaf, mark tombstone, merge if underfull
//! - **Range scan**: Cursor-based forward/reverse iteration
//!
//! # Out-of-Place Writes
//!
//! In the full implementation, "modify" operations create new page versions.
//! For now, we mutate nodes directly. The PMT integration comes later.

use crate::allocator::PageAllocator;
use crate::btree::node::{BlobPointer, InsertError, Node, SplitError, ValueRef};
use std::collections::HashSet;

/// Page ID type (index into the page store).
pub type PageId = u32;

/// Result of a lookup operation.
#[derive(Debug)]
pub enum LookupResult<'a> {
    /// Key found with inline value.
    Found(&'a [u8]),
    /// Key found with blob pointer.
    Blob(crate::btree::node::BlobPointer),
    /// Key is deleted (tombstone).
    Deleted,
    /// Key not found.
    NotFound,
}

/// A simple in-memory B-tree.
///
/// This is the core B-tree logic. In the full implementation, this will be
/// backed by the buffer manager and PMT. For now, it stores nodes in a Vec.
pub struct BTree {
    /// All nodes stored in memory. Index 0 is always the root.
    nodes: Vec<Node>,
    /// Page ID of the root node.
    root: PageId,
    /// Logical pages that may have changed since the last published generation.
    dirty_pages: HashSet<PageId>,
    /// Single owner of stable logical page IDs used by this tree.
    page_allocator: PageAllocator,
}

impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

impl BTree {
    /// Create a new empty B-tree with a single leaf root.
    pub fn new() -> Self {
        let root = Node::new_leaf();
        Self {
            nodes: vec![root],
            root: 0,
            dirty_pages: HashSet::from([0]),
            page_allocator: PageAllocator::new(),
        }
    }

    /// Create a B-tree from existing nodes (for loading from disk).
    pub fn from_nodes(nodes: Vec<Node>, root: PageId) -> Self {
        Self::from_nodes_with_allocator(nodes, root, PageAllocator::new())
    }

    /// Create a B-tree from existing nodes and preserve its page allocator.
    pub fn from_nodes_with_allocator(
        nodes: Vec<Node>,
        root: PageId,
        mut page_allocator: PageAllocator,
    ) -> Self {
        page_allocator.advance_next_id(nodes.len() as u64);
        Self {
            nodes,
            root,
            dirty_pages: HashSet::new(),
            page_allocator,
        }
    }

    /// Replace the tree's logical page allocator during storage bootstrap.
    pub fn with_page_allocator(mut self, page_allocator: PageAllocator) -> Self {
        self.page_allocator = page_allocator;
        self.page_allocator
            .advance_next_id(self.nodes.len() as u64);
        self
    }

    /// Access the logical page allocator for durable checkpoints.
    pub fn page_allocator(&self) -> &PageAllocator {
        &self.page_allocator
    }

    /// Mutably access the logical page allocator.
    pub fn page_allocator_mut(&mut self) -> &mut PageAllocator {
        &mut self.page_allocator
    }

    /// Move the allocator out while replacing it with a fresh allocator.
    pub fn take_page_allocator(&mut self) -> PageAllocator {
        std::mem::take(&mut self.page_allocator)
    }

    /// Add a node at a specific page ID (for loading from disk).
    pub fn add_node(&mut self, node: Node, page_id: PageId) {
        let idx = page_id as usize;
        if idx >= self.nodes.len() {
            self.nodes.resize_with(idx + 1, Node::new_leaf);
        }
        self.nodes[idx] = node;
        self.dirty_pages.remove(&page_id);
    }

    /// Get the root page ID.
    pub fn root_id(&self) -> PageId {
        self.root
    }

    /// Number of nodes in the tree.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Return the logical pages changed since the last published generation.
    pub fn dirty_page_ids(&self) -> Vec<PageId> {
        let mut pages: Vec<_> = self.dirty_pages.iter().copied().collect();
        pages.sort_unstable();
        pages
    }

    /// Mark the current in-memory tree as clean after manifest publication.
    pub fn clear_dirty(&mut self) {
        self.dirty_pages.clear();
    }

    /// Get a reference to a node by page ID.
    pub fn node(&self, id: PageId) -> Option<&Node> {
        self.nodes.get(id as usize)
    }

    /// Get a mutable reference to a node by page ID.
    fn node_mut(&mut self, id: PageId) -> Option<&mut Node> {
        self.dirty_pages.insert(id);
        self.nodes.get_mut(id as usize)
    }

    /// Allocate a new node and return its page ID.
    fn alloc_node(&mut self, node: Node) -> Result<PageId, BTreeError> {
        let id = self.page_allocator.alloc();
        if id > u32::MAX as u64 {
            return Err(BTreeError::PageIdExhausted);
        }
        let id = id as PageId;
        if id as usize >= self.nodes.len() {
            self.nodes.resize_with(id as usize + 1, Node::new_leaf);
        }
        self.nodes[id as usize] = node;
        self.dirty_pages.insert(id);
        Ok(id)
    }

    /// Insert a key-value pair into the B-tree.
    ///
    /// If the key already exists, returns an error.
    /// May cause node splits if the leaf is full.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        let leaf_id = self.find_leaf(key);
        let result = self
            .node_mut(leaf_id)
            .expect("leaf_id should be valid")
            .insert(key, value);

        match result {
            Ok(()) => Ok(()),
            Err(InsertError::PageFull) => {
                let snapshot = self.nodes.clone();
                let root = self.root;
                let dirty_pages = self.dirty_pages.clone();
                let page_allocator = self.page_allocator.clone();
                let result = self.split_and_insert_leaf(leaf_id, key, value);
                if result.is_err() {
                    self.nodes = snapshot;
                    self.root = root;
                    self.dirty_pages = dirty_pages;
                    self.page_allocator = page_allocator;
                }
                result
            }
            Err(InsertError::DuplicateKey(_)) => Err(BTreeError::DuplicateKey),
            Err(e) => Err(BTreeError::InsertFailed(e)),
        }
    }

    /// Insert or update a key-value pair (upsert).
    ///
    /// If the key already exists, replaces the value.
    /// May cause node splits if the leaf is full.
    pub fn upsert(&mut self, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        let leaf_id = self.find_leaf(key);
        let existing_index = self.node(leaf_id).and_then(|node| node.search(key).ok());

        let Some(index) = existing_index else {
            // A missing key follows the fully split-aware insert path.
            return self.insert(key, value);
        };

        let same_size_inline = matches!(
            self.node(leaf_id).and_then(|node| node.value(index)),
            Some(ValueRef::Inline(old_value)) if old_value.len() == value.len()
        );
        if same_size_inline {
            self.node_mut(leaf_id)
                .expect("leaf_id should be valid")
                .replace_value(index, value);
            return Ok(());
        }

        let snapshot = self.nodes.clone();
        let root = self.root;
        let dirty_pages = self.dirty_pages.clone();
        let page_allocator = self.page_allocator.clone();
        let result = match self
            .node_mut(leaf_id)
            .expect("leaf_id should be valid")
            .replace_value_resized(index, value)
        {
            Ok(()) => Ok(()),
            Err(InsertError::PageFull) => {
                self.replace_full_entry_with_split(leaf_id, index, key, value)
            }
            Err(error) => Err(BTreeError::InsertFailed(error)),
        };
        if result.is_err() {
            self.nodes = snapshot;
            self.root = root;
            self.dirty_pages = dirty_pages;
            self.page_allocator = page_allocator;
        }
        result
    }

    /// Lookup a key in the B-tree.
    pub fn lookup(&self, key: &[u8]) -> LookupResult<'_> {
        let leaf_id = self.find_leaf(key);
        let node = self.node(leaf_id).expect("leaf_id should be valid");

        match node.search(key) {
            Ok(idx) => match node.value(idx) {
                Some(ValueRef::Inline(data)) => LookupResult::Found(data),
                Some(ValueRef::Blob(ptr)) => LookupResult::Blob(ptr),
                Some(ValueRef::Tombstone) => LookupResult::Deleted,
                None => LookupResult::NotFound,
            },
            Err(_) => LookupResult::NotFound,
        }
    }

    /// Delete a key by inserting a tombstone.
    ///
    /// Returns true if the key was found (even if already deleted).
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, BTreeError> {
        let leaf_id = self.find_leaf(key);
        let node = self.node_mut(leaf_id).expect("leaf_id should be valid");

        let index = match node.search(key) {
            Ok(index) => index,
            Err(_) => return Ok(false),
        };
        if matches!(node.value(index), Some(ValueRef::Tombstone)) {
            return Ok(false);
        }

        node.insert_tombstone(key)
            .map_err(BTreeError::InsertFailed)?;
        Ok(true)
    }

    /// Insert a blob pointer for a key.
    ///
    /// This stores the blob pointer in the B-tree instead of the actual value.
    pub fn insert_blob(&mut self, key: &[u8], ptr: BlobPointer) -> Result<(), BTreeError> {
        let leaf_id = self.find_leaf(key);
        let node = self.node_mut(leaf_id).expect("leaf_id should be valid");
        node.insert_blob(key, ptr)
            .map_err(BTreeError::InsertFailed)?;
        Ok(())
    }

    /// Create a forward range scan over [start, end).
    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> RangeScan<'_> {
        RangeScan::new(self, start.to_vec(), end.to_vec())
    }

    // -- Internal helpers --

    /// Find the leaf node where `key` should reside.
    fn find_leaf(&self, key: &[u8]) -> PageId {
        let mut current = self.root;

        loop {
            let node = self.node(current).expect("current should be valid");
            if node.is_leaf() {
                return current;
            }

            // Internal node: find the child to descend into.
            //
            // Layout: leftmost_child key_0 child_1 key_1 child_2 ...
            //
            // For key < key_0: go to leftmost_child
            // For key_0 <= key < key_1: go to child_1
            // etc.
            let count = node.count();
            let mut child_id = node.leftmost_child();

            for i in 0..count {
                if let Some(sep_key) = node.key(i)
                    && key < sep_key.as_slice()
                {
                    break;
                }
                child_id = node.child_id(i).unwrap_or(0);
            }

            current = child_id as u32;
        }
    }

    /// Find the path from the root to a target leaf. Each path entry records
    /// the child position within its parent (0 is the leftmost child).
    fn find_path_to_leaf(
        &self,
        current: PageId,
        target: PageId,
        path: &mut Vec<(PageId, usize)>,
    ) -> bool {
        let Some(node) = self.node(current) else {
            return false;
        };
        if node.is_leaf() {
            return current == target;
        }

        let leftmost = node.leftmost_child() as PageId;
        let children: Vec<_> = (0..node.count())
            .filter_map(|index| node.child_id(index).map(|child| child as PageId))
            .collect();

        path.push((current, 0));
        if self.find_path_to_leaf(leftmost, target, path) {
            return true;
        }
        path.pop();

        for (index, child) in children.into_iter().enumerate() {
            path.push((current, index + 1));
            if self.find_path_to_leaf(child, target, path) {
                return true;
            }
            path.pop();
        }
        false
    }

    /// Descend through leftmost children until reaching a leaf.
    fn leftmost_leaf(&self, mut current: PageId) -> Option<PageId> {
        loop {
            let node = self.node(current)?;
            if node.is_leaf() {
                return Some(current);
            }
            let next = node.leftmost_child() as PageId;
            if next == current {
                return None;
            }
            current = next;
        }
    }

    /// Find the leaf immediately to the right of `target` in key order.
    fn next_leaf(&self, target: PageId) -> Option<PageId> {
        let mut path = Vec::new();
        if !self.find_path_to_leaf(self.root, target, &mut path) {
            return None;
        }

        for (parent_id, child_position) in path.into_iter().rev() {
            let parent = self.node(parent_id)?;
            if child_position < parent.count() {
                let next_child = parent.child_id(child_position)? as PageId;
                return self.leftmost_leaf(next_child);
            }
        }
        None
    }

    /// Split a leaf node that's full and insert the key-value.
    fn split_and_insert_leaf(
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

    /// Return a node's recorded parent, validating it before falling back to
    /// a structural search for legacy or externally loaded pages.
    fn parent_of(&self, node_id: PageId) -> Option<PageId> {
        if node_id == self.root {
            return None;
        }

        let recorded = self.node(node_id).map(Node::parent_id);
        if let Some(parent_id) = recorded.filter(|&parent_id| parent_id != 0)
            && self
                .node(parent_id)
                .is_some_and(|parent| !parent.is_leaf())
        {
            return Some(parent_id);
        }

        self.find_parent(self.root, node_id)
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

        let target_id = if key >= median_key.as_slice() {
            right_id
        } else {
            node_id
        };

        self.node_mut(target_id)
            .expect("target_id should be valid")
            .insert_child(key, right_child_id as u64)
            .map_err(BTreeError::InsertFailed)?;

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

    /// Remove an existing entry and insert its replacement, splitting the
    /// leaf when the resized value no longer fits.
    fn replace_full_entry_with_split(
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

/// Error from B-tree operations.
#[derive(Debug, thiserror::Error)]
pub enum BTreeError {
    #[error("duplicate key")]
    DuplicateKey,
    #[error("insert failed: {0}")]
    InsertFailed(InsertError),
    #[error("split failed: {0}")]
    SplitFailed(SplitError),
    #[error("logical page ID exhausted")]
    PageIdExhausted,
}

/// Cursor for range scanning the B-tree.
pub struct RangeScan<'a> {
    tree: &'a BTree,
    start: Vec<u8>,
    end: Vec<u8>,
    current_node: PageId,
    current_index: usize,
    done: bool,
}

impl<'a> RangeScan<'a> {
    fn new(tree: &'a BTree, start: Vec<u8>, end: Vec<u8>) -> Self {
        let leaf_id = tree.find_leaf(&start);
        let node = tree.node(leaf_id).expect("leaf_id should be valid");

        let start_index = match node.search(&start) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        Self {
            tree,
            start,
            end,
            current_node: leaf_id,
            current_index: start_index,
            done: false,
        }
    }
}

impl<'a> Iterator for RangeScan<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let node = self.tree.node(self.current_node)?;

            if self.current_index < node.count() {
                let key = node.key(self.current_index)?;

                if key >= self.end {
                    self.done = true;
                    return None;
                }

                self.current_index += 1;

                // Deletes are represented by a tombstone inserted before the
                // previous value for the same key.  A range scan must expose
                // the logical view, so suppress every later duplicate after
                // the first occurrence (the first occurrence is the newest
                // version and may itself be a tombstone).
                if self.current_index > 1
                    && node.key(self.current_index - 2).as_deref() == Some(key.as_slice())
                {
                    continue;
                }

                if let Some(ValueRef::Inline(value)) = node.value(self.current_index - 1)
                    && key >= self.start
                {
                    return Some((key, value.to_vec()));
                }
                continue;
            }

            let Some(next_leaf) = self.tree.next_leaf(self.current_node) else {
                self.done = true;
                return None;
            };
            self.current_node = next_leaf;
            self.current_index = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_btree_insert_and_lookup() {
        let mut tree = BTree::new();

        tree.insert(b"hello", b"world").unwrap();
        tree.insert(b"foo", b"bar").unwrap();
        tree.insert(b"aaa", b"bbb").unwrap();

        assert!(matches!(
            tree.lookup(b"hello"),
            LookupResult::Found(b"world")
        ));
        assert!(matches!(tree.lookup(b"foo"), LookupResult::Found(b"bar")));
        assert!(matches!(tree.lookup(b"aaa"), LookupResult::Found(b"bbb")));
        assert!(matches!(tree.lookup(b"missing"), LookupResult::NotFound));
    }

    #[test]
    fn test_btree_duplicate_key() {
        let mut tree = BTree::new();

        tree.insert(b"key", b"val1").unwrap();
        assert!(matches!(
            tree.insert(b"key", b"val2"),
            Err(BTreeError::DuplicateKey)
        ));
    }

    #[test]
    fn test_btree_upsert_resizes_existing_value() {
        let mut tree = BTree::new();

        tree.upsert(b"key", b"short").unwrap();
        tree.upsert(b"key", b"a value with a different size")
            .unwrap();
        assert!(matches!(
            tree.lookup(b"key"),
            LookupResult::Found(value) if value == b"a value with a different size"
        ));

        tree.upsert(b"key", b"x").unwrap();
        assert!(matches!(tree.lookup(b"key"), LookupResult::Found(b"x")));

        tree.delete(b"key").unwrap();
        tree.upsert(b"key", b"restored").unwrap();
        assert!(matches!(
            tree.lookup(b"key"),
            LookupResult::Found(b"restored")
        ));
    }

    #[test]
    fn test_btree_delete() {
        let mut tree = BTree::new();

        tree.insert(b"key", b"value").unwrap();
        assert!(matches!(tree.lookup(b"key"), LookupResult::Found(_)));

        tree.delete(b"key").unwrap();
        assert!(matches!(tree.lookup(b"key"), LookupResult::Deleted));
        assert!(tree.range_scan(b"key", b"key~").next().is_none());
        assert!(!tree.delete(b"key").unwrap());

        assert_eq!(tree.delete(b"missing").unwrap(), false);
    }

    #[test]
    fn test_btree_split() {
        let mut tree = BTree::new();

        for i in 0..500 {
            let key = format!("key_{:06}", i);
            let val = format!("val_{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        for i in 0..500 {
            let key = format!("key_{:06}", i);
            let val = format!("val_{:06}", i);
            assert!(matches!(
                tree.lookup(key.as_bytes()),
                LookupResult::Found(v) if v == val.as_bytes()
            ));
        }

        assert!(tree.node_count() > 1);
    }

    #[test]
    fn test_btree_range_scan() {
        let mut tree = BTree::new();

        tree.insert(b"a", b"1").unwrap();
        tree.insert(b"b", b"2").unwrap();
        tree.insert(b"c", b"3").unwrap();
        tree.insert(b"d", b"4").unwrap();
        tree.insert(b"e", b"5").unwrap();

        let results: Vec<_> = tree.range_scan(b"b", b"e").collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"b");
        assert_eq!(results[1].0, b"c");
        assert_eq!(results[2].0, b"d");
    }

    #[test]
    fn test_btree_many_inserts() {
        let mut tree = BTree::new();

        for i in 0..500 {
            let key = format!("key_{:06}", i);
            let val = format!("val_{:06}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        for i in 0..500 {
            let key = format!("key_{:06}", i);
            assert!(matches!(
                tree.lookup(key.as_bytes()),
                LookupResult::Found(_)
            ));
        }
    }

    #[test]
    fn test_btree_sorted_order() {
        let mut tree = BTree::new();

        for i in (0..50).rev() {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            tree.insert(key.as_bytes(), val.as_bytes()).unwrap();
        }

        for i in 0..50 {
            let key = format!("key_{:04}", i);
            assert!(matches!(
                tree.lookup(key.as_bytes()),
                LookupResult::Found(_)
            ));
        }
    }

    #[test]
    fn test_btree_upsert_split_and_leftmost_parent_routing() {
        let mut tree = BTree::new();

        for i in (0..500).rev() {
            let key = format!("key_{i:06}");
            tree.upsert(key.as_bytes(), b"initial")
                .unwrap_or_else(|error| panic!("initial i={i}: {error:?}"));
        }

        for i in 0..500 {
            let key = format!("key_{i:06}");
            let value = format!("updated value with a different size {i}");
            tree.upsert(key.as_bytes(), value.as_bytes()).unwrap();
            assert!(matches!(
                tree.lookup(key.as_bytes()),
                LookupResult::Found(found) if found == value.as_bytes()
            ));
        }

        assert!(tree.node_count() > 2);
    }

    #[test]
    fn test_btree_range_scan_across_split_leaves() {
        let mut tree = BTree::new();
        for i in 0..500 {
            let key = format!("key_{i:06}");
            let value = format!("value_{i:06}");
            tree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let results: Vec<_> = tree.range_scan(b"key_000050", b"key_000450").collect();
        assert_eq!(results.len(), 400);
        assert_eq!(results.first().unwrap().0, b"key_000050");
        assert_eq!(results.last().unwrap().0, b"key_000449");
    }

    proptest! {
        #[test]
        fn prop_btree_mutations_match_reference_model(
            operations in prop::collection::vec(
                (0u8..64, prop::collection::vec(any::<u8>(), 0..48), any::<bool>()),
                1..200
            )
        ) {
            let mut tree = BTree::new();
            let mut reference = BTreeMap::new();

            for (key_id, value, is_write) in operations {
                let key = format!("key-{key_id:03}");
                if is_write {
                    tree.upsert(key.as_bytes(), &value).unwrap();
                    reference.insert(key.into_bytes(), value);
                } else {
                    let expected = reference.remove(key.as_bytes()).is_some();
                    prop_assert_eq!(tree.delete(key.as_bytes()).unwrap(), expected);
                }

                for (reference_key, reference_value) in &reference {
                    prop_assert!(matches!(
                        tree.lookup(reference_key),
                        LookupResult::Found(value) if value == reference_value.as_slice()
                    ));
                }
            }

            let actual: Vec<_> = tree.range_scan(b"key-000", b"key-999").collect();
            let expected: Vec<_> = reference.into_iter().collect();
            prop_assert_eq!(actual, expected);
        }
    }
}
