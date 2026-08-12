//! B-tree logical mutation entrypoints and rollback ownership.
//!
//! This module owns public insert, upsert, delete, and blob-pointer mutation
//! APIs. It keeps candidate changes atomic when split or resized replacement
//! fails, then delegates structural propagation to `split.rs`. `tree.rs`
//! remains the owner of B-tree state, while `routing.rs` owns descent and
//! checked leaf navigation.

use super::{BTree, BTreeError};
use crate::btree::node::{BlobPointer, InsertError, ValueRef};

impl BTree {
    /// Insert a key-value pair into the B-tree.
    ///
    /// If the key already exists, returns an error.
    /// May cause node splits if the leaf is full.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        let leaf_id = self.find_leaf(key)?;
        let result = self
            .node_mut(leaf_id)
            .ok_or(BTreeError::MissingPage(leaf_id))?
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
        let leaf_id = self.find_leaf(key)?;
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
                .ok_or(BTreeError::MissingPage(leaf_id))?
                .replace_value(index, value)
                .map_err(BTreeError::InsertFailed)?;
            return Ok(());
        }

        let snapshot = self.nodes.clone();
        let root = self.root;
        let dirty_pages = self.dirty_pages.clone();
        let page_allocator = self.page_allocator.clone();
        let result = match self
            .node_mut(leaf_id)
            .ok_or(BTreeError::MissingPage(leaf_id))?
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

    /// Delete a key by inserting a tombstone.
    ///
    /// Returns true if the key was found (even if already deleted).
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, BTreeError> {
        let leaf_id = self.find_leaf(key)?;
        let node = self
            .node_mut(leaf_id)
            .ok_or(BTreeError::MissingPage(leaf_id))?;

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
        let leaf_id = self.find_leaf(key)?;
        match self
            .node_mut(leaf_id)
            .ok_or(BTreeError::MissingPage(leaf_id))?
            .insert_blob(key, ptr)
        {
            Ok(()) => Ok(()),
            Err(InsertError::PageFull) => {
                let snapshot = self.nodes.clone();
                let root = self.root;
                let dirty_pages = self.dirty_pages.clone();
                let page_allocator = self.page_allocator.clone();
                let result = self.split_and_insert_blob_leaf(leaf_id, key, ptr);
                if result.is_err() {
                    self.nodes = snapshot;
                    self.root = root;
                    self.dirty_pages = dirty_pages;
                    self.page_allocator = page_allocator;
                }
                result
            }
            Err(error) => Err(BTreeError::InsertFailed(error)),
        }
    }

    /// Insert or replace a blob pointer for a key.
    pub fn upsert_blob(&mut self, key: &[u8], ptr: BlobPointer) -> Result<(), BTreeError> {
        let leaf_id = self.find_leaf(key)?;
        let Some(index) = self.node(leaf_id).and_then(|node| node.search(key).ok()) else {
            return match self
                .node_mut(leaf_id)
                .ok_or(BTreeError::MissingPage(leaf_id))?
                .insert_blob(key, ptr)
            {
                Ok(()) => Ok(()),
                Err(InsertError::PageFull) => {
                    let snapshot = self.nodes.clone();
                    let root = self.root;
                    let dirty_pages = self.dirty_pages.clone();
                    let page_allocator = self.page_allocator.clone();
                    let result = self.split_and_insert_blob_leaf(leaf_id, key, ptr);
                    if result.is_err() {
                        self.nodes = snapshot;
                        self.root = root;
                        self.dirty_pages = dirty_pages;
                        self.page_allocator = page_allocator;
                    }
                    result
                }
                Err(error) => Err(BTreeError::InsertFailed(error)),
            };
        };

        let snapshot = self.nodes.clone();
        let root = self.root;
        let dirty_pages = self.dirty_pages.clone();
        let page_allocator = self.page_allocator.clone();
        let result = (|| {
            self.node_mut(leaf_id)
                .ok_or(BTreeError::MissingPage(leaf_id))?
                .remove_entry(index)
                .map_err(BTreeError::InsertFailed)?;
            match self
                .node_mut(leaf_id)
                .ok_or(BTreeError::MissingPage(leaf_id))?
                .insert_blob(key, ptr)
            {
                Ok(()) => Ok(()),
                Err(InsertError::PageFull) => self.split_and_insert_blob_leaf(leaf_id, key, ptr),
                Err(error) => Err(BTreeError::InsertFailed(error)),
            }
        })();
        if result.is_err() {
            self.nodes = snapshot;
            self.root = root;
            self.dirty_pages = dirty_pages;
            self.page_allocator = page_allocator;
        }
        result
    }
}
