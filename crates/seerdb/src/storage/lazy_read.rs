//! Logical lookup and range traversal over resident and PMT-backed pages.
//!
//! This module owns the query policy for a sparse reopen: it resolves a key
//! through the resident mutation overlay first, falls back to the immutable
//! PMT generation for unloaded pages, and traverses retained roots without
//! mutating storage state. Physical page decoding and cache ownership remain
//! in [`super::read_path`].

use super::StorageEngine;
use crate::btree::{BTreeError, LookupResult, ValueRef};
use crate::error::{Error, Result};
use crate::mvcc::PMT;
use std::collections::{BTreeMap, HashSet};

impl StorageEngine {
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

    /// Look up a key in a retained PMT-selected root generation.
    pub fn lookup_at(&self, root_page_id: u64, pmt: &PMT, key: &[u8]) -> Result<LookupResult> {
        if pmt.is_empty() {
            if root_page_id == 0 {
                return Ok(LookupResult::NotFound);
            }
            return Err(Error::Corruption(
                "empty historical PMT names a non-zero root page".into(),
            ));
        }
        let root_page_id = u32::try_from(root_page_id)
            .map_err(|_| Error::Corruption("historical root exceeds logical ID width".into()))?;
        if !pmt.contains(root_page_id as u64) {
            return Err(Error::Corruption(
                "historical root is missing from its PMT checkpoint".into(),
            ));
        }
        self.lookup_lazy_with_pmt(pmt, root_page_id, key)
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

    /// Scan a range in a retained PMT-selected root generation.
    pub fn range_at(
        &self,
        root_page_id: u64,
        pmt: &PMT,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, LookupResult)>> {
        if pmt.is_empty() {
            if root_page_id == 0 {
                return Ok(Vec::new());
            }
            return Err(Error::Corruption(
                "empty historical PMT names a non-zero root page".into(),
            ));
        }
        let root_page_id = u32::try_from(root_page_id)
            .map_err(|_| Error::Corruption("historical root exceeds logical ID width".into()))?;
        if !pmt.contains(root_page_id as u64) {
            return Err(Error::Corruption(
                "historical root is missing from its PMT checkpoint".into(),
            ));
        }
        self.range_lazy_with_pmt(pmt, root_page_id, start, end)
    }

    fn lookup_lazy(&self, root_page_id: u32, key: &[u8]) -> Result<LookupResult> {
        self.lookup_lazy_with_pmt(&self.pmt, root_page_id, key)
    }

    fn lookup_lazy_with_pmt(
        &self,
        pmt: &PMT,
        root_page_id: u32,
        key: &[u8],
    ) -> Result<LookupResult> {
        let leaf_id = self.find_leaf_page(pmt, root_page_id, key)?;
        let node = self.read_node_arc_from_pmt(pmt, leaf_id as u64)?;
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
        self.range_lazy_with_pmt(&self.pmt, root_page_id, start, end)
    }

    fn range_lazy_with_pmt(
        &self,
        pmt: &PMT,
        root_page_id: u32,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, LookupResult)>> {
        let mut results = Vec::new();
        let mut current = self.find_leaf_page(pmt, root_page_id, start)?;
        let mut first_leaf = true;
        let mut previous_key = None;

        loop {
            let node = self.read_node_arc_from_pmt(pmt, current as u64)?;
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

            let parent_hint = node.parent_id();
            let Some(next) = self.next_leaf_page(pmt, root_page_id, current, parent_hint)? else {
                return Ok(results);
            };
            current = next;
        }
    }

    fn find_leaf_page(&self, pmt: &PMT, root_page_id: u32, key: &[u8]) -> Result<u32> {
        let mut current = root_page_id;
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(current) {
                return Err(Error::Corruption(
                    "cycle detected during lazy B-tree descent".into(),
                ));
            }
            let node = self.read_node_arc_from_pmt(pmt, current as u64)?;
            if node.is_leaf() {
                return Ok(current);
            }
            let child_id = node
                .child_for_key(key)
                .ok_or_else(|| Error::Corruption("lazy internal routing is malformed".into()))?;
            current = u32::try_from(child_id).map_err(|_| {
                Error::Corruption("lazy internal child ID exceeds logical width".into())
            })?;
        }
    }

    fn find_path_to_leaf_page(
        &self,
        pmt: &PMT,
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
            let node = self.read_node_arc_from_pmt(pmt, current as u64)?;
            if node.is_leaf() {
                return Ok(current == target);
            }
            let mut children = Vec::with_capacity(node.count() + 1);
            children.push(u32::try_from(node.leftmost_child()).map_err(|_| {
                Error::Corruption("lazy internal child ID exceeds logical width".into())
            })?);
            for index in 0..node.count() {
                let child = node
                    .child_id(index)
                    .ok_or_else(|| Error::Corruption("lazy internal child is malformed".into()))?;
                children.push(u32::try_from(child).map_err(|_| {
                    Error::Corruption("lazy internal child ID exceeds logical width".into())
                })?);
            }
            for (position, child) in children.into_iter().enumerate() {
                path.push((current, position));
                if self.find_path_to_leaf_page(pmt, child, target, path, active)? {
                    return Ok(true);
                }
                path.pop();
            }
            Ok(false)
        })();
        active.remove(&current);
        result
    }

    fn leftmost_leaf_page(&self, pmt: &PMT, mut current: u32) -> Result<Option<u32>> {
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(current) {
                return Err(Error::Corruption(
                    "cycle detected during lazy next-leaf traversal".into(),
                ));
            }
            let node = self.read_node_arc_from_pmt(pmt, current as u64)?;
            if node.is_leaf() {
                return Ok(Some(current));
            }
            current = u32::try_from(node.leftmost_child()).map_err(|_| {
                Error::Corruption("lazy internal child ID exceeds logical width".into())
            })?;
        }
    }

    fn next_leaf_from_parent_hint(
        &self,
        pmt: &PMT,
        root_page_id: u32,
        target: u32,
        parent_hint: u32,
    ) -> Option<Option<u32>> {
        let mut current = target;
        let mut visited = HashSet::new();

        loop {
            if !visited.insert(current) {
                return None;
            }

            let parent_id = if current == target {
                parent_hint
            } else {
                self.read_node_arc_from_pmt(pmt, current as u64)
                    .ok()?
                    .parent_id()
            };
            if parent_id == 0 {
                return (current == root_page_id).then_some(None);
            }

            let parent = self.read_node_arc_from_pmt(pmt, parent_id as u64).ok()?;
            if !parent.is_internal() {
                return None;
            }
            let child_position = if parent.leftmost_child() == current as u64 {
                0
            } else {
                (0..parent.count())
                    .find(|&index| parent.child_id(index) == Some(current as u64))
                    .map(|index| index + 1)?
            };

            if child_position < parent.count() {
                let next_child = parent.child_id(child_position)?;
                let next_child = u32::try_from(next_child).ok()?;
                return self.leftmost_leaf_page(pmt, next_child).ok();
            }
            current = parent_id;
        }
    }

    fn next_leaf_page(
        &self,
        pmt: &PMT,
        root_page_id: u32,
        target: u32,
        parent_hint: u32,
    ) -> Result<Option<u32>> {
        if let Some(next_leaf) =
            self.next_leaf_from_parent_hint(pmt, root_page_id, target, parent_hint)
        {
            return Ok(next_leaf);
        }

        let mut path = Vec::new();
        let mut active = HashSet::new();
        if !self.find_path_to_leaf_page(pmt, root_page_id, target, &mut path, &mut active)? {
            return Err(Error::Corruption(
                "lazy range leaf is not reachable from root".into(),
            ));
        }
        for (parent_id, child_position) in path.into_iter().rev() {
            let parent = self.read_node_arc_from_pmt(pmt, parent_id as u64)?;
            if child_position < parent.count() {
                let child = parent
                    .child_id(child_position)
                    .ok_or_else(|| Error::Corruption("lazy internal child is malformed".into()))?;
                return self.leftmost_leaf_page(
                    pmt,
                    u32::try_from(child).map_err(|_| {
                        Error::Corruption("lazy internal child ID exceeds logical width".into())
                    })?,
                );
            }
        }
        Ok(None)
    }
}
