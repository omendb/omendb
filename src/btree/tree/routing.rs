//! B-tree descent and checked leaf-navigation ownership.
//!
//! This module owns the routing algorithms shared by logical mutation,
//! lookup, and range traversal. It validates every page and edge it follows
//! so malformed structure becomes a typed `BTreeError` rather than a panic or
//! silent end-of-stream result.

use super::{BTree, BTreeError, PageId};
use std::collections::HashSet;

/// Depth bound that replaces per-descent visited sets on the hot lookup
/// path. A well-formed tree with page-sized nodes cannot approach this.
const MAX_ROUTING_DEPTH: u32 = 128;

impl BTree {
    /// Find the leaf node where `key` should reside.
    pub(super) fn find_leaf(&self, key: &[u8]) -> Result<PageId, BTreeError> {
        let mut current = self.root;
        // Cycle guard without per-descent allocation: a well-formed tree
        // with page-sized nodes reaches this depth only after astronomically
        // many keys, so any real routing cycle exceeds it immediately.
        let mut depth = 0u32;

        loop {
            if depth >= MAX_ROUTING_DEPTH {
                return Err(BTreeError::Corruption(
                    "cycle detected during B-tree descent".into(),
                ));
            }
            depth += 1;
            let node = self.node(current).ok_or(BTreeError::MissingPage(current))?;;
            if node.is_leaf() {
                return Ok(current);
            }

            // Internal node: use an allocation-free upper-bound search over
            // its separator array. Equal separators route to the child on
            // their right.
            let child_id = node
                .child_for_key(key)
                .ok_or_else(|| BTreeError::Corruption("internal routing is malformed".into()))?;

            if child_id > u32::MAX as u64 {
                return Err(BTreeError::Corruption(
                    "internal child page ID exceeds the logical ID width".into(),
                ));
            }
            current = child_id as u32;
        }
    }

    /// Find a target leaf while validating every page and routing edge that
    /// the search visits. A cursor must not turn corruption into an ordinary
    /// end-of-stream condition.
    fn find_path_to_leaf_checked(
        &self,
        current: PageId,
        target: PageId,
        path: &mut Vec<(PageId, usize)>,
        active: &mut HashSet<PageId>,
    ) -> Result<bool, BTreeError> {
        if !active.insert(current) {
            return Err(BTreeError::Corruption(
                "cycle detected while locating range leaf".into(),
            ));
        }

        let result = (|| {
            let node = self
                .node(current)
                .ok_or_else(|| BTreeError::Corruption("range page is missing".into()))?;
            if node.is_leaf() {
                return Ok(current == target);
            }

            let leftmost = node.leftmost_child();
            if leftmost > u32::MAX as u64 {
                return Err(BTreeError::Corruption(
                    "internal child page ID exceeds the logical ID width".into(),
                ));
            }
            let mut children = Vec::with_capacity(node.count() + 1);
            children.push(leftmost as PageId);
            for index in 0..node.count() {
                let child = node
                    .child_id(index)
                    .ok_or_else(|| BTreeError::Corruption("internal child is malformed".into()))?;
                if child > u32::MAX as u64 {
                    return Err(BTreeError::Corruption(
                        "internal child page ID exceeds the logical ID width".into(),
                    ));
                }
                children.push(child as PageId);
            }

            for (position, child) in children.into_iter().enumerate() {
                path.push((current, position));
                if self.find_path_to_leaf_checked(child, target, path, active)? {
                    return Ok(true);
                }
                path.pop();
            }
            Ok(false)
        })();

        active.remove(&current);
        result
    }

    /// Descend through leftmost children, reporting malformed routing state.
    fn leftmost_leaf_checked(&self, mut current: PageId) -> Result<Option<PageId>, BTreeError> {
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(current) {
                return Err(BTreeError::Corruption(
                    "cycle detected while locating next range leaf".into(),
                ));
            }
            let node = self
                .node(current)
                .ok_or_else(|| BTreeError::Corruption("range page is missing".into()))?;
            if node.is_leaf() {
                return Ok(Some(current));
            }

            let next = node.leftmost_child();
            if next > u32::MAX as u64 {
                return Err(BTreeError::Corruption(
                    "internal child page ID exceeds the logical ID width".into(),
                ));
            }
            current = next as PageId;
        }
    }

    /// Find the leaf immediately to the right of `target`, validating the
    /// route and returning corruption instead of silently stopping a scan.
    fn next_leaf_from_parent_hint(
        &self,
        target: PageId,
        parent_hint: u32,
    ) -> Option<Option<PageId>> {
        let mut current = target;
        let mut visited = HashSet::new();

        loop {
            if !visited.insert(current) {
                return None;
            }

            let parent_id = if current == target {
                parent_hint
            } else {
                self.node(current)?.parent_id()
            };
            if parent_id == 0 {
                return (current == self.root).then_some(None);
            }

            let parent = self.node(parent_id as PageId)?;
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
                let next_child = u32::try_from(next_child).ok()? as PageId;
                return self.leftmost_leaf_checked(next_child).ok();
            }
            current = parent_id as PageId;
        }
    }

    pub(super) fn next_leaf_checked(
        &self,
        target: PageId,
        parent_hint: u32,
    ) -> Result<Option<PageId>, BTreeError> {
        if let Some(next_leaf) = self.next_leaf_from_parent_hint(target, parent_hint) {
            return Ok(next_leaf);
        }

        let mut path = Vec::new();
        let mut active = HashSet::new();
        if !self.find_path_to_leaf_checked(self.root, target, &mut path, &mut active)? {
            return Err(BTreeError::Corruption(
                "range leaf is not reachable from the root".into(),
            ));
        }

        for (parent_id, child_position) in path.into_iter().rev() {
            let parent = self
                .node(parent_id)
                .ok_or_else(|| BTreeError::Corruption("range parent page is missing".into()))?;
            if child_position < parent.count() {
                let next_child = parent
                    .child_id(child_position)
                    .ok_or_else(|| BTreeError::Corruption("internal child is malformed".into()))?;
                if next_child > u32::MAX as u64 {
                    return Err(BTreeError::Corruption(
                        "internal child page ID exceeds the logical ID width".into(),
                    ));
                }
                return self.leftmost_leaf_checked(next_child as PageId);
            }
        }
        Ok(None)
    }
}
