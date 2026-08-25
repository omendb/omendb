//! Forward range-scan and bounded-maintenance cursor ownership.

use super::{BTree, BTreeError, LookupResult, PageId};
use crate::btree::node::ValueRef;

/// Public forward range scan over `[start, end)`.
pub struct RangeScan<'a> {
    tree: &'a BTree,
    cursor: RangeCursor,
}

/// Owned position for a checked forward range scan.
///
/// The cursor does not borrow the tree, so a maintenance operation can keep
/// its position between bounded calls while the source tree remains fixed.
#[derive(Debug, Clone)]
pub(crate) struct RangeCursor {
    start: Vec<u8>,
    end: Vec<u8>,
    current_node: PageId,
    current_index: usize,
    done: bool,
}

impl<'a> RangeScan<'a> {
    pub(super) fn new(tree: &'a BTree, start: Vec<u8>, end: Vec<u8>) -> Result<Self, BTreeError> {
        Ok(Self {
            tree,
            cursor: RangeCursor::new(tree, start, end)?,
        })
    }
}

impl RangeCursor {
    pub(super) fn new(tree: &BTree, start: Vec<u8>, end: Vec<u8>) -> Result<Self, BTreeError> {
        let leaf_id = tree.find_leaf(&start)?;
        let node = tree
            .node(leaf_id)
            .ok_or_else(|| BTreeError::Corruption("range leaf page is missing".into()))?;

        let start_index = match node.search(&start) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        Ok(Self {
            start,
            end,
            current_node: leaf_id,
            current_index: start_index,
            done: false,
        })
    }

    pub(crate) fn next(
        &mut self,
        tree: &BTree,
    ) -> Option<Result<(Vec<u8>, LookupResult), BTreeError>> {
        if self.done {
            return None;
        }

        loop {
            let Some(node) = tree.node(self.current_node) else {
                self.done = true;
                return Some(Err(BTreeError::Corruption("range page is missing".into())));
            };

            if self.current_index < node.count() {
                let Some(key) = node.key(self.current_index) else {
                    self.done = true;
                    return Some(Err(BTreeError::Corruption("range key is malformed".into())));
                };

                if key >= self.end {
                    self.done = true;
                    return None;
                }

                self.current_index += 1;

                // Deletes are represented by a tombstone inserted before the
                // previous value for the same key. A range scan must expose
                // the logical view, so suppress every later duplicate after
                // the first occurrence.
                if self.current_index > 1
                    && node
                        .key(self.current_index - 2)
                        .is_some_and(|previous| previous == key)
                {
                    continue;
                }

                match node.value(self.current_index - 1) {
                    Some(ValueRef::Inline(value)) if key >= self.start => {
                        return Some(Ok((key, LookupResult::Found(value.to_vec()))));
                    }
                    Some(ValueRef::Blob(pointer)) if key >= self.start => {
                        return Some(Ok((key, LookupResult::Blob(pointer))));
                    }
                    Some(ValueRef::Inline(_))
                    | Some(ValueRef::Blob(_))
                    | Some(ValueRef::Tombstone) => {}
                    None => {
                        self.done = true;
                        return Some(Err(BTreeError::Corruption(
                            "range value payload is malformed".into(),
                        )));
                    }
                }
                continue;
            }

            let parent_hint = node.parent_id();
            let next_leaf = match tree.next_leaf_checked(self.current_node, parent_hint) {
                Ok(next_leaf) => next_leaf,
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            };
            let Some(next_leaf) = next_leaf else {
                self.done = true;
                return None;
            };
            self.current_node = next_leaf;
            self.current_index = 0;
        }
    }
}

impl<'a> Iterator for RangeScan<'a> {
    type Item = Result<(Vec<u8>, LookupResult), BTreeError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next(self.tree)
    }
}
