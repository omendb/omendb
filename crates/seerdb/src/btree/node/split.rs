//! B-tree node split and rebuild ownership.

use super::PAGE_SIZE;
use super::page_format::SLOT_SIZE;
use super::{Node, SplitError, ValueRef, ValueType};

impl Node {
    /// Split this node, returning a new right sibling.
    ///
    /// The median key is returned separately for insertion into the parent.
    /// For leaves, the median and all greater keys move right. For internal
    /// nodes, the median moves to the parent and keys greater than it move
    /// right.
    ///
    /// The midpoint balances the byte mass of the two halves, not the
    /// entry count. Entries vary widely in size (a change-record leaf
    /// beside small status records), so a count-balanced split can leave
    /// one half nearly full; a byte-balanced split guarantees each half
    /// has room for one more entry of any size that fits an empty page.
    pub fn split(&mut self) -> Result<(Vec<u8>, Node), SplitError> {
        let count = self.count();
        if count < 2 {
            return Err(SplitError::TooFewKeys);
        }

        let mid = self.split_midpoint(count);
        let median_key = self.key(mid).ok_or(SplitError::Corruption)?;

        let mut right = if self.is_leaf() {
            Node::new_leaf()
        } else {
            Node::new_internal()
        };

        if self.is_leaf() {
            for index in mid..count {
                let key = self.key(index).ok_or(SplitError::Corruption)?;
                let value = self.value(index).ok_or(SplitError::Corruption)?;
                let insertion_point = right.upper_bound(&key);
                match value {
                    ValueRef::Inline(data) => right
                        .insert_leaf_value(&key, ValueType::Inline, data, insertion_point)
                        .map_err(|_| SplitError::InsertFailed)?,
                    ValueRef::Blob(pointer) => {
                        let bytes = pointer.to_bytes();
                        right
                            .insert_leaf_value(
                                &key,
                                ValueType::BlobPointer,
                                &bytes,
                                insertion_point,
                            )
                            .map_err(|_| SplitError::InsertFailed)?;
                    }
                    ValueRef::Tombstone => right
                        .insert_leaf_value(&key, ValueType::Tombstone, &[], insertion_point)
                        .map_err(|_| SplitError::InsertFailed)?,
                }
            }
        } else {
            // The median separator moves to the parent. The right node starts
            // at the child immediately after that separator.
            let right_leftmost = self.child_id(mid).ok_or(SplitError::Corruption)?;
            right.set_leftmost_child(right_leftmost);

            for index in (mid + 1)..count {
                let key = self.key(index).ok_or(SplitError::Corruption)?;
                let child = self.child_id(index).ok_or(SplitError::Corruption)?;
                right
                    .insert_child(&key, child)
                    .map_err(|_| SplitError::InsertFailed)?;
            }
        }

        // Rebuild both retained halves so discarded physical entry bytes are
        // reclaimed. Merely reducing the slot count would leave stale offsets
        // consuming the page after repeated splits.
        if self.is_leaf() {
            let parent_id = self.parent_id();
            let mut left = Node::new_leaf();
            left.set_parent_id(parent_id);
            for index in 0..mid {
                let key = self.key(index).ok_or(SplitError::Corruption)?;
                let value = self.value(index).ok_or(SplitError::Corruption)?;
                let insertion_point = left.upper_bound(&key);
                match value {
                    ValueRef::Inline(data) => left
                        .insert_leaf_value(&key, ValueType::Inline, data, insertion_point)
                        .map_err(|_| SplitError::InsertFailed)?,
                    ValueRef::Blob(pointer) => {
                        let bytes = pointer.to_bytes();
                        left.insert_leaf_value(
                            &key,
                            ValueType::BlobPointer,
                            &bytes,
                            insertion_point,
                        )
                        .map_err(|_| SplitError::InsertFailed)?;
                    }
                    ValueRef::Tombstone => left
                        .insert_leaf_value(&key, ValueType::Tombstone, &[], insertion_point)
                        .map_err(|_| SplitError::InsertFailed)?,
                }
            }
            *self = left;
        } else {
            let parent_id = self.parent_id();
            let leftmost_child = self.leftmost_child();
            let mut left = Node::new_internal();
            left.set_parent_id(parent_id);
            left.set_leftmost_child(leftmost_child);
            for index in 0..mid {
                let key = self.key(index).ok_or(SplitError::Corruption)?;
                let child = self.child_id(index).ok_or(SplitError::Corruption)?;
                left.insert_child(&key, child)
                    .map_err(|_| SplitError::InsertFailed)?;
            }
            *self = left;
        }

        Ok((median_key, right))
    }

    /// Choose a leaf split midpoint that balances the byte mass of the
    /// two halves. Falls back to the count midpoint when sizes are
    /// unavailable. Always keeps at least one entry on each side.
    fn split_midpoint(&self, count: usize) -> usize {
        let midpoint = count / 2;
        if !self.is_leaf() {
            return midpoint;
        }
        let entry_bytes = |index: usize| -> Option<usize> {
            let offset = self.slot_offset(index);
            let upper = (0..count)
                .map(|other| self.slot_offset(other))
                .filter(|&other_offset| other_offset > offset)
                .min()
                .unwrap_or(PAGE_SIZE);
            // Slot overhead is charged per entry so the mass reflects both
            // the encoded entry and its fixed bookkeeping cost.
            Some(upper.saturating_sub(offset) + SLOT_SIZE)
        };
        let total: usize = (0..count)
            .map(|index| entry_bytes(index).unwrap_or(0))
            .sum();
        let mut best = midpoint;
        let mut best_distance = usize::MAX;
        for candidate in 1..count {
            let left: usize = (0..candidate)
                .map(|index| entry_bytes(index).unwrap_or(0))
                .sum();
            let distance = left.abs_diff(total - left);
            if distance < best_distance {
                best_distance = distance;
                best = candidate;
            }
        }
        best
    }
}
