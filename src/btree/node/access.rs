//! Read-only node access and lookup ownership.

use super::page_format::{BLOB_POINTER_SIZE, HEADER_SIZE, SLOT_SIZE};
use super::{BlobPointer, Node, PageType, ValueRef};

impl Node {
    /// Whether this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        self.header().page_type == PageType::Leaf
    }

    /// Whether this is an internal node.
    pub fn is_internal(&self) -> bool {
        self.header().page_type == PageType::Internal
    }

    /// Number of key-value pairs in this node.
    pub fn count(&self) -> usize {
        self.header().count as usize
    }

    // -- Slot array helpers -------------------------------------------------

    /// Offset where the slot array starts (right after header).
    pub(super) const fn slot_array_start() -> usize {
        HEADER_SIZE
    }

    /// Read the entry offset for slot `index`.
    pub(super) fn slot_offset(&self, index: usize) -> usize {
        let start = Self::slot_array_start() + index * SLOT_SIZE;
        u16::from_le_bytes([self.data[start], self.data[start + 1]]) as usize
    }

    /// Write the entry offset for slot `index`.
    pub(super) fn set_slot_offset(&mut self, index: usize, offset: u16) {
        let start = Self::slot_array_start() + index * SLOT_SIZE;
        self.data[start..start + 2].copy_from_slice(&offset.to_le_bytes());
    }

    /// Read the key length stored in the slot for `index`.
    pub(super) fn slot_key_len(&self, index: usize) -> u16 {
        let start = Self::slot_array_start() + index * SLOT_SIZE + 2;
        u16::from_le_bytes([self.data[start], self.data[start + 1]])
    }

    /// Write the key length for slot `index`.
    pub(super) fn set_slot_key_len(&mut self, index: usize, key_len: u16) {
        let start = Self::slot_array_start() + index * SLOT_SIZE + 2;
        self.data[start..start + 2].copy_from_slice(&key_len.to_le_bytes());
    }

    // -- Entry access -------------------------------------------------------
    //
    // Each entry stored at the slot's offset:
    //   [prefix_len: u16] [suffix_len: u16] [suffix: bytes] [value_type: u8] [value: bytes]

    /// Read an entry's prefix length at the given slot.
    pub(super) fn entry_prefix_len(&self, index: usize) -> u16 {
        let off = self.slot_offset(index);
        u16::from_le_bytes([self.data[off], self.data[off + 1]])
    }

    /// Read an entry's suffix length at the given slot.
    pub(super) fn entry_suffix_len(&self, index: usize) -> u16 {
        let off = self.slot_offset(index) + 2;
        u16::from_le_bytes([self.data[off], self.data[off + 1]])
    }

    pub(super) fn has_prefix_compression(&self) -> bool {
        (0..self.count()).any(|index| self.entry_prefix_len(index) != 0)
    }

    /// Reconstruct the full key at slot `index`.
    ///
    /// For index 0, the prefix is always empty (no previous key).
    /// For index > 0, we read the prefix from the previous key.
    pub fn key(&self, index: usize) -> Option<Vec<u8>> {
        if index >= self.count() {
            return None;
        }

        let prefix_len = self.entry_prefix_len(index) as usize;
        let suffix_len = self.entry_suffix_len(index) as usize;
        let entry_off = self.slot_offset(index);
        let suffix_start = entry_off + 4;

        let suffix = &self.data[suffix_start..suffix_start + suffix_len];

        if index == 0 || prefix_len == 0 {
            return Some(suffix.to_vec());
        }

        let prev_key = self.key(index - 1)?;
        if prefix_len > prev_key.len() {
            return None;
        }
        let mut full_key = prev_key[..prefix_len].to_vec();
        full_key.extend_from_slice(suffix);
        Some(full_key)
    }

    /// Read the value at slot `index` in a leaf node.
    ///
    /// Returns `None` if out of bounds or if this is an internal node.
    pub fn value(&self, index: usize) -> Option<ValueRef<'_>> {
        if index >= self.count() || !self.is_leaf() {
            return None;
        }

        let entry_off = self.slot_offset(index);
        let suffix_len = self.entry_suffix_len(index) as usize;
        let vt_off = entry_off + 4 + suffix_len;
        let value_type = self.data[vt_off];

        match value_type {
            0x00 => {
                let val_start = vt_off + 1;
                let this_offset = entry_off;
                let val_end = (0..self.count())
                    .filter(|&i| i != index)
                    .map(|i| self.slot_offset(i))
                    .filter(|&off| off > this_offset)
                    .min()
                    .unwrap_or(super::PAGE_SIZE);
                Some(ValueRef::Inline(&self.data[val_start..val_end]))
            }
            0x01 => {
                let ptr_start = vt_off + 1;
                let ptr = &self.data[ptr_start..ptr_start + BLOB_POINTER_SIZE];
                let file_id = u32::from_le_bytes([ptr[0], ptr[1], ptr[2], ptr[3]]);
                let offset = u64::from_le_bytes([
                    ptr[4], ptr[5], ptr[6], ptr[7], ptr[8], ptr[9], ptr[10], ptr[11],
                ]);
                let length = u32::from_le_bytes([ptr[12], ptr[13], ptr[14], ptr[15]]);
                Some(ValueRef::Blob(BlobPointer {
                    file_id,
                    offset,
                    length,
                }))
            }
            0x02 => Some(ValueRef::Tombstone),
            _ => None,
        }
    }

    /// Read the child page ID at slot `index` in an internal node.
    ///
    /// Internal nodes store child pointers as the value part of each entry.
    pub fn child_id(&self, index: usize) -> Option<u64> {
        if index >= self.count() || !self.is_internal() {
            return None;
        }

        let entry_off = self.slot_offset(index);
        let suffix_len = self.entry_suffix_len(index) as usize;
        let child_off = entry_off + 4 + suffix_len;
        if child_off + 8 > super::PAGE_SIZE {
            return None;
        }
        Some(u64::from_le_bytes([
            self.data[child_off],
            self.data[child_off + 1],
            self.data[child_off + 2],
            self.data[child_off + 3],
            self.data[child_off + 4],
            self.data[child_off + 5],
            self.data[child_off + 6],
            self.data[child_off + 7],
        ]))
    }

    /// Compare a stored separator or leaf key with a lookup key without
    /// allocating when the entry uses the current self-contained encoding.
    ///
    /// Older prefix-compressed pages retain the allocating reconstruction
    /// path through [`Self::key`].
    pub(super) fn compare_key(&self, index: usize, key: &[u8]) -> Option<std::cmp::Ordering> {
        if index >= self.count() {
            return None;
        }
        let prefix_len = self.entry_prefix_len(index) as usize;
        let suffix_len = self.entry_suffix_len(index) as usize;
        if prefix_len == 0 {
            let start = self.slot_offset(index).checked_add(4)?;
            let end = start.checked_add(suffix_len)?;
            return self.data.get(start..end).map(|stored| stored.cmp(key));
        }
        self.key(index).map(|stored| stored.as_slice().cmp(key))
    }

    /// Select the child that owns `key` in an internal node.
    ///
    /// Internal separators route equal keys to the child on their right, so
    /// this is an upper-bound search over the separator array.
    pub fn child_for_key(&self, key: &[u8]) -> Option<u64> {
        if !self.is_internal() {
            return None;
        }

        let mut lo = 0;
        let mut hi = self.count();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.compare_key(mid, key)? {
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }

        if lo == 0 {
            Some(self.leftmost_child())
        } else {
            self.child_id(lo - 1)
        }
    }

    /// Binary search for a key in this node.
    ///
    /// Returns `Ok(index)` of the first occurrence if found, or `Err(index)`
    /// where `index` is the insertion point.
    pub fn search(&self, key: &[u8]) -> Result<usize, usize> {
        let count = self.count();
        if count == 0 {
            return Err(0);
        }

        let mut lo = 0;
        let mut hi = count;
        let mut result = None;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.compare_key(mid, key) {
                Some(std::cmp::Ordering::Less) => lo = mid + 1,
                Some(std::cmp::Ordering::Equal) => {
                    result = Some(mid);
                    hi = mid;
                }
                Some(std::cmp::Ordering::Greater) => hi = mid,
                None => return Err(mid),
            }
        }

        match result {
            Some(index) => Ok(index),
            None => Err(lo),
        }
    }

    /// Iterate over keys in sorted order.
    pub fn keys(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
        (0..self.count()).filter_map(move |index| self.key(index))
    }
}
