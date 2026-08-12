//! Leaf-node mutation, compaction, and value encoding ownership.

use super::page_format::{HEADER_SIZE, SLOT_SIZE};
use super::{BlobPointer, InsertError, Node, PAGE_SIZE, ValueRef, ValueType};

impl Node {
    /// Calculate the offset where a new entry should be placed.
    ///
    /// Entries are packed from the end of the page. New entries go just before
    /// the lowest existing entry offset, leaving the slot array at the front.
    pub(in crate::btree::node) fn new_entry_offset(&self, entry_size: usize) -> Option<usize> {
        let count = self.count();
        if count == 0 {
            return Some(PAGE_SIZE - entry_size);
        }

        let min_offset = (0..count)
            .map(|index| self.slot_offset(index))
            .min()
            .unwrap_or(PAGE_SIZE);

        if min_offset < entry_size {
            None
        } else {
            Some(min_offset - entry_size)
        }
    }

    /// Insert a key-value pair into this leaf node.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), InsertError> {
        if !self.is_leaf() {
            return Err(InsertError::WrongNodeType);
        }

        if let Ok(index) = self.search(key) {
            return Err(InsertError::DuplicateKey(index));
        }

        let insertion_point = self.search(key).unwrap_err();
        self.insert_leaf_value(key, ValueType::Inline, value, insertion_point)
    }

    /// Insert a leaf value at an already selected slot position.
    ///
    /// The caller controls duplicate ordering. Public mutation APIs reject
    /// duplicate live keys, while internal rebuilds use the upper bound so
    /// tombstone/version history is preserved in its original order.
    pub(super) fn insert_leaf_value(
        &mut self,
        key: &[u8],
        value_type: ValueType,
        value: &[u8],
        insertion_point: usize,
    ) -> Result<(), InsertError> {
        if !self.is_leaf() || insertion_point > self.count() {
            return Err(InsertError::WrongNodeType);
        }

        // Prefix compression is relative to the preceding key. Inserting in
        // the middle changes that predecessor for every following entry, so
        // retain the logical entries and rebuild their encoded forms instead
        // of leaving stale prefix lengths in place.
        if insertion_point < self.count() && self.has_prefix_compression() {
            let parent_id = self.parent_id();
            let mut entries = Vec::with_capacity(self.count() + 1);
            for index in 0..self.count() {
                let entry_key = self.key(index).ok_or(InsertError::WrongNodeType)?;
                let (entry_type, entry_value) =
                    match self.value(index).ok_or(InsertError::WrongNodeType)? {
                        ValueRef::Inline(data) => (ValueType::Inline, data.to_vec()),
                        ValueRef::Blob(pointer) => {
                            (ValueType::BlobPointer, pointer.to_bytes().to_vec())
                        }
                        ValueRef::Tombstone => (ValueType::Tombstone, Vec::new()),
                    };
                entries.push((entry_key, entry_type, entry_value));
            }
            entries.insert(insertion_point, (key.to_vec(), value_type, value.to_vec()));

            let mut replacement = Self::new_leaf();
            replacement.set_parent_id(parent_id);
            for (entry_key, entry_type, entry_value) in entries {
                let append_at = replacement.count();
                replacement.insert_leaf_value_raw(
                    &entry_key,
                    entry_type,
                    &entry_value,
                    append_at,
                )?;
            }
            *self = replacement;
            return Ok(());
        }

        self.insert_leaf_value_raw(key, value_type, value, insertion_point)
    }

    fn insert_leaf_value_raw(
        &mut self,
        key: &[u8],
        value_type: ValueType,
        value: &[u8],
        insertion_point: usize,
    ) -> Result<(), InsertError> {
        if !self.is_leaf() {
            return Err(InsertError::WrongNodeType);
        }

        // New mutations use self-contained keys. Existing compressed pages
        // are normalized by the wrapper above when a middle mutation touches
        // them.
        let prefix_len = 0_u16;
        let suffix = key;

        let entry_size = 4 + suffix.len() + 1 + value.len();
        if entry_size > PAGE_SIZE - HEADER_SIZE - SLOT_SIZE {
            return Err(InsertError::EntryTooLarge);
        }
        let count = self.count();

        let slot_array_end = Self::slot_array_start() + (count + 1) * SLOT_SIZE;
        let entry_offset = self
            .new_entry_offset(entry_size)
            .ok_or(InsertError::PageFull)?;

        if slot_array_end + SLOT_SIZE > entry_offset {
            return Err(InsertError::PageFull);
        }

        let mut pos = entry_offset;
        self.data[pos..pos + 2].copy_from_slice(&prefix_len.to_le_bytes());
        pos += 2;
        self.data[pos..pos + 2].copy_from_slice(&(suffix.len() as u16).to_le_bytes());
        pos += 2;
        self.data[pos..pos + suffix.len()].copy_from_slice(suffix);
        pos += suffix.len();
        self.data[pos] = value_type as u8;
        pos += 1;
        self.data[pos..pos + value.len()].copy_from_slice(value);

        for index in (insertion_point..count).rev() {
            let old_offset = self.slot_offset(index) as u16;
            let old_key_len = self.slot_key_len(index);
            self.set_slot_offset(index + 1, old_offset);
            self.set_slot_key_len(index + 1, old_key_len);
        }

        self.set_slot_offset(insertion_point, entry_offset as u16);
        self.set_slot_key_len(insertion_point, prefix_len + suffix.len() as u16);

        let mut header = self.header();
        header.count += 1;
        let new_slot_array_end = Self::slot_array_start() + header.count as usize * SLOT_SIZE;
        let min_entry = (0..header.count as usize)
            .map(|index| self.slot_offset(index))
            .min()
            .unwrap_or(PAGE_SIZE);
        header.free_space = (min_entry - new_slot_array_end) as u32;
        self.set_header(&header);

        Ok(())
    }

    /// Return the insertion point after all existing versions of `key`.
    pub(super) fn upper_bound(&self, key: &[u8]) -> usize {
        let mut index = match self.search(key) {
            Ok(index) | Err(index) => index,
        };
        while index < self.count() && self.key(index).as_deref() == Some(key) {
            index += 1;
        }
        index
    }

    /// Replace a value in-place when the encoded value size is unchanged.
    pub fn replace_value(&mut self, index: usize, new_value: &[u8]) -> Result<(), InsertError> {
        if !self.is_leaf() {
            return Err(InsertError::WrongNodeType);
        }
        if index >= self.count() {
            return Err(InsertError::InvalidIndex(index));
        }

        let entry_off = self.slot_offset(index);
        let suffix_len = self.entry_suffix_len(index) as usize;
        let vt_off = entry_off + 4 + suffix_len;
        let val_start = vt_off + 1;

        let old_value_type = self.data[vt_off];
        if old_value_type != ValueType::Inline as u8 {
            return Err(InsertError::WrongNodeType);
        }

        let val_end = (0..self.count())
            .filter(|&i| i != index)
            .map(|i| self.slot_offset(i))
            .filter(|&off| off > entry_off)
            .min()
            .unwrap_or(PAGE_SIZE);
        let old_size = val_end
            .checked_sub(val_start)
            .ok_or(InsertError::WrongNodeType)?;
        if new_value.len() != old_size {
            return Err(InsertError::ValueSizeMismatch {
                expected: old_size,
                actual: new_value.len(),
            });
        }

        self.data[val_start..val_start + new_value.len()].copy_from_slice(new_value);
        self.data[vt_off] = ValueType::Inline as u8;
        Ok(())
    }

    /// Replace an entry with a value of any size, rebuilding the leaf when
    /// an in-place replacement is not possible.
    ///
    /// The original node remains unchanged when the rebuilt page cannot fit.
    pub fn replace_value_resized(
        &mut self,
        index: usize,
        new_value: &[u8],
    ) -> Result<(), InsertError> {
        if !self.is_leaf() || index >= self.count() {
            return Err(InsertError::WrongNodeType);
        }

        let key = self.key(index).ok_or(InsertError::WrongNodeType)?;
        let original = self.clone();
        self.remove_entry(index)?;
        while let Ok(duplicate_index) = self.search(&key) {
            self.remove_entry(duplicate_index)?;
        }
        match self.insert(&key, new_value) {
            Ok(()) => Ok(()),
            Err(error) => {
                *self = original;
                Err(error)
            }
        }
    }

    /// Remove one leaf entry and compact the remaining entries.
    pub fn remove_entry(&mut self, index: usize) -> Result<(), InsertError> {
        if !self.is_leaf() || index >= self.count() {
            return Err(InsertError::WrongNodeType);
        }

        enum OwnedValue {
            Inline(Vec<u8>),
            Blob(BlobPointer),
            Tombstone,
        }

        let parent_id = self.parent_id();
        let mut entries = Vec::with_capacity(self.count() - 1);
        for entry_index in 0..self.count() {
            if entry_index == index {
                continue;
            }
            let key = self.key(entry_index).ok_or(InsertError::WrongNodeType)?;
            let value = match self.value(entry_index).ok_or(InsertError::WrongNodeType)? {
                ValueRef::Inline(value) => OwnedValue::Inline(value.to_vec()),
                ValueRef::Blob(pointer) => OwnedValue::Blob(pointer),
                ValueRef::Tombstone => OwnedValue::Tombstone,
            };
            entries.push((key, value));
        }

        let mut replacement = Self::new_leaf();
        replacement.set_parent_id(parent_id);
        for (key, value) in entries {
            let insertion_point = replacement.upper_bound(&key);
            let result = match value {
                OwnedValue::Inline(value) => {
                    replacement.insert_leaf_value(&key, ValueType::Inline, &value, insertion_point)
                }
                OwnedValue::Blob(pointer) => {
                    let bytes = pointer.to_bytes();
                    replacement.insert_leaf_value(
                        &key,
                        ValueType::BlobPointer,
                        &bytes,
                        insertion_point,
                    )
                }
                OwnedValue::Tombstone => {
                    replacement.insert_leaf_value(&key, ValueType::Tombstone, &[], insertion_point)
                }
            };
            result?;
        }

        *self = replacement;
        Ok(())
    }

    /// Insert a key with a tombstone marker (delete).
    pub fn insert_tombstone(&mut self, key: &[u8]) -> Result<(), InsertError> {
        if !self.is_leaf() {
            return Err(InsertError::WrongNodeType);
        }

        let insertion_point = match self.search(key) {
            Ok(index) => {
                self.remove_entry(index)?;
                self.search(key).unwrap_or_else(|index| index)
            }
            Err(index) => index,
        };
        self.insert_leaf_value(key, ValueType::Tombstone, &[], insertion_point)
    }

    /// Insert a key with a blob pointer value.
    pub fn insert_blob(&mut self, key: &[u8], pointer: BlobPointer) -> Result<(), InsertError> {
        if !self.is_leaf() {
            return Err(InsertError::WrongNodeType);
        }

        if let Ok(index) = self.search(key) {
            return Err(InsertError::DuplicateKey(index));
        }

        let insertion_point = self.search(key).unwrap_err();
        let bytes = pointer.to_bytes();
        self.insert_leaf_value(key, ValueType::BlobPointer, &bytes, insertion_point)
    }
}
