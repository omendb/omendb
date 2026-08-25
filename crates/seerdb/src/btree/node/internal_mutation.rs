//! Internal-node separator and child-pointer mutation ownership.

use super::page_format::{HEADER_SIZE, SLOT_SIZE};
use super::{InsertError, Node, PAGE_SIZE};

impl Node {
    /// Insert a child pointer for an internal node.
    ///
    /// The leftmost child (before the first key) is stored separately via
    /// `set_leftmost_child` / `leftmost_child`.
    pub fn insert_child(&mut self, key: &[u8], child_id: u64) -> Result<(), InsertError> {
        if !self.is_internal() {
            return Err(InsertError::WrongNodeType);
        }

        let insertion_point = match self.search(key) {
            Ok(index) | Err(index) => index,
        };

        // Internal keys use the same predecessor-relative compression as leaf
        // keys. Rebuild when inserting into the middle so subsequent
        // separators remain decodable and keep their routing bounds.
        if insertion_point < self.count() && self.has_prefix_compression() {
            let parent_id = self.parent_id();
            let leftmost_child = self.leftmost_child();
            let mut entries = Vec::with_capacity(self.count() + 1);
            for index in 0..self.count() {
                let entry_key = self.key(index).ok_or(InsertError::WrongNodeType)?;
                let entry_child = self.child_id(index).ok_or(InsertError::WrongNodeType)?;
                entries.push((entry_key, entry_child));
            }
            entries.insert(insertion_point, (key.to_vec(), child_id));

            let mut replacement = Self::new_internal();
            replacement.set_parent_id(parent_id);
            replacement.set_leftmost_child(leftmost_child);
            for (entry_key, entry_child) in entries {
                let append_at = replacement.count();
                replacement.insert_child_raw(&entry_key, entry_child, append_at)?;
            }
            *self = replacement;
            return Ok(());
        }

        self.insert_child_raw(key, child_id, insertion_point)
    }

    fn insert_child_raw(
        &mut self,
        key: &[u8],
        child_id: u64,
        insertion_point: usize,
    ) -> Result<(), InsertError> {
        if !self.is_internal() || insertion_point > self.count() {
            return Err(InsertError::WrongNodeType);
        }

        let prefix_len = 0_u16;
        let suffix = key;
        let entry_size = 4 + suffix.len() + 8;
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
        self.data[pos..pos + 8].copy_from_slice(&child_id.to_le_bytes());

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
}
