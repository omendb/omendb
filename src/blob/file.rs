//! Blob file: append-only file for storing large values.
//!
//! Each blob record is:
//! ```text
//! [key_prefix: 8 bytes] [length: u32] [value: bytes] [crc32c: u32]
//! ```

use super::BlobRecord;
use std::collections::BTreeSet;

/// An append-only blob file.
///
/// Blob files store large values that don't fit in B-tree nodes.
/// Records are appended sequentially and never modified in place.
#[derive(Clone)]
pub struct BlobFile {
    /// File ID (unique identifier).
    file_id: u32,
    /// In-memory buffer of records.
    records: Vec<BlobRecord>,
    /// Serialized offset of each record, kept in the same order as `records`.
    /// This is derived state used for binary-search pointer lookup.
    record_offsets: Vec<u64>,
    /// Current offset (total bytes written).
    offset: u64,
    /// Number of valid (non-deleted) entries.
    valid_count: usize,
    /// Number of deleted entries.
    deleted_count: usize,
    /// Record offsets that have been logically deleted.
    deleted_offsets: BTreeSet<u64>,
}

impl BlobFile {
    /// Create a new empty blob file.
    pub fn new(file_id: u32) -> Self {
        Self {
            file_id,
            records: Vec::new(),
            record_offsets: Vec::new(),
            offset: 0,
            valid_count: 0,
            deleted_count: 0,
            deleted_offsets: BTreeSet::new(),
        }
    }

    /// Get the file ID.
    pub fn file_id(&self) -> u32 {
        self.file_id
    }

    /// Current write offset.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Number of records in the file.
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Number of valid (non-deleted) entries.
    pub fn valid_count(&self) -> usize {
        self.valid_count
    }

    /// Number of deleted entries.
    pub fn deleted_count(&self) -> usize {
        self.deleted_count
    }

    /// Whether the file needs garbage collection (>50% deleted).
    pub fn needs_gc(&self) -> bool {
        let total = self.valid_count + self.deleted_count;
        total > 0 && self.deleted_count > total / 2
    }

    /// Append a value and return the offset and length.
    pub fn append(&mut self, key_prefix: [u8; 8], value: Vec<u8>) -> (u64, u32) {
        let record = BlobRecord::new(key_prefix, value);
        let length = record.value.len() as u32;
        let current_offset = self.offset;

        self.offset += record.serialized_size() as u64;
        self.valid_count += 1;
        self.record_offsets.push(current_offset);
        self.records.push(record);

        (current_offset, length)
    }

    /// Roll back the most recent append before the record is published.
    ///
    /// Blob appends are staged in memory until the owning B-tree mutation
    /// succeeds. Restricting rollback to the tail preserves append ordering
    /// and avoids exposing a partially applied blob mutation to later writes.
    pub(crate) fn rollback_append(&mut self, offset: u64, length: u32) -> bool {
        let Some(record) = self.records.last() else {
            return false;
        };
        let expected_offset = self
            .offset
            .saturating_sub(record.serialized_size() as u64);
        if expected_offset != offset
            || record.value.len() != length as usize
            || self.deleted_offsets.contains(&offset)
        {
            return false;
        }

        self.records.pop();
        self.record_offsets.pop();
        self.offset = offset;
        self.valid_count = self.valid_count.saturating_sub(1);
        true
    }

    pub(crate) fn serialized_size(&self) -> Option<u64> {
        self.records.iter().try_fold(0u64, |size, record| {
            size.checked_add(u64::try_from(record.serialized_size()).ok()?)
        })
    }

    pub(crate) fn can_mark_deleted(&self, offset: u64) -> bool {
        self.has_record_at(offset) && !self.deleted_offsets.contains(&offset)
    }

    /// Read a value at the given offset and length.
    pub fn read(&self, offset: u64, length: u32) -> Option<&[u8]> {
        let index = self.record_offsets.binary_search(&offset).ok()?;
        let record = self.records.get(index)?;
        (record.value.len() == length as usize).then_some(record.value.as_slice())
    }

    /// Mark an entry as deleted (for GC).
    pub fn mark_deleted(&mut self, offset: u64) -> bool {
        if !self.has_record_at(offset) || !self.deleted_offsets.insert(offset) {
            return false;
        }

        self.deleted_count += 1;
        self.valid_count = self.valid_count.saturating_sub(1);
        true
    }

    /// Restore persisted deletion metadata after loading the record stream.
    pub(crate) fn restore_deleted(&mut self, offsets: &[u64]) -> Option<()> {
        for &offset in offsets {
            if !self.mark_deleted(offset) {
                return None;
            }
        }
        Some(())
    }

    /// Return deleted record offsets in stable order for persistence.
    pub(crate) fn deleted_offsets(&self) -> impl Iterator<Item = u64> + '_ {
        self.deleted_offsets.iter().copied()
    }

    pub(crate) fn clear_deletion_metadata(&mut self) {
        self.deleted_offsets.clear();
        self.deleted_count = 0;
        self.valid_count = self.records.len();
    }

    /// Mark every record deleted for a pointer-rewriting compaction.
    pub(crate) fn mark_all_deleted(&mut self) {
        self.deleted_offsets.clear();
        for &offset in &self.record_offsets {
            self.deleted_offsets.insert(offset);
        }
        self.deleted_count = self.records.len();
        self.valid_count = 0;
    }

    fn has_record_at(&self, offset: u64) -> bool {
        self.record_offsets.binary_search(&offset).is_ok()
    }

    /// Serialize all records to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for record in &self.records {
            buf.extend_from_slice(&record.to_bytes());
        }
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(file_id: u32, buf: &[u8]) -> Option<Self> {
        let mut records = Vec::new();
        let mut record_offsets = Vec::new();
        let mut pos = 0;

        while pos < buf.len() {
            record_offsets.push(u64::try_from(pos).ok()?);
            let record = BlobRecord::from_bytes(&buf[pos..])?;
            let size = record.serialized_size();
            records.push(record);
            pos += size;
        }

        let offset = records.iter().map(|r| r.serialized_size() as u64).sum();
        let valid_count = records.len();

        Some(Self {
            file_id,
            records,
            record_offsets,
            offset,
            valid_count,
            deleted_count: 0,
            deleted_offsets: BTreeSet::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_file_append() {
        let mut file = BlobFile::new(1);
        let (offset, length) = file.append([0; 8], vec![1, 2, 3]);

        assert_eq!(offset, 0);
        assert_eq!(length, 3);
        assert_eq!(file.record_count(), 1);
    }

    #[test]
    fn test_blob_file_read() {
        let mut file = BlobFile::new(1);
        let (offset, length) = file.append([0; 8], vec![10, 20, 30]);

        let data = file.read(offset, length).unwrap();
        assert_eq!(data, &[10, 20, 30]);
    }

    #[test]
    fn test_blob_file_indexed_lookup_survives_many_records_and_reopen() {
        let mut file = BlobFile::new(1);
        let mut pointers = Vec::new();
        for index in 0..256u16 {
            pointers.push(file.append([0; 8], vec![index as u8; usize::from(index) + 1]));
        }

        let (offset, length) = pointers[193];
        assert_eq!(file.read(offset, length), Some(&vec![193; 194][..]));
        assert!(file.read(offset + 1, length).is_none());
        assert!(file.mark_deleted(offset));
        assert!(!file.mark_deleted(offset));

        let restored = BlobFile::from_bytes(1, &file.to_bytes()).unwrap();
        assert_eq!(
            restored.read(pointers[192].0, pointers[192].1),
            Some(&vec![192; 193][..])
        );
        assert_eq!(restored.read(offset, length), Some(&vec![193; 194][..]));
    }

    #[test]
    fn test_blob_file_gc() {
        let mut file = BlobFile::new(1);
        let (offset1, _) = file.append([0; 8], vec![1]);
        let (offset2, _) = file.append([0; 8], vec![2]);
        file.append([0; 8], vec![3]);

        assert!(!file.needs_gc());

        assert!(file.mark_deleted(offset1));
        assert!(file.mark_deleted(offset2));
        assert!(!file.mark_deleted(offset2));

        assert!(file.needs_gc());
    }

    #[test]
    fn test_blob_file_serialization() {
        let mut file = BlobFile::new(1);
        file.append([1, 2, 3, 4, 5, 6, 7, 8], vec![10, 20, 30]);
        file.append([9, 10, 11, 12, 13, 14, 15, 16], vec![40, 50, 60]);

        let bytes = file.to_bytes();
        let restored = BlobFile::from_bytes(1, &bytes).unwrap();

        assert_eq!(restored.record_count(), 2);
    }

    #[test]
    fn test_blob_file_rejects_truncated_record() {
        let mut file = BlobFile::new(1);
        file.append([0; 8], vec![1, 2, 3]);
        let mut bytes = file.to_bytes();
        bytes.pop();

        assert!(BlobFile::from_bytes(1, &bytes).is_none());
    }
}
