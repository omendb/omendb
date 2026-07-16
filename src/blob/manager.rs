//! Blob file manager.
//!
//! Manages multiple blob files for KV separation. Handles appending,
//! reading, and garbage collection of blob files.

use crate::blob::file::BlobFile;
use crate::btree::node::BlobPointer;
use std::collections::HashSet;

/// Default threshold for blob separation (1KB).
pub const DEFAULT_BLOB_THRESHOLD: usize = 1024;

const BLOB_FORMAT_MAGIC: [u8; 8] = *b"SEERBLB1";
const BLOB_FORMAT_VERSION: u32 = 1;

/// Manages blob files for KV separation.
///
/// Large values (>blob_threshold) are stored in blob files.
/// The B-tree stores blob pointers instead of the actual values.
pub struct BlobManager {
    /// Active blob files.
    files: Vec<BlobFile>,
    /// Next file ID.
    next_file_id: u32,
    /// Threshold for blob separation (in bytes).
    threshold: usize,
    /// Generation whose blob metadata was last durably serialized.
    generation_id: u64,
}

impl BlobManager {
    /// Create a new blob manager with the default threshold.
    pub fn new() -> Self {
        Self::with_threshold(DEFAULT_BLOB_THRESHOLD)
    }

    /// Create a new blob manager with a custom threshold.
    pub fn with_threshold(threshold: usize) -> Self {
        Self {
            files: Vec::new(),
            next_file_id: 1,
            threshold,
            generation_id: 0,
        }
    }

    /// Get the blob threshold.
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// Return the durable generation associated with persisted metadata.
    pub(crate) fn generation_id(&self) -> u64 {
        self.generation_id
    }

    /// Associate the next serialized blob image with a generation.
    pub(crate) fn set_generation(&mut self, generation_id: u64) {
        self.generation_id = generation_id;
    }

    /// Drop deletion marks when the blob image is newer than the manifest.
    pub(crate) fn clear_deletion_metadata(&mut self) {
        for file in &mut self.files {
            file.clear_deletion_metadata();
        }
    }

    /// Number of blob files.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Whether a value should be stored in a blob file.
    pub fn should_separate(&self, value_len: usize) -> bool {
        value_len > self.threshold
    }

    /// Append a value to the active blob file and return a pointer.
    pub fn append(&mut self, key: &[u8], value: Vec<u8>) -> BlobPointer {
        // Get or create the active blob file.
        if self.files.is_empty() {
            self.create_new_file();
        }

        let file = self.files.last_mut().expect("blob file should exist");
        let key_prefix = Self::make_key_prefix(key);
        let (offset, length) = file.append(key_prefix, value);

        BlobPointer {
            file_id: file.file_id(),
            offset,
            length,
        }
    }

    /// Roll back a blob append whose B-tree pointer was not installed.
    pub(crate) fn rollback_append(&mut self, pointer: &BlobPointer) -> bool {
        let Some(file) = self.files.last_mut() else {
            return false;
        };
        if file.file_id() != pointer.file_id
            || !file.rollback_append(pointer.offset, pointer.length)
        {
            return false;
        }

        if file.record_count() == 0 {
            self.files.pop();
        }
        true
    }

    /// Read a value from a blob file.
    pub fn read(&self, ptr: &BlobPointer) -> Option<&[u8]> {
        self.files
            .iter()
            .find(|f| f.file_id() == ptr.file_id)
            .and_then(|f| f.read(ptr.offset, ptr.length))
    }

    /// Mark an entry as deleted (for GC).
    pub fn mark_deleted(&mut self, ptr: &BlobPointer) -> bool {
        if let Some(file) = self.files.iter_mut().find(|f| f.file_id() == ptr.file_id) {
            return file.mark_deleted(ptr.offset);
        }
        false
    }

    /// Get files that need garbage collection.
    pub fn files_needing_gc(&self) -> Vec<u32> {
        self.files
            .iter()
            .filter(|f| f.needs_gc())
            .map(|f| f.file_id())
            .collect()
    }

    /// Run garbage collection on files that need it.
    ///
    /// Only fully dead files are reclaimable without rewriting live pointers.
    /// Mixed files remain available for a future pointer-rewriting compactor.
    /// Returns the number of entries reclaimed.
    pub fn gc(&mut self) -> usize {
        let files_to_gc: Vec<_> = self
            .files
            .iter()
            .filter(|file| file.needs_gc() && file.valid_count() == 0)
            .map(|file| file.file_id())
            .collect();
        if files_to_gc.is_empty() {
            return 0;
        }

        let mut reclaimed = 0;
        for file_id in files_to_gc {
            // Find the file.
            let file_idx = self.files.iter().position(|f| f.file_id() == file_id);
            if let Some(idx) = file_idx {
                let file = &self.files[idx];
                let total = file.record_count();
                let valid = file.valid_count();
                reclaimed += total - valid;
                self.files.remove(idx);
            }
        }

        reclaimed
    }

    /// Get the number of valid entries across all files.
    pub fn total_valid_entries(&self) -> usize {
        self.files.iter().map(|f| f.valid_count()).sum()
    }

    /// Get the number of deleted entries across all files.
    pub fn total_deleted_entries(&self) -> usize {
        self.files.iter().map(|f| f.deleted_count()).sum()
    }

    /// Create a new blob file.
    fn create_new_file(&mut self) {
        let file_id = self.next_file_id;
        self.next_file_id += 1;
        self.files.push(BlobFile::new(file_id));
    }

    /// Make a key prefix (first 8 bytes, padded with zeros if shorter).
    fn make_key_prefix(key: &[u8]) -> [u8; 8] {
        let mut prefix = [0u8; 8];
        let len = key.len().min(8);
        prefix[..len].copy_from_slice(&key[..len]);
        prefix
    }

    /// Serialize all blob files to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.extend_from_slice(&BLOB_FORMAT_MAGIC);
        buf.extend_from_slice(&BLOB_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.threshold as u64).to_le_bytes());
        buf.extend_from_slice(&self.generation_id.to_le_bytes());

        // Write number of files.
        buf.extend_from_slice(&(self.files.len() as u32).to_le_bytes());

        for file in &self.files {
            // Write file ID.
            buf.extend_from_slice(&file.file_id().to_le_bytes());
            // Write file data.
            let file_data = file.to_bytes();
            buf.extend_from_slice(&(file_data.len() as u64).to_le_bytes());
            buf.extend_from_slice(&file_data);
            let deleted_offsets: Vec<_> = file.deleted_offsets().collect();
            buf.extend_from_slice(&(deleted_offsets.len() as u32).to_le_bytes());
            for offset in deleted_offsets {
                buf.extend_from_slice(&offset.to_le_bytes());
            }
        }

        let total_length = buf.len().saturating_add(12) as u64;
        buf.extend_from_slice(&total_length.to_le_bytes());
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.starts_with(&BLOB_FORMAT_MAGIC) {
            return Self::from_versioned_bytes(buf);
        }

        Self::from_legacy_bytes(buf)
    }

    fn from_versioned_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 12 {
            return None;
        }
        let total_length = u64::from_le_bytes(
            buf[buf.len() - 12..buf.len() - 4]
                .try_into()
                .ok()?,
        );
        if total_length != u64::try_from(buf.len()).ok()? {
            return None;
        }
        let stored_checksum = u32::from_le_bytes(buf[buf.len() - 4..].try_into().ok()?);
        if stored_checksum != crc32c::crc32c(&buf[..buf.len() - 4]) {
            return None;
        }

        let payload = &buf[..buf.len() - 12];
        let mut cursor = Cursor::new(payload);
        if cursor.take(BLOB_FORMAT_MAGIC.len())? != BLOB_FORMAT_MAGIC {
            return None;
        }

        if cursor.u32()? != BLOB_FORMAT_VERSION {
            return None;
        }
        let threshold = usize::try_from(cursor.u64()?).ok()?;
        let generation_id = cursor.u64()?;
        let num_files = usize::try_from(cursor.u32()?).ok()?;
        if num_files > cursor.remaining() / 16 {
            return None;
        }

        let mut files = Vec::with_capacity(num_files);
        let mut next_file_id = 1u32;
        let mut file_ids = HashSet::with_capacity(num_files);

        for _ in 0..num_files {
            let file_id = cursor.u32()?;
            if file_id == 0 || file_id == u32::MAX || !file_ids.insert(file_id) {
                return None;
            }

            let data_len = usize::try_from(cursor.u64()?).ok()?;
            let data = cursor.take(data_len)?;
            let mut file = BlobFile::from_bytes(file_id, data)?;
            let deleted_count = usize::try_from(cursor.u32()?).ok()?;
            if deleted_count > file.record_count()
                || deleted_count > cursor.remaining() / std::mem::size_of::<u64>()
            {
                return None;
            }
            let mut deleted_offsets = Vec::with_capacity(deleted_count);
            for _ in 0..deleted_count {
                deleted_offsets.push(cursor.u64()?);
            }
            file.restore_deleted(&deleted_offsets)?;
            next_file_id = next_file_id.max(file_id.checked_add(1)?);
            files.push(file);
        }

        cursor.finish()?;

        Some(Self {
            files,
            next_file_id,
            threshold,
            generation_id,
        })
    }

    fn from_legacy_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 4 {
            return None;
        }

        let mut cursor = Cursor::new(buf);
        let num_files = usize::try_from(cursor.u32()?).ok()?;
        if num_files > cursor.remaining() / 8 {
            return None;
        }
        let mut files = Vec::with_capacity(num_files);
        let mut next_file_id = 1u32;
        let mut file_ids = HashSet::with_capacity(num_files);

        for _ in 0..num_files {
            let file_id = cursor.u32()?;
            if file_id == 0 || file_id == u32::MAX || !file_ids.insert(file_id) {
                return None;
            }
            let data_len = usize::try_from(cursor.u32()?).ok()?;
            let data = cursor.take(data_len)?;
            let file = BlobFile::from_bytes(file_id, data)?;
            next_file_id = next_file_id.max(file_id.checked_add(1)?);
            files.push(file);
        }

        cursor.finish()?;
        Some(Self {
            files,
            next_file_id,
            threshold: DEFAULT_BLOB_THRESHOLD,
            generation_id: 0,
        })
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        let end = self.position.checked_add(length)?;
        let bytes = self.bytes.get(self.position..end)?;
        self.position = end;
        Some(bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn finish(self) -> Option<()> {
        (self.position == self.bytes.len()).then_some(())
    }
}

impl Default for BlobManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_manager_new() {
        let bm = BlobManager::new();
        assert_eq!(bm.threshold(), DEFAULT_BLOB_THRESHOLD);
        assert_eq!(bm.file_count(), 0);
    }

    #[test]
    fn test_blob_manager_rejects_trailing_bytes() {
        let mut bm = BlobManager::new();
        bm.append(b"key", vec![1; 1500]);
        let mut bytes = bm.to_bytes();
        bytes.push(0xA5);

        assert!(BlobManager::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_blob_manager_rejects_corrupt_container_checksum() {
        let mut bm = BlobManager::new();
        bm.append(b"key", vec![1; 1500]);
        let mut bytes = bm.to_bytes();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xA5;

        assert!(BlobManager::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_should_separate() {
        let bm = BlobManager::new();
        assert!(!bm.should_separate(100));
        assert!(!bm.should_separate(1024));
        assert!(bm.should_separate(1025));
    }

    #[test]
    fn test_blob_append_and_read() {
        let mut bm = BlobManager::new();
        let value = vec![42u8; 2000]; // > 1KB threshold

        let ptr = bm.append(b"test_key", value.clone());
        assert_eq!(ptr.length, 2000);

        let read_value = bm.read(&ptr).unwrap();
        assert_eq!(read_value, &value);
    }

    #[test]
    fn test_blob_multiple_appends() {
        let mut bm = BlobManager::new();

        let ptr1 = bm.append(b"key1", vec![1; 1500]);
        let ptr2 = bm.append(b"key2", vec![2; 1500]);

        assert_eq!(bm.read(&ptr1).unwrap(), &vec![1; 1500]);
        assert_eq!(bm.read(&ptr2).unwrap(), &vec![2; 1500]);
    }

    #[test]
    fn test_blob_gc() {
        let mut bm = BlobManager::new();

        let ptr1 = bm.append(b"key1", vec![1; 1500]);
        let ptr2 = bm.append(b"key2", vec![2; 1500]);
        let ptr3 = bm.append(b"key3", vec![3; 1500]);

        assert!(bm.files_needing_gc().is_empty());

        // Mark enough entries as deleted to trigger GC.
        bm.mark_deleted(&ptr1);
        bm.mark_deleted(&ptr2);

        assert!(!bm.files_needing_gc().is_empty());
        assert_eq!(bm.gc(), 0);
        assert_eq!(bm.read(&ptr3), Some(&vec![3; 1500][..]));
    }

    #[test]
    fn test_blob_gc_reclaims_fully_dead_file() {
        let mut bm = BlobManager::new();
        let ptr = bm.append(b"key", vec![1; 1500]);
        assert!(bm.mark_deleted(&ptr));

        assert_eq!(bm.gc(), 1);
        assert_eq!(bm.file_count(), 0);
    }

    #[test]
    fn test_blob_rollback_only_removes_unpublished_tail() {
        let mut bm = BlobManager::new();
        let first = bm.append(b"first", vec![1; 1500]);
        let second = bm.append(b"second", vec![2; 1600]);

        assert!(!bm.rollback_append(&first));
        assert!(bm.rollback_append(&second));
        assert_eq!(
            bm.read(&first).map(|value| (value.len(), value[0])),
            Some((1500, 1))
        );
        assert!(bm.read(&second).is_none());
        assert!(!bm.rollback_append(&second));

        assert!(bm.rollback_append(&first));
        assert_eq!(bm.file_count(), 0);
    }

    #[test]
    fn test_blob_roundtrip_preserves_deletion_metadata() {
        let mut bm = BlobManager::with_threshold(2048);
        let ptr = bm.append(b"key", vec![1; 1500]);
        assert!(bm.mark_deleted(&ptr));

        let restored = BlobManager::from_bytes(&bm.to_bytes()).unwrap();
        assert_eq!(restored.threshold(), 2048);
        assert_eq!(restored.files_needing_gc(), vec![ptr.file_id]);

        let mut restored = restored;
        assert_eq!(restored.gc(), 1);
        assert_eq!(restored.file_count(), 0);
    }

    #[test]
    fn test_blob_manager_accepts_legacy_format() {
        let mut file = BlobFile::new(1);
        file.append([0; 8], vec![1; 1500]);
        let file_data = file.to_bytes();
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&1u32.to_le_bytes());
        legacy.extend_from_slice(&1u32.to_le_bytes());
        legacy.extend_from_slice(&(file_data.len() as u32).to_le_bytes());
        legacy.extend_from_slice(&file_data);

        let restored = BlobManager::from_bytes(&legacy).unwrap();
        assert_eq!(restored.file_count(), 1);
        assert_eq!(restored.total_valid_entries(), 1);
    }

    #[test]
    fn test_blob_manager_rejects_future_format_and_duplicate_ids() {
        let mut manager = BlobManager::new();
        manager.append(b"key", vec![1; 1500]);
        let mut future = manager.to_bytes();
        future[8..12].copy_from_slice(&2u32.to_le_bytes());
        assert!(BlobManager::from_bytes(&future).is_none());

        manager.files.push(BlobFile::new(2));
        let mut duplicate = manager.to_bytes();
        // Header is 32 bytes; skip the first file descriptor and its data.
        let second_file_id = 32 + 4 + 8 + manager.files[0].to_bytes().len() + 4;
        duplicate[second_file_id..second_file_id + 4]
            .copy_from_slice(&manager.files[0].file_id().to_le_bytes());
        assert!(BlobManager::from_bytes(&duplicate).is_none());
    }

    #[test]
    fn test_blob_key_prefix() {
        let prefix = BlobManager::make_key_prefix(b"hello");
        assert_eq!(&prefix, b"hello\0\0\0");

        let prefix = BlobManager::make_key_prefix(b"hello_world!");
        assert_eq!(&prefix, b"hello_wo");
    }
}
