//! Blob file manager.
//!
//! Manages multiple blob files for KV separation. Handles appending,
//! reading, and garbage collection of blob files.

use crate::blob::file::BlobFile;
use crate::btree::node::BlobPointer;

/// Default threshold for blob separation (1KB).
pub const DEFAULT_BLOB_THRESHOLD: usize = 1024;

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
        }
    }

    /// Get the blob threshold.
    pub fn threshold(&self) -> usize {
        self.threshold
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

    /// Read a value from a blob file.
    pub fn read(&self, ptr: &BlobPointer) -> Option<&[u8]> {
        self.files
            .iter()
            .find(|f| f.file_id() == ptr.file_id)
            .and_then(|f| f.read(ptr.offset, ptr.length))
    }

    /// Mark an entry as deleted (for GC).
    pub fn mark_deleted(&mut self, ptr: &BlobPointer) {
        if let Some(file) = self.files.iter_mut().find(|f| f.file_id() == ptr.file_id) {
            file.mark_deleted(ptr.offset);
        }
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

        // Write number of files.
        buf.extend_from_slice(&(self.files.len() as u32).to_le_bytes());

        for file in &self.files {
            // Write file ID.
            buf.extend_from_slice(&file.file_id().to_le_bytes());
            // Write file data.
            let file_data = file.to_bytes();
            buf.extend_from_slice(&(file_data.len() as u32).to_le_bytes());
            buf.extend_from_slice(&file_data);
        }

        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 4 {
            return None;
        }

        let num_files = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let mut files = Vec::with_capacity(num_files);
        let mut next_file_id = 1u32;
        let mut pos = 4;

        for _ in 0..num_files {
            if buf.len() < pos + 8 {
                return None;
            }

            let file_id = u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
            let data_len =
                u32::from_le_bytes([buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]])
                    as usize;
            pos += 8;

            if buf.len() < pos + data_len {
                return None;
            }

            let file = BlobFile::from_bytes(file_id, &buf[pos..pos + data_len])?;
            next_file_id = next_file_id.max(file_id + 1);
            files.push(file);
            pos += data_len;
        }

        if pos != buf.len() {
            return None;
        }

        Some(Self {
            files,
            next_file_id,
            threshold: DEFAULT_BLOB_THRESHOLD,
        })
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
        bm.mark_deleted(&ptr);

        assert_eq!(bm.gc(), 1);
        assert_eq!(bm.file_count(), 0);
    }

    #[test]
    fn test_blob_key_prefix() {
        let prefix = BlobManager::make_key_prefix(b"hello");
        assert_eq!(&prefix, b"hello\0\0\0");

        let prefix = BlobManager::make_key_prefix(b"hello_world!");
        assert_eq!(&prefix, b"hello_wo");
    }
}
