//! Blob file manager.
//!
//! Manages multiple blob files for KV separation. Handles appending,
//! reading, and garbage collection of blob files.

use crate::blob::file::{BlobFile, BlobRecord};
use crate::btree::node::BlobPointer;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

#[path = "image_format.rs"]
mod image_format;
#[path = "segment_catalog.rs"]
mod segment_catalog;

/// Default threshold for blob separation (1KB).
pub const DEFAULT_BLOB_THRESHOLD: usize = 1024;

const DEFAULT_SEGMENT_TARGET_SIZE: u64 = 64 * 1024 * 1024;

/// Manages blob files for KV separation.
///
/// Large values (>blob_threshold) are stored in blob files.
/// The B-tree stores blob pointers instead of the actual values.
/// Cloning a manager shares immutable blob files; mutations clone only the
/// affected file, keeping candidate batch state isolated without copying the
/// entire blob catalog.
#[derive(Clone)]
pub struct BlobManager {
    /// Active blob files.
    files: Vec<Arc<BlobFile>>,
    /// Next file ID.
    next_file_id: u32,
    /// Threshold for blob separation (in bytes).
    threshold: usize,
    /// Generation whose blob metadata was last durably serialized.
    generation_id: u64,
    /// Whether records live in separate append-only segment files.
    segmented: bool,
    /// Catalog length already durable for each segment. A failed publication
    /// may leave an ignored suffix in a segment, so the next catalog always
    /// appends from this frontier rather than trusting physical file length.
    persisted_lengths: HashMap<u32, u64>,
    /// Deletion offsets already represented by the durable catalog frontier.
    persisted_deleted_offsets: HashMap<u32, BTreeSet<u64>>,
    /// Generation represented by the durable catalog frontier.
    persisted_generation_id: u64,
    /// Number of delta frames after the full catalog anchor.
    catalog_delta_count: u32,
    /// Whether a full or delta catalog has been durably initialized.
    catalog_persisted: bool,
    /// Target size for the active segmented blob file. A record larger than
    /// this target is kept intact in its own segment.
    segment_target_size: u64,
}

/// Bounded cursor shared by the blob image and segmented catalog parsers.
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
            segmented: false,
            persisted_lengths: HashMap::new(),
            persisted_deleted_offsets: HashMap::new(),
            persisted_generation_id: 0,
            catalog_delta_count: 0,
            catalog_persisted: false,
            segment_target_size: DEFAULT_SEGMENT_TARGET_SIZE,
        }
    }

    /// Create a manager for a newly created database with an explicit layout.
    pub(crate) fn with_threshold_and_mode(threshold: usize, segmented: bool) -> Self {
        let mut manager = Self::with_threshold(threshold);
        manager.segmented = segmented;
        manager
    }

    #[cfg(test)]
    fn with_threshold_and_mode_and_segment_size(
        threshold: usize,
        segmented: bool,
        segment_target_size: u64,
    ) -> Self {
        let mut manager = Self::with_threshold_and_mode(threshold, segmented);
        manager.segment_target_size = segment_target_size;
        manager
    }

    #[cfg(test)]
    pub(crate) fn set_segment_target_size_for_test(&mut self, segment_target_size: u64) {
        self.segment_target_size = segment_target_size;
    }

    /// Whether this manager uses the separate segment/catalog layout.
    pub(crate) fn is_segmented(&self) -> bool {
        self.segmented
    }

    pub(crate) fn is_segment_catalog(buf: &[u8]) -> bool {
        segment_catalog::is_segment_catalog(buf)
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
            Arc::make_mut(file).clear_deletion_metadata();
        }
    }

    /// Mark all existing records dead before a pointer-rewriting compaction.
    ///
    /// The active B-tree values are copied into a new file afterward. Keeping
    /// the old records in the candidate image is required until its manifest
    /// is durable; the database also keeps the prior serialized image aside so
    /// an interrupted rewrite can restore the exact old root image.
    pub(crate) fn mark_all_deleted(&mut self) {
        for file in &mut self.files {
            Arc::make_mut(file).mark_all_deleted();
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
        if self.files.is_empty() || self.should_rollover(value.len()) {
            self.create_new_file();
        }

        let file = Arc::make_mut(self.files.last_mut().expect("blob file should exist"));
        let key_prefix = Self::make_key_prefix(key);
        let (offset, length) = file.append(key_prefix, value);

        BlobPointer {
            file_id: file.file_id(),
            offset,
            length,
        }
    }

    /// Start a fresh file for pointer-rewriting compaction.
    pub(crate) fn begin_compaction_file(&mut self) -> Option<u32> {
        let file_id = self.next_file_id;
        if file_id == 0 || file_id == u32::MAX {
            return None;
        }
        self.next_file_id = file_id.checked_add(1)?;
        self.files.push(Arc::new(BlobFile::new(file_id)));
        self.persisted_lengths.entry(file_id).or_insert(0);
        Some(file_id)
    }

    /// Roll back a blob append whose B-tree pointer was not installed.
    pub(crate) fn rollback_append(&mut self, pointer: &BlobPointer) -> bool {
        let Some(file) = self.files.last_mut() else {
            return false;
        };
        let file = Arc::make_mut(file);
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

    fn can_mark_deleted(&self, pointer: &BlobPointer) -> bool {
        self.files
            .iter()
            .find(|file| file.file_id() == pointer.file_id)
            .is_some_and(|file| file.can_mark_deleted(pointer.offset))
    }

    fn should_rollover(&self, value_len: usize) -> bool {
        if !self.segmented {
            return false;
        }

        let Some(file) = self.files.last() else {
            return false;
        };
        let Some(current_size) = file.serialized_size() else {
            return false;
        };
        let Some(record_size) = BlobRecord::OVERHEAD_SIZE
            .checked_add(value_len)
            .and_then(|size| u64::try_from(size).ok())
        else {
            return true;
        };

        current_size > 0
            && current_size
                .checked_add(record_size)
                .is_none_or(|size| size > self.segment_target_size)
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
        if let Some(file) = self
            .files
            .iter_mut()
            .find(|file| file.file_id() == ptr.file_id)
        {
            return Arc::make_mut(file).mark_deleted(ptr.offset);
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

    /// Whether GC can remove at least one fully dead blob file without
    /// rewriting any live pointers.
    pub(crate) fn has_reclaimable_files(&self) -> bool {
        self.files
            .iter()
            .any(|file| file.needs_gc() && file.valid_count() == 0)
    }

    /// Run the low-level sweep on files that are fully dead.
    ///
    /// The database-level `DB::gc()` performs pointer rewriting for mixed
    /// files before calling this method. This method itself never rewrites
    /// live pointers.
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
        self.begin_compaction_file()
            .expect("blob file ID space exhausted");
    }

    /// Make a key prefix (first 8 bytes, padded with zeros if shorter).
    fn make_key_prefix(key: &[u8]) -> [u8; 8] {
        let mut prefix = [0u8; 8];
        let len = key.len().min(8);
        prefix[..len].copy_from_slice(&key[..len]);
        prefix
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
    fn test_blob_manager_clone_isolated_after_mutation() {
        let mut original = BlobManager::new();
        let pointer = original.append(b"key", vec![1; 1500]);

        let mut candidate = original.clone();
        assert!(candidate.mark_deleted(&pointer));
        let candidate_pointer = candidate.append(b"another", vec![2; 1600]);

        assert_eq!(original.total_valid_entries(), 1);
        assert_eq!(original.total_deleted_entries(), 0);
        assert_eq!(original.read(&pointer), Some(&vec![1; 1500][..]));
        assert!(original.read(&candidate_pointer).is_none());
        assert_eq!(candidate.total_valid_entries(), 1);
        assert_eq!(candidate.total_deleted_entries(), 1);
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
    fn test_blob_projected_size_matches_serialized_image() {
        let mut bm = BlobManager::new();
        assert_eq!(bm.serialized_size(), Some(bm.to_bytes().len() as u64));

        let projected_append = bm.projected_serialized_size(None, Some(1_500)).unwrap();
        let pointer = bm.append(b"key", vec![1; 1_500]);
        assert_eq!(projected_append, bm.to_bytes().len() as u64);
        assert_eq!(bm.serialized_size(), Some(projected_append));

        let projected_delete = bm.projected_serialized_size(Some(&pointer), None).unwrap();
        assert!(bm.mark_deleted(&pointer));
        assert_eq!(projected_delete, bm.to_bytes().len() as u64);
        assert_eq!(bm.serialized_size(), Some(projected_delete));
    }

    #[test]
    fn test_segmented_append_rolls_over_at_target_and_accounts_for_header() {
        let mut manager = BlobManager::with_threshold_and_mode_and_segment_size(1024, true, 64);
        let first = manager.append(b"first", vec![1; 40]);
        manager.capture_persisted_state();
        let projected = manager
            .projected_segment_write_size(None, Some(40))
            .expect("segmented projection should fit");

        let second = manager.append(b"second", vec![2; 40]);
        assert_eq!(first.file_id, 1);
        assert_eq!(first.offset, 0);
        assert_eq!(second.file_id, 2);
        assert_eq!(second.offset, 0);
        assert_eq!(manager.segment_file_ids(), vec![1, 2]);
        assert_eq!(projected, manager.segment_write_size().unwrap());
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
    fn test_segment_catalog_roundtrip_ignores_unpublished_suffix() {
        let mut manager = BlobManager::with_threshold_and_mode(1024, true);
        let pointer = manager.append(b"key", vec![7; 1500]);
        manager.set_generation(9);
        let catalog = manager.to_segment_catalog_bytes();
        let mut segment = manager.segment_bytes(pointer.file_id).unwrap();
        segment.extend_from_slice(b"unpublished suffix");
        let segments = HashMap::from([(pointer.file_id, segment)]);

        let restored =
            BlobManager::from_segment_catalog_with_delta_log(&catalog, &segments, &[], None)
                .unwrap();
        assert!(restored.is_segmented());
        assert_eq!(restored.generation_id(), 9);
        assert_eq!(restored.read(&pointer), Some(&vec![7; 1500][..]));
        assert_eq!(restored.persisted_segment_length(pointer.file_id), 1516);
    }

    #[test]
    fn test_segment_catalog_delta_roundtrip_and_torn_suffix() {
        let mut manager = BlobManager::with_threshold_and_mode(1024, true);
        let first = manager.append(b"first", vec![1; 1500]);
        manager.set_generation(1);
        let anchor = manager.to_segment_catalog_bytes();
        manager.capture_persisted_state();

        let second = manager.append(b"second", vec![2; 1500]);
        manager.set_generation(2);
        let delta = manager.to_segment_catalog_delta_bytes().unwrap();
        let segments =
            HashMap::from([(first.file_id, manager.segment_bytes(first.file_id).unwrap())]);
        let restored =
            BlobManager::from_segment_catalog_with_delta_log(&anchor, &segments, &delta, Some(2))
                .unwrap();
        assert_eq!(restored.generation_id(), 2);
        assert_eq!(restored.read(&first), Some(&vec![1; 1500][..]));
        assert_eq!(restored.read(&second), Some(&vec![2; 1500][..]));

        let mut torn = delta;
        torn.pop();
        let old =
            BlobManager::from_segment_catalog_with_delta_log(&anchor, &segments, &torn, Some(1))
                .unwrap();
        assert_eq!(old.generation_id(), 1);
        assert!(
            BlobManager::from_segment_catalog_with_delta_log(&anchor, &segments, &torn, Some(2),)
                .is_none()
        );
    }

    #[test]
    fn test_blob_manager_rejects_future_format_and_duplicate_ids() {
        let mut manager = BlobManager::new();
        manager.append(b"key", vec![1; 1500]);
        let mut future = manager.to_bytes();
        future[8..12].copy_from_slice(&2u32.to_le_bytes());
        assert!(BlobManager::from_bytes(&future).is_none());

        manager.files.push(Arc::new(BlobFile::new(2)));
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
