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

/// Errors returned by blob-manager mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BlobManagerError {
    /// No valid file ID remains for a new append-only blob file.
    #[error("blob file ID space exhausted")]
    FileIdExhausted,
}

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
    ///
    /// The append is rejected without changing the manager when no file ID
    /// remains for a new active file.
    pub fn append(&mut self, key: &[u8], value: Vec<u8>) -> Result<BlobPointer, BlobManagerError> {
        // Get or create the active blob file.
        if self.files.is_empty() || self.should_rollover(value.len()) {
            self.create_new_file()?;
        }

        let file = Arc::make_mut(
            self.files
                .last_mut()
                .ok_or(BlobManagerError::FileIdExhausted)?,
        );
        let key_prefix = Self::make_key_prefix(key);
        let (offset, length) = file.append(key_prefix, value);

        Ok(BlobPointer {
            file_id: file.file_id(),
            offset,
            length,
        })
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
    fn create_new_file(&mut self) -> Result<(), BlobManagerError> {
        self.begin_compaction_file()
            .ok_or(BlobManagerError::FileIdExhausted)
            .map(|_| ())
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
#[path = "manager_tests.rs"]
mod tests;
