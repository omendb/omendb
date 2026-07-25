//! Blob file manager.
//!
//! Manages multiple blob files for KV separation. Handles appending,
//! reading, and garbage collection of blob files.

use crate::blob::file::{BlobFile, BlobRecord};
use crate::btree::node::BlobPointer;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// Default threshold for blob separation (1KB).
pub const DEFAULT_BLOB_THRESHOLD: usize = 1024;

const BLOB_FORMAT_MAGIC: [u8; 8] = *b"SEERBLB1";
const BLOB_FORMAT_VERSION: u32 = 1;
pub(crate) const SEGMENT_CATALOG_MAGIC: [u8; 8] = *b"SEERBLC1";
const SEGMENT_CATALOG_VERSION: u32 = 1;
pub(crate) const SEGMENT_CATALOG_DELTA_MAGIC: [u8; 8] = *b"SEERBCD1";
const SEGMENT_CATALOG_DELTA_VERSION: u32 = 1;
pub(crate) const MAX_SEGMENT_CATALOG_DELTAS: u32 = 64;
const DEFAULT_SEGMENT_TARGET_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
enum SegmentCatalogDeltaEntry {
    Upsert {
        file_id: u32,
        data_len: u64,
        deleted_offsets: Vec<u64>,
    },
    Remove {
        file_id: u32,
    },
}

#[derive(Debug)]
struct SegmentCatalogDelta {
    generation_id: u64,
    parent_generation_id: u64,
    entries: Vec<SegmentCatalogDeltaEntry>,
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
        buf.starts_with(&SEGMENT_CATALOG_MAGIC)
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

    /// Return the serialized image size without allocating a second image.
    pub(crate) fn serialized_size(&self) -> Option<u64> {
        let header = BLOB_FORMAT_MAGIC
            .len()
            .checked_add(4)?
            .checked_add(8)?
            .checked_add(8)?
            .checked_add(4)?;
        let footer = 8usize.checked_add(4)?;
        let mut size = u64::try_from(header.checked_add(footer)?).ok()?;

        for file in &self.files {
            size = size.checked_add(4 + 8 + 4)?;
            size = size.checked_add(file.serialized_size()?)?;
            size = size.checked_add(
                u64::try_from(file.deleted_count())
                    .ok()?
                    .checked_mul(std::mem::size_of::<u64>() as u64)?,
            )?;
        }

        Some(size)
    }

    /// Predict the next serialized image size for one mutation.
    pub(crate) fn projected_serialized_size(
        &self,
        retired: Option<&BlobPointer>,
        appended_value_len: Option<usize>,
    ) -> Option<u64> {
        let mut size = self.serialized_size()?;
        if let Some(pointer) = retired
            && self.can_mark_deleted(pointer)
        {
            size = size.checked_add(std::mem::size_of::<u64>() as u64)?;
        }
        if let Some(value_len) = appended_value_len {
            u32::try_from(value_len).ok()?;
            let record_size = BlobRecord::OVERHEAD_SIZE.checked_add(value_len)?;
            if self.files.is_empty() {
                size = size.checked_add(4 + 8 + 4)?;
            }
            size = size.checked_add(u64::try_from(record_size).ok()?)?;
        }
        Some(size)
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

    /// Return the checksummed catalog for the segmented layout. Record bytes
    /// are deliberately absent: the catalog names a prefix of each segment
    /// and stores only deletion metadata needed by the active root.
    pub(crate) fn to_segment_catalog_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&SEGMENT_CATALOG_MAGIC);
        buf.extend_from_slice(&SEGMENT_CATALOG_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.threshold as u64).to_le_bytes());
        buf.extend_from_slice(&self.generation_id.to_le_bytes());
        buf.extend_from_slice(&(self.files.len() as u32).to_le_bytes());
        for file in &self.files {
            buf.extend_from_slice(&file.file_id().to_le_bytes());
            buf.extend_from_slice(&(file.to_bytes().len() as u64).to_le_bytes());
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

    fn segment_catalog_delta_entries(&self) -> Option<Vec<SegmentCatalogDeltaEntry>> {
        let mut file_ids = BTreeSet::new();
        file_ids.extend(self.files.iter().map(|file| file.file_id()));
        file_ids.extend(self.persisted_lengths.keys().copied());

        let mut entries = Vec::new();
        for file_id in file_ids {
            let current = self.files.iter().find(|file| file.file_id() == file_id);
            let Some(file) = current else {
                entries.push(SegmentCatalogDeltaEntry::Remove { file_id });
                continue;
            };

            let data_len = file.serialized_size()?;
            let previous_len = self.persisted_lengths.get(&file_id).copied();
            let previous_deleted = self
                .persisted_deleted_offsets
                .get(&file_id)
                .cloned()
                .unwrap_or_default();
            let deleted_offsets = file
                .deleted_offsets()
                .filter(|offset| !previous_deleted.contains(offset))
                .collect::<Vec<_>>();
            if previous_len != Some(data_len)
                || !self.persisted_deleted_offsets.contains_key(&file_id)
                || !deleted_offsets.is_empty()
            {
                entries.push(SegmentCatalogDeltaEntry::Upsert {
                    file_id,
                    data_len,
                    deleted_offsets,
                });
            }
        }
        Some(entries)
    }

    /// Serialize one append-only catalog delta frame. The frame has its own
    /// length and checksum so an interrupted suffix can be ignored while an
    /// authoritative manifest still points at the previous generation.
    pub(crate) fn to_segment_catalog_delta_bytes(&self) -> Option<Vec<u8>> {
        let entries = self.segment_catalog_delta_entries()?;
        let mut buf = Vec::new();
        buf.extend_from_slice(&SEGMENT_CATALOG_DELTA_MAGIC);
        buf.extend_from_slice(&SEGMENT_CATALOG_DELTA_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&self.generation_id.to_le_bytes());
        buf.extend_from_slice(&self.persisted_generation_id.to_le_bytes());
        buf.extend_from_slice(&u32::try_from(entries.len()).ok()?.to_le_bytes());
        for entry in entries {
            match entry {
                SegmentCatalogDeltaEntry::Upsert {
                    file_id,
                    data_len,
                    deleted_offsets,
                } => {
                    buf.extend_from_slice(&file_id.to_le_bytes());
                    buf.extend_from_slice(&0u32.to_le_bytes());
                    buf.extend_from_slice(&data_len.to_le_bytes());
                    buf.extend_from_slice(
                        &u32::try_from(deleted_offsets.len()).ok()?.to_le_bytes(),
                    );
                    for offset in deleted_offsets {
                        buf.extend_from_slice(&offset.to_le_bytes());
                    }
                }
                SegmentCatalogDeltaEntry::Remove { file_id } => {
                    buf.extend_from_slice(&file_id.to_le_bytes());
                    buf.extend_from_slice(&1u32.to_le_bytes());
                }
            }
        }
        let frame_length = u64::try_from(buf.len().checked_add(4)?).ok()?;
        buf[12..20].copy_from_slice(&frame_length.to_le_bytes());
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        Some(buf)
    }

    /// Return the valid prefix of an append-only delta log. A short, torn, or
    /// corrupt suffix is intentionally excluded from the prefix so the next
    /// writer can truncate it before appending a new frame.
    pub(crate) fn segment_catalog_delta_prefix_len(buf: &[u8]) -> Option<usize> {
        let mut position = 0usize;
        while position < buf.len() {
            let remaining = buf.len().checked_sub(position)?;
            if remaining < 44 {
                break;
            }
            let frame = buf.get(position..)?;
            if frame.get(..8)? != SEGMENT_CATALOG_DELTA_MAGIC
                || u32::from_le_bytes(frame.get(8..12)?.try_into().ok()?)
                    != SEGMENT_CATALOG_DELTA_VERSION
            {
                break;
            }
            let frame_length =
                usize::try_from(u64::from_le_bytes(frame.get(12..20)?.try_into().ok()?)).ok()?;
            if !(44..=remaining).contains(&frame_length) {
                break;
            }
            let frame = frame.get(..frame_length)?;
            let stored_checksum =
                u32::from_le_bytes(frame.get(frame_length - 4..)?.try_into().ok()?);
            if stored_checksum != crc32c::crc32c(frame.get(..frame_length - 4)?) {
                break;
            }
            if Self::parse_segment_catalog_delta_frame(frame).is_none() {
                break;
            }
            position = position.checked_add(frame_length)?;
        }
        Some(position)
    }

    /// Return the valid delta-log prefix that does not advance beyond the
    /// authoritative catalog generation. A failed publication can leave a
    /// complete future frame behind; the next writer must discard that
    /// abandoned branch before appending a retry for the same generation.
    pub(crate) fn segment_catalog_delta_prefix_len_through_generation(
        buf: &[u8],
        generation_id: u64,
    ) -> Option<usize> {
        let prefix = Self::segment_catalog_delta_prefix_len(buf)?;
        let mut position = 0usize;
        while position < prefix {
            let frame_length = usize::try_from(u64::from_le_bytes(
                buf.get(position + 12..position + 20)?.try_into().ok()?,
            ))
            .ok()?;
            let end = position.checked_add(frame_length)?;
            let frame = Self::parse_segment_catalog_delta_frame(buf.get(position..end)?)?;
            if frame.generation_id > generation_id {
                break;
            }
            position = end;
        }
        Some(position)
    }

    fn parse_segment_catalog_delta_frame(buf: &[u8]) -> Option<SegmentCatalogDelta> {
        if buf.len() < 44
            || buf.get(..8)? != SEGMENT_CATALOG_DELTA_MAGIC
            || u32::from_le_bytes(buf.get(8..12)?.try_into().ok()?) != SEGMENT_CATALOG_DELTA_VERSION
            || usize::try_from(u64::from_le_bytes(buf.get(12..20)?.try_into().ok()?)).ok()?
                != buf.len()
        {
            return None;
        }
        let stored_checksum = u32::from_le_bytes(buf.get(buf.len() - 4..)?.try_into().ok()?);
        if stored_checksum != crc32c::crc32c(buf.get(..buf.len() - 4)?) {
            return None;
        }

        let generation_id = u64::from_le_bytes(buf.get(20..28)?.try_into().ok()?);
        let parent_generation_id = u64::from_le_bytes(buf.get(28..36)?.try_into().ok()?);
        let entry_count =
            usize::try_from(u32::from_le_bytes(buf.get(36..40)?.try_into().ok()?)).ok()?;
        let mut cursor = Cursor::new(buf.get(40..buf.len() - 4)?);
        let mut entries = Vec::with_capacity(entry_count);
        let mut previous_file_id = 0u32;
        for _ in 0..entry_count {
            let file_id = cursor.u32()?;
            if file_id == 0 || file_id <= previous_file_id {
                return None;
            }
            previous_file_id = file_id;
            match cursor.u32()? {
                0 => {
                    let data_len = cursor.u64()?;
                    let deleted_count = usize::try_from(cursor.u32()?).ok()?;
                    if deleted_count > cursor.remaining() / std::mem::size_of::<u64>() {
                        return None;
                    }
                    let mut deleted_offsets = Vec::with_capacity(deleted_count);
                    let mut previous_offset = None;
                    for _ in 0..deleted_count {
                        let offset = cursor.u64()?;
                        if previous_offset.is_some_and(|previous| offset <= previous) {
                            return None;
                        }
                        previous_offset = Some(offset);
                        deleted_offsets.push(offset);
                    }
                    entries.push(SegmentCatalogDeltaEntry::Upsert {
                        file_id,
                        data_len,
                        deleted_offsets,
                    });
                }
                1 => entries.push(SegmentCatalogDeltaEntry::Remove { file_id }),
                _ => return None,
            }
        }
        cursor.finish()?;
        Some(SegmentCatalogDelta {
            generation_id,
            parent_generation_id,
            entries,
        })
    }

    fn parse_segment_catalog_delta_log(buf: &[u8]) -> Option<Vec<SegmentCatalogDelta>> {
        let prefix = Self::segment_catalog_delta_prefix_len(buf)?;
        let mut position = 0usize;
        let mut deltas = Vec::new();
        while position < prefix {
            let frame_length = usize::try_from(u64::from_le_bytes(
                buf.get(position + 12..position + 20)?.try_into().ok()?,
            ))
            .ok()?;
            let end = position.checked_add(frame_length)?;
            deltas.push(Self::parse_segment_catalog_delta_frame(
                buf.get(position..end)?,
            )?);
            position = end;
        }
        Some(deltas)
    }

    /// Return the current serialized bytes for one segment. The caller can
    /// compare its length with [`Self::persisted_segment_length`] and append
    /// only the new suffix.
    pub(crate) fn segment_bytes(&self, file_id: u32) -> Option<Vec<u8>> {
        self.files
            .iter()
            .find(|file| file.file_id() == file_id)
            .map(|file| file.to_bytes())
    }

    pub(crate) fn persisted_segment_length(&self, file_id: u32) -> u64 {
        self.persisted_lengths.get(&file_id).copied().unwrap_or(0)
    }

    pub(crate) fn persisted_segment_catalog_generation(&self) -> u64 {
        self.persisted_generation_id
    }

    /// Bytes that the next segmented publication will write: the catalog plus
    /// any record suffixes not yet covered by a durable catalog frontier.
    pub(crate) fn segment_write_size(&self) -> Option<u64> {
        let mut size = if !self.catalog_persisted || self.catalog_needs_consolidation() {
            u64::try_from(self.to_segment_catalog_bytes().len()).ok()?
        } else {
            u64::try_from(self.to_segment_catalog_delta_bytes()?.len()).ok()?
        };
        for file in &self.files {
            let current = file.serialized_size()?;
            let persisted = self.persisted_segment_length(file.file_id());
            if current < persisted {
                return None;
            }
            size = size.checked_add(current - persisted)?;
        }
        Some(size)
    }

    /// Conservative bytes admitted before one append/deletion mutation.
    pub(crate) fn projected_segment_write_size(
        &self,
        retired: Option<&BlobPointer>,
        appended_value_len: Option<usize>,
    ) -> Option<u64> {
        let mut size = self.segment_write_size()?;
        if let Some(pointer) = retired
            && self.can_mark_deleted(pointer)
        {
            size = size.checked_add(std::mem::size_of::<u64>() as u64)?;
        }
        if let Some(value_len) = appended_value_len {
            let record_size = BlobRecord::OVERHEAD_SIZE.checked_add(value_len)?;
            if self.files.is_empty() || self.should_rollover(value_len) {
                let entry_overhead =
                    if !self.catalog_persisted || self.catalog_needs_consolidation() {
                        4 + 8 + 4
                    } else {
                        4 + 4 + 8 + 4
                    };
                size = size.checked_add(entry_overhead)?;
            }
            size = size.checked_add(u64::try_from(record_size).ok()?)?;
        }
        Some(size)
    }

    pub(crate) fn mark_segment_delta_persisted(&mut self) {
        self.capture_persisted_state();
        self.catalog_delta_count = self.catalog_delta_count.saturating_add(1);
    }

    pub(crate) fn mark_segment_catalog_consolidated(&mut self) {
        self.capture_persisted_state();
        self.catalog_delta_count = 0;
    }

    fn capture_persisted_state(&mut self) {
        for file in &self.files {
            self.persisted_lengths
                .insert(file.file_id(), file.to_bytes().len() as u64);
            self.persisted_deleted_offsets
                .insert(file.file_id(), file.deleted_offsets().collect());
        }
        self.persisted_lengths
            .retain(|file_id, _| self.files.iter().any(|file| file.file_id() == *file_id));
        self.persisted_deleted_offsets
            .retain(|file_id, _| self.files.iter().any(|file| file.file_id() == *file_id));
        self.persisted_generation_id = self.generation_id;
        self.catalog_persisted = true;
    }

    pub(crate) fn segment_file_ids(&self) -> Vec<u32> {
        self.files.iter().map(|file| file.file_id()).collect()
    }

    pub(crate) fn catalog_needs_consolidation(&self) -> bool {
        self.catalog_persisted && self.catalog_delta_count >= MAX_SEGMENT_CATALOG_DELTAS
    }

    /// Load a full catalog anchor and apply the manifest-selected delta path.
    /// Frames from abandoned future generations are ignored; a selected
    /// generation is valid only when its parent chain reaches the anchor.
    pub(crate) fn from_segment_catalog_with_delta_log(
        buf: &[u8],
        segments: &HashMap<u32, Vec<u8>>,
        delta_log: &[u8],
        target_generation: Option<u64>,
    ) -> Option<Self> {
        let mut manager = Self::from_segment_catalog_base(buf, segments)?;
        let target_generation = target_generation.unwrap_or(manager.generation_id);
        if target_generation < manager.generation_id {
            return None;
        }
        let deltas = Self::parse_segment_catalog_delta_log(delta_log)?;
        let mut path = Vec::new();
        let mut current_generation = target_generation;
        let mut visited = HashSet::new();
        while current_generation != manager.generation_id {
            if !visited.insert(current_generation)
                || path.len() >= MAX_SEGMENT_CATALOG_DELTAS as usize
            {
                return None;
            }
            let mut matches = deltas
                .iter()
                .filter(|delta| delta.generation_id == current_generation);
            let delta = matches.next()?;
            if matches.next().is_some()
                || delta.parent_generation_id >= delta.generation_id
                || delta.parent_generation_id < manager.generation_id
            {
                return None;
            }
            current_generation = delta.parent_generation_id;
            path.push(delta);
        }
        for delta in path.into_iter().rev() {
            manager.apply_segment_catalog_delta(delta, segments)?;
        }
        Some(manager)
    }

    fn from_segment_catalog_base(buf: &[u8], segments: &HashMap<u32, Vec<u8>>) -> Option<Self> {
        if buf.len() < 12 {
            return None;
        }
        let total_length = u64::from_le_bytes(buf[buf.len() - 12..buf.len() - 4].try_into().ok()?);
        if total_length != u64::try_from(buf.len()).ok()? {
            return None;
        }
        let stored_checksum = u32::from_le_bytes(buf[buf.len() - 4..].try_into().ok()?);
        if stored_checksum != crc32c::crc32c(&buf[..buf.len() - 4]) {
            return None;
        }

        let payload = &buf[..buf.len() - 12];
        let mut cursor = Cursor::new(payload);
        if cursor.take(SEGMENT_CATALOG_MAGIC.len())? != SEGMENT_CATALOG_MAGIC
            || cursor.u32()? != SEGMENT_CATALOG_VERSION
        {
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
        let mut persisted_lengths = HashMap::with_capacity(num_files);
        let mut persisted_deleted_offsets = HashMap::with_capacity(num_files);
        for _ in 0..num_files {
            let file_id = cursor.u32()?;
            if file_id == 0 || file_id == u32::MAX || !file_ids.insert(file_id) {
                return None;
            }
            let data_len = usize::try_from(cursor.u64()?).ok()?;
            let segment = segments.get(&file_id)?;
            let data = segment.get(..data_len)?;
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
            persisted_lengths.insert(file_id, data_len as u64);
            persisted_deleted_offsets.insert(file_id, deleted_offsets.into_iter().collect());
            files.push(Arc::new(file));
        }
        cursor.finish()?;
        Some(Self {
            files,
            next_file_id,
            threshold,
            generation_id,
            segmented: true,
            persisted_lengths,
            persisted_deleted_offsets,
            persisted_generation_id: generation_id,
            catalog_delta_count: 0,
            catalog_persisted: true,
            segment_target_size: DEFAULT_SEGMENT_TARGET_SIZE,
        })
    }

    fn apply_segment_catalog_delta(
        &mut self,
        delta: &SegmentCatalogDelta,
        segments: &HashMap<u32, Vec<u8>>,
    ) -> Option<()> {
        if self.generation_id != delta.parent_generation_id {
            return None;
        }
        for entry in &delta.entries {
            match entry {
                SegmentCatalogDeltaEntry::Upsert {
                    file_id,
                    data_len,
                    deleted_offsets,
                } => {
                    let previous_len = self.persisted_lengths.get(file_id).copied();
                    if previous_len.is_some_and(|previous| *data_len < previous) {
                        return None;
                    }
                    let data_len = usize::try_from(*data_len).ok()?;
                    let segment = segments.get(file_id)?.get(..data_len)?;
                    let mut file = BlobFile::from_bytes(*file_id, segment)?;
                    let mut all_deleted = self
                        .persisted_deleted_offsets
                        .get(file_id)
                        .cloned()
                        .unwrap_or_default();
                    for offset in deleted_offsets {
                        if !all_deleted.insert(*offset) {
                            return None;
                        }
                    }
                    file.restore_deleted(&all_deleted.iter().copied().collect::<Vec<_>>())?;
                    if let Some(index) = self
                        .files
                        .iter()
                        .position(|file| file.file_id() == *file_id)
                    {
                        self.files[index] = Arc::new(file);
                    } else {
                        self.files.push(Arc::new(file));
                    }
                    self.persisted_lengths.insert(*file_id, data_len as u64);
                    self.persisted_deleted_offsets.insert(*file_id, all_deleted);
                    self.next_file_id = self.next_file_id.max(file_id.checked_add(1)?);
                }
                SegmentCatalogDeltaEntry::Remove { file_id } => {
                    if self.files.iter().all(|file| file.file_id() != *file_id) {
                        return None;
                    }
                    self.files.retain(|file| file.file_id() != *file_id);
                    self.persisted_lengths.remove(file_id);
                    self.persisted_deleted_offsets.remove(file_id);
                }
            }
        }
        self.generation_id = delta.generation_id;
        self.persisted_generation_id = delta.generation_id;
        self.catalog_delta_count = self.catalog_delta_count.saturating_add(1);
        self.catalog_persisted = true;
        Some(())
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
        let total_length = u64::from_le_bytes(buf[buf.len() - 12..buf.len() - 4].try_into().ok()?);
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
            files.push(Arc::new(file));
        }

        cursor.finish()?;

        Some(Self {
            files,
            next_file_id,
            threshold,
            generation_id,
            segmented: false,
            persisted_lengths: HashMap::new(),
            persisted_deleted_offsets: HashMap::new(),
            persisted_generation_id: 0,
            catalog_delta_count: 0,
            catalog_persisted: false,
            segment_target_size: DEFAULT_SEGMENT_TARGET_SIZE,
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
            files.push(Arc::new(file));
        }

        cursor.finish()?;
        Some(Self {
            files,
            next_file_id,
            threshold: DEFAULT_BLOB_THRESHOLD,
            generation_id: 0,
            segmented: false,
            persisted_lengths: HashMap::new(),
            persisted_deleted_offsets: HashMap::new(),
            persisted_generation_id: 0,
            catalog_delta_count: 0,
            catalog_persisted: false,
            segment_target_size: DEFAULT_SEGMENT_TARGET_SIZE,
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
