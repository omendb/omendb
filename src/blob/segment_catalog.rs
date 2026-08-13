//! Segmented blob catalog frontier and replay state.
//!
//! The versioned SEERBLC1/SEERBCD1 byte protocols live in
//! [`super::segment_catalog_format`]. The parent `BlobManager` remains the
//! authority for live blob files and GC; this module owns catalog projection,
//! durable frontier bookkeeping, and replay into live state.

use super::cursor::Cursor;
use super::segment_catalog_format::{
    self, MAX_SEGMENT_CATALOG_DELTAS, SEGMENT_CATALOG_MAGIC, SEGMENT_CATALOG_VERSION,
    SegmentCatalogDelta, SegmentCatalogDeltaEntry,
};
use super::{BlobFile, BlobManager, BlobRecord, DEFAULT_SEGMENT_TARGET_SIZE};
use crate::btree::node::BlobPointer;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// Durable frontier and replay bookkeeping for the segmented catalog.
///
/// `BlobManager` owns live files and pointer mutation. This value owns the
/// derived catalog frontier that describes which segment bytes and deletion
/// offsets are already durable.
#[derive(Clone, Default)]
pub(super) struct SegmentCatalogState {
    /// Catalog length already durable for each segment.
    pub(super) persisted_lengths: HashMap<u32, u64>,
    /// Deletion offsets already represented by the durable catalog frontier.
    pub(super) persisted_deleted_offsets: HashMap<u32, BTreeSet<u64>>,
    /// Generation represented by the durable catalog frontier.
    pub(super) persisted_generation_id: u64,
    /// Number of delta frames after the full catalog anchor.
    pub(super) catalog_delta_count: u32,
    /// Whether a full or delta catalog has been durably initialized.
    pub(super) catalog_persisted: bool,
}

impl BlobManager {
    fn segment_catalog_delta_entries(&self) -> Option<Vec<SegmentCatalogDeltaEntry>> {
        let mut file_ids = BTreeSet::new();
        file_ids.extend(self.files.iter().map(|file| file.file_id()));
        file_ids.extend(self.catalog.persisted_lengths.keys().copied());

        let mut entries = Vec::new();
        for file_id in file_ids {
            let current = self.files.iter().find(|file| file.file_id() == file_id);
            let Some(file) = current else {
                entries.push(SegmentCatalogDeltaEntry::Remove { file_id });
                continue;
            };

            let data_len = file.serialized_size()?;
            let previous_len = self.catalog.persisted_lengths.get(&file_id).copied();
            let previous_deleted = self
                .catalog
                .persisted_deleted_offsets
                .get(&file_id)
                .cloned()
                .unwrap_or_default();
            let deleted_offsets = file
                .deleted_offsets()
                .filter(|offset| !previous_deleted.contains(offset))
                .collect::<Vec<_>>();
            if previous_len != Some(data_len)
                || !self
                    .catalog
                    .persisted_deleted_offsets
                    .contains_key(&file_id)
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
        segment_catalog_format::encode_segment_catalog_delta(self, &entries)
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
        self.catalog
            .persisted_lengths
            .get(&file_id)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn persisted_segment_catalog_generation(&self) -> u64 {
        self.catalog.persisted_generation_id
    }

    /// Bytes that the next segmented publication will write: the catalog plus
    /// any record suffixes not yet covered by a durable catalog frontier.
    pub(crate) fn segment_write_size(&self) -> Option<u64> {
        let mut size = if !self.catalog.catalog_persisted || self.catalog_needs_consolidation() {
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
                    if !self.catalog.catalog_persisted || self.catalog_needs_consolidation() {
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
        self.catalog.catalog_delta_count = self.catalog.catalog_delta_count.saturating_add(1);
    }

    pub(crate) fn mark_segment_catalog_consolidated(&mut self) {
        self.capture_persisted_state();
        self.catalog.catalog_delta_count = 0;
    }

    pub(super) fn capture_persisted_state(&mut self) {
        for file in &self.files {
            self.catalog
                .persisted_lengths
                .insert(file.file_id(), file.to_bytes().len() as u64);
            self.catalog
                .persisted_deleted_offsets
                .insert(file.file_id(), file.deleted_offsets().collect());
        }
        self.catalog
            .persisted_lengths
            .retain(|file_id, _| self.files.iter().any(|file| file.file_id() == *file_id));
        self.catalog
            .persisted_deleted_offsets
            .retain(|file_id, _| self.files.iter().any(|file| file.file_id() == *file_id));
        self.catalog.persisted_generation_id = self.generation_id;
        self.catalog.catalog_persisted = true;
    }

    pub(crate) fn segment_file_ids(&self) -> Vec<u32> {
        self.files.iter().map(|file| file.file_id()).collect()
    }

    pub(crate) fn catalog_needs_consolidation(&self) -> bool {
        self.catalog.catalog_persisted
            && self.catalog.catalog_delta_count >= MAX_SEGMENT_CATALOG_DELTAS
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
        let deltas = segment_catalog_format::parse_segment_catalog_delta_log_through_generation(
            delta_log,
            target_generation,
        )?;
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
            catalog: SegmentCatalogState {
                persisted_lengths,
                persisted_deleted_offsets,
                persisted_generation_id: generation_id,
                catalog_delta_count: 0,
                catalog_persisted: true,
            },
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
                    let previous_len = self.catalog.persisted_lengths.get(file_id).copied();
                    if previous_len.is_some_and(|previous| *data_len < previous) {
                        return None;
                    }
                    let data_len = usize::try_from(*data_len).ok()?;
                    let segment = segments.get(file_id)?.get(..data_len)?;
                    let mut file = BlobFile::from_bytes(*file_id, segment)?;
                    let mut all_deleted = self
                        .catalog
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
                    self.catalog
                        .persisted_lengths
                        .insert(*file_id, data_len as u64);
                    self.catalog
                        .persisted_deleted_offsets
                        .insert(*file_id, all_deleted);
                    self.next_file_id = self.next_file_id.max(file_id.checked_add(1)?);
                }
                SegmentCatalogDeltaEntry::Remove { file_id } => {
                    if self.files.iter().all(|file| file.file_id() != *file_id) {
                        return None;
                    }
                    self.files.retain(|file| file.file_id() != *file_id);
                    self.catalog.persisted_lengths.remove(file_id);
                    self.catalog.persisted_deleted_offsets.remove(file_id);
                }
            }
        }
        self.generation_id = delta.generation_id;
        self.catalog.persisted_generation_id = delta.generation_id;
        self.catalog.catalog_delta_count = self.catalog.catalog_delta_count.saturating_add(1);
        self.catalog.catalog_persisted = true;
        Some(())
    }
}
