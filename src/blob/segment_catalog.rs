//! Segmented blob catalog encoding, replay, and persisted-frontier state.
//!
//! This module owns the SEERBLC1 anchor and SEERBCD1 delta protocols. The
//! parent `BlobManager` remains the authority for live blob files and GC;
//! this module owns the segmented catalog representation and durable
//! frontier bookkeeping.

use super::cursor::Cursor;
use super::{BlobFile, BlobManager, BlobRecord, DEFAULT_SEGMENT_TARGET_SIZE};
use crate::btree::node::BlobPointer;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

const SEGMENT_CATALOG_MAGIC: [u8; 8] = *b"SEERBLC1";
const SEGMENT_CATALOG_VERSION: u32 = 1;
const SEGMENT_CATALOG_DELTA_MAGIC: [u8; 8] = *b"SEERBCD1";
const SEGMENT_CATALOG_DELTA_VERSION: u32 = 1;
const MAX_SEGMENT_CATALOG_DELTAS: u32 = 64;

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

pub(super) fn is_segment_catalog(buf: &[u8]) -> bool {
    buf.starts_with(&SEGMENT_CATALOG_MAGIC)
}

impl BlobManager {
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

    pub(super) fn capture_persisted_state(&mut self) {
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
}
