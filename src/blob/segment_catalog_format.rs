//! Durable bytes for the segmented blob catalog.
//!
//! This module owns the versioned SEERBLC1 anchor and SEERBCD1 delta
//! protocols: framing, checksums, bounded parsing, and prefix recovery. The
//! parent `BlobManager` remains the owner of live segment state and catalog
//! frontier transitions.

use super::BlobManager;
use super::cursor::Cursor;

pub(super) const SEGMENT_CATALOG_MAGIC: [u8; 8] = *b"SEERBLC1";
pub(super) const SEGMENT_CATALOG_VERSION: u32 = 1;
pub(super) const SEGMENT_CATALOG_DELTA_MAGIC: [u8; 8] = *b"SEERBCD1";
pub(super) const SEGMENT_CATALOG_DELTA_VERSION: u32 = 1;
pub(super) const MAX_SEGMENT_CATALOG_DELTAS: u32 = 64;

#[derive(Debug)]
pub(super) enum SegmentCatalogDeltaEntry {
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
pub(super) struct SegmentCatalogDelta {
    pub(super) generation_id: u64,
    pub(super) parent_generation_id: u64,
    pub(super) entries: Vec<SegmentCatalogDeltaEntry>,
}

pub(super) fn is_segment_catalog(buf: &[u8]) -> bool {
    buf.starts_with(&SEGMENT_CATALOG_MAGIC)
}

impl BlobManager {
    /// Return the checksummed catalog for the segmented layout. Record bytes
    /// are deliberately absent: the catalog names a prefix of each segment
    /// and stores only deletion metadata needed by the active root.
    pub(crate) fn to_segment_catalog_bytes(&self) -> Vec<u8> {
        encode_segment_catalog(self)
    }

    /// Return the valid prefix of an append-only delta log. A short, torn, or
    /// corrupt suffix is intentionally excluded from the prefix so the next
    /// writer can truncate it before appending a new frame.
    #[cfg(test)]
    pub(crate) fn segment_catalog_delta_prefix_len(buf: &[u8]) -> Option<usize> {
        delta_prefix_len(buf)
    }

    /// Return the valid delta-log prefix that does not advance beyond the
    /// authoritative catalog generation. A failed publication can leave a
    /// complete future frame behind; the next writer must discard that
    /// abandoned branch before appending a retry for the same generation.
    pub(crate) fn segment_catalog_delta_prefix_len_through_generation(
        buf: &[u8],
        generation_id: u64,
    ) -> Option<usize> {
        delta_prefix_len_through_generation(buf, generation_id)
    }
}

fn encode_segment_catalog(manager: &BlobManager) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&SEGMENT_CATALOG_MAGIC);
    buf.extend_from_slice(&SEGMENT_CATALOG_VERSION.to_le_bytes());
    buf.extend_from_slice(&(manager.threshold as u64).to_le_bytes());
    buf.extend_from_slice(&manager.generation_id.to_le_bytes());
    buf.extend_from_slice(&(manager.files.len() as u32).to_le_bytes());
    for file in &manager.files {
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

pub(super) fn encode_segment_catalog_delta(
    manager: &BlobManager,
    entries: &[SegmentCatalogDeltaEntry],
) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&SEGMENT_CATALOG_DELTA_MAGIC);
    buf.extend_from_slice(&SEGMENT_CATALOG_DELTA_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes());
    buf.extend_from_slice(&manager.generation_id.to_le_bytes());
    buf.extend_from_slice(&manager.catalog.persisted_generation_id.to_le_bytes());
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
                buf.extend_from_slice(&u32::try_from(deleted_offsets.len()).ok()?.to_le_bytes());
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

#[cfg(test)]
fn delta_prefix_len(buf: &[u8]) -> Option<usize> {
    let mut position = 0usize;
    while let Some((end, _)) = delta_frame_header(buf, position) {
        let frame = buf.get(position..end)?;
        if parse_segment_catalog_delta_frame(frame).is_none() {
            break;
        }
        position = end;
    }
    Some(position)
}

fn delta_prefix_len_through_generation(buf: &[u8], generation_id: u64) -> Option<usize> {
    let mut position = 0usize;
    while let Some((end, frame_generation)) = delta_frame_header(buf, position) {
        if frame_generation > generation_id {
            break;
        }
        let frame = buf.get(position..end)?;
        if parse_segment_catalog_delta_frame(frame).is_none() {
            break;
        }
        position = end;
    }
    Some(position)
}

/// Read only the fixed header needed to identify the frame boundary and
/// generation. Callers can stop at a future generation before parsing its
/// entry count, checksum, or variable-sized vectors.
fn delta_frame_header(buf: &[u8], position: usize) -> Option<(usize, u64)> {
    let frame = buf.get(position..)?;
    if frame.len() < 44
        || frame.get(..8)? != SEGMENT_CATALOG_DELTA_MAGIC
        || u32::from_le_bytes(frame.get(8..12)?.try_into().ok()?) != SEGMENT_CATALOG_DELTA_VERSION
    {
        return None;
    }
    let frame_length =
        usize::try_from(u64::from_le_bytes(frame.get(12..20)?.try_into().ok()?)).ok()?;
    if !(44..=frame.len()).contains(&frame_length) {
        return None;
    }
    let end = position.checked_add(frame_length)?;
    let generation_id = u64::from_le_bytes(frame.get(20..28)?.try_into().ok()?);
    Some((end, generation_id))
}

fn parse_segment_catalog_delta_frame(buf: &[u8]) -> Option<SegmentCatalogDelta> {
    if buf.len() < 44
        || buf.get(..8)? != SEGMENT_CATALOG_DELTA_MAGIC
        || u32::from_le_bytes(buf.get(8..12)?.try_into().ok()?) != SEGMENT_CATALOG_DELTA_VERSION
        || usize::try_from(u64::from_le_bytes(buf.get(12..20)?.try_into().ok()?)).ok()? != buf.len()
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

pub(super) fn parse_segment_catalog_delta_log_through_generation(
    buf: &[u8],
    target_generation: u64,
) -> Option<Vec<SegmentCatalogDelta>> {
    let mut position = 0usize;
    let mut deltas = Vec::new();
    while let Some((end, frame_generation)) = delta_frame_header(buf, position) {
        // The manifest-selected generation is authoritative. Any later frame
        // belongs to an abandoned publication branch and must not be parsed
        // or allowed to grow replay memory.
        if frame_generation > target_generation {
            break;
        }
        if deltas.len() >= MAX_SEGMENT_CATALOG_DELTAS as usize {
            return None;
        }
        let frame = buf.get(position..end)?;
        let Some(delta) = parse_segment_catalog_delta_frame(frame) else {
            break;
        };
        deltas.push(delta);
        position = end;
    }
    Some(deltas)
}
