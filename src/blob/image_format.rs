//! Compatibility whole-image encoding and validation for blob state.

use super::{BlobManager, Cursor, DEFAULT_BLOB_THRESHOLD, DEFAULT_SEGMENT_TARGET_SIZE};
use crate::blob::file::{BlobFile, BlobRecord};
use crate::btree::node::BlobPointer;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const BLOB_FORMAT_MAGIC: [u8; 8] = *b"SEERBLB1";
const BLOB_FORMAT_VERSION: u32 = 1;

impl BlobManager {
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

    /// Serialize all blob files to the compatibility whole-image format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.extend_from_slice(&BLOB_FORMAT_MAGIC);
        buf.extend_from_slice(&BLOB_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.threshold as u64).to_le_bytes());
        buf.extend_from_slice(&self.generation_id.to_le_bytes());

        buf.extend_from_slice(&(self.files.len() as u32).to_le_bytes());

        for file in &self.files {
            buf.extend_from_slice(&file.file_id().to_le_bytes());
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

    /// Deserialize a whole-image or legacy blob image.
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
