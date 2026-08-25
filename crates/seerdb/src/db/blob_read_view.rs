//! Generation-bound blob descriptors for [`ReadView`](super::ReadView).
//!
//! This module owns only the read-side resource state needed to resolve blob
//! pointers from a captured generation. `DB` remains responsible for choosing
//! and retaining the generation; `BlobManager` remains responsible for the
//! mutable blob image and catalog.

use super::*;
use std::collections::HashMap;

pub(super) struct BlobReadView {
    files: HashMap<u32, File>,
    bases: HashMap<u32, u64>,
}

impl BlobReadView {
    pub(super) fn open(path: &Path, blobs: &BlobManager) -> Result<Self> {
        if blobs.is_segmented() {
            let mut files = HashMap::new();
            let mut bases = HashMap::new();
            for file_id in blobs.segment_file_ids() {
                let file = OpenOptions::new()
                    .read(true)
                    .open(blob_segment_path(path, file_id))?;
                files.insert(file_id, file);
                bases.insert(file_id, 0);
            }
            return Ok(Self { files, bases });
        }

        let file_ids = blobs.segment_file_ids();
        if file_ids.is_empty() {
            return Ok(Self {
                files: HashMap::new(),
                bases: HashMap::new(),
            });
        }
        let file = OpenOptions::new().read(true).open(path.join(BLOB_FILE))?;
        let file_len = file.metadata()?.len();
        let mut files = HashMap::new();
        let mut bases = HashMap::new();
        let mut cursor;
        let mut header = [0u8; 32];
        if file_len >= header.len() as u64 {
            read_exact_at(&file, 0, &mut header)?;
        }

        if header[..8] == *b"SEERBLB1" {
            if decode_u32(&header[8..12])? != 1 {
                return Err(Error::Corruption("unsupported blob image version".into()));
            }
            let count = decode_u32(&header[28..32])? as usize;
            cursor = 32;
            for _ in 0..count {
                let mut descriptor = [0u8; 12];
                read_exact_at(&file, cursor, &mut descriptor)?;
                let file_id = decode_u32(&descriptor[..4])?;
                let data_len = decode_u64(&descriptor[4..12])?;
                let base = cursor
                    .checked_add(12)
                    .ok_or_else(|| Error::Corruption("blob image offset overflow".into()))?;
                let data_end = base
                    .checked_add(data_len)
                    .ok_or_else(|| Error::Corruption("blob image length overflow".into()))?;
                if file_id == 0 || data_end > file_len || files.contains_key(&file_id) {
                    return Err(Error::Corruption("invalid blob image descriptor".into()));
                }
                files.insert(file_id, file.try_clone()?);
                bases.insert(file_id, base);
                cursor = data_end;
                let mut deleted_count = [0u8; 4];
                read_exact_at(&file, cursor, &mut deleted_count)?;
                let deleted_bytes = u64::from(decode_u32(&deleted_count)?)
                    .checked_mul(8)
                    .ok_or_else(|| Error::Corruption("blob deletion metadata overflow".into()))?;
                cursor = cursor
                    .checked_add(4)
                    .and_then(|offset| offset.checked_add(deleted_bytes))
                    .ok_or_else(|| Error::Corruption("blob image offset overflow".into()))?;
            }
        } else {
            let mut count_bytes = [0u8; 4];
            read_exact_at(&file, 0, &mut count_bytes)?;
            let count = decode_u32(&count_bytes)? as usize;
            cursor = 4;
            for _ in 0..count {
                let mut descriptor = [0u8; 8];
                read_exact_at(&file, cursor, &mut descriptor)?;
                let file_id = decode_u32(&descriptor[..4])?;
                let data_len = u64::from(decode_u32(&descriptor[4..8])?);
                let base = cursor
                    .checked_add(8)
                    .ok_or_else(|| Error::Corruption("blob image offset overflow".into()))?;
                let data_end = base
                    .checked_add(data_len)
                    .ok_or_else(|| Error::Corruption("blob image length overflow".into()))?;
                if file_id == 0 || data_end > file_len || files.contains_key(&file_id) {
                    return Err(Error::Corruption("invalid legacy blob descriptor".into()));
                }
                files.insert(file_id, file.try_clone()?);
                bases.insert(file_id, base);
                cursor = data_end;
            }
        }

        for file_id in file_ids {
            if !files.contains_key(&file_id) {
                return Err(Error::Corruption(format!(
                    "blob image is missing file {file_id}"
                )));
            }
        }
        Ok(Self { files, bases })
    }

    pub(super) fn read(&self, pointer: &BlobPointer) -> Result<Vec<u8>> {
        let file = self.files.get(&pointer.file_id).ok_or_else(|| {
            Error::Corruption(format!(
                "blob pointer names missing file {}",
                pointer.file_id
            ))
        })?;
        let base = *self.bases.get(&pointer.file_id).ok_or_else(|| {
            Error::Corruption(format!(
                "blob pointer has no base for file {}",
                pointer.file_id
            ))
        })?;
        let offset = base
            .checked_add(pointer.offset)
            .ok_or_else(|| Error::Corruption("blob pointer offset overflow".into()))?;
        let mut header = [0u8; 12];
        read_exact_at(file, offset, &mut header)?;
        let length = decode_u32(&header[8..12])?;
        if length != pointer.length {
            return Err(Error::Corruption(
                "blob pointer length does not match record".into(),
            ));
        }
        let value_len = usize::try_from(length)
            .map_err(|_| Error::Corruption("blob value length overflows memory".into()))?;
        let record_len = 12usize
            .checked_add(value_len)
            .ok_or_else(|| Error::Corruption("blob record length overflow".into()))?;
        let mut record = vec![0u8; record_len];
        read_exact_at(file, offset, &mut record)?;
        let mut crc_bytes = [0u8; 4];
        read_exact_at(
            file,
            offset
                .checked_add(record_len as u64)
                .ok_or_else(|| Error::Corruption("blob record offset overflow".into()))?,
            &mut crc_bytes,
        )?;
        let stored_crc = u32::from_le_bytes(crc_bytes);
        if stored_crc != crc32c::crc32c(&record) {
            return Err(Error::Corruption("blob record checksum mismatch".into()));
        }
        Ok(record[12..].to_vec())
    }
}
