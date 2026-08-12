//! Versioned PMT/allocator checkpoint and metadata-delta byte formats.
//!
//! This module owns only the durable byte representation: headers, version
//! checks, checksums, bounded lengths, ordering, and record encoding. File
//! publication, delta-chain traversal, retention validation, and sizing policy
//! remain in `metadata.rs`.

use crate::allocator::PageAllocator;
use crate::error::{Error, Result};
use crate::mvcc::{PMT, PageMapping};
use crate::storage::format::FORMAT_VERSION;

pub(super) const META_MAGIC: [u8; 8] = *b"SEERMET1";
pub(super) const META_DELTA_MAGIC: [u8; 8] = *b"SEERMDL1";
const META_DELTA_VERSION: u32 = 1;
pub(super) const META_DELTA_HEADER_SIZE: usize = 8 + 4 + 8 + 4 + 4 + 4;
pub(super) const META_DELTA_CHECKSUM_SIZE: usize = 4;
pub(super) const MAX_META_DELTA_CHAIN: usize = 64;

pub(super) struct MetaDelta {
    pub(super) parent_checkpoint_id: u64,
    pub(super) updates: Vec<(u64, PageMapping)>,
    pub(super) removals: Vec<u64>,
    pub(super) allocator: PageAllocator,
}

pub(super) fn decode_delta(data: &[u8]) -> Result<MetaDelta> {
    if data.len() < META_DELTA_HEADER_SIZE + META_DELTA_CHECKSUM_SIZE {
        return Err(Error::Corruption("metadata delta is truncated".into()));
    }
    let version = u32::from_le_bytes(
        data[8..12]
            .try_into()
            .map_err(|_| Error::Corruption("metadata delta version is truncated".into()))?,
    );
    if version != META_DELTA_VERSION {
        return Err(Error::Corruption(format!(
            "unsupported metadata delta version {version}"
        )));
    }

    let checksum_offset = data
        .len()
        .checked_sub(META_DELTA_CHECKSUM_SIZE)
        .ok_or_else(|| Error::Corruption("metadata delta checksum is truncated".into()))?;
    let expected = u32::from_le_bytes(
        data[checksum_offset..]
            .try_into()
            .map_err(|_| Error::Corruption("metadata delta checksum is truncated".into()))?,
    );
    if crc32c::crc32c(&data[..checksum_offset]) != expected {
        return Err(Error::Corruption("metadata delta checksum mismatch".into()));
    }

    let parent_checkpoint_id = u64::from_le_bytes(
        data[12..20]
            .try_into()
            .map_err(|_| Error::Corruption("metadata delta parent is truncated".into()))?,
    );
    let update_count = u32::from_le_bytes(
        data[20..24]
            .try_into()
            .map_err(|_| Error::Corruption("metadata delta update count is truncated".into()))?,
    ) as usize;
    let removal_count = u32::from_le_bytes(
        data[24..28]
            .try_into()
            .map_err(|_| Error::Corruption("metadata delta removal count is truncated".into()))?,
    ) as usize;
    let allocator_len =
        u32::from_le_bytes(data[28..32].try_into().map_err(|_| {
            Error::Corruption("metadata delta allocator length is truncated".into())
        })?) as usize;

    let update_bytes = update_count
        .checked_mul(8 + PageMapping::SERIALIZED_SIZE)
        .ok_or_else(|| Error::Corruption("metadata delta updates overflow".into()))?;
    let removal_bytes = removal_count
        .checked_mul(8)
        .ok_or_else(|| Error::Corruption("metadata delta removals overflow".into()))?;
    let allocator_start = META_DELTA_HEADER_SIZE
        .checked_add(update_bytes)
        .and_then(|offset| offset.checked_add(removal_bytes))
        .ok_or_else(|| Error::Corruption("metadata delta layout overflows".into()))?;
    let checksum_expected_end = allocator_start
        .checked_add(allocator_len)
        .and_then(|offset| offset.checked_add(META_DELTA_CHECKSUM_SIZE))
        .ok_or_else(|| Error::Corruption("metadata delta layout overflows".into()))?;
    if checksum_expected_end != data.len() {
        return Err(Error::Corruption(
            "metadata delta has trailing or truncated bytes".into(),
        ));
    }

    let mut updates = Vec::with_capacity(update_count);
    let mut offset = META_DELTA_HEADER_SIZE;
    let mut previous_page = None;
    for _ in 0..update_count {
        let page_id = u64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .map_err(|_| Error::Corruption("metadata delta page ID is truncated".into()))?,
        );
        if previous_page.is_some_and(|previous| page_id <= previous) {
            return Err(Error::Corruption(
                "metadata delta updates are not strictly sorted".into(),
            ));
        }
        previous_page = Some(page_id);
        offset += 8;
        let mapping_end = offset + PageMapping::SERIALIZED_SIZE;
        let mapping = PageMapping::from_bytes(
            data[offset..mapping_end]
                .try_into()
                .map_err(|_| Error::Corruption("metadata delta mapping is truncated".into()))?,
        );
        if mapping.version == u64::MAX {
            return Err(Error::Corruption(
                "metadata delta mapping version is exhausted".into(),
            ));
        }
        updates.push((page_id, mapping));
        offset = mapping_end;
    }

    let mut removals = Vec::with_capacity(removal_count);
    let mut previous_page = None;
    for _ in 0..removal_count {
        let page_id = u64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .map_err(|_| Error::Corruption("metadata delta removal is truncated".into()))?,
        );
        if previous_page.is_some_and(|previous| page_id <= previous) {
            return Err(Error::Corruption(
                "metadata delta removals are not strictly sorted".into(),
            ));
        }
        previous_page = Some(page_id);
        removals.push(page_id);
        offset += 8;
    }

    if updates
        .iter()
        .any(|(page_id, _)| removals.binary_search(page_id).is_ok())
    {
        return Err(Error::Corruption(
            "metadata delta updates and removals overlap".into(),
        ));
    }

    let allocator =
        PageAllocator::from_bytes(&data[allocator_start..allocator_start + allocator_len])
            .ok_or_else(|| Error::Corruption("metadata delta allocator is invalid".into()))?;
    Ok(MetaDelta {
        parent_checkpoint_id,
        updates,
        removals,
        allocator,
    })
}

pub(super) fn decode_checkpoint(data: &[u8]) -> Result<(PMT, PageAllocator)> {
    const HEADER_SIZE: usize = META_MAGIC.len() + 4;
    const CHECKSUM_SIZE: usize = 4;
    if data.len() < HEADER_SIZE + CHECKSUM_SIZE {
        return Err(Error::Corruption("meta file is truncated".into()));
    }

    let version = u32::from_le_bytes(
        data[META_MAGIC.len()..HEADER_SIZE]
            .try_into()
            .map_err(|_| Error::Corruption("meta version is truncated".into()))?,
    );
    if version != FORMAT_VERSION {
        return Err(Error::Corruption(format!(
            "unsupported meta format version {version}"
        )));
    }

    let checksum_offset = data.len() - CHECKSUM_SIZE;
    let expected = u32::from_le_bytes(
        data[checksum_offset..]
            .try_into()
            .map_err(|_| Error::Corruption("meta checksum is truncated".into()))?,
    );
    let actual = crc32c::crc32c(&data[..checksum_offset]);
    if expected != actual {
        return Err(Error::Corruption("meta checksum mismatch".into()));
    }

    decode_legacy_checkpoint(&data[HEADER_SIZE..checksum_offset])
}

pub(super) fn decode_legacy_checkpoint(data: &[u8]) -> Result<(PMT, PageAllocator)> {
    if data.len() < 4 {
        return Err(Error::Corruption("meta file too small".into()));
    }

    let pmt_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

    let pmt_end = 4usize
        .checked_add(pmt_len)
        .ok_or_else(|| Error::Corruption("meta PMT length overflows".into()))?;
    let alloc_len_start = pmt_end;
    let alloc_len_end = alloc_len_start
        .checked_add(4)
        .ok_or_else(|| Error::Corruption("meta allocator length overflows".into()))?;
    if data.len() < alloc_len_end {
        return Err(Error::Corruption("meta file truncated".into()));
    }

    let pmt = PMT::from_bytes(&data[4..pmt_end])
        .ok_or_else(|| Error::Corruption("invalid PMT data".into()))?;

    let alloc_offset = alloc_len_start;
    let alloc_len = u32::from_le_bytes([
        data[alloc_offset],
        data[alloc_offset + 1],
        data[alloc_offset + 2],
        data[alloc_offset + 3],
    ]) as usize;

    let alloc_end = alloc_len_end
        .checked_add(alloc_len)
        .ok_or_else(|| Error::Corruption("meta allocator length overflows".into()))?;
    if data.len() != alloc_end {
        return Err(Error::Corruption(
            if data.len() < alloc_end {
                "meta allocator data is truncated"
            } else {
                "meta file has trailing bytes"
            }
            .into(),
        ));
    }

    let alloc_data = &data[alloc_len_end..alloc_end];
    let allocator = PageAllocator::from_bytes(alloc_data)
        .ok_or_else(|| Error::Corruption("invalid allocator data".into()))?;

    Ok((pmt, allocator))
}

pub(super) fn encode_checkpoint(pmt: &PMT, allocator: &PageAllocator) -> Result<Vec<u8>> {
    let pmt_bytes = pmt.to_bytes();
    let alloc_bytes = allocator.to_bytes();

    let pmt_len = u32::try_from(pmt_bytes.len())
        .map_err(|_| Error::InvalidArgument("PMT checkpoint is too large".into()))?;
    let alloc_len = u32::try_from(alloc_bytes.len())
        .map_err(|_| Error::InvalidArgument("allocator checkpoint is too large".into()))?;

    let mut buf =
        Vec::with_capacity(META_MAGIC.len() + 4 + 4 + pmt_bytes.len() + 4 + alloc_bytes.len() + 4);
    buf.extend_from_slice(&META_MAGIC);
    buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&pmt_len.to_le_bytes());
    buf.extend_from_slice(&pmt_bytes);
    buf.extend_from_slice(&alloc_len.to_le_bytes());
    buf.extend_from_slice(&alloc_bytes);
    let checksum = crc32c::crc32c(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());
    Ok(buf)
}

pub(super) fn encode_delta(
    parent_checkpoint_id: u64,
    parent_pmt: &PMT,
    pmt: &PMT,
    allocator: &PageAllocator,
) -> Result<Vec<u8>> {
    let mut updates = pmt
        .iter()
        .filter_map(|(page_id, mapping)| {
            (parent_pmt.get(page_id) != Some(mapping)).then_some((page_id, *mapping))
        })
        .collect::<Vec<_>>();
    updates.sort_unstable_by_key(|(page_id, _)| *page_id);
    let mut removals = parent_pmt
        .iter()
        .filter_map(|(page_id, _)| (!pmt.contains(page_id)).then_some(page_id))
        .collect::<Vec<_>>();
    removals.sort_unstable();

    let update_count = u32::try_from(updates.len())
        .map_err(|_| Error::InvalidArgument("metadata delta has too many updates".into()))?;
    let removal_count = u32::try_from(removals.len())
        .map_err(|_| Error::InvalidArgument("metadata delta has too many removals".into()))?;
    let allocator_bytes = allocator.to_bytes();
    let allocator_len = u32::try_from(allocator_bytes.len())
        .map_err(|_| Error::InvalidArgument("metadata delta allocator is too large".into()))?;

    let total_len = META_DELTA_HEADER_SIZE
        .checked_add(
            updates
                .len()
                .checked_mul(8 + PageMapping::SERIALIZED_SIZE)
                .ok_or(Error::DiskFull)?,
        )
        .and_then(|size| size.checked_add(removals.len().checked_mul(8)?))
        .and_then(|size| size.checked_add(allocator_bytes.len()))
        .and_then(|size| size.checked_add(META_DELTA_CHECKSUM_SIZE))
        .ok_or(Error::DiskFull)?;
    let mut buf = Vec::with_capacity(total_len);
    buf.extend_from_slice(&META_DELTA_MAGIC);
    buf.extend_from_slice(&META_DELTA_VERSION.to_le_bytes());
    buf.extend_from_slice(&parent_checkpoint_id.to_le_bytes());
    buf.extend_from_slice(&update_count.to_le_bytes());
    buf.extend_from_slice(&removal_count.to_le_bytes());
    buf.extend_from_slice(&allocator_len.to_le_bytes());
    for (page_id, mapping) in updates {
        buf.extend_from_slice(&page_id.to_le_bytes());
        buf.extend_from_slice(&mapping.to_bytes());
    }
    for page_id in removals {
        buf.extend_from_slice(&page_id.to_le_bytes());
    }
    buf.extend_from_slice(&allocator_bytes);
    let checksum = crc32c::crc32c(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());
    Ok(buf)
}
