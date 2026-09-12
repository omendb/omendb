//! Borrowed compatibility reader for the current SeerDB v3 B-tree page.
//!
//! This decoder exists only to qualify the new buffer-owned B-tree path against
//! the current proven page codec. It does not preserve generation publication:
//! the legacy `write_generation` header field is covered by the checksum but is
//! otherwise ignored by vNext.

use crate::btree::BlobPointer;
use std::cmp::Ordering;

pub(super) const PAGE_SIZE: usize = crate::btree::PAGE_SIZE;
const HEADER_SIZE: usize = 48;
const SLOT_SIZE: usize = 4;
const BLOB_POINTER_SIZE: usize = 16;
const MAGIC: u32 = 0x5345_4552;
const PAGE_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PageType {
    Internal,
    Leaf,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) struct PageDecodeError(pub(super) &'static str);

#[derive(Debug, Clone, Copy)]
struct Header {
    page_type: PageType,
    count: usize,
    free_space: usize,
    checksum: u64,
    leftmost_child: u64,
}

pub(super) enum PageValue<'a> {
    Inline(&'a [u8]),
    Blob(BlobPointer),
    Tombstone,
}

pub(super) struct NodePage<'a> {
    data: &'a [u8],
    header: Header,
}

impl<'a> NodePage<'a> {
    pub(super) fn parse(data: &'a [u8]) -> Result<Self, PageDecodeError> {
        if data.len() != PAGE_SIZE {
            return Err(PageDecodeError("page size does not match v3 B-tree codec"));
        }

        if read_u32(data, 0) != Some(MAGIC) {
            return Err(PageDecodeError("invalid page magic"));
        }
        if read_u32(data, 4) != Some(PAGE_VERSION) {
            return Err(PageDecodeError("unsupported page version"));
        }
        let page_type = match read_u32(data, 8) {
            Some(1) => PageType::Internal,
            Some(2) => PageType::Leaf,
            _ => return Err(PageDecodeError("invalid page type")),
        };
        let count = read_u32(data, 12).ok_or(PageDecodeError("truncated page count"))? as usize;
        let free_space =
            read_u32(data, 16).ok_or(PageDecodeError("truncated free-space field"))? as usize;
        let checksum = read_u64(data, 20).ok_or(PageDecodeError("truncated checksum"))?;
        let leftmost_child =
            read_u64(data, 32).ok_or(PageDecodeError("truncated leftmost child"))?;

        let page = Self {
            data,
            header: Header {
                page_type,
                count,
                free_space,
                checksum,
                leftmost_child,
            },
        };
        page.validate_checksum()?;
        page.validate_layout()?;
        Ok(page)
    }

    pub(super) fn is_leaf(&self) -> bool {
        self.header.page_type == PageType::Leaf
    }

    pub(super) fn search(&self, key: &[u8]) -> Option<Result<usize, usize>> {
        let mut lo = 0usize;
        let mut hi = self.header.count;
        let mut result = None;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.compare_key(mid, key)? {
                Ordering::Less => lo = mid + 1,
                Ordering::Equal => {
                    result = Some(mid);
                    hi = mid;
                }
                Ordering::Greater => hi = mid,
            }
        }

        Some(match result {
            Some(index) => Ok(index),
            None => Err(lo),
        })
    }

    pub(super) fn child_for_key(&self, key: &[u8]) -> Option<u64> {
        if self.header.page_type != PageType::Internal {
            return None;
        }

        let mut lo = 0usize;
        let mut hi = self.header.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.compare_key(mid, key)? {
                Ordering::Less | Ordering::Equal => lo = mid + 1,
                Ordering::Greater => hi = mid,
            }
        }

        if lo == 0 {
            Some(self.header.leftmost_child)
        } else {
            self.child_id(lo - 1)
        }
    }

    pub(super) fn value(&self, index: usize) -> Option<PageValue<'a>> {
        if self.header.page_type != PageType::Leaf || index >= self.header.count {
            return None;
        }
        let (payload_start, entry_end) = self.payload_bounds(index)?;
        let value_type = *self.data.get(payload_start)?;
        let value_start = payload_start.checked_add(1)?;

        match value_type {
            0x00 => Some(PageValue::Inline(self.data.get(value_start..entry_end)?)),
            0x01 if value_start.checked_add(BLOB_POINTER_SIZE) == Some(entry_end) => {
                let bytes = self.data.get(value_start..entry_end)?;
                Some(PageValue::Blob(BlobPointer {
                    file_id: read_u32(bytes, 0)?,
                    offset: read_u64(bytes, 4)?,
                    length: read_u32(bytes, 12)?,
                }))
            }
            0x02 if value_start == entry_end => Some(PageValue::Tombstone),
            _ => None,
        }
    }

    fn validate_checksum(&self) -> Result<(), PageDecodeError> {
        let mut checksum = crc32c::crc32c(&self.data[..20]);
        checksum =
            crc32c::crc32c_combine(checksum, crc32c::crc32c(&self.data[28..]), PAGE_SIZE - 28);
        if self.header.checksum != checksum as u64 {
            return Err(PageDecodeError("page checksum mismatch"));
        }
        Ok(())
    }

    fn validate_layout(&self) -> Result<(), PageDecodeError> {
        let max_slots = (PAGE_SIZE - HEADER_SIZE) / SLOT_SIZE;
        if self.header.count > max_slots {
            return Err(PageDecodeError("slot count exceeds page capacity"));
        }
        let slot_end = HEADER_SIZE
            .checked_add(
                self.header
                    .count
                    .checked_mul(SLOT_SIZE)
                    .ok_or(PageDecodeError("slot array overflow"))?,
            )
            .ok_or(PageDecodeError("slot array overflow"))?;
        if slot_end > PAGE_SIZE {
            return Err(PageDecodeError("slot array exceeds page"));
        }

        let mut offsets = Vec::with_capacity(self.header.count);
        for index in 0..self.header.count {
            let offset = self
                .slot_offset(index)
                .ok_or(PageDecodeError("truncated slot"))?;
            if offset < slot_end || offset >= PAGE_SIZE {
                return Err(PageDecodeError("entry offset is outside entry area"));
            }
            offsets.push(offset);
        }

        let mut sorted_offsets = offsets.clone();
        sorted_offsets.sort_unstable();
        if sorted_offsets
            .windows(2)
            .any(|window| window[0] == window[1])
        {
            return Err(PageDecodeError("duplicate entry offsets"));
        }

        let min_entry = offsets.iter().copied().min().unwrap_or(PAGE_SIZE);
        if self.header.free_space != min_entry.saturating_sub(slot_end) {
            return Err(PageDecodeError("free-space field disagrees with layout"));
        }

        let mut previous_key = Vec::new();
        for (index, &offset) in offsets.iter().enumerate() {
            let sorted_index = sorted_offsets
                .binary_search(&offset)
                .map_err(|_| PageDecodeError("entry offset is not indexed"))?;
            let entry_end = sorted_offsets
                .get(sorted_index + 1)
                .copied()
                .unwrap_or(PAGE_SIZE);
            if entry_end <= offset || offset.checked_add(4).is_none_or(|end| end > entry_end) {
                return Err(PageDecodeError("entry header exceeds entry bounds"));
            }

            let prefix_len = read_u16(self.data, offset)
                .ok_or(PageDecodeError("truncated key prefix length"))?
                as usize;
            let suffix_len = read_u16(self.data, offset + 2)
                .ok_or(PageDecodeError("truncated key suffix length"))?
                as usize;
            let suffix_start = offset + 4;
            let suffix_end = suffix_start
                .checked_add(suffix_len)
                .ok_or(PageDecodeError("key suffix overflows"))?;
            if suffix_end > entry_end {
                return Err(PageDecodeError("key suffix exceeds entry bounds"));
            }
            if prefix_len > previous_key.len() {
                return Err(PageDecodeError("key prefix exceeds predecessor"));
            }
            let key_len = prefix_len
                .checked_add(suffix_len)
                .ok_or(PageDecodeError("key length overflows"))?;
            if self.slot_key_len(index) != Some(key_len) {
                return Err(PageDecodeError("slot key length disagrees with entry"));
            }

            let mut current_key = Vec::with_capacity(key_len);
            current_key.extend_from_slice(&previous_key[..prefix_len]);
            current_key.extend_from_slice(&self.data[suffix_start..suffix_end]);
            if previous_key.as_slice() > current_key.as_slice() {
                return Err(PageDecodeError("keys are not sorted"));
            }
            previous_key = current_key;

            match self.header.page_type {
                PageType::Internal => {
                    if suffix_end.checked_add(8) != Some(entry_end) {
                        return Err(PageDecodeError("internal child payload is malformed"));
                    }
                }
                PageType::Leaf => {
                    let value_type = *self
                        .data
                        .get(suffix_end)
                        .ok_or(PageDecodeError("leaf value type is missing"))?;
                    let value_start = suffix_end + 1;
                    match value_type {
                        0x00 if value_start <= entry_end => {}
                        0x01 if value_start.checked_add(BLOB_POINTER_SIZE) == Some(entry_end) => {}
                        0x02 if value_start == entry_end => {}
                        _ => return Err(PageDecodeError("leaf value payload is malformed")),
                    }
                }
            }
        }

        Ok(())
    }

    fn compare_key(&self, index: usize, key: &[u8]) -> Option<Ordering> {
        let (prefix_len, suffix) = self.key_parts(index)?;
        if prefix_len == 0 {
            return Some(suffix.cmp(key));
        }
        self.key(index).map(|stored| stored.as_slice().cmp(key))
    }

    fn key(&self, index: usize) -> Option<Vec<u8>> {
        if index >= self.header.count {
            return None;
        }
        let mut key = Vec::new();
        for current in 0..=index {
            let (prefix_len, suffix) = self.key_parts(current)?;
            if prefix_len > key.len() {
                return None;
            }
            key.truncate(prefix_len);
            key.extend_from_slice(suffix);
        }
        Some(key)
    }

    fn key_parts(&self, index: usize) -> Option<(usize, &'a [u8])> {
        if index >= self.header.count {
            return None;
        }
        let offset = self.slot_offset(index)?;
        let prefix_len = read_u16(self.data, offset)? as usize;
        let suffix_len = read_u16(self.data, offset + 2)? as usize;
        let start = offset.checked_add(4)?;
        let end = start.checked_add(suffix_len)?;
        Some((prefix_len, self.data.get(start..end)?))
    }

    fn child_id(&self, index: usize) -> Option<u64> {
        if self.header.page_type != PageType::Internal || index >= self.header.count {
            return None;
        }
        let (payload_start, entry_end) = self.payload_bounds(index)?;
        if payload_start.checked_add(8) != Some(entry_end) {
            return None;
        }
        read_u64(self.data, payload_start)
    }

    fn payload_bounds(&self, index: usize) -> Option<(usize, usize)> {
        let offset = self.slot_offset(index)?;
        let suffix_len = read_u16(self.data, offset + 2)? as usize;
        let payload_start = offset.checked_add(4)?.checked_add(suffix_len)?;
        let entry_end = self.entry_end(index)?;
        (payload_start <= entry_end).then_some((payload_start, entry_end))
    }

    fn entry_end(&self, index: usize) -> Option<usize> {
        let offset = self.slot_offset(index)?;
        let mut end = PAGE_SIZE;
        for other in 0..self.header.count {
            if other == index {
                continue;
            }
            let candidate = self.slot_offset(other)?;
            if candidate > offset && candidate < end {
                end = candidate;
            }
        }
        Some(end)
    }

    fn slot_offset(&self, index: usize) -> Option<usize> {
        if index >= self.header.count {
            return None;
        }
        let start = HEADER_SIZE.checked_add(index.checked_mul(SLOT_SIZE)?)?;
        read_u16(self.data, start).map(usize::from)
    }

    fn slot_key_len(&self, index: usize) -> Option<usize> {
        if index >= self.header.count {
            return None;
        }
        let start = HEADER_SIZE
            .checked_add(index.checked_mul(SLOT_SIZE)?)?
            .checked_add(2)?;
        read_u16(self.data, start).map(usize::from)
    }
}

fn read_u16(data: &[u8], start: usize) -> Option<u16> {
    let bytes: [u8; 2] = data.get(start..start.checked_add(2)?)?.try_into().ok()?;
    Some(u16::from_le_bytes(bytes))
}

fn read_u32(data: &[u8], start: usize) -> Option<u32> {
    let bytes: [u8; 4] = data.get(start..start.checked_add(4)?)?.try_into().ok()?;
    Some(u32::from_le_bytes(bytes))
}

fn read_u64(data: &[u8], start: usize) -> Option<u64> {
    let bytes: [u8; 8] = data.get(start..start.checked_add(8)?)?.try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}
