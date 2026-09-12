//! Native vNext B-tree page layout.
//!
//! This format is intentionally independent of the legacy generation-COW node.
//! It uses a slotted variable-record page with explicit payload lengths, a
//! cached four-byte key head, and B-link high-fence/right-sibling metadata.
//! Page size is supplied by the buffer pool rather than fixed in the format.
//! Physical checksums/page-LSNs belong in the kernel materialization envelope,
//! not in the access-method hot-path header.

use super::super::PageId;
use crate::btree::BlobPointer;
use std::cmp::Ordering;

const MAGIC: u32 = 0x4f4d_4234; // "OMB4"
const VERSION: u16 = 4;
pub(super) const HEADER_SIZE: usize = 64;
const SLOT_SIZE: usize = 12;
const NONE_PAGE: u64 = u64::MAX;
const FLAG_HIGH_FENCE: u8 = 0x01;
const TAG_INLINE: u8 = 0;
const TAG_BLOB: u8 = 1;
const TAG_TOMBSTONE: u8 = 2;
const BLOB_SIZE: usize = 16;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum PageKind {
    Internal = 1,
    Leaf = 2,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) struct PageError(pub(super) &'static str);

#[derive(Debug, Clone, Copy)]
struct Header {
    kind: PageKind,
    flags: u8,
    count: usize,
    lower: usize,
    upper: usize,
    prefix_offset: usize,
    prefix_len: usize,
    high_offset: usize,
    high_len: usize,
    right_sibling: Option<PageId>,
    leftmost_child: Option<PageId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LeafValueOwned {
    Inline(Vec<u8>),
    Blob(BlobPointer),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LeafEntryOwned {
    pub(super) key: Vec<u8>,
    pub(super) value: LeafValueOwned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InternalEntryOwned {
    pub(super) key: Vec<u8>,
    pub(super) child: PageId,
}

pub(super) enum PageValue<'a> {
    Inline(&'a [u8]),
    Blob(BlobPointer),
    Tombstone,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum InsertResult {
    Inserted,
    Duplicate,
    Full,
    FollowRight(PageId),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum RemoveResult {
    Removed,
    Missing,
    FollowRight(PageId),
}

#[derive(Debug, Clone, Copy)]
struct Slot {
    offset: usize,
    key_len: usize,
    payload_len: usize,
    head: u32,
}

pub(super) struct PageRef<'a> {
    data: &'a [u8],
    header: Header,
}

impl<'a> PageRef<'a> {
    pub(super) fn parse(data: &'a [u8]) -> Result<Self, PageError> {
        if data.len() < HEADER_SIZE || data.len() > u32::MAX as usize {
            return Err(PageError("page size is outside v4 bounds"));
        }
        if read_u32(data, 0) != Some(MAGIC) || read_u16(data, 4) != Some(VERSION) {
            return Err(PageError("invalid v4 page magic/version"));
        }
        let kind = match *data.get(6).ok_or(PageError("missing page kind"))? {
            1 => PageKind::Internal,
            2 => PageKind::Leaf,
            _ => return Err(PageError("invalid page kind")),
        };
        let flags = *data.get(7).ok_or(PageError("missing page flags"))?;
        let count = read_u32(data, 8).ok_or(PageError("missing slot count"))? as usize;
        let lower = read_u32(data, 12).ok_or(PageError("missing lower bound"))? as usize;
        let upper = read_u32(data, 16).ok_or(PageError("missing upper bound"))? as usize;
        let prefix_offset =
            read_u32(data, 20).ok_or(PageError("missing prefix offset"))? as usize;
        let prefix_len = read_u32(data, 24).ok_or(PageError("missing prefix length"))? as usize;
        let high_offset =
            read_u32(data, 28).ok_or(PageError("missing high-fence offset"))? as usize;
        let high_len =
            read_u32(data, 32).ok_or(PageError("missing high-fence length"))? as usize;
        let right_raw = read_u64(data, 36).ok_or(PageError("missing right sibling"))?;
        let leftmost_raw = read_u64(data, 44).ok_or(PageError("missing leftmost child"))?;

        let expected_lower = HEADER_SIZE
            .checked_add(
                count
                    .checked_mul(SLOT_SIZE)
                    .ok_or(PageError("slot array overflow"))?,
            )
            .ok_or(PageError("slot array overflow"))?;
        if lower != expected_lower || lower > upper || upper > data.len() {
            return Err(PageError("invalid page free-space bounds"));
        }

        let has_high = flags & FLAG_HIGH_FENCE != 0;
        if has_high != (right_raw != NONE_PAGE) {
            return Err(PageError("high fence and right sibling disagree"));
        }
        if has_high {
            checked_slice(data, high_offset, high_len)
                .ok_or(PageError("high fence lies outside page"))?;
        } else if high_offset != 0 || high_len != 0 {
            return Err(PageError("unused high-fence fields are nonzero"));
        }
        if prefix_len != 0 {
            checked_slice(data, prefix_offset, prefix_len)
                .ok_or(PageError("prefix lies outside page"))?;
        } else if prefix_offset != 0 {
            return Err(PageError("unused prefix offset is nonzero"));
        }
        if kind == PageKind::Leaf && leftmost_raw != NONE_PAGE {
            return Err(PageError("leaf page has a leftmost child"));
        }
        if kind == PageKind::Internal && leftmost_raw == NONE_PAGE {
            return Err(PageError("internal page lacks a leftmost child"));
        }

        Ok(Self {
            data,
            header: Header {
                kind,
                flags,
                count,
                lower,
                upper,
                prefix_offset,
                prefix_len,
                high_offset,
                high_len,
                right_sibling: (right_raw != NONE_PAGE).then_some(PageId::new(right_raw)),
                leftmost_child: (leftmost_raw != NONE_PAGE).then_some(PageId::new(leftmost_raw)),
            },
        })
    }

    pub(super) fn is_leaf(&self) -> bool {
        self.header.kind == PageKind::Leaf
    }

    pub(super) fn count(&self) -> usize {
        self.header.count
    }

    pub(super) fn right_sibling(&self) -> Option<PageId> {
        self.header.right_sibling
    }

    pub(super) fn leftmost_child(&self) -> Option<PageId> {
        self.header.leftmost_child
    }

    pub(super) fn high_fence(&self) -> Result<Option<&'a [u8]>, PageError> {
        if self.header.flags & FLAG_HIGH_FENCE == 0 {
            return Ok(None);
        }
        checked_slice(self.data, self.header.high_offset, self.header.high_len)
            .map(Some)
            .ok_or(PageError("high fence is malformed"))
    }

    fn prefix(&self) -> Result<&'a [u8], PageError> {
        if self.header.prefix_len == 0 {
            return Ok(&[]);
        }
        checked_slice(
            self.data,
            self.header.prefix_offset,
            self.header.prefix_len,
        )
        .ok_or(PageError("prefix is malformed"))
    }

    pub(super) fn follow_right(&self, key: &[u8]) -> Result<Option<PageId>, PageError> {
        let Some(high) = self.high_fence()? else {
            return Ok(None);
        };
        if key >= high {
            return self
                .header
                .right_sibling
                .map(Some)
                .ok_or(PageError("high fence has no right sibling"));
        }
        Ok(None)
    }

    pub(super) fn search(&self, key: &[u8]) -> Result<Result<usize, usize>, PageError> {
        let mut lo = 0usize;
        let mut hi = self.header.count;
        let mut found = None;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.compare_key(mid, key)? {
                Ordering::Less => lo = mid + 1,
                Ordering::Equal => {
                    found = Some(mid);
                    hi = mid;
                }
                Ordering::Greater => hi = mid,
            }
        }
        Ok(found.map_or_else(|| Err(lo), Ok))
    }

    pub(super) fn child_for_key(&self, key: &[u8]) -> Result<PageId, PageError> {
        if self.header.kind != PageKind::Internal {
            return Err(PageError("leaf page cannot route to a child"));
        }
        let insertion = match self.search(key)? {
            Ok(index) => {
                let mut upper = index + 1;
                while upper < self.header.count
                    && self.compare_key(upper, key)? == Ordering::Equal
                {
                    upper += 1;
                }
                upper
            }
            Err(index) => index,
        };
        if insertion == 0 {
            return self
                .header
                .leftmost_child
                .ok_or(PageError("internal page lacks leftmost child"));
        }
        self.child(insertion - 1)
    }

    pub(super) fn value(&self, index: usize) -> Result<PageValue<'a>, PageError> {
        if self.header.kind != PageKind::Leaf {
            return Err(PageError("internal page has no leaf value"));
        }
        let slot = self.slot(index)?;
        let payload_start = slot
            .offset
            .checked_add(slot.key_len)
            .ok_or(PageError("leaf payload offset overflow"))?;
        let tag = *self
            .data
            .get(payload_start)
            .ok_or(PageError("leaf value tag is missing"))?;
        let value_start = payload_start + 1;
        let value = checked_slice(self.data, value_start, slot.payload_len)
            .ok_or(PageError("leaf value exceeds page"))?;
        match tag {
            TAG_INLINE => Ok(PageValue::Inline(value)),
            TAG_BLOB if value.len() == BLOB_SIZE => Ok(PageValue::Blob(BlobPointer {
                file_id: read_u32(value, 0).ok_or(PageError("blob file id missing"))?,
                offset: read_u64(value, 4).ok_or(PageError("blob offset missing"))?,
                length: read_u32(value, 12).ok_or(PageError("blob length missing"))?,
            })),
            TAG_TOMBSTONE if value.is_empty() => Ok(PageValue::Tombstone),
            _ => Err(PageError("invalid leaf value encoding")),
        }
    }

    pub(super) fn leaf_entries_owned(&self) -> Result<Vec<LeafEntryOwned>, PageError> {
        if self.header.kind != PageKind::Leaf {
            return Err(PageError("internal page has no leaf entries"));
        }
        let mut entries = Vec::with_capacity(self.header.count);
        for index in 0..self.header.count {
            let key = self.full_key(index)?;
            let value = match self.value(index)? {
                PageValue::Inline(value) => LeafValueOwned::Inline(value.to_vec()),
                PageValue::Blob(pointer) => LeafValueOwned::Blob(pointer),
                PageValue::Tombstone => LeafValueOwned::Tombstone,
            };
            entries.push(LeafEntryOwned { key, value });
        }
        Ok(entries)
    }

    pub(super) fn internal_entries_owned(&self) -> Result<Vec<InternalEntryOwned>, PageError> {
        if self.header.kind != PageKind::Internal {
            return Err(PageError("leaf page has no internal entries"));
        }
        let mut entries = Vec::with_capacity(self.header.count);
        for index in 0..self.header.count {
            entries.push(InternalEntryOwned {
                key: self.full_key(index)?,
                child: self.child(index)?,
            });
        }
        Ok(entries)
    }

    pub(super) fn validate_full(&self) -> Result<(), PageError> {
        let mut previous: Option<Vec<u8>> = None;
        let mut spans = Vec::with_capacity(self.header.count + 2);
        for index in 0..self.header.count {
            let slot = self.slot(index)?;
            let entry_len = slot
                .key_len
                .checked_add(1)
                .and_then(|len| {
                    if self.header.kind == PageKind::Leaf {
                        len.checked_add(slot.payload_len)
                    } else {
                        slot.key_len.checked_add(8)
                    }
                })
                .ok_or(PageError("entry length overflow"))?;
            let end = slot
                .offset
                .checked_add(entry_len)
                .ok_or(PageError("entry bounds overflow"))?;
            if slot.offset < self.header.upper || end > self.data.len() {
                return Err(PageError("entry lies outside heap"));
            }
            spans.push((slot.offset, end));

            let key = self.full_key(index)?;
            if previous.as_deref().is_some_and(|prev| prev > key.as_slice()) {
                return Err(PageError("page keys are not sorted"));
            }
            previous = Some(key);
            if self.header.kind == PageKind::Leaf {
                let _ = self.value(index)?;
            } else {
                let _ = self.child(index)?;
            }
        }
        if self.header.prefix_len != 0 {
            spans.push((
                self.header.prefix_offset,
                self.header.prefix_offset + self.header.prefix_len,
            ));
        }
        if self.header.flags & FLAG_HIGH_FENCE != 0 {
            spans.push((
                self.header.high_offset,
                self.header.high_offset + self.header.high_len,
            ));
        }
        spans.sort_unstable();
        if spans.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(PageError("heap objects overlap"));
        }
        Ok(())
    }

    fn compare_key(&self, index: usize, key: &[u8]) -> Result<Ordering, PageError> {
        let prefix = self.prefix()?;
        let slot = self.slot(index)?;
        let suffix = checked_slice(self.data, slot.offset, slot.key_len)
            .ok_or(PageError("key suffix exceeds page"))?;

        if key.starts_with(prefix) {
            let search_suffix = &key[prefix.len()..];
            let search_head = key_head(search_suffix);
            if slot.head != search_head {
                return Ok(slot.head.cmp(&search_head));
            }
        }
        Ok(compare_concat(prefix, suffix, key))
    }

    fn full_key(&self, index: usize) -> Result<Vec<u8>, PageError> {
        let prefix = self.prefix()?;
        let slot = self.slot(index)?;
        let suffix = checked_slice(self.data, slot.offset, slot.key_len)
            .ok_or(PageError("key suffix exceeds page"))?;
        let mut key = Vec::with_capacity(prefix.len() + suffix.len());
        key.extend_from_slice(prefix);
        key.extend_from_slice(suffix);
        Ok(key)
    }

    fn child(&self, index: usize) -> Result<PageId, PageError> {
        if self.header.kind != PageKind::Internal {
            return Err(PageError("leaf page has no child pointer"));
        }
        let slot = self.slot(index)?;
        if slot.payload_len != 8 {
            return Err(PageError("internal child payload length is invalid"));
        }
        let start = slot
            .offset
            .checked_add(slot.key_len)
            .ok_or(PageError("child offset overflow"))?;
        let child = read_u64(self.data, start).ok_or(PageError("child pointer is missing"))?;
        if child == NONE_PAGE {
            return Err(PageError("reserved page id used as child"));
        }
        Ok(PageId::new(child))
    }

    fn slot(&self, index: usize) -> Result<Slot, PageError> {
        if index >= self.header.count {
            return Err(PageError("slot index out of bounds"));
        }
        let start = HEADER_SIZE + index * SLOT_SIZE;
        let offset = read_u32(self.data, start).ok_or(PageError("slot offset missing"))? as usize;
        let key_len = read_u16(self.data, start + 4).ok_or(PageError("slot key length missing"))?
            as usize;
        let payload_len =
            read_u16(self.data, start + 6).ok_or(PageError("slot payload length missing"))?
                as usize;
        let head = read_u32(self.data, start + 8).ok_or(PageError("slot head missing"))?;
        Ok(Slot {
            offset,
            key_len,
            payload_len,
            head,
        })
    }
}

pub(super) fn empty_leaf(page_size: usize) -> Result<Vec<u8>, PageError> {
    build_leaf(page_size, None, None, &[])
}

pub(super) fn build_leaf(
    page_size: usize,
    high_fence: Option<&[u8]>,
    right_sibling: Option<PageId>,
    entries: &[LeafEntryOwned],
) -> Result<Vec<u8>, PageError> {
    build_page(
        page_size,
        PageKind::Leaf,
        None,
        high_fence,
        right_sibling,
        entries.iter().map(|entry| EncodedEntry {
            key: &entry.key,
            payload: EncodedPayload::Leaf(&entry.value),
        }),
        entries.len(),
    )
}

pub(super) fn build_internal(
    page_size: usize,
    high_fence: Option<&[u8]>,
    right_sibling: Option<PageId>,
    leftmost_child: PageId,
    entries: &[InternalEntryOwned],
) -> Result<Vec<u8>, PageError> {
    if leftmost_child.get() == NONE_PAGE {
        return Err(PageError("reserved page id used as leftmost child"));
    }
    build_page(
        page_size,
        PageKind::Internal,
        Some(leftmost_child),
        high_fence,
        right_sibling,
        entries.iter().map(|entry| EncodedEntry {
            key: &entry.key,
            payload: EncodedPayload::Child(entry.child),
        }),
        entries.len(),
    )
}

pub(super) fn try_insert_inline(
    data: &mut [u8],
    key: &[u8],
    value: &[u8],
) -> Result<InsertResult, PageError> {
    let page = PageRef::parse(data)?;
    if !page.is_leaf() {
        return Err(PageError("inline insert targeted an internal page"));
    }
    if let Some(right) = page.follow_right(key)? {
        return Ok(InsertResult::FollowRight(right));
    }
    let insertion = match page.search(key)? {
        Ok(_) => return Ok(InsertResult::Duplicate),
        Err(index) => index,
    };
    let prefix = page.prefix()?.to_vec();
    if !key.starts_with(&prefix) {
        return Err(PageError("inserted key does not share page prefix"));
    }
    if value.len() > u16::MAX as usize {
        return Err(PageError("inline value exceeds v4 slot length"));
    }
    let suffix_len = key.len() - prefix.len();
    if suffix_len > u16::MAX as usize {
        return Err(PageError("key suffix exceeds v4 slot length"));
    }
    drop(page);

    let entry_len = suffix_len
        .checked_add(1)
        .and_then(|len| len.checked_add(value.len()))
        .ok_or(PageError("entry length overflow"))?;
    if !has_contiguous_space(data, entry_len)? {
        compact(data)?;
    }
    if !has_contiguous_space(data, entry_len)? {
        return Ok(InsertResult::Full);
    }

    insert_slot_and_entry(
        data,
        insertion,
        &key[prefix.len()..],
        TAG_INLINE,
        value,
    )?;
    Ok(InsertResult::Inserted)
}

pub(super) fn try_insert_internal(
    data: &mut [u8],
    key: &[u8],
    child: PageId,
) -> Result<InsertResult, PageError> {
    if child.get() == NONE_PAGE {
        return Err(PageError("reserved page id used as child"));
    }
    let page = PageRef::parse(data)?;
    if page.is_leaf() {
        return Err(PageError("separator insert targeted a leaf"));
    }
    if let Some(right) = page.follow_right(key)? {
        return Ok(InsertResult::FollowRight(right));
    }
    let insertion = match page.search(key)? {
        Ok(index) | Err(index) => index,
    };
    let prefix = page.prefix()?.to_vec();
    if !key.starts_with(&prefix) {
        return Err(PageError("separator does not share page prefix"));
    }
    let suffix_len = key.len() - prefix.len();
    if suffix_len > u16::MAX as usize {
        return Err(PageError("separator suffix exceeds v4 slot length"));
    }
    drop(page);

    let entry_len = suffix_len
        .checked_add(8)
        .ok_or(PageError("internal entry length overflow"))?;
    if !has_contiguous_space(data, entry_len)? {
        compact(data)?;
    }
    if !has_contiguous_space(data, entry_len)? {
        return Ok(InsertResult::Full);
    }

    insert_internal_slot(data, insertion, &key[prefix.len()..], child)?;
    Ok(InsertResult::Inserted)
}

pub(super) fn remove_leaf(data: &mut [u8], key: &[u8]) -> Result<RemoveResult, PageError> {
    let page = PageRef::parse(data)?;
    if !page.is_leaf() {
        return Err(PageError("leaf removal targeted an internal page"));
    }
    if let Some(right) = page.follow_right(key)? {
        return Ok(RemoveResult::FollowRight(right));
    }
    let index = match page.search(key)? {
        Ok(index) => index,
        Err(_) => return Ok(RemoveResult::Missing),
    };
    let count = page.count();
    drop(page);

    let start = HEADER_SIZE + index * SLOT_SIZE;
    let end = HEADER_SIZE + count * SLOT_SIZE;
    if start + SLOT_SIZE < end {
        data.copy_within(start + SLOT_SIZE..end, start);
    }
    data[end - SLOT_SIZE..end].fill(0);
    write_u32(data, 8, (count - 1) as u32)?;
    write_u32(data, 12, (HEADER_SIZE + (count - 1) * SLOT_SIZE) as u32)?;
    Ok(RemoveResult::Removed)
}

pub(super) fn choose_leaf_split(entries: &[LeafEntryOwned]) -> Result<usize, PageError> {
    choose_split(entries.len(), |index| {
        leaf_entry_cost(&entries[index]).unwrap_or(usize::MAX)
    })
}

pub(super) fn choose_internal_split(entries: &[InternalEntryOwned]) -> Result<usize, PageError> {
    choose_split(entries.len(), |index| {
        SLOT_SIZE + entries[index].key.len() + 8
    })
}

fn choose_split<F>(count: usize, mut cost: F) -> Result<usize, PageError>
where
    F: FnMut(usize) -> usize,
{
    if count < 2 {
        return Err(PageError("cannot split fewer than two entries"));
    }
    let total = (0..count).try_fold(0usize, |sum, index| {
        sum.checked_add(cost(index)).ok_or(PageError("split cost overflow"))
    })?;
    let mut left = 0usize;
    let mut best = 1usize;
    let mut best_distance = usize::MAX;
    for candidate in 1..count {
        left = left
            .checked_add(cost(candidate - 1))
            .ok_or(PageError("split cost overflow"))?;
        let distance = left.abs_diff(total - left);
        if distance < best_distance {
            best = candidate;
            best_distance = distance;
        }
    }
    Ok(best)
}

fn leaf_entry_cost(entry: &LeafEntryOwned) -> Result<usize, PageError> {
    let payload = match &entry.value {
        LeafValueOwned::Inline(value) => value.len(),
        LeafValueOwned::Blob(_) => BLOB_SIZE,
        LeafValueOwned::Tombstone => 0,
    };
    SLOT_SIZE
        .checked_add(entry.key.len())
        .and_then(|len| len.checked_add(1 + payload))
        .ok_or(PageError("leaf entry cost overflow"))
}

fn compact(data: &mut [u8]) -> Result<(), PageError> {
    let page = PageRef::parse(data)?;
    let high = page.high_fence()?.map(ToOwned::to_owned);
    let right = page.right_sibling();
    let rebuilt = if page.is_leaf() {
        let entries = page.leaf_entries_owned()?;
        build_leaf(data.len(), high.as_deref(), right, &entries)?
    } else {
        let leftmost = page
            .leftmost_child()
            .ok_or(PageError("internal page lacks leftmost child"))?;
        let entries = page.internal_entries_owned()?;
        build_internal(data.len(), high.as_deref(), right, leftmost, &entries)?
    };
    data.copy_from_slice(&rebuilt);
    Ok(())
}

fn has_contiguous_space(data: &[u8], entry_len: usize) -> Result<bool, PageError> {
    let page = PageRef::parse(data)?;
    let needed_lower = page
        .header
        .lower
        .checked_add(SLOT_SIZE)
        .ok_or(PageError("slot growth overflow"))?;
    let needed_upper = page.header.upper.checked_sub(entry_len);
    Ok(needed_upper.is_some_and(|upper| needed_lower <= upper))
}

fn insert_slot_and_entry(
    data: &mut [u8],
    insertion: usize,
    suffix: &[u8],
    tag: u8,
    payload: &[u8],
) -> Result<(), PageError> {
    let page = PageRef::parse(data)?;
    let count = page.count();
    let lower = page.header.lower;
    let upper = page.header.upper;
    if insertion > count || suffix.len() > u16::MAX as usize || payload.len() > u16::MAX as usize {
        return Err(PageError("leaf insertion metadata exceeds slot bounds"));
    }
    drop(page);

    let entry_len = suffix.len() + 1 + payload.len();
    let entry_offset = upper
        .checked_sub(entry_len)
        .ok_or(PageError("leaf entry offset underflow"))?;
    let slot_start = HEADER_SIZE + insertion * SLOT_SIZE;
    if slot_start < lower {
        data.copy_within(slot_start..lower, slot_start + SLOT_SIZE);
    }
    data[entry_offset..entry_offset + suffix.len()].copy_from_slice(suffix);
    data[entry_offset + suffix.len()] = tag;
    data[entry_offset + suffix.len() + 1..entry_offset + entry_len].copy_from_slice(payload);
    write_slot(
        data,
        insertion,
        Slot {
            offset: entry_offset,
            key_len: suffix.len(),
            payload_len: payload.len(),
            head: key_head(suffix),
        },
    )?;
    write_u32(data, 8, (count + 1) as u32)?;
    write_u32(data, 12, (lower + SLOT_SIZE) as u32)?;
    write_u32(data, 16, entry_offset as u32)?;
    Ok(())
}

fn insert_internal_slot(
    data: &mut [u8],
    insertion: usize,
    suffix: &[u8],
    child: PageId,
) -> Result<(), PageError> {
    let page = PageRef::parse(data)?;
    let count = page.count();
    let lower = page.header.lower;
    let upper = page.header.upper;
    if insertion > count || suffix.len() > u16::MAX as usize {
        return Err(PageError("internal insertion metadata exceeds slot bounds"));
    }
    drop(page);

    let entry_len = suffix.len() + 8;
    let entry_offset = upper
        .checked_sub(entry_len)
        .ok_or(PageError("internal entry offset underflow"))?;
    let slot_start = HEADER_SIZE + insertion * SLOT_SIZE;
    if slot_start < lower {
        data.copy_within(slot_start..lower, slot_start + SLOT_SIZE);
    }
    data[entry_offset..entry_offset + suffix.len()].copy_from_slice(suffix);
    data[entry_offset + suffix.len()..entry_offset + entry_len]
        .copy_from_slice(&child.get().to_le_bytes());
    write_slot(
        data,
        insertion,
        Slot {
            offset: entry_offset,
            key_len: suffix.len(),
            payload_len: 8,
            head: key_head(suffix),
        },
    )?;
    write_u32(data, 8, (count + 1) as u32)?;
    write_u32(data, 12, (lower + SLOT_SIZE) as u32)?;
    write_u32(data, 16, entry_offset as u32)?;
    Ok(())
}

enum EncodedPayload<'a> {
    Leaf(&'a LeafValueOwned),
    Child(PageId),
}

struct EncodedEntry<'a> {
    key: &'a [u8],
    payload: EncodedPayload<'a>,
}

fn build_page<'a, I>(
    page_size: usize,
    kind: PageKind,
    leftmost_child: Option<PageId>,
    high_fence: Option<&[u8]>,
    right_sibling: Option<PageId>,
    entries: I,
    count: usize,
) -> Result<Vec<u8>, PageError>
where
    I: IntoIterator<Item = EncodedEntry<'a>>,
{
    if page_size < HEADER_SIZE + SLOT_SIZE || page_size > u32::MAX as usize {
        return Err(PageError("page size is outside v4 bounds"));
    }
    if high_fence.is_some() != right_sibling.is_some() {
        return Err(PageError("high fence and right sibling must appear together"));
    }
    let mut data = vec![0u8; page_size];
    let mut upper = page_size;

    let (high_offset, high_len, flags) = if let Some(high) = high_fence {
        upper = upper
            .checked_sub(high.len())
            .ok_or(PageError("high fence does not fit page"))?;
        data[upper..upper + high.len()].copy_from_slice(high);
        (upper, high.len(), FLAG_HIGH_FENCE)
    } else {
        (0, 0, 0)
    };

    let prefix: &[u8] = &[];
    let (prefix_offset, prefix_len) = if prefix.is_empty() {
        (0, 0)
    } else {
        upper = upper
            .checked_sub(prefix.len())
            .ok_or(PageError("prefix does not fit page"))?;
        data[upper..upper + prefix.len()].copy_from_slice(prefix);
        (upper, prefix.len())
    };

    let lower = HEADER_SIZE
        .checked_add(
            count
                .checked_mul(SLOT_SIZE)
                .ok_or(PageError("slot array overflow"))?,
        )
        .ok_or(PageError("slot array overflow"))?;
    if lower > upper {
        return Err(PageError("slot array does not fit page"));
    }

    let mut actual_count = 0usize;
    for (index, entry) in entries.into_iter().enumerate() {
        if index >= count || entry.key.len() > u16::MAX as usize {
            return Err(PageError("entry count/key length exceeds page metadata"));
        }
        let (tag, payload): (Option<u8>, Vec<u8>) = match entry.payload {
            EncodedPayload::Leaf(LeafValueOwned::Inline(value)) => {
                (Some(TAG_INLINE), value.clone())
            }
            EncodedPayload::Leaf(LeafValueOwned::Blob(pointer)) => {
                (Some(TAG_BLOB), pointer.to_bytes().to_vec())
            }
            EncodedPayload::Leaf(LeafValueOwned::Tombstone) => {
                (Some(TAG_TOMBSTONE), Vec::new())
            }
            EncodedPayload::Child(child) => {
                if child.get() == NONE_PAGE {
                    return Err(PageError("reserved page id used as child"));
                }
                (None, child.get().to_le_bytes().to_vec())
            }
        };
        if payload.len() > u16::MAX as usize {
            return Err(PageError("entry payload exceeds slot length"));
        }
        let entry_len = entry
            .key
            .len()
            .checked_add(payload.len())
            .and_then(|len| len.checked_add(usize::from(tag.is_some())))
            .ok_or(PageError("entry length overflow"))?;
        upper = upper
            .checked_sub(entry_len)
            .ok_or(PageError("entry heap does not fit page"))?;
        if upper < lower {
            return Err(PageError("page does not fit encoded entries"));
        }
        let mut pos = upper;
        data[pos..pos + entry.key.len()].copy_from_slice(entry.key);
        pos += entry.key.len();
        if let Some(tag) = tag {
            data[pos] = tag;
            pos += 1;
        }
        data[pos..pos + payload.len()].copy_from_slice(&payload);
        write_slot(
            &mut data,
            index,
            Slot {
                offset: upper,
                key_len: entry.key.len(),
                payload_len: payload.len(),
                head: key_head(entry.key),
            },
        )?;
        actual_count += 1;
    }
    if actual_count != count {
        return Err(PageError("entry iterator count disagrees with header"));
    }

    write_u32(&mut data, 0, MAGIC)?;
    write_u16(&mut data, 4, VERSION)?;
    data[6] = kind as u8;
    data[7] = flags;
    write_u32(&mut data, 8, count as u32)?;
    write_u32(&mut data, 12, lower as u32)?;
    write_u32(&mut data, 16, upper as u32)?;
    write_u32(&mut data, 20, prefix_offset as u32)?;
    write_u32(&mut data, 24, prefix_len as u32)?;
    write_u32(&mut data, 28, high_offset as u32)?;
    write_u32(&mut data, 32, high_len as u32)?;
    write_u64(
        &mut data,
        36,
        right_sibling.map_or(NONE_PAGE, PageId::get),
    )?;
    write_u64(
        &mut data,
        44,
        leftmost_child.map_or(NONE_PAGE, PageId::get),
    )?;
    PageRef::parse(&data)?.validate_full()?;
    Ok(data)
}

fn write_slot(data: &mut [u8], index: usize, slot: Slot) -> Result<(), PageError> {
    let start = HEADER_SIZE
        .checked_add(
            index
                .checked_mul(SLOT_SIZE)
                .ok_or(PageError("slot offset overflow"))?,
        )
        .ok_or(PageError("slot offset overflow"))?;
    write_u32(
        data,
        start,
        u32::try_from(slot.offset).map_err(|_| PageError("slot offset exceeds u32"))?,
    )?;
    write_u16(
        data,
        start + 4,
        u16::try_from(slot.key_len).map_err(|_| PageError("key length exceeds u16"))?,
    )?;
    write_u16(
        data,
        start + 6,
        u16::try_from(slot.payload_len).map_err(|_| PageError("payload length exceeds u16"))?,
    )?;
    write_u32(data, start + 8, slot.head)
}

fn compare_concat(prefix: &[u8], suffix: &[u8], key: &[u8]) -> Ordering {
    prefix
        .iter()
        .chain(suffix.iter())
        .copied()
        .cmp(key.iter().copied())
}

fn key_head(key: &[u8]) -> u32 {
    let mut bytes = [0u8; 4];
    let count = key.len().min(bytes.len());
    bytes[..count].copy_from_slice(&key[..count]);
    u32::from_be_bytes(bytes)
}

fn checked_slice(data: &[u8], start: usize, len: usize) -> Option<&[u8]> {
    data.get(start..start.checked_add(len)?)
}

fn read_u16(data: &[u8], start: usize) -> Option<u16> {
    let bytes: [u8; 2] = checked_slice(data, start, 2)?.try_into().ok()?;
    Some(u16::from_le_bytes(bytes))
}

fn read_u32(data: &[u8], start: usize) -> Option<u32> {
    let bytes: [u8; 4] = checked_slice(data, start, 4)?.try_into().ok()?;
    Some(u32::from_le_bytes(bytes))
}

fn read_u64(data: &[u8], start: usize) -> Option<u64> {
    let bytes: [u8; 8] = checked_slice(data, start, 8)?.try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}

fn write_u16(data: &mut [u8], start: usize, value: u16) -> Result<(), PageError> {
    let end = start
        .checked_add(2)
        .ok_or(PageError("u16 write offset overflow"))?;
    let target = data
        .get_mut(start..end)
        .ok_or(PageError("u16 write exceeds page"))?;
    target.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_u32(data: &mut [u8], start: usize, value: u32) -> Result<(), PageError> {
    let end = start
        .checked_add(4)
        .ok_or(PageError("u32 write offset overflow"))?;
    let target = data
        .get_mut(start..end)
        .ok_or(PageError("u32 write exceeds page"))?;
    target.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_u64(data: &mut [u8], start: usize, value: u64) -> Result<(), PageError> {
    let end = start
        .checked_add(8)
        .ok_or(PageError("u64 write offset overflow"))?;
    let target = data
        .get_mut(start..end)
        .ok_or(PageError("u64 write exceeds page"))?;
    target.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_insert_remove_and_compaction_preserve_order() {
        let mut page = empty_leaf(512).expect("leaf builds");
        for key in [b"alpha".as_slice(), b"beta", b"delta", b"gamma"] {
            assert_eq!(
                try_insert_inline(&mut page, key, key).expect("insert succeeds"),
                InsertResult::Inserted
            );
        }
        assert_eq!(
            try_insert_inline(&mut page, b"beta", b"duplicate").expect("duplicate checks"),
            InsertResult::Duplicate
        );
        assert_eq!(
            remove_leaf(&mut page, b"delta").expect("remove succeeds"),
            RemoveResult::Removed
        );
        let parsed = PageRef::parse(&page).expect("page parses");
        parsed.validate_full().expect("layout validates");
        let keys: Vec<_> = parsed
            .leaf_entries_owned()
            .expect("entries decode")
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        assert_eq!(keys, vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]);
    }

    #[test]
    fn high_fence_routes_equal_keys_right() {
        let entries = vec![LeafEntryOwned {
            key: b"a".to_vec(),
            value: LeafValueOwned::Inline(b"left".to_vec()),
        }];
        let page = build_leaf(512, Some(b"m"), Some(PageId::new(7)), &entries)
            .expect("fenced leaf builds");
        let parsed = PageRef::parse(&page).expect("page parses");
        assert_eq!(parsed.follow_right(b"l").expect("route"), None);
        assert_eq!(
            parsed.follow_right(b"m").expect("route"),
            Some(PageId::new(7))
        );
    }

    #[test]
    fn internal_upper_bound_routes_separator_right() {
        let entries = vec![InternalEntryOwned {
            key: b"m".to_vec(),
            child: PageId::new(2),
        }];
        let page = build_internal(512, None, None, PageId::new(1), &entries)
            .expect("internal page builds");
        let parsed = PageRef::parse(&page).expect("page parses");
        assert_eq!(parsed.child_for_key(b"a").expect("left route"), PageId::new(1));
        assert_eq!(parsed.child_for_key(b"m").expect("right route"), PageId::new(2));
    }
}
