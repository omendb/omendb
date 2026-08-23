use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::fault::{FaultInjector, FaultPoint};
use crate::model::Key;
use crate::{DbError, Result};

pub const PACKED_PAGE_BYTES: usize = 4096;
const HEADER_BYTES: usize = 64;
const PAYLOAD_BYTES: usize = PACKED_PAGE_BYTES - HEADER_BYTES;
const ENTRY_HEADER_BYTES: usize = 21;
const MAGIC: [u8; 4] = *b"DBRP";
const VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackBudget {
    pub max_pages: usize,
    pub max_entries: usize,
    pub max_bytes: usize,
}

impl PackBudget {
    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            max_pages: usize::MAX,
            max_entries: usize::MAX,
            max_bytes: usize::MAX,
        }
    }

    #[must_use]
    pub const fn is_bounded(self) -> bool {
        self.max_pages != usize::MAX
            && self.max_entries != usize::MAX
            && self.max_bytes != usize::MAX
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackReport {
    pub pages: usize,
    pub entries: usize,
    pub bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedPage {
    bytes: Box<[u8; PACKED_PAGE_BYTES]>,
}

impl PackedPage {
    #[must_use]
    pub fn bytes(&self) -> &[u8; PACKED_PAGE_BYTES] {
        &self.bytes
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        u64::from_le_bytes(self.bytes[8..16].try_into().expect("generation width"))
    }

    #[must_use]
    pub fn ordinal(&self) -> u32 {
        u32::from_le_bytes(self.bytes[16..20].try_into().expect("ordinal width"))
    }

    #[must_use]
    pub fn entry_count(&self) -> usize {
        u16::from_le_bytes(self.bytes[20..22].try_into().expect("entry count width")) as usize
    }

    #[must_use]
    pub fn first_key(&self) -> Key {
        Key(self.bytes[24..40].try_into().expect("first key width"))
    }

    #[must_use]
    pub fn last_key(&self) -> Key {
        Key(self.bytes[40..56].try_into().expect("last key width"))
    }

    fn from_entries(generation: u64, ordinal: u32, entries: &[(Key, Vec<u8>)]) -> Result<Self> {
        if entries.is_empty() {
            return Err(DbError::InvalidState(
                "packed page cannot be empty".to_owned(),
            ));
        }
        if entries.len() > u16::MAX as usize {
            return Err(DbError::InvalidState(
                "packed page has too many entries".to_owned(),
            ));
        }
        let mut bytes = Box::new([0_u8; PACKED_PAGE_BYTES]);
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4..6].copy_from_slice(&VERSION.to_le_bytes());
        bytes[8..16].copy_from_slice(&generation.to_le_bytes());
        bytes[16..20].copy_from_slice(&ordinal.to_le_bytes());
        bytes[20..22].copy_from_slice(&(entries.len() as u16).to_le_bytes());
        bytes[24..40].copy_from_slice(&entries[0].0.0);
        bytes[40..56].copy_from_slice(&entries[entries.len() - 1].0.0);

        let mut cursor = HEADER_BYTES;
        for (key, value) in entries {
            let end = cursor
                .checked_add(ENTRY_HEADER_BYTES)
                .and_then(|offset| offset.checked_add(value.len()))
                .ok_or(DbError::ValueTooLarge(value.len()))?;
            if end > PACKED_PAGE_BYTES {
                return Err(DbError::ValueTooLarge(value.len()));
            }
            bytes[cursor..cursor + 16].copy_from_slice(&key.0);
            bytes[cursor + 16..cursor + 20].copy_from_slice(
                &(u32::try_from(value.len()).map_err(|_| DbError::ValueTooLarge(value.len()))?)
                    .to_le_bytes(),
            );
            bytes[cursor + 20] = 1;
            bytes[cursor + ENTRY_HEADER_BYTES..end].copy_from_slice(value);
            cursor = end;
        }
        bytes[22..24].copy_from_slice(&((cursor - HEADER_BYTES) as u16).to_le_bytes());
        let checksum = page_checksum(&bytes);
        bytes[56..60].copy_from_slice(&checksum.to_le_bytes());
        Ok(Self { bytes })
    }

    pub fn decode(bytes: &[u8], expected_generation: u64, expected_ordinal: u32) -> Result<Self> {
        if bytes.len() != PACKED_PAGE_BYTES {
            return Err(corruption_error("packed page has the wrong size"));
        }
        let mut page = Box::new([0_u8; PACKED_PAGE_BYTES]);
        page.copy_from_slice(bytes);
        if page[..4] != MAGIC
            || u16::from_le_bytes(page[4..6].try_into().expect("version width")) != VERSION
            || u64::from_le_bytes(page[8..16].try_into().expect("generation width"))
                != expected_generation
            || u32::from_le_bytes(page[16..20].try_into().expect("ordinal width"))
                != expected_ordinal
        {
            return Err(corruption_error("packed page identity mismatch"));
        }
        let payload_len =
            u16::from_le_bytes(page[22..24].try_into().expect("payload width")) as usize;
        if payload_len > PAYLOAD_BYTES || page_checksum(&page) != stored_checksum(&page) {
            return Err(corruption_error("packed page checksum or payload mismatch"));
        }
        let page = Self { bytes: page };
        page.validate_entries(payload_len)?;
        Ok(page)
    }

    pub fn entries(&self) -> Result<Vec<(Key, Vec<u8>)>> {
        let payload_len =
            u16::from_le_bytes(self.bytes[22..24].try_into().expect("payload width")) as usize;
        self.validate_entries(payload_len)?;
        let count = self.entry_count();
        let mut cursor = HEADER_BYTES;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let key = Key(self.bytes[cursor..cursor + 16]
                .try_into()
                .expect("key width"));
            let length = u32::from_le_bytes(
                self.bytes[cursor + 16..cursor + 20]
                    .try_into()
                    .expect("value length width"),
            ) as usize;
            let end = cursor + ENTRY_HEADER_BYTES + length;
            entries.push((key, self.bytes[cursor + ENTRY_HEADER_BYTES..end].to_vec()));
            cursor = end;
        }
        Ok(entries)
    }

    fn validate_entries(&self, payload_len: usize) -> Result<()> {
        let count = self.entry_count();
        let mut cursor = HEADER_BYTES;
        let payload_end = HEADER_BYTES + payload_len;
        let mut previous = None;
        for _ in 0..count {
            let header_end = cursor
                .checked_add(ENTRY_HEADER_BYTES)
                .ok_or_else(|| corruption_error("packed entry header overflow"))?;
            if header_end > payload_end {
                return Err(corruption_error("packed entry header exceeds payload"));
            }
            let key = Key(self.bytes[cursor..cursor + 16]
                .try_into()
                .expect("key width"));
            if previous.is_some_and(|candidate| candidate >= key) {
                return Err(corruption_error("packed keys are not strictly ordered"));
            }
            previous = Some(key);
            let length = u32::from_le_bytes(
                self.bytes[cursor + 16..cursor + 20]
                    .try_into()
                    .expect("value length width"),
            ) as usize;
            let end = header_end
                .checked_add(length)
                .ok_or_else(|| corruption_error("packed value length overflow"))?;
            if self.bytes[cursor + 20] != 1 || end > payload_end {
                return Err(corruption_error("packed value exceeds payload"));
            }
            cursor = end;
        }
        if cursor != payload_end
            || (count > 0
                && (self.first_key() > self.last_key() || previous != Some(self.last_key())))
        {
            return Err(corruption_error("packed page range metadata mismatch"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedRange {
    generation: u64,
    pages: Vec<PackedPage>,
    report: PackReport,
    checksum: u32,
}

impl PackedRange {
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn pages(&self) -> &[PackedPage] {
        &self.pages
    }

    #[must_use]
    pub fn report(&self) -> PackReport {
        self.report
    }

    /// Return the identity of the complete derived range artifact.
    ///
    /// Page checksums protect individual pages. The range checksum also binds
    /// their ordered contents to the checkpoint manifest, so a different
    /// valid page set cannot be substituted under the same generation.
    #[must_use]
    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    pub fn write(&self, path: &Path, faults: &mut dyn FaultInjector) -> Result<u64> {
        let mut file =
            File::create(path).map_err(|source| io_error("create packed range", source))?;
        for page in &self.pages {
            if let Err(error) = faults.check(FaultPoint::ShortWrite) {
                file.write_all(&page.bytes[..7])
                    .map_err(|source| io_error("write partial packed range", source))?;
                return Err(error);
            }
            if let Err(error) = faults.check(FaultPoint::TornWrite) {
                file.write_all(&page.bytes[..PACKED_PAGE_BYTES / 2])
                    .map_err(|source| io_error("write torn packed range", source))?;
                return Err(error);
            }
            file.write_all(page.bytes())
                .map_err(|source| io_error("write packed range", source))?;
        }
        faults.check(FaultPoint::PackedPageSync)?;
        file.sync_all()
            .map_err(|source| io_error("sync packed range", source))?;
        Ok(self.report.bytes as u64)
    }

    pub fn read(path: &Path, generation: u64) -> Result<Self> {
        let mut file = File::open(path).map_err(|source| io_error("open packed range", source))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| io_error("read packed range", source))?;
        let checksum = crc32c::crc32c(&bytes);
        if bytes.len() % PACKED_PAGE_BYTES != 0 {
            return Err(corruption_error("packed range is not page aligned"));
        }
        let mut pages = Vec::with_capacity(bytes.len() / PACKED_PAGE_BYTES);
        let mut previous_last = None;
        for (ordinal, bytes) in bytes.as_chunks::<PACKED_PAGE_BYTES>().0.iter().enumerate() {
            let page = PackedPage::decode(
                bytes,
                generation,
                u32::try_from(ordinal)
                    .map_err(|_| DbError::InvalidState("too many packed pages".to_owned()))?,
            )?;
            if previous_last.is_some_and(|last| last >= page.first_key()) {
                return Err(corruption_error("packed page ranges are not ordered"));
            }
            previous_last = Some(page.last_key());
            pages.push(page);
        }
        let entries = pages.iter().map(PackedPage::entry_count).sum();
        Ok(Self {
            generation,
            report: PackReport {
                pages: pages.len(),
                entries,
                bytes: bytes.len(),
            },
            pages,
            checksum,
        })
    }

    pub fn entries(&self) -> Result<Vec<(Key, Vec<u8>)>> {
        let mut entries = Vec::with_capacity(self.report.entries);
        for page in &self.pages {
            entries.extend(page.entries()?);
        }
        Ok(entries)
    }

    pub fn get(&self, key: Key) -> Result<Option<Vec<u8>>> {
        let page = self
            .pages
            .binary_search_by(|page| {
                if key < page.first_key() {
                    std::cmp::Ordering::Greater
                } else if key > page.last_key() {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
            .map(|index| &self.pages[index]);
        let Some(page) = page else {
            return Ok(None);
        };
        Ok(page
            .entries()?
            .into_iter()
            .find_map(|(candidate, value)| (candidate == key).then_some(value)))
    }
}

pub fn pack_sorted(
    generation: u64,
    entries: &[(Key, Vec<u8>)],
    budget: PackBudget,
) -> Result<PackedRange> {
    if budget.max_pages == 0 || budget.max_entries == 0 || budget.max_bytes < PACKED_PAGE_BYTES {
        return Err(DbError::InvalidState(
            "invalid packed-range budget".to_owned(),
        ));
    }
    let mut pages = Vec::new();
    let mut current = Vec::new();
    let mut used_bytes = 0_usize;
    for (position, (key, value)) in entries.iter().enumerate() {
        if position > 0 && entries[position - 1].0 >= *key {
            return Err(DbError::InvalidState(
                "packed input is not strictly ordered".to_owned(),
            ));
        }
        let entry_bytes = ENTRY_HEADER_BYTES
            .checked_add(value.len())
            .ok_or(DbError::ValueTooLarge(value.len()))?;
        if entry_bytes > PAYLOAD_BYTES {
            return Err(DbError::ValueTooLarge(value.len()));
        }
        if position >= budget.max_entries {
            return Err(DbError::InvalidState(
                "packed entry budget exceeded".to_owned(),
            ));
        }
        if used_bytes
            .checked_add(entry_bytes)
            .is_none_or(|bytes| bytes > budget.max_bytes)
        {
            return Err(DbError::InvalidState(
                "packed byte budget exceeded".to_owned(),
            ));
        }
        if !current.is_empty()
            && current
                .iter()
                .map(|(_, value): &(Key, Vec<u8>)| ENTRY_HEADER_BYTES + value.len())
                .sum::<usize>()
                .saturating_add(entry_bytes)
                > PAYLOAD_BYTES
        {
            let ordinal = u32::try_from(pages.len())
                .map_err(|_| DbError::InvalidState("too many packed pages".to_owned()))?;
            pages.push(PackedPage::from_entries(generation, ordinal, &current)?);
            current.clear();
            if pages.len() >= budget.max_pages {
                return Err(DbError::InvalidState(
                    "packed page budget exceeded".to_owned(),
                ));
            }
        }
        current.push((*key, value.clone()));
        used_bytes += entry_bytes;
    }
    if !current.is_empty() {
        let ordinal = u32::try_from(pages.len())
            .map_err(|_| DbError::InvalidState("too many packed pages".to_owned()))?;
        pages.push(PackedPage::from_entries(generation, ordinal, &current)?);
    }
    let checksum = range_checksum(&pages);
    Ok(PackedRange {
        generation,
        report: PackReport {
            pages: pages.len(),
            entries: entries.len(),
            bytes: pages.len() * PACKED_PAGE_BYTES,
        },
        pages,
        checksum,
    })
}

fn range_checksum(pages: &[PackedPage]) -> u32 {
    let mut bytes = Vec::with_capacity(pages.len() * PACKED_PAGE_BYTES);
    for page in pages {
        bytes.extend_from_slice(page.bytes());
    }
    crc32c::crc32c(&bytes)
}

fn page_checksum(page: &[u8; PACKED_PAGE_BYTES]) -> u32 {
    let mut bytes = Vec::with_capacity(PACKED_PAGE_BYTES - 4);
    bytes.extend_from_slice(&page[..56]);
    bytes.extend_from_slice(&page[60..]);
    crc32c::crc32c(&bytes)
}

fn stored_checksum(page: &[u8; PACKED_PAGE_BYTES]) -> u32 {
    u32::from_le_bytes(page[56..60].try_into().expect("checksum width"))
}

fn corruption_error(reason: &str) -> DbError {
    DbError::Corruption {
        artifact: "packed range page",
        reason: reason.to_owned(),
    }
}

fn io_error(operation: &'static str, source: std::io::Error) -> DbError {
    DbError::Io { operation, source }
}

#[cfg(test)]
mod tests {
    use super::{PACKED_PAGE_BYTES, PackBudget, PackedPage, pack_sorted};
    use crate::DbError;
    use crate::model::Key;

    fn entries(count: usize) -> Vec<(Key, Vec<u8>)> {
        (0..count)
            .map(|index| (Key::new(1, index as u64), vec![index as u8; 32]))
            .collect()
    }

    #[test]
    fn packs_sorted_entries_into_immutable_pages_and_reads_ranges() {
        let range = pack_sorted(7, &entries(300), PackBudget::unlimited()).expect("pack");
        assert!(range.report().pages > 1);
        assert_eq!(
            range.get(Key::new(1, 299)).expect("lookup"),
            Some(vec![299_u16 as u8; 32])
        );
        assert_eq!(range.get(Key::new(2, 1)).expect("miss"), None);
        for (ordinal, page) in range.pages().iter().enumerate() {
            let decoded = PackedPage::decode(page.bytes(), 7, ordinal as u32).expect("decode");
            assert_eq!(decoded.bytes().len(), PACKED_PAGE_BYTES);
        }
    }

    #[test]
    fn rejects_unsorted_input_budget_overflow_and_corruption() {
        let mut unsorted = entries(2);
        unsorted.swap(0, 1);
        assert!(matches!(
            pack_sorted(1, &unsorted, PackBudget::unlimited()),
            Err(DbError::InvalidState(_))
        ));
        assert!(matches!(
            pack_sorted(
                1,
                &entries(300),
                PackBudget {
                    max_pages: 1,
                    max_entries: usize::MAX,
                    max_bytes: usize::MAX,
                }
            ),
            Err(DbError::InvalidState(_))
        ));
        let range = pack_sorted(1, &entries(1), PackBudget::unlimited()).expect("pack");
        let mut corrupt = *range.pages()[0].bytes();
        corrupt[56] ^= 1;
        assert!(matches!(
            PackedPage::decode(&corrupt, 1, 0),
            Err(DbError::Corruption { .. })
        ));
    }
}
