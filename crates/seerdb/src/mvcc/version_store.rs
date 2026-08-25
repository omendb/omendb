//! Append-oriented logical MVCC version storage.
//!
//! The version store is deliberately separate from physical page versions and
//! the recovery WAL. It stores logical before-images addressed by a stable
//! `VersionId`; the current ordered-tree record will eventually keep only its
//! newest state and an undo head. This module is an isolated foundation for
//! that migration and does not make inline version chains authoritative.

use crate::error::{Error, Result};
use crate::storage::format::{CommitSeq, TxnId, VersionId};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 4] = b"SVE1";
const CURRENT_MAGIC: &[u8; 4] = b"SVC1";
const VALUE_TOMBSTONE: u32 = u32::MAX;
const FIXED_HEADER_BYTES: u64 = 4 + 8 + 8 + 8 + 8 + 4;
const CHECKSUM_BYTES: u64 = 4;
const MAX_VERSION_BYTES: usize = 16 * 1024 * 1024;

/// One logical before-image in the append-oriented version store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VersionRecord {
    pub(crate) id: VersionId,
    pub(crate) previous: Option<VersionId>,
    pub(crate) transaction: TxnId,
    pub(crate) commit: CommitSeq,
    pub(crate) value: Option<Vec<u8>>,
}

/// Newest value and the head of its logical before-image chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CurrentRecord {
    pub(crate) transaction: TxnId,
    pub(crate) commit: CommitSeq,
    pub(crate) undo_head: Option<VersionId>,
    pub(crate) value: Option<Vec<u8>>,
}

/// A value version resolved through the current record or undo store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VisibleVersion {
    pub(crate) transaction: TxnId,
    pub(crate) commit: CommitSeq,
    pub(crate) value: Option<Vec<u8>>,
}

impl CurrentRecord {
    pub(crate) fn absent() -> Self {
        Self {
            transaction: TxnId::new(0),
            commit: CommitSeq::new(0),
            undo_head: None,
            value: None,
        }
    }
}

/// Encode the newest value stored in an ordered-tree record.
pub(crate) fn encode_current(record: &CurrentRecord) -> Result<Vec<u8>> {
    let value_length = record
        .value
        .as_ref()
        .map(|value| {
            u32::try_from(value.len())
                .map_err(|_| Error::InvalidArgument("MVCC current value is too large".into()))
        })
        .transpose()?
        .unwrap_or(VALUE_TOMBSTONE);
    let mut bytes = Vec::with_capacity(32 + record.value.as_ref().map_or(0, Vec::len));
    bytes.extend_from_slice(CURRENT_MAGIC);
    bytes.extend_from_slice(&record.transaction.get().to_be_bytes());
    bytes.extend_from_slice(&record.commit.get().to_be_bytes());
    bytes.extend_from_slice(&record.undo_head.map_or(0, VersionId::get).to_be_bytes());
    bytes.extend_from_slice(&value_length.to_be_bytes());
    if let Some(value) = &record.value {
        bytes.extend_from_slice(value);
    }
    if bytes.len() > MAX_VERSION_BYTES {
        return Err(Error::InvalidArgument(
            "MVCC current record exceeds the retention limit".into(),
        ));
    }
    Ok(bytes)
}

/// Decode the newest value stored in an ordered-tree record.
pub(crate) fn decode_current(bytes: Option<&[u8]>) -> Result<CurrentRecord> {
    let Some(bytes) = bytes else {
        return Ok(CurrentRecord::absent());
    };
    if bytes.len() < 32 || bytes.len() > MAX_VERSION_BYTES || &bytes[..4] != CURRENT_MAGIC {
        return Err(Error::Corruption(
            "MVCC current record has an invalid envelope".into(),
        ));
    }
    let transaction = TxnId::new(read_u64(&bytes[4..12], "transaction")?);
    let commit = CommitSeq::new(read_u64(&bytes[12..20], "commit")?);
    let undo_raw = read_u64(&bytes[20..28], "undo head")?;
    let value_length = u32::from_be_bytes(
        bytes[28..32]
            .try_into()
            .map_err(|_| Error::Corruption("invalid MVCC current value length".into()))?,
    );
    let value = if value_length == VALUE_TOMBSTONE {
        None
    } else {
        let length = value_length as usize;
        let end = 32usize
            .checked_add(length)
            .ok_or_else(|| Error::Corruption("MVCC current value length overflows".into()))?;
        bytes
            .get(32..end)
            .ok_or_else(|| Error::Corruption("truncated MVCC current value".into()))?
            .to_vec()
            .into()
    };
    let expected = 32usize
        .checked_add(value.as_ref().map_or(0, Vec::len))
        .ok_or_else(|| Error::Corruption("MVCC current record length overflows".into()))?;
    if expected != bytes.len() {
        return Err(Error::Corruption(
            "MVCC current record has trailing bytes".into(),
        ));
    }
    Ok(CurrentRecord {
        transaction,
        commit,
        undo_head: (undo_raw != 0).then(|| VersionId::new(undo_raw)),
        value,
    })
}

/// Resolve the newest version visible to a snapshot.
pub(crate) fn visible_current(
    store: &mut VersionStore,
    current: &CurrentRecord,
    snapshot: CommitSeq,
) -> Result<Option<VisibleVersion>> {
    if current.commit <= snapshot {
        return Ok(Some(VisibleVersion {
            transaction: current.transaction,
            commit: current.commit,
            value: current.value.clone(),
        }));
    }
    let mut head = current.undo_head;
    while let Some(id) = head {
        let record = store.get(id)?;
        if record.commit <= snapshot {
            return Ok(Some(VisibleVersion {
                transaction: record.transaction,
                commit: record.commit,
                value: record.value,
            }));
        }
        head = record.previous;
    }
    Ok(None)
}

fn read_u64(bytes: &[u8], field: &str) -> Result<u64> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        Error::Corruption(format!("invalid MVCC current {field}"))
    })?))
}

/// Append-only version file with an in-memory logical-ID index.
///
/// The file is scanned on open and a truncated final frame is discarded. A
/// complete frame with a bad checksum is corruption; it is never silently
/// treated as an absent version. The file is not a second WAL: callers must
/// order its sync with the transaction WAL and commit decision.
pub(crate) struct VersionStore {
    file: File,
    offsets: BTreeMap<VersionId, u64>,
    next_id: VersionId,
}

impl VersionStore {
    /// Create a new empty version store and its parent directory entry.
    pub(crate) fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            file,
            offsets: BTreeMap::new(),
            next_id: VersionId::new(1),
        })
    }

    /// Open an existing store, indexing valid frames and refusing corruption.
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut store = Self {
            file,
            offsets: BTreeMap::new(),
            next_id: VersionId::new(1),
        };
        store.scan_existing()?;
        Ok(store)
    }

    /// Return the number of indexed version records.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Append one before-image and return its stable logical identity.
    pub(crate) fn append(
        &mut self,
        previous: Option<VersionId>,
        transaction: TxnId,
        commit: CommitSeq,
        value: Option<&[u8]>,
    ) -> Result<VersionId> {
        if previous.is_some_and(|id| !self.offsets.contains_key(&id)) {
            return Err(Error::Corruption(
                "MVCC version points to an unknown predecessor".into(),
            ));
        }
        let id = self.next_id;
        let value_length = value
            .map(|value| {
                u32::try_from(value.len())
                    .map_err(|_| Error::InvalidArgument("MVCC version value is too large".into()))
            })
            .transpose()?
            .unwrap_or(VALUE_TOMBSTONE);
        let value_len = value.map_or(0, <[u8]>::len);
        let frame_len = usize::try_from(FIXED_HEADER_BYTES + CHECKSUM_BYTES)
            .expect("fixed version frame fits usize")
            .checked_add(value_len)
            .ok_or_else(|| Error::InvalidArgument("MVCC version frame is too large".into()))?;
        if frame_len > MAX_VERSION_BYTES {
            return Err(Error::InvalidArgument(
                "MVCC version frame exceeds the retention limit".into(),
            ));
        }

        let offset = self.file.seek(SeekFrom::End(0))?;
        let mut frame = Vec::with_capacity(frame_len);
        frame.extend_from_slice(MAGIC);
        frame.extend_from_slice(&id.get().to_be_bytes());
        frame.extend_from_slice(&previous.map_or(0, VersionId::get).to_be_bytes());
        frame.extend_from_slice(&transaction.get().to_be_bytes());
        frame.extend_from_slice(&commit.get().to_be_bytes());
        frame.extend_from_slice(&value_length.to_be_bytes());
        if let Some(value) = value {
            frame.extend_from_slice(value);
        }
        let checksum = crc32c::crc32c(&frame);
        frame.extend_from_slice(&checksum.to_be_bytes());
        self.file.write_all(&frame)?;
        self.offsets.insert(id, offset);
        self.next_id = VersionId::new(
            id.get()
                .checked_add(1)
                .ok_or_else(|| Error::Wal("MVCC version ID exhausted".into()))?,
        );
        Ok(id)
    }

    /// Make appended version records durable.
    pub(crate) fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Read one logical version by ID.
    pub(crate) fn get(&mut self, id: VersionId) -> Result<VersionRecord> {
        let offset = *self
            .offsets
            .get(&id)
            .ok_or_else(|| Error::Corruption(format!("unknown MVCC version {}", id.get())))?;
        self.read_at(offset)
    }

    fn scan_existing(&mut self) -> Result<()> {
        let length = self.file.metadata()?.len();
        let mut offset = 0;
        while offset < length {
            let remaining = length - offset;
            if remaining < FIXED_HEADER_BYTES + CHECKSUM_BYTES {
                self.file.set_len(offset)?;
                break;
            }
            self.file.seek(SeekFrom::Start(offset))?;
            let mut fixed = [0u8; FIXED_HEADER_BYTES as usize];
            self.file.read_exact(&mut fixed)?;
            if &fixed[..4] != MAGIC {
                return Err(Error::Corruption("invalid MVCC version magic".into()));
            }
            let value_length = u32::from_be_bytes(
                fixed[36..40]
                    .try_into()
                    .map_err(|_| Error::Corruption("invalid MVCC version value length".into()))?,
            );
            let value_bytes = if value_length == VALUE_TOMBSTONE {
                0
            } else {
                let value_bytes = u64::from(value_length);
                if value_bytes > MAX_VERSION_BYTES as u64 {
                    return Err(Error::Corruption(
                        "MVCC version value exceeds the retention limit".into(),
                    ));
                }
                value_bytes
            };
            let frame_bytes = FIXED_HEADER_BYTES
                .checked_add(CHECKSUM_BYTES)
                .and_then(|length| length.checked_add(value_bytes))
                .ok_or_else(|| Error::Corruption("MVCC version frame length overflows".into()))?;
            if remaining < frame_bytes {
                self.file.set_len(offset)?;
                break;
            }
            let record = self.read_at(offset)?;
            let id = record.id;
            if self.offsets.insert(id, offset).is_some() {
                return Err(Error::Corruption("duplicate MVCC version ID".into()));
            }
            if id != self.next_id {
                return Err(Error::Corruption(
                    "MVCC version IDs are not contiguous".into(),
                ));
            }
            if let Some(previous) = record.previous {
                if previous >= id {
                    return Err(Error::Corruption(
                        "MVCC version predecessor is not older".into(),
                    ));
                }
                if !self.offsets.contains_key(&previous) {
                    return Err(Error::Corruption(
                        "MVCC version predecessor is unknown".into(),
                    ));
                }
            }
            self.next_id = VersionId::new(
                id.get()
                    .checked_add(1)
                    .ok_or_else(|| Error::Wal("MVCC version ID exhausted".into()))?,
            );
            offset = offset
                .checked_add(frame_bytes)
                .ok_or_else(|| Error::Corruption("MVCC version file length overflows".into()))?;
        }
        Ok(())
    }

    fn read_at(&mut self, offset: u64) -> Result<VersionRecord> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut fixed = [0u8; FIXED_HEADER_BYTES as usize];
        self.file.read_exact(&mut fixed)?;
        if &fixed[..4] != MAGIC {
            return Err(Error::Corruption("invalid MVCC version magic".into()));
        }
        let id = VersionId::new(u64::from_be_bytes(
            fixed[4..12]
                .try_into()
                .map_err(|_| Error::Corruption("invalid MVCC version ID".into()))?,
        ));
        let previous_raw = u64::from_be_bytes(
            fixed[12..20]
                .try_into()
                .map_err(|_| Error::Corruption("invalid MVCC version predecessor".into()))?,
        );
        let transaction =
            TxnId::new(u64::from_be_bytes(fixed[20..28].try_into().map_err(
                |_| Error::Corruption("invalid MVCC version transaction".into()),
            )?));
        let commit =
            CommitSeq::new(u64::from_be_bytes(fixed[28..36].try_into().map_err(
                |_| Error::Corruption("invalid MVCC version commit".into()),
            )?));
        let value_length = u32::from_be_bytes(
            fixed[36..40]
                .try_into()
                .map_err(|_| Error::Corruption("invalid MVCC version value length".into()))?,
        );
        let value = if value_length == VALUE_TOMBSTONE {
            None
        } else {
            let length = value_length as usize;
            if length > MAX_VERSION_BYTES {
                return Err(Error::Corruption(
                    "MVCC version value exceeds the retention limit".into(),
                ));
            }
            let mut value = vec![0; length];
            self.file.read_exact(&mut value)?;
            Some(value)
        };
        let mut checksum_bytes = [0u8; 4];
        self.file.read_exact(&mut checksum_bytes)?;
        let checksum = u32::from_be_bytes(checksum_bytes);

        let mut frame = fixed.to_vec();
        if let Some(value) = &value {
            frame.extend_from_slice(value);
        }
        if crc32c::crc32c(&frame) != checksum {
            return Err(Error::Corruption("MVCC version checksum mismatch".into()));
        }
        Ok(VersionRecord {
            id,
            previous: (previous_raw != 0).then(|| VersionId::new(previous_raw)),
            transaction,
            commit,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn appends_and_reopens_logical_predecessors() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("seerdb.mvcc");
        let mut store = VersionStore::create(&path).expect("create");
        let first = store
            .append(None, TxnId::new(7), CommitSeq::new(1), None)
            .expect("append first");
        let second = store
            .append(Some(first), TxnId::new(8), CommitSeq::new(2), Some(b"old"))
            .expect("append second");
        store.sync().expect("sync");
        drop(store);

        let mut reopened = VersionStore::open(&path).expect("open");
        assert_eq!(reopened.len(), 2);
        assert_eq!(reopened.get(first).expect("first").value, None);
        assert_eq!(
            reopened.get(second).expect("second"),
            VersionRecord {
                id: second,
                previous: Some(first),
                transaction: TxnId::new(8),
                commit: CommitSeq::new(2),
                value: Some(b"old".to_vec()),
            }
        );
    }

    #[test]
    fn truncated_tail_is_discarded_on_reopen() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("seerdb.mvcc");
        let mut store = VersionStore::create(&path).expect("create");
        store
            .append(None, TxnId::new(1), CommitSeq::new(1), Some(b"value"))
            .expect("append");
        store.sync().expect("sync");
        drop(store);
        let length = std::fs::metadata(&path).expect("metadata").len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open")
            .set_len(length - 2)
            .expect("truncate");

        let mut reopened = VersionStore::open(&path).expect("open truncated");
        assert_eq!(reopened.len(), 0);
        let id = reopened
            .append(None, TxnId::new(2), CommitSeq::new(2), Some(b"next"))
            .expect("append after truncation");
        assert_eq!(id, VersionId::new(1));
    }

    #[test]
    fn current_record_resolves_append_history() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("seerdb.mvcc");
        let mut store = VersionStore::create(&path).expect("create");
        let head = store
            .append(None, TxnId::new(1), CommitSeq::new(1), Some(b"old"))
            .expect("append");
        let current = CurrentRecord {
            transaction: TxnId::new(2),
            commit: CommitSeq::new(2),
            undo_head: Some(head),
            value: Some(b"new".to_vec()),
        };
        let encoded = encode_current(&current).expect("encode current");
        assert_eq!(
            decode_current(Some(&encoded)).expect("decode current"),
            current
        );
        assert_eq!(
            visible_current(&mut store, &current, CommitSeq::new(1))
                .expect("visible old")
                .and_then(|version| version.value),
            Some(b"old".to_vec())
        );
        assert_eq!(
            visible_current(&mut store, &current, CommitSeq::new(2))
                .expect("visible new")
                .and_then(|version| version.value),
            Some(b"new".to_vec())
        );
    }

    #[test]
    fn checksum_corruption_is_rejected() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("seerdb.mvcc");
        let mut store = VersionStore::create(&path).expect("create");
        store
            .append(None, TxnId::new(1), CommitSeq::new(1), Some(b"value"))
            .expect("append");
        store.sync().expect("sync");
        drop(store);
        let length = std::fs::metadata(&path).expect("metadata").len();
        let mut file = OpenOptions::new().write(true).open(&path).expect("open");
        file.seek(SeekFrom::Start(length - 1)).expect("seek");
        file.write_all(&[0xFF]).expect("corrupt");
        assert!(matches!(
            VersionStore::open(&path),
            Err(Error::Corruption(message)) if message.contains("checksum")
        ));
    }

    #[test]
    fn unknown_predecessor_is_rejected() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("seerdb.mvcc");
        let mut store = VersionStore::create(&path).expect("create");
        assert!(matches!(
            store.append(
                Some(VersionId::new(99)),
                TxnId::new(1),
                CommitSeq::new(1),
                None,
            ),
            Err(Error::Corruption(message)) if message.contains("unknown predecessor")
        ));
    }
}
