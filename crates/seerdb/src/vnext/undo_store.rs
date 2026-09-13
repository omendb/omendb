//! Append-only complete before-image storage for vNext MVCC.
//!
//! The transaction WAL remains the authority for commit outcome and ordering.
//! This store owns logical undo records referenced by `MvccRecord::undo_head`.
//! A future page materializer must not persist a current record that depends on
//! an undo version until both the transaction WAL and this store's durability
//! frontier cover the dependency.
//!
//! Recovery validates a checksummed fixed header before trusting its length.
//! It truncates only an incomplete final header or frame, and fails closed on
//! complete corruption. Retained undo links must point strictly backwards.

use super::{MvccCodecError, MvccRecord, VersionId};
use durable_fs::{SyncClass, fsync_dir_chain, sync_file_all, sync_file_data};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const FRAME_MAGIC: [u8; 4] = *b"OMU1";
const FRAME_VERSION: u8 = 2;
const HEADER_CHECKSUM_OFFSET: usize = 20;
const FRAME_HEADER_SIZE: usize = 24;
const FRAME_CHECKSUM_SIZE: usize = 4;
const MAX_UNDO_RECORD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
struct UndoFrame {
    offset: u64,
    length: usize,
}

struct UndoState {
    file: File,
    index: Vec<UndoFrame>,
    end_offset: u64,
}

/// Append-only complete before-image store used by the vNext MVCC layer.
///
/// Appends and durability barriers are serialized only at this file boundary.
/// A failed append or sync fences further mutation until reopen because the
/// physical outcome may be uncertain. Indexed reads remain available while
/// fenced because a partial failed append is never published into the index.
/// The database owner must exclude other handles/processes for this path.
pub struct UndoStore {
    sync_class: SyncClass,
    state: Mutex<UndoState>,
    durable_version: AtomicU64,
    fenced: AtomicBool,
}

impl UndoStore {
    /// Open or create an undo file and repair an incomplete final frame.
    ///
    /// Existing complete frames and their directory entries are synchronized
    /// before returning: existence after a process restart does not prove that
    /// an earlier creation barrier finished. Scanning retains only one frame's
    /// payload at a time plus the version-offset index.
    pub fn open(path: impl AsRef<Path>, sync_class: SyncClass) -> Result<Self, UndoStoreError> {
        let path = path.as_ref();
        let parent = publication_parent(path);
        fs::create_dir_all(parent).map_err(|source| UndoStoreError::Io {
            operation: UndoIoOperation::Open,
            source,
        })?;

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|source| UndoStoreError::Io {
                operation: UndoIoOperation::Open,
                source,
            })?;
        fsync_dir_chain(parent).map_err(|source| UndoStoreError::Io {
            operation: UndoIoOperation::Open,
            source,
        })?;

        let (index, end_offset) = scan_and_repair(&mut file, sync_class)?;
        sync_file_all(&file, sync_class).map_err(|source| UndoStoreError::Io {
            operation: UndoIoOperation::Open,
            source,
        })?;
        file.seek(SeekFrom::Start(end_offset))
            .map_err(|source| UndoStoreError::Io {
                operation: UndoIoOperation::Open,
                source,
            })?;
        let durable = u64::try_from(index.len()).map_err(|_| UndoStoreError::VersionIdExhausted)?;

        Ok(Self {
            sync_class,
            state: Mutex::new(UndoState {
                file,
                index,
                end_offset,
            }),
            durable_version: AtomicU64::new(durable),
            fenced: AtomicBool::new(false),
        })
    }

    /// Append one complete logical before-image and return its stable ID.
    ///
    /// Encoding, size and predecessor validation happen before file I/O. Once
    /// file I/O has started, any error fences the store until reopen.
    pub fn append(&self, record: &MvccRecord) -> Result<VersionId, UndoStoreError> {
        let payload = record.to_bytes()?;
        if payload.len() > MAX_UNDO_RECORD_BYTES {
            return Err(UndoStoreError::RecordTooLarge);
        }

        let mut state = self.state.lock().map_err(|_| UndoStoreError::Poisoned)?;
        self.ensure_open()?;
        let next = state
            .index
            .len()
            .checked_add(1)
            .ok_or(UndoStoreError::VersionIdExhausted)?;
        let raw_id = u64::try_from(next).map_err(|_| UndoStoreError::VersionIdExhausted)?;
        if raw_id == 0 {
            return Err(UndoStoreError::VersionIdExhausted);
        }
        let id = VersionId::new(raw_id);
        validate_predecessor(record, id)?;
        let frame = encode_frame(id, &payload)?;
        let frame_length =
            u64::try_from(frame.len()).map_err(|_| UndoStoreError::RecordTooLarge)?;
        let offset = state.end_offset;
        let end_offset = offset
            .checked_add(frame_length)
            .ok_or(UndoStoreError::FileOffsetExhausted)?;

        state
            .file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| state.file.write_all(&frame))
            .map_err(|source| {
                self.fenced.store(true, Ordering::Release);
                UndoStoreError::Io {
                    operation: UndoIoOperation::Append,
                    source,
                }
            })?;
        state.index.push(UndoFrame {
            offset,
            length: frame.len(),
        });
        state.end_offset = end_offset;
        Ok(id)
    }

    /// Read and revalidate one complete before-image by ID.
    pub fn get(&self, id: VersionId) -> Result<MvccRecord, UndoStoreError> {
        let index = version_index(id)?;
        let mut state = self.state.lock().map_err(|_| UndoStoreError::Poisoned)?;
        let frame = *state
            .index
            .get(index)
            .ok_or(UndoStoreError::MissingVersion(id))?;
        read_frame(&mut state.file, frame, id)
    }

    /// Make every currently appended undo record durable.
    ///
    /// The requested ID must already exist. Because append and sync share the
    /// same file-operation mutex, one successful barrier covers all records that
    /// were appended before the barrier, so the durable frontier advances to the
    /// latest indexed version rather than merely the requested ID.
    pub fn sync_through(&self, id: VersionId) -> Result<VersionId, UndoStoreError> {
        let requested = version_index(id)?;
        let state = self.state.lock().map_err(|_| UndoStoreError::Poisoned)?;
        self.ensure_open()?;
        if requested >= state.index.len() {
            return Err(UndoStoreError::MissingVersion(id));
        }

        let current = self.durable_version.load(Ordering::Acquire);
        if id.get() <= current {
            return Ok(VersionId::new(current));
        }

        sync_file_data(&state.file, self.sync_class).map_err(|source| {
            self.fenced.store(true, Ordering::Release);
            UndoStoreError::Io {
                operation: UndoIoOperation::Sync,
                source,
            }
        })?;
        let latest =
            u64::try_from(state.index.len()).map_err(|_| UndoStoreError::VersionIdExhausted)?;
        self.durable_version.store(latest, Ordering::Release);
        Ok(VersionId::new(latest))
    }

    /// Highest undo version confirmed durable by this open handle.
    #[must_use]
    pub fn durable_version(&self) -> Option<VersionId> {
        let raw = self.durable_version.load(Ordering::Acquire);
        (raw != 0).then_some(VersionId::new(raw))
    }

    /// Whether an uncertain append/sync outcome has fenced further mutation.
    #[must_use]
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }

    fn ensure_open(&self) -> Result<(), UndoStoreError> {
        if self.is_fenced() {
            Err(UndoStoreError::Fenced)
        } else {
            Ok(())
        }
    }
}

/// File operation associated with an undo-store I/O failure.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum UndoIoOperation {
    /// Opening or creating the store.
    Open,
    /// Appending one complete frame.
    Append,
    /// Reading an indexed frame.
    Read,
    /// Synchronizing the durability frontier.
    Sync,
    /// Truncating and synchronizing an incomplete final frame during recovery.
    Repair,
}

/// Failure from the append-only vNext undo store.
#[derive(Debug, thiserror::Error)]
pub enum UndoStoreError {
    /// Further mutation is blocked until recovery/reopen.
    #[error("vNext undo store is fenced until recovery/reopen")]
    Fenced,
    /// Version zero is reserved for “no undo head”.
    #[error("undo version ID zero is reserved")]
    ReservedVersion,
    /// The requested version does not exist in this store.
    #[error("undo version {0:?} is not present")]
    MissingVersion(VersionId),
    /// A before-image must link only to an already appended, nonzero version.
    #[error("undo version {version:?} has invalid predecessor {previous:?}")]
    InvalidPredecessor {
        /// Version containing the link.
        version: VersionId,
        /// Reserved, self-referential or forward link.
        previous: VersionId,
    },
    /// The logical version ID domain has been exhausted.
    #[error("undo version ID space is exhausted")]
    VersionIdExhausted,
    /// The physical file-offset domain has been exhausted.
    #[error("undo file offset space is exhausted")]
    FileOffsetExhausted,
    /// The encoded before-image exceeds the retained frame bound.
    #[error("undo record exceeds the retained frame size bound")]
    RecordTooLarge,
    /// A complete retained frame is invalid and recovery must fail closed.
    #[error("undo store is corrupt: {0}")]
    Corruption(&'static str),
    /// The embedded MVCC envelope is invalid.
    #[error(transparent)]
    Codec(#[from] MvccCodecError),
    /// The store's file-operation mutex was poisoned.
    #[error("undo store operation lock is poisoned")]
    Poisoned,
    /// A filesystem operation failed.
    #[error("undo store {operation:?} failed: {source}")]
    Io {
        /// Operation that failed.
        operation: UndoIoOperation,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
}

fn publication_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn version_index(id: VersionId) -> Result<usize, UndoStoreError> {
    let raw = id.get();
    if raw == 0 {
        return Err(UndoStoreError::ReservedVersion);
    }
    usize::try_from(raw - 1).map_err(|_| UndoStoreError::MissingVersion(id))
}

fn validate_predecessor(record: &MvccRecord, version: VersionId) -> Result<(), UndoStoreError> {
    if let Some(previous) = record
        .undo_head()
        .filter(|previous| previous.get() == 0 || previous.get() >= version.get())
    {
        return Err(UndoStoreError::InvalidPredecessor { version, previous });
    }
    Ok(())
}

fn encode_frame(id: VersionId, payload: &[u8]) -> Result<Vec<u8>, UndoStoreError> {
    if payload.len() > MAX_UNDO_RECORD_BYTES {
        return Err(UndoStoreError::RecordTooLarge);
    }
    let payload_len = u32::try_from(payload.len()).map_err(|_| UndoStoreError::RecordTooLarge)?;
    let capacity = FRAME_HEADER_SIZE
        .checked_add(payload.len())
        .and_then(|length| length.checked_add(FRAME_CHECKSUM_SIZE))
        .ok_or(UndoStoreError::RecordTooLarge)?;
    let mut frame = Vec::with_capacity(capacity);
    frame.extend_from_slice(&FRAME_MAGIC);
    frame.push(FRAME_VERSION);
    frame.push(0);
    frame.extend_from_slice(&0u16.to_le_bytes());
    frame.extend_from_slice(&id.get().to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    let header_checksum = crc32c::crc32c(&frame);
    frame.extend_from_slice(&header_checksum.to_le_bytes());
    frame.extend_from_slice(payload);
    let checksum = crc32c::crc32c(&frame);
    frame.extend_from_slice(&checksum.to_le_bytes());
    Ok(frame)
}

fn scan_and_repair(
    file: &mut File,
    sync_class: SyncClass,
) -> Result<(Vec<UndoFrame>, u64), UndoStoreError> {
    let length = file.metadata().map_err(open_error)?.len();
    file.seek(SeekFrom::Start(0)).map_err(open_error)?;
    let mut index = Vec::new();
    let mut offset = 0u64;
    let mut header = [0u8; FRAME_HEADER_SIZE];

    while offset < length {
        let remaining = length - offset;
        if remaining < FRAME_HEADER_SIZE as u64 {
            return repair_tail(file, index, offset, sync_class);
        }
        file.read_exact(&mut header).map_err(open_error)?;
        validate_header(&header)?;
        let raw_id =
            read_u64(&header, 8).ok_or(UndoStoreError::Corruption("missing version ID"))?;
        let expected_id = u64::try_from(index.len())
            .map_err(|_| UndoStoreError::VersionIdExhausted)?
            .checked_add(1)
            .ok_or(UndoStoreError::VersionIdExhausted)?;
        if raw_id != expected_id {
            return Err(UndoStoreError::Corruption(
                "retained undo version IDs are not contiguous",
            ));
        }
        let payload_len = read_u32(&header, 16)
            .ok_or(UndoStoreError::Corruption("missing payload length"))?
            as usize;
        if payload_len > MAX_UNDO_RECORD_BYTES {
            return Err(UndoStoreError::Corruption(
                "retained undo payload exceeds the size bound",
            ));
        }
        let frame_len = FRAME_HEADER_SIZE
            .checked_add(payload_len)
            .and_then(|length| length.checked_add(FRAME_CHECKSUM_SIZE))
            .ok_or(UndoStoreError::Corruption("undo frame length overflows"))?;
        let frame_length =
            u64::try_from(frame_len).map_err(|_| UndoStoreError::FileOffsetExhausted)?;
        if remaining < frame_length {
            return repair_tail(file, index, offset, sync_class);
        }
        let mut frame = vec![0u8; frame_len];
        frame[..FRAME_HEADER_SIZE].copy_from_slice(&header);
        file.read_exact(&mut frame[FRAME_HEADER_SIZE..])
            .map_err(open_error)?;
        validate_complete_frame(&frame, VersionId::new(raw_id))?;
        index.push(UndoFrame {
            offset,
            length: frame_len,
        });
        offset += frame_length;
    }

    Ok((index, offset))
}

fn open_error(source: io::Error) -> UndoStoreError {
    UndoStoreError::Io {
        operation: UndoIoOperation::Open,
        source,
    }
}

fn repair_tail(
    file: &mut File,
    index: Vec<UndoFrame>,
    valid_len: u64,
    sync_class: SyncClass,
) -> Result<(Vec<UndoFrame>, u64), UndoStoreError> {
    file.set_len(valid_len)
        .map_err(|source| UndoStoreError::Io {
            operation: UndoIoOperation::Repair,
            source,
        })?;
    sync_file_all(file, sync_class).map_err(|source| UndoStoreError::Io {
        operation: UndoIoOperation::Repair,
        source,
    })?;
    Ok((index, valid_len))
}

fn read_frame(
    file: &mut File,
    frame: UndoFrame,
    expected_id: VersionId,
) -> Result<MvccRecord, UndoStoreError> {
    let mut bytes = vec![0u8; frame.length];
    file.seek(SeekFrom::Start(frame.offset))
        .and_then(|_| file.read_exact(&mut bytes))
        .map_err(|source| UndoStoreError::Io {
            operation: UndoIoOperation::Read,
            source,
        })?;
    validate_complete_frame(&bytes, expected_id)
}

fn validate_header(header: &[u8]) -> Result<(), UndoStoreError> {
    if header.len() != FRAME_HEADER_SIZE || header[..4] != FRAME_MAGIC {
        return Err(UndoStoreError::Corruption("invalid undo frame magic"));
    }
    if header[4] != FRAME_VERSION || header[5] != 0 || header[6..8] != [0, 0] {
        return Err(UndoStoreError::Corruption(
            "unsupported undo frame version or flags",
        ));
    }
    if read_u32(header, HEADER_CHECKSUM_OFFSET)
        != Some(crc32c::crc32c(&header[..HEADER_CHECKSUM_OFFSET]))
    {
        return Err(UndoStoreError::Corruption("undo header checksum mismatch"));
    }
    Ok(())
}

fn validate_complete_frame(
    frame: &[u8],
    expected_id: VersionId,
) -> Result<MvccRecord, UndoStoreError> {
    if frame.len() < FRAME_HEADER_SIZE + FRAME_CHECKSUM_SIZE {
        return Err(UndoStoreError::Corruption("truncated complete undo frame"));
    }
    let header = &frame[..FRAME_HEADER_SIZE];
    validate_header(header)?;
    let raw_id = read_u64(header, 8).ok_or(UndoStoreError::Corruption("missing version ID"))?;
    if raw_id != expected_id.get() {
        return Err(UndoStoreError::Corruption(
            "undo frame ID disagrees with index",
        ));
    }
    let payload_len =
        read_u32(header, 16).ok_or(UndoStoreError::Corruption("missing payload length"))? as usize;
    let expected_len = FRAME_HEADER_SIZE
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(FRAME_CHECKSUM_SIZE))
        .ok_or(UndoStoreError::Corruption("undo frame length overflows"))?;
    if frame.len() != expected_len {
        return Err(UndoStoreError::Corruption(
            "undo frame length disagrees with header",
        ));
    }
    let checksum_offset = expected_len - FRAME_CHECKSUM_SIZE;
    let stored = u32::from_le_bytes(
        frame[checksum_offset..]
            .try_into()
            .map_err(|_| UndoStoreError::Corruption("invalid undo checksum field"))?,
    );
    if crc32c::crc32c(&frame[..checksum_offset]) != stored {
        return Err(UndoStoreError::Corruption("undo frame checksum mismatch"));
    }
    let record = MvccRecord::from_bytes(&frame[FRAME_HEADER_SIZE..checksum_offset])?;
    validate_predecessor(&record, expected_id)?;
    Ok(record)
}

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    Some(u32::from_le_bytes(bytes.get(offset..end)?.try_into().ok()?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Option<u64> {
    let end = offset.checked_add(8)?;
    Some(u64::from_le_bytes(bytes.get(offset..end)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{CommitSeq, MvccValue, RecordOwner, TxnId};

    fn record(txn: u64, previous: Option<u64>, value: &[u8]) -> MvccRecord {
        MvccRecord::new(
            RecordOwner::Transaction(TxnId::new(txn)),
            previous.map(VersionId::new),
            MvccValue::Inline(value.to_vec()),
        )
    }

    #[test]
    fn append_read_and_group_sync_advance_one_frontier() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("undo.log");
        let store = UndoStore::open(&path, SyncClass::KernelBarrier).expect("store opens");
        assert_eq!(store.durable_version(), None);

        let first = store
            .append(&record(1, None, b"first"))
            .expect("first appends");
        let second = store
            .append(&record(2, Some(first.get()), b"second"))
            .expect("second appends");
        assert_eq!(first, VersionId::new(1));
        assert_eq!(second, VersionId::new(2));
        assert_eq!(store.durable_version(), None);
        assert_eq!(
            store.get(first).expect("first reads"),
            record(1, None, b"first")
        );
        assert_eq!(
            store.get(second).expect("second reads"),
            record(2, Some(1), b"second")
        );

        assert_eq!(
            store.sync_through(first).expect("sync succeeds"),
            VersionId::new(2)
        );
        assert_eq!(store.durable_version(), Some(VersionId::new(2)));
    }

    #[test]
    fn reopen_preserves_versions_and_next_identity() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("undo.log");
        {
            let store = UndoStore::open(&path, SyncClass::KernelBarrier).expect("store opens");
            let first = store.append(&record(3, None, b"alpha")).expect("append");
            store.sync_through(first).expect("sync");
        }

        let store = UndoStore::open(&path, SyncClass::KernelBarrier).expect("store reopens");
        assert_eq!(store.durable_version(), Some(VersionId::new(1)));
        assert_eq!(
            store.get(VersionId::new(1)).expect("version reads"),
            record(3, None, b"alpha")
        );
        assert_eq!(
            store.append(&record(4, Some(1), b"beta")).expect("append"),
            VersionId::new(2)
        );
    }

    #[test]
    fn reopen_truncates_only_an_incomplete_final_frame() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("undo.log");
        let original_len;
        {
            let store = UndoStore::open(&path, SyncClass::KernelBarrier).expect("store opens");
            let first = store.append(&record(5, None, b"stable")).expect("append");
            store.sync_through(first).expect("sync");
            original_len = fs::metadata(&path).expect("metadata").len();
        }
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("raw append opens");
        file.write_all(&FRAME_MAGIC[..2])
            .expect("partial tail writes");
        file.sync_all().expect("partial tail persists");
        drop(file);

        let store = UndoStore::open(&path, SyncClass::KernelBarrier).expect("store repairs");
        assert_eq!(fs::metadata(&path).expect("metadata").len(), original_len);
        assert_eq!(
            store.get(VersionId::new(1)).expect("version survives"),
            record(5, None, b"stable")
        );
    }

    #[test]
    fn every_truncation_preserves_complete_undo_versions() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("undo.log");
        let first = encode_frame(
            VersionId::new(1),
            &record(1, None, b"first").to_bytes().expect("payload"),
        )
        .expect("frame");
        let second = encode_frame(
            VersionId::new(2),
            &record(2, Some(1), b"second").to_bytes().expect("payload"),
        )
        .expect("frame");
        for cut in 1..second.len() {
            let mut bytes = first.clone();
            bytes.extend_from_slice(&second[..cut]);
            fs::write(&path, bytes).expect("write partial file");
            let store = UndoStore::open(&path, SyncClass::KernelBarrier).expect("repair");
            assert_eq!(
                store.durable_version(),
                Some(VersionId::new(1)),
                "cut {cut}"
            );
            assert_eq!(fs::read(&path).expect("read repaired file"), first);
            assert_eq!(
                store.get(VersionId::new(1)).expect("read"),
                record(1, None, b"first")
            );
        }
    }

    #[test]
    fn corrupt_lengths_fail_without_truncating_the_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("undo.log");
        let frame = encode_frame(
            VersionId::new(1),
            &record(1, None, b"stable").to_bytes().expect("payload"),
        )
        .expect("frame");
        for byte in 16..20 {
            for bit in 0..8 {
                let mut corrupt = frame.clone();
                corrupt[byte] ^= 1 << bit;
                fs::write(&path, &corrupt).expect("write corruption");
                assert!(matches!(
                    UndoStore::open(&path, SyncClass::KernelBarrier),
                    Err(UndoStoreError::Corruption("undo header checksum mismatch"))
                ));
                assert_eq!(fs::read(&path).expect("read unchanged file"), corrupt);
            }
        }
    }

    #[test]
    fn invalid_predecessors_do_not_append_or_fence() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("undo.log");
        let store = UndoStore::open(&path, SyncClass::KernelBarrier).expect("open");
        for previous in [0, 1, 9] {
            assert!(matches!(
                store.append(&record(1, Some(previous), b"invalid")),
                Err(UndoStoreError::InvalidPredecessor { .. })
            ));
        }
        assert_eq!(fs::metadata(&path).expect("metadata").len(), 0);
        assert_eq!(store.durable_version(), None);
        assert!(!store.is_fenced());
        assert_eq!(
            store.append(&record(1, None, b"valid")).expect("append"),
            VersionId::new(1)
        );
    }

    #[test]
    fn checksummed_self_and_forward_links_fail_closed_on_reopen() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("undo.log");
        for previous in [1, 2] {
            let frame = encode_frame(
                VersionId::new(1),
                &record(1, Some(previous), b"invalid")
                    .to_bytes()
                    .expect("payload"),
            )
            .expect("frame");
            fs::write(&path, &frame).expect("write invalid link");
            assert!(matches!(
                UndoStore::open(&path, SyncClass::KernelBarrier),
                Err(UndoStoreError::InvalidPredecessor { .. })
            ));
            assert_eq!(fs::read(&path).expect("read unchanged file"), frame);
        }
    }

    #[test]
    fn complete_checksum_corruption_fails_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("undo.log");
        {
            let store = UndoStore::open(&path, SyncClass::KernelBarrier).expect("store opens");
            let first = store.append(&record(7, None, b"stable")).expect("append");
            store.sync_through(first).expect("sync");
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("raw file opens");
        file.seek(SeekFrom::Start(FRAME_HEADER_SIZE as u64))
            .expect("seek");
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).expect("read byte");
        byte[0] ^= 0x80;
        file.seek(SeekFrom::Start(FRAME_HEADER_SIZE as u64))
            .expect("seek back");
        file.write_all(&byte).expect("corruption writes");
        file.sync_all().expect("corruption persists");
        drop(file);

        assert!(matches!(
            UndoStore::open(&path, SyncClass::KernelBarrier),
            Err(UndoStoreError::Corruption("undo frame checksum mismatch"))
        ));
    }

    #[test]
    fn zero_and_missing_versions_fail_without_changing_frontier() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("undo.log");
        let store = UndoStore::open(&path, SyncClass::KernelBarrier).expect("store opens");
        assert!(matches!(
            store.get(VersionId::new(0)),
            Err(UndoStoreError::ReservedVersion)
        ));
        assert!(matches!(
            store.sync_through(VersionId::new(1)),
            Err(UndoStoreError::MissingVersion(id)) if id == VersionId::new(1)
        ));
        assert_eq!(store.durable_version(), None);
    }

    #[test]
    fn frozen_and_tombstone_records_round_trip_through_store() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("undo.log");
        let store = UndoStore::open(&path, SyncClass::KernelBarrier).expect("store opens");
        let frozen = MvccRecord::new(
            RecordOwner::Frozen(CommitSeq::new(9)),
            None,
            MvccValue::Tombstone,
        );
        let id = store.append(&frozen).expect("append");
        assert_eq!(store.get(id).expect("read"), frozen);
    }
}
