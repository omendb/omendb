//! Segmented local-file implementation of the vNext transaction log device.
//!
//! Every retained segment starts with a mandatory 40-byte WAL container header
//! that binds the record region to a store incarnation and to the segment ID in
//! its own filename. Transactions never straddle segments: if one encoded
//! transaction would pass the configured target, append rotates before writing
//! it. `sync_through` flushes every segment that may contain unsynchronized
//! bytes up to the target LSN and also syncs the directory after segment
//! creation. Recovery validates every header before parsing its record region
//! and truncates only an incomplete suffix on the final segment; complete
//! corruption fails closed.

use super::log::MIN_RECORD_BYTES;
use super::store::ComponentLease;
use super::{
    LogDevice, LogParseStatus, LogRecord, Lsn, StoreComponent, StoreDirectory, StoreError,
    StoreIncarnation, parse_log_prefix_frames,
};
use durable_fs::{SyncClass, fsync_dir, fsync_dir_chain, sync_file_all, sync_file_data};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const SEGMENT_PREFIX: &str = "wal-";
const SEGMENT_SUFFIX: &str = ".seg";

/// Magic of the mandatory 40-byte WAL segment container header.
pub(super) const WAL_HEADER_MAGIC: [u8; 4] = *b"OMWL";
/// Container version of the mandatory WAL segment header.
pub(super) const WAL_HEADER_VERSION: u8 = 1;
/// Fixed size of the mandatory WAL segment header.
pub(super) const WAL_HEADER_BYTES: usize = 40;
const WAL_HEADER_CHECKSUM_OFFSET: usize = 36;

/// Local segmented WAL configuration.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SegmentedLogConfig {
    /// Total physical bytes in one segment, including the mandatory header. One
    /// transaction batch must fit wholly within the record region and within the
    /// packed LSN offset domain.
    pub segment_bytes: u64,
    /// Durability class used for hot append-data barriers and recovery truncation.
    pub sync_class: SyncClass,
}

impl Default for SegmentedLogConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 64 * 1024 * 1024,
            sync_class: SyncClass::DeviceBarrier,
        }
    }
}

struct SegmentState {
    segment: u64,
    /// Absolute physical file offset of the next append. Always at least the
    /// header size.
    offset: u64,
    file: File,
    dirty_segments: BTreeSet<u64>,
    directory_dirty: bool,
}

/// Baseline local segmented WAL device.
///
/// The implementation uses one mutex for physical file-position/rotation state,
/// not for transaction validation or page installation. The `LogDevice` seam
/// permits replacing this with atomic reservation plus positional writes or a
/// remote ordered log without changing transaction/recovery code.
pub struct SegmentedFileLogDevice {
    store: StoreIncarnation,
    config: SegmentedLogConfig,
    state: Mutex<SegmentState>,
    /// Declared last so the file handle closes before the component claim and
    /// the directory ownership are released.
    _lease: ComponentLease,
}

impl SegmentedFileLogDevice {
    /// Create a new WAL container bound to an owned store.
    ///
    /// The first segment's mandatory header is written and synchronized before
    /// this returns. An existing WAL directory is never adopted.
    pub fn create(store: &StoreDirectory, config: SegmentedLogConfig) -> io::Result<Self> {
        validate_config(config)?;
        let lease = store.claim(StoreComponent::Wal).map_err(store_io)?;
        let incarnation = lease.incarnation();
        let directory = lease.wal_directory();
        fs::create_dir_all(&directory).map_err(|source| {
            io::Error::new(
                source.kind(),
                format!("create WAL directory {directory:?}: {source}"),
            )
        })?;
        fsync_dir_chain(&directory)?;
        let file = create_segment(&directory, incarnation, 0, config.sync_class)?;
        Ok(Self {
            store: incarnation,
            config,
            state: Mutex::new(SegmentState {
                segment: 0,
                offset: WAL_HEADER_BYTES as u64,
                file,
                dirty_segments: BTreeSet::new(),
                directory_dirty: false,
            }),
            _lease: lease,
        })
    }

    /// Open an existing WAL container and repair `only` a torn final suffix.
    ///
    /// Every retained segment's mandatory header and complete record prefix are
    /// validated before any truncation. Missing components are never created,
    /// foreign incarnations fail closed, and a successful open does not report a
    /// durable frontier.
    pub fn open(store: &StoreDirectory, config: SegmentedLogConfig) -> io::Result<Self> {
        validate_config(config)?;
        let lease = store.claim(StoreComponent::Wal).map_err(store_io)?;
        let incarnation = lease.incarnation();
        let directory = lease.wal_directory();
        if !directory.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "WAL directory is missing",
            ));
        }
        let segments = list_segments(&directory)?;
        if segments.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no retained WAL segment",
            ));
        }
        validate_segment_sequence(&segments)?;

        let final_segment = *segments.last().expect("segment list is nonempty");
        let mut final_valid_len = 0u64;
        for &segment in &segments {
            let path = segment_path(&directory, segment);
            let bytes = read_segment(&path, config.segment_bytes)?;
            validate_segment_header(&bytes, incarnation, segment, &path)?;
            let (frames, status) = parse_log_prefix_frames(&bytes[WAL_HEADER_BYTES..]);
            match status {
                LogParseStatus::Complete => {
                    if segment == final_segment {
                        final_valid_len = bytes.len() as u64;
                    }
                }
                LogParseStatus::Incomplete if segment == final_segment => {
                    final_valid_len = WAL_HEADER_BYTES as u64
                        + frames.last().map_or(0, |frame| frame.end_offset());
                }
                LogParseStatus::Incomplete => {
                    return Err(corrupt(
                        "nonfinal WAL segment is not a complete record prefix",
                    ));
                }
                LogParseStatus::Corrupt => {
                    return Err(corrupt("WAL record region is corrupt"));
                }
            }
        }

        let path = segment_path(&directory, final_segment);
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let metadata_len = file.metadata()?.len();
        if final_valid_len > metadata_len {
            return Err(corrupt(
                "WAL repair length exceeds the final segment length",
            ));
        }
        if final_valid_len < metadata_len {
            file.set_len(final_valid_len)?;
            sync_file_all(&file, config.sync_class)?;
        }
        file.seek(SeekFrom::Start(final_valid_len))?;

        Ok(Self {
            store: incarnation,
            config,
            state: Mutex::new(SegmentState {
                segment: final_segment,
                offset: final_valid_len,
                file,
                dirty_segments: segments.into_iter().collect(),
                directory_dirty: true,
            }),
            _lease: lease,
        })
    }

    /// Parse every retained segment into records paired with exact end-LSNs.
    ///
    /// This is a recovery baseline, not the eventual streaming replay path.
    /// Every retained segment must be fully framed because `open` has already
    /// truncated any incomplete suffix from the final segment. Parsing alone
    /// does not establish a durability barrier: the recovery coordinator must
    /// synchronize the recovered prefix before publishing its durable frontier.
    pub fn recover_records(&self) -> io::Result<Vec<(Lsn, LogRecord)>> {
        let directory = self._lease.wal_directory();
        let segments = list_segments(&directory)?;
        validate_segment_sequence(&segments)?;
        let mut output = Vec::new();
        for segment in segments {
            let path = segment_path(&directory, segment);
            let bytes = read_segment(&path, self.config.segment_bytes)?;
            validate_segment_header(&bytes, self.store, segment, &path)?;
            let (frames, status) = parse_log_prefix_frames(&bytes[WAL_HEADER_BYTES..]);
            if status != LogParseStatus::Complete {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL segment is not a complete valid record prefix",
                ));
            }
            for frame in frames {
                let lsn = frame
                    .end_lsn(segment, WAL_HEADER_BYTES as u64)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "WAL record LSN overflow")
                    })?;
                output.push((lsn, frame.into_record()));
            }
        }
        Ok(output)
    }

    /// Store incarnation bound to this device.
    #[must_use]
    pub const fn store(&self) -> StoreIncarnation {
        self.store
    }

    /// Return the configured segment target.
    #[must_use]
    pub const fn segment_bytes(&self) -> u64 {
        self.config.segment_bytes
    }

    fn rotate(state: &mut SegmentState, directory: &Path, device: &Self) -> io::Result<()> {
        let next = state.segment.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::StorageFull, "WAL segment ID exhausted")
        })?;
        if next > u32::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "WAL segment ID exhausted packed LSN domain",
            ));
        }
        let file = create_segment(directory, device.store, next, device.config.sync_class)?;
        state.segment = next;
        state.offset = WAL_HEADER_BYTES as u64;
        state.file = file;
        state.directory_dirty = true;
        Ok(())
    }
}

impl LogDevice for SegmentedFileLogDevice {
    fn append(&self, bytes: &[u8]) -> io::Result<Lsn> {
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot append an empty WAL batch",
            ));
        }
        let length = u64::try_from(bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "WAL batch length overflow")
        })?;
        let capacity = self.config.segment_bytes - WAL_HEADER_BYTES as u64;
        if length > capacity || length > Lsn::MAX_OFFSET {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL transaction batch exceeds one segment record region",
            ));
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("segmented WAL state poisoned"))?;
        let end = state.offset.checked_add(length).ok_or_else(|| {
            io::Error::new(io::ErrorKind::StorageFull, "WAL segment offset overflow")
        })?;
        if end > self.config.segment_bytes || end > Lsn::MAX_OFFSET {
            let directory = self._lease.wal_directory();
            Self::rotate(&mut state, &directory, self)?;
        }

        let start = state.offset;
        state.file.seek(SeekFrom::Start(start))?;
        state.file.write_all(bytes)?;
        let end = start + length;
        state.offset = end;
        let segment = state.segment;
        state.dirty_segments.insert(segment);
        Lsn::from_wal_position(segment, end)
            .ok_or_else(|| io::Error::new(io::ErrorKind::StorageFull, "WAL LSN domain exhausted"))
    }

    fn sync_through(&self, lsn: Lsn) -> io::Result<()> {
        let directory = self._lease.wal_directory();
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("segmented WAL state poisoned"))?;
        if lsn.segment() > state.segment
            || (lsn.segment() == state.segment && lsn.offset() > state.offset)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "requested WAL sync frontier was never appended",
            ));
        }

        let targets: Vec<u64> = state
            .dirty_segments
            .range(..=lsn.segment())
            .copied()
            .collect();
        for segment in &targets {
            if *segment == state.segment {
                sync_file_data(&state.file, self.config.sync_class)?;
            } else {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(segment_path(&directory, *segment))?;
                sync_file_data(&file, self.config.sync_class)?;
            }
        }
        for segment in targets {
            state.dirty_segments.remove(&segment);
        }

        if state.directory_dirty {
            fsync_dir(&directory)?;
            state.directory_dirty = false;
        }
        Ok(())
    }
}

/// Encode a mandatory WAL segment header.
pub(super) fn encode_wal_header(
    incarnation: StoreIncarnation,
    segment: u64,
) -> [u8; WAL_HEADER_BYTES] {
    let mut bytes = [0u8; WAL_HEADER_BYTES];
    bytes[..4].copy_from_slice(&WAL_HEADER_MAGIC);
    bytes[4] = WAL_HEADER_VERSION;
    bytes[6..8].copy_from_slice(&(WAL_HEADER_BYTES as u16).to_le_bytes());
    bytes[8..24].copy_from_slice(incarnation.as_bytes());
    bytes[24..32].copy_from_slice(&segment.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes[..WAL_HEADER_CHECKSUM_OFFSET]);
    bytes[WAL_HEADER_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

/// Decode a mandatory WAL segment header in fail-closed validation order:
/// framing, then CRC, then nonzero incarnation, then expected incarnation, then
/// the filename segment ID.
pub(super) fn decode_wal_header(
    header: &[u8],
    expected_store: StoreIncarnation,
    expected_segment: u64,
    path: &Path,
) -> io::Result<()> {
    let corruption = |reason: &'static str| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("WAL segment {path:?}: {reason}"),
        )
    };
    if header.len() != WAL_HEADER_BYTES {
        return Err(corruption("header must be exactly 40 bytes"));
    }
    if header[..4] != WAL_HEADER_MAGIC {
        return Err(corruption("invalid header magic"));
    }
    if header[4] != WAL_HEADER_VERSION {
        return Err(corruption("unsupported container version"));
    }
    if header[5] != 0 {
        return Err(corruption("unsupported header flags"));
    }
    if u16::from_le_bytes([header[6], header[7]]) != WAL_HEADER_BYTES as u16 {
        return Err(corruption("invalid header size field"));
    }
    if header[32..WAL_HEADER_CHECKSUM_OFFSET]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(corruption("nonzero header reserved field"));
    }
    let stored = u32::from_le_bytes(
        header[WAL_HEADER_CHECKSUM_OFFSET..]
            .try_into()
            .map_err(|_| corruption("invalid header checksum field"))?,
    );
    if crc32c::crc32c(&header[..WAL_HEADER_CHECKSUM_OFFSET]) != stored {
        return Err(corruption("header checksum mismatch"));
    }
    let raw: [u8; 16] = header[8..24]
        .try_into()
        .map_err(|_| corruption("invalid header incarnation field"))?;
    let actual = StoreIncarnation::from_bytes(raw)
        .ok_or_else(|| corruption("header incarnation is all zeroes"))?;
    if actual != expected_store {
        return Err(corruption("header incarnation does not match the store"));
    }
    let segment = u64::from_le_bytes(
        header[24..32]
            .try_into()
            .map_err(|_| corruption("invalid header segment field"))?,
    );
    if segment > u32::MAX as u64 {
        return Err(corruption(
            "header segment ID does not fit the packed LSN domain",
        ));
    }
    if segment != expected_segment {
        return Err(corruption("header segment ID disagrees with its filename"));
    }
    Ok(())
}

fn validate_config(config: SegmentedLogConfig) -> io::Result<()> {
    let minimum = WAL_HEADER_BYTES as u64 + MIN_RECORD_BYTES as u64;
    if config.segment_bytes < minimum || config.segment_bytes > Lsn::MAX_OFFSET {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "WAL segment target must hold the header and one record within the packed LSN offset domain",
        ));
    }
    Ok(())
}

fn validate_segment_header(
    bytes: &[u8],
    expected_store: StoreIncarnation,
    expected_segment: u64,
    path: &Path,
) -> io::Result<()> {
    if bytes.len() < WAL_HEADER_BYTES {
        return Err(corrupt("WAL segment is shorter than its mandatory header"));
    }
    decode_wal_header(
        &bytes[..WAL_HEADER_BYTES],
        expected_store,
        expected_segment,
        path,
    )
}

fn read_segment(path: &Path, segment_bytes: u64) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    if length > segment_bytes || length > Lsn::MAX_OFFSET {
        return Err(corrupt("WAL segment exceeds configured or LSN size bound"));
    }
    let capacity =
        usize::try_from(length).map_err(|_| corrupt("WAL segment does not fit address space"))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn create_segment(
    directory: &Path,
    incarnation: StoreIncarnation,
    segment: u64,
    sync_class: SyncClass,
) -> io::Result<File> {
    let path = segment_path(directory, segment);
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let header = encode_wal_header(incarnation, segment);
    file.write_all(&header)?;
    sync_file_all(&file, sync_class)?;
    fsync_dir(directory)?;
    file.seek(SeekFrom::Start(WAL_HEADER_BYTES as u64))?;
    Ok(file)
}

fn segment_path(directory: &Path, segment: u64) -> PathBuf {
    directory.join(format!("{SEGMENT_PREFIX}{segment:08x}{SEGMENT_SUFFIX}"))
}

fn list_segments(directory: &Path) -> io::Result<Vec<u64>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(SEGMENT_PREFIX) || !name.ends_with(SEGMENT_SUFFIX) {
            continue;
        }
        if !entry.file_type()?.is_file() {
            return Err(corrupt("WAL segment path is not a regular file"));
        }
        let encoded = &name[SEGMENT_PREFIX.len()..name.len() - SEGMENT_SUFFIX.len()];
        if encoded.len() != 8 {
            return Err(corrupt("malformed WAL segment filename"));
        }
        let segment =
            u64::from_str_radix(encoded, 16).map_err(|_| corrupt("malformed WAL segment ID"))?;
        segments.push(segment);
    }
    segments.sort_unstable();
    Ok(segments)
}

fn validate_segment_sequence(segments: &[u64]) -> io::Result<()> {
    for pair in segments.windows(2) {
        if pair[0] == pair[1] {
            return Err(corrupt("duplicate WAL segment filename"));
        }
        if pair[1] != pair[0] + 1 {
            return Err(corrupt("retained WAL segment sequence contains a gap"));
        }
    }
    Ok(())
}

fn corrupt(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

/// Map store-ownership failures onto the WAL device's `io::Result` surface.
fn store_io(error: StoreError) -> io::Error {
    let kind = match &error {
        StoreError::Busy | StoreError::ComponentBusy(_) => io::ErrorKind::WouldBlock,
        StoreError::MissingLock | StoreError::MissingIdentity => io::ErrorKind::NotFound,
        StoreError::AlreadyExists | StoreError::NotEmpty => io::ErrorKind::AlreadyExists,
        StoreError::UnsupportedPlatform => io::ErrorKind::Unsupported,
        StoreError::Corruption { .. }
        | StoreError::ForeignIncarnation { .. }
        | StoreError::InvalidPath(_) => io::ErrorKind::InvalidData,
        StoreError::InvalidRandomIncarnation | StoreError::Random(_) => io::ErrorKind::Other,
        StoreError::Io { source, .. } => source.kind(),
    };
    io::Error::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::ids::test_incarnation;
    use crate::vnext::{
        CommitSeq, DurableLog, LogRecord, ObjectAuthority, PreparedLogBatch, RecoveryAssembler,
        StorageObjectDescriptor, StorageObjectId, StoreDirectory, Transaction, TxnId,
    };
    use std::sync::Arc;

    fn object() -> StorageObjectDescriptor {
        StorageObjectDescriptor::new(StorageObjectId::new(1), ObjectAuthority::Authoritative)
    }

    fn batch(txn_id: u64, csn: u64, key: &[u8]) -> PreparedLogBatch {
        let mut txn = Transaction::new(TxnId::new(txn_id), CommitSeq::new(csn - 1));
        txn.stage_ordered_put(object(), key.to_vec(), vec![b'x'; 32])
            .expect("mutation stages");
        txn.begin_validation().expect("validation begins");
        txn.mark_prepared(CommitSeq::new(csn))
            .expect("transaction prepares");
        PreparedLogBatch::from_transaction(&txn).expect("batch encodes")
    }

    fn config(segment_bytes: u64) -> SegmentedLogConfig {
        SegmentedLogConfig {
            segment_bytes,
            sync_class: SyncClass::KernelBarrier,
        }
    }

    fn wal_dir(directory: &Path) -> PathBuf {
        directory.join("wal")
    }

    fn encode_ok(incarnation: StoreIncarnation, segment: u64) -> [u8; WAL_HEADER_BYTES] {
        encode_wal_header(incarnation, segment)
    }

    fn expect_decode_failure(header: &[u8], store: StoreIncarnation, segment: u64) {
        assert!(
            decode_wal_header(header, store, segment, Path::new("wal-00000000.seg")).is_err(),
            "header must fail validation"
        );
    }

    #[test]
    fn wal_header_golden_layout_and_crc_coverage() {
        let incarnation = test_incarnation(7);
        let segment = 0x0102_0304u64;
        let header = encode_ok(incarnation, segment);
        assert_eq!(&header[..4], b"OMWL");
        assert_eq!(header[4], WAL_HEADER_VERSION);
        assert_eq!(header[5], 0);
        assert_eq!(u16::from_le_bytes([header[6], header[7]]), 40);
        assert_eq!(&header[8..24], incarnation.as_bytes());
        assert_eq!(
            u64::from_le_bytes(header[24..32].try_into().expect("segment")),
            segment
        );
        assert_eq!(&header[32..36], &[0, 0, 0, 0]);
        assert_eq!(
            u32::from_le_bytes(header[36..40].try_into().expect("checksum")),
            crc32c::crc32c(&header[..36])
        );
        decode_wal_header(&header, incarnation, segment, Path::new("wal-01020304.seg"))
            .expect("golden header validates");
    }

    #[test]
    fn wal_header_field_mutations_fail_closed() {
        let incarnation = test_incarnation(3);
        let segment = 2u64;
        let header = encode_ok(incarnation, segment);
        let foreign = test_incarnation(4);

        let mutate = |offset: usize, value: u8| {
            let mut bytes = header;
            bytes[offset] = value;
            bytes
        };

        // Framing fields fail before the CRC is recomputed.
        expect_decode_failure(&mutate(0, b'X'), incarnation, segment);
        expect_decode_failure(&mutate(4, 2), incarnation, segment);
        expect_decode_failure(&mutate(5, 1), incarnation, segment);
        let mut wrong_size = header;
        wrong_size[6] = 41;
        expect_decode_failure(&wrong_size, incarnation, segment);
        expect_decode_failure(&mutate(32, 1), incarnation, segment);

        // Zero and foreign incarnations fail after a valid CRC.
        let zero = encode_ok(
            StoreIncarnation::from_bytes([1; 16]).expect("nonzero"),
            segment,
        );
        let mut zero_incarnation = zero;
        zero_incarnation[8..24].fill(0);
        let crc = crc32c::crc32c(&zero_incarnation[..36]);
        zero_incarnation[36..40].copy_from_slice(&crc.to_le_bytes());
        expect_decode_failure(&zero_incarnation, incarnation, segment);

        expect_decode_failure(&header, foreign, segment);

        // A CRC mismatch fails even though every framing field is intact.
        expect_decode_failure(&mutate(36, header[36] ^ 0x01), incarnation, segment);

        // The filename segment ID must agree with the header.
        expect_decode_failure(&header, incarnation, segment + 1);
    }

    #[test]
    fn wal_header_truncations_and_extra_bytes_fail() {
        let incarnation = test_incarnation(3);
        let header = encode_ok(incarnation, 0);
        for cut in 0..WAL_HEADER_BYTES {
            expect_decode_failure(&header[..cut], incarnation, 0);
        }
        let mut extended = header.to_vec();
        extended.push(0);
        expect_decode_failure(&extended, incarnation, 0);
    }

    #[test]
    fn rotates_whole_transactions_and_recovers_exact_commit_lsns() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(200);
        let device = Arc::new(SegmentedFileLogDevice::create(&store, config).expect("device"));
        let log = DurableLog::new(device.clone());
        let first = batch(1, 1, b"alpha");
        let second = batch(2, 2, b"beta");
        let first_ticket = log.append(&first).expect("first appends");
        let second_ticket = log.append(&second).expect("second appends");
        assert!(second_ticket.decision_lsn().segment() >= first_ticket.decision_lsn().segment());
        assert!(
            second_ticket.decision_lsn().segment() > 0,
            "test must rotate"
        );
        assert!(
            first_ticket.decision_lsn().offset() > WAL_HEADER_BYTES as u64,
            "returned LSN includes the mandatory header preamble"
        );
        log.sync_through(second_ticket.decision_lsn())
            .expect("group durability succeeds");
        assert_eq!(device.store(), store.incarnation());
        drop(log);
        drop(device);

        let reopened = SegmentedFileLogDevice::open(&store, config).expect("reopens");
        let records = reopened.recover_records().expect("records recover");
        let mut assembler = RecoveryAssembler::new();
        let mut committed = Vec::new();
        for (lsn, record) in records {
            if let Some(txn) = assembler.push(lsn, record).expect("recovery validates") {
                committed.push(txn);
            }
        }
        assert_eq!(committed.len(), 2);
        assert_eq!(committed[0].position().lsn, first_ticket.decision_lsn());
        assert_eq!(committed[1].position().lsn, second_ticket.decision_lsn());
        assert!(
            reopened
                .recover_records()
                .expect("records recover")
                .iter()
                .all(|(lsn, _)| lsn.offset() > WAL_HEADER_BYTES as u64),
            "recovered LSNs include the mandatory header preamble"
        );
    }

    #[test]
    fn recovered_segments_require_new_durability_barriers() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(64);
        let device = SegmentedFileLogDevice::create(&store, config).expect("create");
        let first = LogRecord::Abort(TxnId::new(1)).to_bytes().expect("encode");
        let second = LogRecord::Abort(TxnId::new(2)).to_bytes().expect("encode");
        let first_lsn = device.append(&first).expect("append first");
        let second_lsn = device.append(&second).expect("append second");
        assert_eq!(first_lsn.segment(), 0);
        assert_eq!(first_lsn.offset(), 64);
        assert_eq!(second_lsn.segment(), 1);
        assert_eq!(second_lsn.offset(), 64);
        // Simulate process restart without making either append durable first.
        drop(device);

        let reopened = SegmentedFileLogDevice::open(&store, config).expect("reopen");
        {
            let state = reopened.state.lock().expect("state");
            assert_eq!(state.dirty_segments, BTreeSet::from([0, 1]));
            assert!(state.directory_dirty);
        }
        reopened.sync_through(first_lsn).expect("first barrier");
        {
            let state = reopened.state.lock().expect("state");
            assert_eq!(state.dirty_segments, BTreeSet::from([1]));
            assert!(!state.directory_dirty);
        }
        reopened.sync_through(second_lsn).expect("second barrier");
        assert!(
            reopened
                .state
                .lock()
                .expect("state")
                .dirty_segments
                .is_empty()
        );
        assert_eq!(reopened.recover_records().expect("recover").len(), 2);
    }

    #[test]
    fn valid_empty_wal_container_reopens() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(4096);
        {
            let device = SegmentedFileLogDevice::create(&store, config).expect("create");
            assert_eq!(device.store(), store.incarnation());
            assert!(device.recover_records().expect("empty").is_empty());
        }
        let reopened = SegmentedFileLogDevice::open(&store, config).expect("reopen");
        assert!(reopened.recover_records().expect("empty").is_empty());
        assert_eq!(
            fs::metadata(segment_path(&wal_dir(directory.path()), 0))
                .expect("metadata")
                .len(),
            WAL_HEADER_BYTES as u64
        );
    }

    #[test]
    fn open_never_creates_a_missing_wal_component() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(4096);
        assert!(matches!(
            SegmentedFileLogDevice::open(&store, config),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        ));
        assert!(!wal_dir(directory.path()).exists());
    }

    #[test]
    fn reopen_truncates_only_incomplete_final_record() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(4096);
        let device = SegmentedFileLogDevice::create(&store, config).expect("device");
        let first = LogRecord::Abort(TxnId::new(1))
            .to_bytes()
            .expect("first encodes");
        let second = LogRecord::Abort(TxnId::new(2))
            .to_bytes()
            .expect("second encodes");
        let first_lsn = device.append(&first).expect("first appends");
        device.sync_through(first_lsn).expect("first syncs");
        let active = segment_path(&wal_dir(directory.path()), first_lsn.segment());
        drop(device);

        let mut file = OpenOptions::new()
            .append(true)
            .open(&active)
            .expect("active segment opens");
        file.write_all(&second[..second.len() - 2])
            .expect("torn suffix writes");
        file.sync_all().expect("test suffix reaches disk");
        drop(file);

        let reopened = SegmentedFileLogDevice::open(&store, config).expect("reopens");
        assert_eq!(
            fs::metadata(&active).expect("metadata").len(),
            WAL_HEADER_BYTES as u64 + first.len() as u64
        );
        assert_eq!(
            reopened.recover_records().expect("records recover"),
            vec![(first_lsn, LogRecord::Abort(TxnId::new(1)))]
        );
    }

    #[test]
    fn corrupt_record_region_is_not_truncated_on_reopen() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(4096);
        let device = SegmentedFileLogDevice::create(&store, config).expect("create");
        let record = LogRecord::Abort(TxnId::new(7)).to_bytes().expect("encode");
        let lsn = device.append(&record).expect("append");
        device.sync_through(lsn).expect("sync");
        drop(device);

        let active = segment_path(&wal_dir(directory.path()), 0);
        let mut corrupt = fs::read(&active).expect("read");
        corrupt[WAL_HEADER_BYTES] ^= 0x80;
        fs::write(&active, &corrupt).expect("write corruption");
        assert!(matches!(
            SegmentedFileLogDevice::open(&store, config),
            Err(error) if error.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(fs::read(&active).expect("read unchanged file"), corrupt);
    }

    #[test]
    fn header_corruption_fails_without_truncation() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(4096);
        let device = SegmentedFileLogDevice::create(&store, config).expect("create");
        let record = LogRecord::Abort(TxnId::new(7)).to_bytes().expect("encode");
        let lsn = device.append(&record).expect("append");
        device.sync_through(lsn).expect("sync");
        drop(device);

        let active = segment_path(&wal_dir(directory.path()), 0);
        let mut corrupt = fs::read(&active).expect("read");
        corrupt[3] ^= 0x01;
        fs::write(&active, &corrupt).expect("write corruption");
        assert!(matches!(
            SegmentedFileLogDevice::open(&store, config),
            Err(error) if error.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(fs::read(&active).expect("read unchanged file"), corrupt);
    }

    #[test]
    fn header_truncations_fail_without_truncating_the_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(4096);
        let device = SegmentedFileLogDevice::create(&store, config).expect("create");
        let record = LogRecord::Abort(TxnId::new(1)).to_bytes().expect("encode");
        let lsn = device.append(&record).expect("append");
        device.sync_through(lsn).expect("sync");
        drop(device);

        let active = segment_path(&wal_dir(directory.path()), 0);
        let original = fs::read(&active).expect("read");
        for cut in 0..WAL_HEADER_BYTES {
            fs::write(&active, &original[..cut]).expect("truncate");
            assert!(
                SegmentedFileLogDevice::open(&store, config).is_err(),
                "truncation at {cut} must fail"
            );
            assert_eq!(
                fs::read(&active).expect("read unchanged file"),
                original[..cut],
                "truncation at {cut} must not be repaired"
            );
        }
    }

    #[test]
    fn incomplete_nonfinal_segment_fails_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(64);
        let device = SegmentedFileLogDevice::create(&store, config).expect("create");
        let first = LogRecord::Abort(TxnId::new(1)).to_bytes().expect("encode");
        let second = LogRecord::Abort(TxnId::new(2)).to_bytes().expect("encode");
        device.append(&first).expect("first");
        device.append(&second).expect("second rotates");
        drop(device);

        let first_path = segment_path(&wal_dir(directory.path()), 0);
        let mut bytes = fs::read(&first_path).expect("read");
        bytes.truncate(bytes.len() - 2);
        fs::write(&first_path, &bytes).expect("truncate nonfinal");
        assert!(matches!(
            SegmentedFileLogDevice::open(&store, config),
            Err(error) if error.kind() == io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn complete_corruption_and_segment_gaps_fail_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(4096);
        let device = SegmentedFileLogDevice::create(&store, config).expect("create");
        let record = LogRecord::Abort(TxnId::new(7)).to_bytes().expect("encode");
        let lsn = device.append(&record).expect("append");
        device.sync_through(lsn).expect("sync");
        let active = segment_path(&wal_dir(directory.path()), 0);
        drop(device);

        let mut corrupt = fs::read(&active).expect("segment reads");
        corrupt[WAL_HEADER_BYTES + 8] ^= 0x40;
        fs::write(&active, &corrupt).expect("corrupt segment writes");
        assert!(matches!(
            SegmentedFileLogDevice::open(&store, config),
            Err(error) if error.kind() == io::ErrorKind::InvalidData
        ));

        let gap_directory = tempfile::tempdir().expect("gap tempdir");
        let gap_store = StoreDirectory::create(gap_directory.path()).expect("gap store");
        let gap_wal = wal_dir(gap_directory.path());
        fs::create_dir_all(&gap_wal).expect("wal dir");
        File::create(segment_path(&gap_wal, 3)).expect("segment three");
        File::create(segment_path(&gap_wal, 5)).expect("segment five");
        assert!(matches!(
            SegmentedFileLogDevice::open(&gap_store, config),
            Err(error) if error.kind() == io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn segment_id_disagreement_with_filename_fails_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(4096);
        let device = SegmentedFileLogDevice::create(&store, config).expect("create");
        drop(device);
        let active = segment_path(&wal_dir(directory.path()), 0);
        let mut bytes = fs::read(&active).expect("read");
        bytes[24..32].copy_from_slice(&1u64.to_le_bytes());
        let crc = crc32c::crc32c(&bytes[..36]);
        bytes[36..40].copy_from_slice(&crc.to_le_bytes());
        fs::write(&active, &bytes).expect("write");
        assert!(matches!(
            SegmentedFileLogDevice::open(&store, config),
            Err(error) if error.kind() == io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn foreign_store_incarnation_fails_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(4096);
        {
            let device = SegmentedFileLogDevice::create(&store, config).expect("create");
            drop(device);
        }
        let foreign_dir = tempfile::tempdir().expect("foreign tempdir");
        let foreign_store = StoreDirectory::create(foreign_dir.path()).expect("foreign store");
        // Move the WAL directory under a different store incarnation.
        fs::rename(wal_dir(directory.path()), wal_dir(foreign_dir.path())).expect("move wal");
        assert!(matches!(
            SegmentedFileLogDevice::open(&foreign_store, config),
            Err(error) if error.kind() == io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn store_binding_accessors_and_component_claim_are_enforced() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(4096);
        let device = SegmentedFileLogDevice::create(&store, config).expect("create");
        assert_eq!(device.store(), store.incarnation());
        assert_eq!(device.segment_bytes(), 4096);
        assert!(matches!(
            SegmentedFileLogDevice::create(&store, config),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn configured_segment_must_hold_header_and_one_record() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        assert!(matches!(
            SegmentedFileLogDevice::create(&store, config(55)),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        // 56 is the exact minimum: 40-byte header plus a 16-byte record.
        let device = SegmentedFileLogDevice::create(&store, config(56)).expect("minimum");
        let record = LogRecord::Abort(TxnId::new(1)).to_bytes().expect("encode");
        assert!(matches!(
            device.append(&record),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
    }

    #[test]
    fn earlier_corrupt_segment_leaves_repairable_final_tail_untouched() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = StoreDirectory::create(directory.path()).expect("store");
        let config = config(64);
        let device = SegmentedFileLogDevice::create(&store, config).expect("create");
        let first = LogRecord::Abort(TxnId::new(1)).to_bytes().expect("encode");
        let second = LogRecord::Abort(TxnId::new(2)).to_bytes().expect("encode");
        device.append(&first).expect("first");
        device.append(&second).expect("second rotates");
        drop(device);

        let wal = wal_dir(directory.path());
        let first_path = segment_path(&wal, 0);
        let final_path = segment_path(&wal, 1);
        let mut corrupt = fs::read(&first_path).expect("read first");
        corrupt[WAL_HEADER_BYTES + 11] ^= 0x01;
        fs::write(&first_path, &corrupt).expect("write corrupt");
        let mut tail = OpenOptions::new()
            .append(true)
            .open(&final_path)
            .expect("open final");
        tail.write_all(&second[..second.len() - 2])
            .expect("torn tail");
        tail.sync_all().expect("sync tail");
        drop(tail);
        let final_len_before = fs::metadata(&final_path).expect("metadata").len();

        assert!(matches!(
            SegmentedFileLogDevice::open(&store, config),
            Err(error) if error.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(
            fs::metadata(&final_path).expect("metadata").len(),
            final_len_before,
            "a repairable final tail must stay untouched while an earlier segment is corrupt"
        );
    }
}
