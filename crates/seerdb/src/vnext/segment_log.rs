//! Segmented local-file implementation of the vNext transaction log device.
//!
//! Transactions never straddle segments: if one encoded transaction would pass
//! the configured target, append rotates before writing it. `sync_through`
//! flushes every segment that may contain unsynchronized bytes up to the target
//! LSN and also syncs the directory after segment creation. Recovery truncates
//! only an incomplete suffix on the final segment; complete corruption fails
//! closed.

use super::{LogDevice, LogParseStatus, LogRecord, Lsn, parse_log_prefix_frames};
use durable_fs::{SyncClass, fsync_dir, fsync_dir_chain, sync_file_all, sync_file_data};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const SEGMENT_PREFIX: &str = "wal-";
const SEGMENT_SUFFIX: &str = ".seg";

/// Local segmented WAL configuration.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SegmentedLogConfig {
    /// Target maximum bytes stored in one segment. One transaction batch must
    /// fit wholly within this bound and within the packed LSN offset domain.
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
    directory: PathBuf,
    config: SegmentedLogConfig,
    state: Mutex<SegmentState>,
}

impl SegmentedFileLogDevice {
    /// Open or create a segmented WAL directory and repair a torn final suffix.
    pub fn open(directory: impl AsRef<Path>, config: SegmentedLogConfig) -> io::Result<Self> {
        validate_config(config)?;
        let directory = directory.as_ref().to_path_buf();
        let existed = directory.exists();
        fs::create_dir_all(&directory)?;
        if !existed {
            fsync_dir_chain(&directory)?;
        }

        let segments = list_segments(&directory)?;
        validate_segment_sequence(&segments)?;
        let (segment, file, offset, directory_dirty) = if let Some(&segment) = segments.last() {
            let path = segment_path(&directory, segment);
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            let metadata_len = file.metadata()?.len();
            if metadata_len > config.segment_bytes || metadata_len > Lsn::MAX_OFFSET {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL segment exceeds configured or LSN size bound",
                ));
            }
            let valid_len = repair_final_segment(&mut file, metadata_len, config.sync_class)?;
            file.seek(SeekFrom::Start(valid_len))?;
            (segment, file, valid_len, false)
        } else {
            let segment = 0;
            let file = create_segment(&directory, segment)?;
            (segment, file, 0, true)
        };

        Ok(Self {
            directory,
            config,
            state: Mutex::new(SegmentState {
                segment,
                offset,
                file,
                dirty_segments: BTreeSet::new(),
                directory_dirty,
            }),
        })
    }

    /// Parse every retained segment into records paired with exact end-LSNs.
    ///
    /// This is a recovery baseline, not the eventual streaming replay path.
    /// Every retained segment must be fully framed because `open` has already
    /// truncated any incomplete suffix from the final segment.
    pub fn recover_records(&self) -> io::Result<Vec<(Lsn, LogRecord)>> {
        let segments = list_segments(&self.directory)?;
        validate_segment_sequence(&segments)?;
        let mut output = Vec::new();
        for segment in segments {
            let bytes = fs::read(segment_path(&self.directory, segment))?;
            let (frames, status) = parse_log_prefix_frames(&bytes);
            if status != LogParseStatus::Complete {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL segment is not a complete valid record prefix",
                ));
            }
            for frame in frames {
                let lsn = frame.end_lsn(segment, 0).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "WAL record LSN overflow")
                })?;
                output.push((lsn, frame.into_record()));
            }
        }
        Ok(output)
    }

    /// Return the configured segment target.
    #[must_use]
    pub const fn segment_bytes(&self) -> u64 {
        self.config.segment_bytes
    }

    fn rotate(state: &mut SegmentState, directory: &Path) -> io::Result<()> {
        let next = state.segment.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::StorageFull, "WAL segment ID exhausted")
        })?;
        if next > u32::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "WAL segment ID exhausted packed LSN domain",
            ));
        }
        let file = create_segment(directory, next)?;
        state.segment = next;
        state.offset = 0;
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
        if length > self.config.segment_bytes || length > Lsn::MAX_OFFSET {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL transaction batch exceeds one segment",
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
            Self::rotate(&mut state, &self.directory)?;
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
                    .open(segment_path(&self.directory, *segment))?;
                sync_file_data(&file, self.config.sync_class)?;
            }
        }
        for segment in targets {
            state.dirty_segments.remove(&segment);
        }

        if state.directory_dirty {
            fsync_dir(&self.directory)?;
            state.directory_dirty = false;
        }
        Ok(())
    }
}

fn validate_config(config: SegmentedLogConfig) -> io::Result<()> {
    if config.segment_bytes == 0 || config.segment_bytes > Lsn::MAX_OFFSET {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "WAL segment target must fit the packed LSN offset domain",
        ));
    }
    Ok(())
}

fn create_segment(directory: &Path, segment: u64) -> io::Result<File> {
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(segment_path(directory, segment))
}

fn segment_path(directory: &Path, segment: u64) -> PathBuf {
    directory.join(format!("{SEGMENT_PREFIX}{segment:08x}{SEGMENT_SUFFIX}"))
}

fn list_segments(directory: &Path) -> io::Result<Vec<u64>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(SEGMENT_PREFIX) || !name.ends_with(SEGMENT_SUFFIX) {
            continue;
        }
        let encoded = &name[SEGMENT_PREFIX.len()..name.len() - SEGMENT_SUFFIX.len()];
        if encoded.len() != 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "malformed WAL segment filename",
            ));
        }
        let segment = u64::from_str_radix(encoded, 16)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "malformed WAL segment ID"))?;
        segments.push(segment);
    }
    segments.sort_unstable();
    Ok(segments)
}

fn validate_segment_sequence(segments: &[u64]) -> io::Result<()> {
    for pair in segments.windows(2) {
        if pair[1] != pair[0] + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "retained WAL segment sequence contains a gap",
            ));
        }
    }
    Ok(())
}

fn repair_final_segment(file: &mut File, length: u64, sync_class: SyncClass) -> io::Result<u64> {
    file.seek(SeekFrom::Start(0))?;
    let capacity = usize::try_from(length).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL segment does not fit address space",
        )
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)?;
    let (frames, status) = parse_log_prefix_frames(&bytes);
    match status {
        LogParseStatus::Complete => Ok(length),
        LogParseStatus::Incomplete => {
            let valid = frames.last().map_or(0, |frame| frame.end_offset());
            file.set_len(valid)?;
            sync_file_all(file, sync_class)?;
            Ok(valid)
        }
        LogParseStatus::Corrupt => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "complete WAL corruption in final segment",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{
        CommitSeq, DurableLog, LogRecord, ObjectAuthority, PreparedLogBatch, RecoveryAssembler,
        StorageObjectDescriptor, StorageObjectId, Transaction, TxnId,
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

    #[test]
    fn rotates_whole_transactions_and_recovers_exact_commit_lsns() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = SegmentedLogConfig {
            segment_bytes: 160,
            sync_class: SyncClass::KernelBarrier,
        };
        let device =
            Arc::new(SegmentedFileLogDevice::open(directory.path(), config).expect("device opens"));
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
        log.sync_through(second_ticket.decision_lsn())
            .expect("group durability succeeds");
        drop(log);
        drop(device);

        let reopened = SegmentedFileLogDevice::open(directory.path(), config).expect("reopens");
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
    }

    #[test]
    fn reopen_truncates_only_incomplete_final_record() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = SegmentedLogConfig {
            segment_bytes: 4096,
            sync_class: SyncClass::KernelBarrier,
        };
        let device = SegmentedFileLogDevice::open(directory.path(), config).expect("device opens");
        let first = LogRecord::Abort(TxnId::new(1))
            .to_bytes()
            .expect("first encodes");
        let second = LogRecord::Abort(TxnId::new(2))
            .to_bytes()
            .expect("second encodes");
        let first_lsn = device.append(&first).expect("first appends");
        device.sync_through(first_lsn).expect("first syncs");
        let active = segment_path(directory.path(), first_lsn.segment());
        drop(device);

        let mut file = OpenOptions::new()
            .append(true)
            .open(&active)
            .expect("active segment opens");
        file.write_all(&second[..second.len() - 2])
            .expect("torn suffix writes");
        file.sync_all().expect("test suffix reaches disk");
        drop(file);

        let reopened = SegmentedFileLogDevice::open(directory.path(), config).expect("reopens");
        assert_eq!(
            fs::metadata(&active).expect("metadata").len(),
            first.len() as u64
        );
        assert_eq!(
            reopened.recover_records().expect("records recover"),
            vec![(first_lsn, LogRecord::Abort(TxnId::new(1)))]
        );
    }

    #[test]
    fn complete_corruption_and_segment_gaps_fail_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = SegmentedLogConfig {
            segment_bytes: 4096,
            sync_class: SyncClass::KernelBarrier,
        };
        let device = SegmentedFileLogDevice::open(directory.path(), config).expect("device opens");
        let record = LogRecord::Abort(TxnId::new(7))
            .to_bytes()
            .expect("record encodes");
        let lsn = device.append(&record).expect("record appends");
        device.sync_through(lsn).expect("record syncs");
        let active = segment_path(directory.path(), 0);
        drop(device);

        let mut corrupt = fs::read(&active).expect("segment reads");
        corrupt[8] ^= 0x40;
        fs::write(&active, &corrupt).expect("corrupt segment writes");
        assert!(matches!(
            SegmentedFileLogDevice::open(directory.path(), config),
            Err(error) if error.kind() == io::ErrorKind::InvalidData
        ));

        let gap_directory = tempfile::tempdir().expect("gap tempdir");
        File::create(segment_path(gap_directory.path(), 3)).expect("segment three");
        File::create(segment_path(gap_directory.path(), 5)).expect("segment five");
        assert!(matches!(
            SegmentedFileLogDevice::open(gap_directory.path(), config),
            Err(error) if error.kind() == io::ErrorKind::InvalidData
        ));
    }
}
