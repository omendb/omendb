//! Log-authoritative record framing for storage-kernel vNext.
//!
//! This codec is intentionally independent of the current generation-COW WAL
//! record enum. vNext records transaction-scoped logical mutations followed by
//! a durable commit decision. Recovery may see interleaved transaction records,
//! but a transaction becomes replayable only after its commit decision validates
//! the expected mutation count and digest.
//!
//! The fixed header has its own checksum, including the length. Recovery must
//! validate it before using the length to classify an incomplete final append.

use super::{CommitSeq, Lsn, StorageObjectId, TxnId};

const LOG_FORMAT_VERSION: u16 = 2;
const HEADER_CHECKSUM_OFFSET: usize = 8;
const RECORD_HEADER_SIZE: usize = 12; // length + version + kind + flags + header CRC
const RECORD_TRAILER_SIZE: usize = 4; // whole-record CRC32C
const MIN_RECORD_LENGTH: usize = RECORD_HEADER_SIZE - 4 + RECORD_TRAILER_SIZE;
const MUTATION_FIXED_PAYLOAD: usize = 8 + 4 + 8 + 1 + 4 + 4;

/// Classification of the suffix after parsing a vNext log prefix.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LogParseStatus {
    /// Every input byte belongs to a complete valid record.
    Complete,
    /// The final header or a record with a validated header is truncated.
    Incomplete,
    /// A complete header or record has invalid framing, checksum, or semantics.
    Corrupt,
}

/// Logical mutation kinds currently understood by vNext recovery.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum MutationKind {
    /// Insert one ordered key/value pair.
    OrderedPut = 1,
    /// Delete one ordered key.
    OrderedDelete = 2,
}

/// One transaction-scoped authoritative logical mutation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LoggedMutation {
    txn_id: TxnId,
    ordinal: u32,
    object: StorageObjectId,
    kind: MutationKind,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl LoggedMutation {
    /// Construct one ordered put mutation.
    #[must_use]
    pub fn ordered_put(
        txn_id: TxnId,
        ordinal: u32,
        object: StorageObjectId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Self {
        Self {
            txn_id,
            ordinal,
            object,
            kind: MutationKind::OrderedPut,
            key,
            value,
        }
    }

    /// Construct one ordered delete mutation.
    #[must_use]
    pub fn ordered_delete(
        txn_id: TxnId,
        ordinal: u32,
        object: StorageObjectId,
        key: Vec<u8>,
    ) -> Self {
        Self {
            txn_id,
            ordinal,
            object,
            kind: MutationKind::OrderedDelete,
            key,
            value: Vec::new(),
        }
    }

    #[must_use]
    pub const fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    #[must_use]
    pub const fn object(&self) -> StorageObjectId {
        self.object
    }

    #[must_use]
    pub const fn kind(&self) -> MutationKind {
        self.kind
    }

    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// Durable logical transaction outcome appended after its mutation records.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CommitDecision {
    txn_id: TxnId,
    csn: CommitSeq,
    mutation_count: u32,
    mutation_digest: u32,
}

impl CommitDecision {
    #[must_use]
    pub const fn new(
        txn_id: TxnId,
        csn: CommitSeq,
        mutation_count: u32,
        mutation_digest: u32,
    ) -> Self {
        Self {
            txn_id,
            csn,
            mutation_count,
            mutation_digest,
        }
    }

    #[must_use]
    pub const fn txn_id(self) -> TxnId {
        self.txn_id
    }

    #[must_use]
    pub const fn csn(self) -> CommitSeq {
        self.csn
    }

    #[must_use]
    pub const fn mutation_count(self) -> u32 {
        self.mutation_count
    }

    #[must_use]
    pub const fn mutation_digest(self) -> u32 {
        self.mutation_digest
    }
}

/// Versioned vNext logical WAL record.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LogRecord {
    Mutation(LoggedMutation),
    Commit(CommitDecision),
    Abort(TxnId),
}

/// One decoded record together with its end offset in the parsed byte slice.
///
/// The offset is relative to the supplied prefix. Recovery can combine it with
/// a WAL segment and base offset to recover the record's exact durable LSN
/// without reparsing framing bytes.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParsedLogRecord {
    record: LogRecord,
    end_offset: u64,
}

impl ParsedLogRecord {
    #[must_use]
    pub fn record(&self) -> &LogRecord {
        &self.record
    }

    #[must_use]
    pub fn into_record(self) -> LogRecord {
        self.record
    }

    #[must_use]
    pub const fn end_offset(&self) -> u64 {
        self.end_offset
    }

    /// Resolve this relative end offset into the repository's packed LSN type.
    #[must_use]
    pub fn end_lsn(&self, segment: u64, base_offset: u64) -> Option<Lsn> {
        let offset = base_offset.checked_add(self.end_offset)?;
        Lsn::from_wal_position(segment, offset)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum LogEncodeError {
    #[error("vNext log record exceeds the u32 framing limit")]
    RecordTooLarge,
}

impl LogRecord {
    /// Serialize a checksummed fixed header, payload, and whole-record CRC32C.
    pub fn to_bytes(&self) -> Result<Vec<u8>, LogEncodeError> {
        let (kind, payload) = match self {
            Self::Mutation(mutation) => (1u8, encode_mutation_payload(mutation)?),
            Self::Commit(decision) => (2u8, encode_commit_payload(*decision)),
            Self::Abort(txn_id) => (3u8, txn_id.get().to_le_bytes().to_vec()),
        };

        let length = (RECORD_HEADER_SIZE - 4)
            .checked_add(payload.len())
            .and_then(|value| value.checked_add(RECORD_TRAILER_SIZE))
            .ok_or(LogEncodeError::RecordTooLarge)?;
        let length_u32 = u32::try_from(length).map_err(|_| LogEncodeError::RecordTooLarge)?;

        let total = 4usize
            .checked_add(length)
            .ok_or(LogEncodeError::RecordTooLarge)?;
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(&length_u32.to_le_bytes());
        bytes.extend_from_slice(&LOG_FORMAT_VERSION.to_le_bytes());
        bytes.push(kind);
        bytes.push(0); // reserved flags; unknown flags fail closed on decode.
        let header_checksum = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&header_checksum.to_le_bytes());
        bytes.extend_from_slice(&payload);
        let checksum = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }
}

/// Compute the canonical digest named by a commit decision.
///
/// The baseline deliberately hashes logical mutation payloads rather than page
/// images or physical placement. Transaction code can replace this allocation
/// with an incremental CRC implementation without changing the durable format.
pub fn mutation_digest(mutations: &[LoggedMutation]) -> Result<u32, LogEncodeError> {
    let mut canonical = Vec::new();
    for mutation in mutations {
        let payload = encode_mutation_payload(mutation)?;
        canonical.extend_from_slice(&payload);
    }
    Ok(crc32c::crc32c(&canonical))
}

/// Parse every complete record in a prefix and classify the remaining suffix.
///
/// This convenience API discards record byte positions. Raw WAL recovery should
/// prefer [`parse_log_prefix_frames`] so it can recover exact record end-LSNs.
#[must_use]
pub fn parse_log_prefix(bytes: &[u8]) -> (Vec<LogRecord>, LogParseStatus) {
    let (frames, status) = parse_log_prefix_frames(bytes);
    (
        frames
            .into_iter()
            .map(ParsedLogRecord::into_record)
            .collect(),
        status,
    )
}

/// Parse complete records while preserving each record's relative end offset.
#[must_use]
pub fn parse_log_prefix_frames(bytes: &[u8]) -> (Vec<ParsedLogRecord>, LogParseStatus) {
    let mut records = Vec::new();
    let mut offset = 0usize;

    while offset < bytes.len() {
        if bytes.len() - offset < RECORD_HEADER_SIZE {
            return (records, LogParseStatus::Incomplete);
        }
        let header = &bytes[offset..offset + RECORD_HEADER_SIZE];
        if !valid_record_header(header) {
            return (records, LogParseStatus::Corrupt);
        }
        let length = u32::from_le_bytes([
            header[0], header[1], header[2], header[3],
        ]) as usize;
        if length < MIN_RECORD_LENGTH {
            return (records, LogParseStatus::Corrupt);
        }
        let Some(total) = 4usize.checked_add(length) else {
            return (records, LogParseStatus::Corrupt);
        };
        if bytes.len() - offset < total {
            return (records, LogParseStatus::Incomplete);
        }
        let end = match offset.checked_add(total) {
            Some(end) => end,
            None => return (records, LogParseStatus::Corrupt),
        };
        let frame = &bytes[offset..end];
        let Some(record) = decode_complete_record(frame) else {
            return (records, LogParseStatus::Corrupt);
        };
        let end_offset = match u64::try_from(end) {
            Ok(end) => end,
            Err(_) => return (records, LogParseStatus::Corrupt),
        };
        records.push(ParsedLogRecord { record, end_offset });
        offset = end;
    }

    (records, LogParseStatus::Complete)
}

fn valid_record_header(header: &[u8]) -> bool {
    if header.len() != RECORD_HEADER_SIZE {
        return false;
    }
    let Some(version) = read_u32(header, 4) else {
        return false;
    };
    // Read version separately from kind/flags; all four bytes are checksummed.
    if version as u16 != LOG_FORMAT_VERSION || header[7] != 0 || !matches!(header[6], 1..=3) {
        return false;
    }
    read_u32(header, HEADER_CHECKSUM_OFFSET)
        == Some(crc32c::crc32c(&header[..HEADER_CHECKSUM_OFFSET]))
}

fn encode_mutation_payload(mutation: &LoggedMutation) -> Result<Vec<u8>, LogEncodeError> {
    let key_len = u32::try_from(mutation.key.len()).map_err(|_| LogEncodeError::RecordTooLarge)?;
    let value_len =
        u32::try_from(mutation.value.len()).map_err(|_| LogEncodeError::RecordTooLarge)?;
    if mutation.kind == MutationKind::OrderedDelete && value_len != 0 {
        return Err(LogEncodeError::RecordTooLarge);
    }

    let capacity = MUTATION_FIXED_PAYLOAD
        .checked_add(mutation.key.len())
        .and_then(|value| value.checked_add(mutation.value.len()))
        .ok_or(LogEncodeError::RecordTooLarge)?;
    let mut payload = Vec::with_capacity(capacity);
    payload.extend_from_slice(&mutation.txn_id.get().to_le_bytes());
    payload.extend_from_slice(&mutation.ordinal.to_le_bytes());
    payload.extend_from_slice(&mutation.object.get().to_le_bytes());
    payload.push(mutation.kind as u8);
    payload.extend_from_slice(&key_len.to_le_bytes());
    payload.extend_from_slice(&value_len.to_le_bytes());
    payload.extend_from_slice(&mutation.key);
    payload.extend_from_slice(&mutation.value);
    Ok(payload)
}

fn encode_commit_payload(decision: CommitDecision) -> Vec<u8> {
    let mut payload = Vec::with_capacity(24);
    payload.extend_from_slice(&decision.txn_id.get().to_le_bytes());
    payload.extend_from_slice(&decision.csn.get().to_le_bytes());
    payload.extend_from_slice(&decision.mutation_count.to_le_bytes());
    payload.extend_from_slice(&decision.mutation_digest.to_le_bytes());
    payload
}

fn decode_complete_record(frame: &[u8]) -> Option<LogRecord> {
    if frame.len() < RECORD_HEADER_SIZE + RECORD_TRAILER_SIZE
        || !valid_record_header(&frame[..RECORD_HEADER_SIZE])
    {
        return None;
    }
    let length = u32::from_le_bytes(frame[0..4].try_into().ok()?) as usize;
    if length < MIN_RECORD_LENGTH || frame.len() != 4usize.checked_add(length)? {
        return None;
    }
    let payload_end = frame.len().checked_sub(RECORD_TRAILER_SIZE)?;
    let stored_checksum = u32::from_le_bytes(frame[payload_end..].try_into().ok()?);
    if stored_checksum != crc32c::crc32c(&frame[..payload_end]) {
        return None;
    }
    let payload = &frame[RECORD_HEADER_SIZE..payload_end];
    match frame[6] {
        1 => decode_mutation_payload(payload).map(LogRecord::Mutation),
        2 => decode_commit_payload(payload).map(LogRecord::Commit),
        3 => {
            let txn = read_u64(payload, 0)?;
            (payload.len() == 8).then_some(LogRecord::Abort(TxnId::new(txn)))
        }
        _ => None,
    }
}

fn decode_mutation_payload(payload: &[u8]) -> Option<LoggedMutation> {
    if payload.len() < MUTATION_FIXED_PAYLOAD {
        return None;
    }
    let txn_id = TxnId::new(read_u64(payload, 0)?);
    let ordinal = read_u32(payload, 8)?;
    let object = StorageObjectId::new(read_u64(payload, 12)?);
    let kind = match *payload.get(20)? {
        1 => MutationKind::OrderedPut,
        2 => MutationKind::OrderedDelete,
        _ => return None,
    };
    let key_len = read_u32(payload, 21)? as usize;
    let value_len = read_u32(payload, 25)? as usize;
    if kind == MutationKind::OrderedDelete && value_len != 0 {
        return None;
    }
    let key_start = MUTATION_FIXED_PAYLOAD;
    let key_end = key_start.checked_add(key_len)?;
    let value_end = key_end.checked_add(value_len)?;
    if value_end != payload.len() {
        return None;
    }
    Some(LoggedMutation {
        txn_id,
        ordinal,
        object,
        kind,
        key: payload[key_start..key_end].to_vec(),
        value: payload[key_end..value_end].to_vec(),
    })
}

fn decode_commit_payload(payload: &[u8]) -> Option<CommitDecision> {
    if payload.len() != 24 {
        return None;
    }
    Some(CommitDecision::new(
        TxnId::new(read_u64(payload, 0)?),
        CommitSeq::new(read_u64(payload, 8)?),
        read_u32(payload, 16)?,
        read_u32(payload, 20)?,
    ))
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

    fn mutations() -> Vec<LoggedMutation> {
        vec![
            LoggedMutation::ordered_put(
                TxnId::new(7),
                0,
                StorageObjectId::new(11),
                b"alpha".to_vec(),
                b"one".to_vec(),
            ),
            LoggedMutation::ordered_delete(
                TxnId::new(7),
                1,
                StorageObjectId::new(13),
                b"beta".to_vec(),
            ),
        ]
    }

    #[test]
    fn mutation_commit_and_abort_round_trip() {
        let mutations = mutations();
        let digest = mutation_digest(&mutations).expect("digest computes");
        let records = [
            LogRecord::Mutation(mutations[0].clone()),
            LogRecord::Mutation(mutations[1].clone()),
            LogRecord::Commit(CommitDecision::new(
                TxnId::new(7),
                CommitSeq::new(19),
                2,
                digest,
            )),
            LogRecord::Abort(TxnId::new(23)),
        ];
        let mut bytes = Vec::new();
        for record in &records {
            bytes.extend_from_slice(&record.to_bytes().expect("record encodes"));
        }
        let (decoded, status) = parse_log_prefix(&bytes);
        assert_eq!(status, LogParseStatus::Complete);
        assert_eq!(decoded, records);
    }

    #[test]
    fn framed_parse_preserves_exact_record_boundaries() {
        let first = LogRecord::Abort(TxnId::new(1))
            .to_bytes()
            .expect("first encodes");
        let second = LogRecord::Abort(TxnId::new(2))
            .to_bytes()
            .expect("second encodes");
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second);

        let (frames, status) = parse_log_prefix_frames(&bytes);
        assert_eq!(status, LogParseStatus::Complete);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].end_offset(), first.len() as u64);
        assert_eq!(frames[1].end_offset(), bytes.len() as u64);
        assert_eq!(
            frames[1].end_lsn(3, 4096),
            Lsn::from_wal_position(3, 4096 + bytes.len() as u64)
        );
    }

    #[test]
    fn torn_suffix_keeps_complete_prefix() {
        let first = LogRecord::Abort(TxnId::new(1))
            .to_bytes()
            .expect("first encodes");
        let second = LogRecord::Abort(TxnId::new(2))
            .to_bytes()
            .expect("second encodes");
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second[..second.len() - 3]);
        let (decoded, status) = parse_log_prefix(&bytes);
        assert_eq!(status, LogParseStatus::Incomplete);
        assert_eq!(decoded, vec![LogRecord::Abort(TxnId::new(1))]);

        let (frames, framed_status) = parse_log_prefix_frames(&bytes);
        assert_eq!(framed_status, LogParseStatus::Incomplete);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].end_offset(), first.len() as u64);
    }

    #[test]
    fn every_truncation_preserves_the_valid_prefix() {
        let first = LogRecord::Abort(TxnId::new(1)).to_bytes().expect("encode");
        let second = LogRecord::Mutation(mutations()[0].clone())
            .to_bytes()
            .expect("encode");
        for cut in 1..second.len() {
            let mut bytes = first.clone();
            bytes.extend_from_slice(&second[..cut]);
            let (records, status) = parse_log_prefix(&bytes);
            assert_eq!(status, LogParseStatus::Incomplete, "cut {cut}");
            assert_eq!(records, vec![LogRecord::Abort(TxnId::new(1))]);
        }
    }

    #[test]
    fn corrupt_length_is_not_a_repairable_torn_tail() {
        let first = LogRecord::Abort(TxnId::new(1)).to_bytes().expect("encode");
        let second = LogRecord::Abort(TxnId::new(2)).to_bytes().expect("encode");
        for byte in 0..4 {
            for bit in 0..8 {
                let mut bytes = first.clone();
                bytes.extend_from_slice(&second);
                bytes[first.len() + byte] ^= 1 << bit;
                let (frames, status) = parse_log_prefix_frames(&bytes);
                assert_eq!(status, LogParseStatus::Corrupt, "byte {byte}, bit {bit}");
                assert_eq!(frames.len(), 1);
                assert_eq!(frames[0].end_offset(), first.len() as u64);
            }
        }
    }

    #[test]
    fn checksum_unknown_kind_version_and_flags_fail_closed() {
        let record = LogRecord::Abort(TxnId::new(9))
            .to_bytes()
            .expect("record encodes");

        let mut checksum = record.clone();
        checksum[RECORD_HEADER_SIZE] ^= 0x80;
        assert_eq!(parse_log_prefix(&checksum).1, LogParseStatus::Corrupt);

        let mut kind = record.clone();
        kind[6] = 0xff;
        rewrite_checksums(&mut kind);
        assert_eq!(parse_log_prefix(&kind).1, LogParseStatus::Corrupt);

        for unsupported in [1u16, LOG_FORMAT_VERSION + 1] {
            let mut version = record.clone();
            version[4..6].copy_from_slice(&unsupported.to_le_bytes());
            rewrite_checksums(&mut version);
            assert_eq!(parse_log_prefix(&version).1, LogParseStatus::Corrupt);
        }

        let mut flags = record;
        flags[7] = 1;
        rewrite_checksums(&mut flags);
        assert_eq!(parse_log_prefix(&flags).1, LogParseStatus::Corrupt);
    }

    #[test]
    fn mutation_digest_is_order_and_object_sensitive() {
        let mut original = mutations();
        let digest = mutation_digest(&original).expect("digest computes");
        original.swap(0, 1);
        assert_ne!(mutation_digest(&original).expect("digest computes"), digest);

        let mut different_object = mutations();
        different_object[0].object = StorageObjectId::new(99);
        assert_ne!(
            mutation_digest(&different_object).expect("digest computes"),
            digest
        );
    }

    fn rewrite_checksums(bytes: &mut [u8]) {
        let header_checksum = crc32c::crc32c(&bytes[..HEADER_CHECKSUM_OFFSET]);
        bytes[HEADER_CHECKSUM_OFFSET..RECORD_HEADER_SIZE]
            .copy_from_slice(&header_checksum.to_le_bytes());
        let end = bytes.len() - RECORD_TRAILER_SIZE;
        let checksum = crc32c::crc32c(&bytes[..end]);
        bytes[end..].copy_from_slice(&checksum.to_le_bytes());
    }
}
