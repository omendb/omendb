//! Log-authoritative record framing for storage-kernel vNext.
//!
//! This codec is intentionally independent of the current generation-COW WAL
//! record enum. vNext records transaction-scoped logical mutations followed by
//! a durable commit decision. Recovery may see interleaved transaction records,
//! but a transaction becomes replayable only after its commit decision validates
//! the expected mutation count and digest.

use super::{CommitSeq, StorageObjectId, TxnId};

const LOG_FORMAT_VERSION: u16 = 1;
const RECORD_HEADER_SIZE: usize = 8; // length + version + kind + flags
const RECORD_TRAILER_SIZE: usize = 4; // crc32c
const MIN_RECORD_LENGTH: usize = 2 + 1 + 1 + RECORD_TRAILER_SIZE;
const MUTATION_FIXED_PAYLOAD: usize = 8 + 4 + 8 + 1 + 4 + 4;

/// Classification of the suffix after parsing a vNext log prefix.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LogParseStatus {
    /// Every input byte belongs to a complete valid record.
    Complete,
    /// The final record is truncated and may be a torn append.
    Incomplete,
    /// A complete record has invalid framing, checksum, version, or semantics.
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum LogEncodeError {
    #[error("vNext log record exceeds the u32 framing limit")]
    RecordTooLarge,
}

impl LogRecord {
    /// Serialize one record as length + version/kind/flags + payload + CRC32C.
    pub fn to_bytes(&self) -> Result<Vec<u8>, LogEncodeError> {
        let (kind, payload) = match self {
            Self::Mutation(mutation) => (1u8, encode_mutation_payload(mutation)?),
            Self::Commit(decision) => (2u8, encode_commit_payload(*decision)),
            Self::Abort(txn_id) => (3u8, txn_id.get().to_le_bytes().to_vec()),
        };

        let length = 2usize
            .checked_add(1)
            .and_then(|value| value.checked_add(1))
            .and_then(|value| value.checked_add(payload.len()))
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
        bytes.extend_from_slice(&payload);
        let checksum = crc32c::crc32c(&bytes[4..]);
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
#[must_use]
pub fn parse_log_prefix(bytes: &[u8]) -> (Vec<LogRecord>, LogParseStatus) {
    let mut records = Vec::new();
    let mut offset = 0usize;

    while offset < bytes.len() {
        if bytes.len() - offset < 4 {
            return (records, LogParseStatus::Incomplete);
        }
        let length = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
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
        let frame = &bytes[offset..offset + total];
        let Some(record) = decode_complete_record(frame) else {
            return (records, LogParseStatus::Corrupt);
        };
        records.push(record);
        offset += total;
    }

    (records, LogParseStatus::Complete)
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
    if frame.len() < RECORD_HEADER_SIZE + RECORD_TRAILER_SIZE {
        return None;
    }
    let length = u32::from_le_bytes(frame[0..4].try_into().ok()?) as usize;
    if length < MIN_RECORD_LENGTH || frame.len() != 4usize.checked_add(length)? {
        return None;
    }
    let payload_end = frame.len().checked_sub(RECORD_TRAILER_SIZE)?;
    let stored_checksum = u32::from_le_bytes(frame[payload_end..].try_into().ok()?);
    if stored_checksum != crc32c::crc32c(&frame[4..payload_end]) {
        return None;
    }
    let version = u16::from_le_bytes(frame[4..6].try_into().ok()?);
    if version != LOG_FORMAT_VERSION || frame[7] != 0 {
        return None;
    }
    let payload = &frame[8..payload_end];
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
    }

    #[test]
    fn checksum_unknown_kind_version_and_flags_fail_closed() {
        let record = LogRecord::Abort(TxnId::new(9))
            .to_bytes()
            .expect("record encodes");

        let mut checksum = record.clone();
        checksum[8] ^= 0x80;
        assert_eq!(parse_log_prefix(&checksum).1, LogParseStatus::Corrupt);

        let mut kind = record.clone();
        kind[6] = 0xff;
        rewrite_checksum(&mut kind);
        assert_eq!(parse_log_prefix(&kind).1, LogParseStatus::Corrupt);

        let mut version = record.clone();
        version[4..6].copy_from_slice(&2u16.to_le_bytes());
        rewrite_checksum(&mut version);
        assert_eq!(parse_log_prefix(&version).1, LogParseStatus::Corrupt);

        let mut flags = record;
        flags[7] = 1;
        rewrite_checksum(&mut flags);
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

    fn rewrite_checksum(bytes: &mut [u8]) {
        let end = bytes.len() - RECORD_TRAILER_SIZE;
        let checksum = crc32c::crc32c(&bytes[4..end]);
        bytes[end..].copy_from_slice(&checksum.to_le_bytes());
    }
}
