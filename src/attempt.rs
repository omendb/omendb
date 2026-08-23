use sha2::{Digest, Sha256};

use crate::model::Mutation;
use crate::{CommitId, DbError, KvMutation, Result};

const RECORD_MAGIC: [u8; 4] = *b"DBAT";
const RECORD_VERSION: u16 = 1;
const RECORD_BYTES: usize = 68;
const SEER_KEY_PREFIX: &[u8] = b"\x00omendb/attempt/v1/";

/// Stable caller-owned identity for one logical commit attempt.
///
/// The identity must be reused only for the same logical mutation batch. It
/// is durable within one database history and is not a replacement for a
/// commit ID or a snapshot identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransactionAttemptId(pub [u8; 16]);

impl TransactionAttemptId {
    #[must_use]
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Durable result recorded for one transaction attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptRecord {
    pub attempt: TransactionAttemptId,
    pub commit: CommitId,
    pub digest: [u8; 32],
}

pub(crate) fn digest_mutations(mutations: &[Mutation]) -> [u8; 32] {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(mutations.len() as u64).to_le_bytes());
    for mutation in mutations {
        match mutation {
            Mutation::Put { key, value } => {
                bytes.push(1);
                bytes.extend_from_slice(&key.0);
                put_bytes(&mut bytes, value);
            }
            Mutation::Delete { key } => {
                bytes.push(2);
                bytes.extend_from_slice(&key.0);
            }
            Mutation::CreateIndex { index, unique } => {
                bytes.push(3);
                bytes.extend_from_slice(&index.0.to_le_bytes());
                bytes.push(u8::from(*unique));
            }
            Mutation::IndexPut {
                index,
                index_key,
                primary,
            } => {
                bytes.push(4);
                bytes.extend_from_slice(&index.0.to_le_bytes());
                put_bytes(&mut bytes, index_key);
                bytes.extend_from_slice(&primary.0);
            }
            Mutation::IndexDelete {
                index,
                index_key,
                primary,
            } => {
                bytes.push(5);
                bytes.extend_from_slice(&index.0.to_le_bytes());
                put_bytes(&mut bytes, index_key);
                bytes.extend_from_slice(&primary.0);
            }
            Mutation::BytePut { key, value } => {
                bytes.push(8);
                put_bytes(&mut bytes, key);
                put_bytes(&mut bytes, value);
            }
            Mutation::ByteDelete { key } => {
                bytes.push(9);
                put_bytes(&mut bytes, key);
            }
            Mutation::ByteIndexPut {
                index,
                index_key,
                primary,
            } => {
                bytes.push(10);
                bytes.extend_from_slice(&index.0.to_le_bytes());
                put_bytes(&mut bytes, index_key);
                put_bytes(&mut bytes, primary);
            }
            Mutation::ByteIndexDelete {
                index,
                index_key,
                primary,
            } => {
                bytes.push(11);
                bytes.extend_from_slice(&index.0.to_le_bytes());
                put_bytes(&mut bytes, index_key);
                put_bytes(&mut bytes, primary);
            }
            Mutation::RecordAttempt { .. } => {
                // Attempt metadata is not part of the logical digest.
            }
            Mutation::ForgetAttempt { .. } => {
                // Attempt metadata is not part of the logical digest.
            }
        }
    }
    digest(&bytes)
}

pub(crate) fn digest_kv_mutations(mutations: &[KvMutation]) -> [u8; 32] {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(mutations.len() as u64).to_le_bytes());
    for mutation in mutations {
        match mutation {
            KvMutation::Put { key, value } => {
                bytes.push(1);
                put_bytes(&mut bytes, key);
                put_bytes(&mut bytes, value);
            }
            KvMutation::Delete { key } => {
                bytes.push(2);
                put_bytes(&mut bytes, key);
            }
        }
    }
    digest(&bytes)
}

pub(crate) fn encode_record(record: AttemptRecord) -> [u8; RECORD_BYTES] {
    let mut bytes = [0; RECORD_BYTES];
    bytes[..4].copy_from_slice(&RECORD_MAGIC);
    bytes[4..6].copy_from_slice(&RECORD_VERSION.to_le_bytes());
    bytes[8..24].copy_from_slice(&record.attempt.0);
    bytes[24..32].copy_from_slice(&record.commit.0.to_le_bytes());
    bytes[32..64].copy_from_slice(&record.digest);
    let checksum = crc32c::crc32c(&bytes[..RECORD_BYTES - 4]);
    bytes[RECORD_BYTES - 4..].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

pub(crate) fn decode_record(bytes: &[u8]) -> Result<AttemptRecord> {
    if bytes.len() != RECORD_BYTES || bytes[..4] != RECORD_MAGIC {
        return Err(DbError::Corruption {
            artifact: "transaction attempt record",
            reason: "invalid record magic or length".to_owned(),
        });
    }
    if u16::from_le_bytes(bytes[4..6].try_into().expect("version width")) != RECORD_VERSION {
        return Err(DbError::Corruption {
            artifact: "transaction attempt record",
            reason: "unsupported record version".to_owned(),
        });
    }
    let expected = u32::from_le_bytes(
        bytes[RECORD_BYTES - 4..]
            .try_into()
            .expect("checksum width"),
    );
    if crc32c::crc32c(&bytes[..RECORD_BYTES - 4]) != expected {
        return Err(DbError::Corruption {
            artifact: "transaction attempt record",
            reason: "record checksum mismatch".to_owned(),
        });
    }
    let mut attempt = [0; 16];
    attempt.copy_from_slice(&bytes[8..24]);
    let mut digest = [0; 32];
    digest.copy_from_slice(&bytes[32..64]);
    Ok(AttemptRecord {
        attempt: TransactionAttemptId(attempt),
        commit: CommitId(u64::from_le_bytes(
            bytes[24..32].try_into().expect("commit width"),
        )),
        digest,
    })
}

pub(crate) fn seer_key(attempt: TransactionAttemptId) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEER_KEY_PREFIX.len() + 16);
    key.extend_from_slice(SEER_KEY_PREFIX);
    key.extend_from_slice(&attempt.0);
    key
}

pub(crate) fn seer_key_range() -> (Vec<u8>, Vec<u8>) {
    let start = SEER_KEY_PREFIX.to_vec();
    let mut end = start.clone();
    for position in (0..end.len()).rev() {
        if end[position] != u8::MAX {
            end[position] += 1;
            end.truncate(position + 1);
            return (start, end);
        }
    }
    (start, vec![u8::MAX])
}

pub(crate) fn decode_seer_key(key: &[u8]) -> Result<TransactionAttemptId> {
    if key.len() != SEER_KEY_PREFIX.len() + 16 || !key.starts_with(SEER_KEY_PREFIX) {
        return Err(DbError::Corruption {
            artifact: "transaction attempt record",
            reason: "attempt key has an invalid namespace or length".to_owned(),
        });
    }
    let mut attempt = [0; 16];
    attempt.copy_from_slice(&key[SEER_KEY_PREFIX.len()..]);
    Ok(TransactionAttemptId(attempt))
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
    bytes.extend_from_slice(value);
}
