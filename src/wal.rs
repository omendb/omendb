use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use crate::fault::{FaultInjector, FaultPoint};
use crate::model::{CommitId, IndexId, Key, Mutation};
use crate::{DbError, Result};

const MAGIC: [u8; 4] = *b"DBWL";
// Version 2 binds the commit identity into the frame checksum. Version 1
// checksummed only the mutation payload, which could accept a corrupted
// commit ID when the resulting sequence still looked monotonic.
const VERSION: u16 = 2;
const HEADER_BYTES: usize = 24;
const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

pub fn append(
    path: &Path,
    commit: CommitId,
    mutations: &[Mutation],
    faults: &mut dyn FaultInjector,
) -> Result<u64> {
    let payload = encode_mutations(mutations)?;
    let mut frame = Vec::with_capacity(HEADER_BYTES + payload.len());
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&VERSION.to_le_bytes());
    frame.extend_from_slice(&0_u16.to_le_bytes());
    frame.extend_from_slice(&commit.0.to_le_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&frame_checksum(commit, &payload).to_le_bytes());
    frame.extend_from_slice(&payload);

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| io_error("open WAL", source))?;
    if let Err(error) = faults.check(FaultPoint::ShortWrite) {
        return write_partial(&mut file, &frame, 7, "short-write WAL", error);
    }
    if let Err(error) = faults.check(FaultPoint::TornWrite) {
        return write_partial(&mut file, &frame, frame.len() / 2, "torn-write WAL", error);
    }
    append_complete(&mut file, &frame, faults)
}

fn append_complete(file: &mut File, frame: &[u8], faults: &mut dyn FaultInjector) -> Result<u64> {
    file.write_all(frame)
        .map_err(|source| io_error("append WAL", source))?;
    faults.check(FaultPoint::AfterWalAppend)?;
    faults.check(FaultPoint::WalSync)?;
    file.sync_data()
        .map_err(|source| io_error("sync WAL", source))?;
    faults.check(FaultPoint::AfterWalSync)?;
    Ok(frame.len() as u64)
}

fn write_partial(
    file: &mut File,
    frame: &[u8],
    length: usize,
    operation: &'static str,
    error: DbError,
) -> Result<u64> {
    file.write_all(&frame[..length.min(frame.len())])
        .map_err(|source| io_error(operation, source))?;
    Err(error)
}

pub fn replay(path: &Path, after: CommitId) -> Result<Vec<(CommitId, Vec<Mutation>)>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(io_error("open WAL for replay", source)),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| io_error("read WAL", source))?;

    let mut offset = 0;
    let mut previous = 0;
    let mut batches = Vec::new();
    while offset < bytes.len() {
        if bytes.len() - offset < HEADER_BYTES {
            break;
        }
        let header = &bytes[offset..offset + HEADER_BYTES];
        if header[..4] != MAGIC
            || u16::from_le_bytes(header[4..6].try_into().expect("version width")) != VERSION
        {
            return corrupt(offset, "invalid frame magic/version");
        }
        let commit = u64::from_le_bytes(header[8..16].try_into().expect("commit width"));
        let length = u32::from_le_bytes(header[16..20].try_into().expect("length width")) as usize;
        if length > MAX_PAYLOAD {
            return corrupt(offset, "frame exceeds maximum payload");
        }
        let end = offset
            .checked_add(HEADER_BYTES)
            .and_then(|value| value.checked_add(length))
            .ok_or_else(|| corrupt_error(offset, "frame length overflows address space"))?;
        if end > bytes.len() {
            break;
        }
        let payload = &bytes[offset + HEADER_BYTES..end];
        let expected = u32::from_le_bytes(header[20..24].try_into().expect("checksum width"));
        if frame_checksum(CommitId(commit), payload) != expected {
            return corrupt(offset, "complete frame checksum mismatch");
        }
        if previous != 0 && commit != previous + 1 {
            return corrupt(offset, "non-monotonic commit sequence");
        }
        let mutations = decode_mutations(payload, offset)?;
        if commit > after.0 {
            batches.push((CommitId(commit), mutations));
        }
        previous = commit;
        offset = end;
    }
    Ok(batches)
}

pub fn truncate(path: &Path, faults: &mut dyn FaultInjector) -> Result<()> {
    faults.check(FaultPoint::WalTruncate)?;
    let file = File::create(path).map_err(|source| io_error("truncate WAL", source))?;
    file.sync_all()
        .map_err(|source| io_error("sync truncated WAL", source))
}

fn encode_mutations(mutations: &[Mutation]) -> Result<Vec<u8>> {
    let count = u32::try_from(mutations.len())
        .map_err(|_| DbError::InvalidState("too many mutations".to_owned()))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&count.to_le_bytes());
    for mutation in mutations {
        match mutation {
            Mutation::Put { key, value } => {
                bytes.push(1);
                bytes.extend_from_slice(&key.0);
                let length =
                    u32::try_from(value.len()).map_err(|_| DbError::ValueTooLarge(value.len()))?;
                bytes.extend_from_slice(&length.to_le_bytes());
                bytes.extend_from_slice(value);
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
                put_bytes(&mut bytes, index_key)?;
                bytes.extend_from_slice(&primary.0);
            }
            Mutation::IndexDelete {
                index,
                index_key,
                primary,
            } => {
                bytes.push(5);
                bytes.extend_from_slice(&index.0.to_le_bytes());
                put_bytes(&mut bytes, index_key)?;
                bytes.extend_from_slice(&primary.0);
            }
            Mutation::BytePut { key, value } => {
                bytes.push(8);
                put_bytes(&mut bytes, key)?;
                put_bytes(&mut bytes, value)?;
            }
            Mutation::ByteDelete { key } => {
                bytes.push(9);
                put_bytes(&mut bytes, key)?;
            }
            Mutation::ByteIndexPut {
                index,
                index_key,
                primary,
            } => {
                bytes.push(10);
                bytes.extend_from_slice(&index.0.to_le_bytes());
                put_bytes(&mut bytes, index_key)?;
                put_bytes(&mut bytes, primary)?;
            }
            Mutation::ByteIndexDelete {
                index,
                index_key,
                primary,
            } => {
                bytes.push(11);
                bytes.extend_from_slice(&index.0.to_le_bytes());
                put_bytes(&mut bytes, index_key)?;
                put_bytes(&mut bytes, primary)?;
            }
            Mutation::RecordAttempt { attempt, digest } => {
                bytes.push(6);
                bytes.extend_from_slice(&attempt.0);
                bytes.extend_from_slice(digest);
            }
            Mutation::ForgetAttempt { attempt } => {
                bytes.push(7);
                bytes.extend_from_slice(&attempt.0);
            }
        }
    }
    if bytes.len() > MAX_PAYLOAD {
        return Err(DbError::InvalidState(
            "WAL payload exceeds maximum".to_owned(),
        ));
    }
    Ok(bytes)
}

fn decode_mutations(bytes: &[u8], offset: usize) -> Result<Vec<Mutation>> {
    if bytes.len() < 4 {
        return corrupt(offset, "missing mutation count");
    }
    let count = u32::from_le_bytes(bytes[..4].try_into().expect("count width"));
    let mut cursor = 4;
    let mut mutations = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let tag = *bytes
            .get(cursor)
            .ok_or_else(|| corrupt_error(offset, "missing mutation tag"))?;
        cursor += 1;
        match tag {
            1 => {
                let key = read_key(bytes, &mut cursor, offset)?;
                let length_end = cursor + 4;
                let length = u32::from_le_bytes(
                    bytes
                        .get(cursor..length_end)
                        .ok_or_else(|| corrupt_error(offset, "missing value length"))?
                        .try_into()
                        .expect("length width"),
                ) as usize;
                cursor = length_end;
                let value_end = cursor
                    .checked_add(length)
                    .ok_or_else(|| corrupt_error(offset, "value length overflow"))?;
                let value = bytes
                    .get(cursor..value_end)
                    .ok_or_else(|| corrupt_error(offset, "truncated value"))?
                    .to_vec();
                cursor = value_end;
                mutations.push(Mutation::Put { key, value });
            }
            2 => mutations.push(Mutation::Delete {
                key: read_key(bytes, &mut cursor, offset)?,
            }),
            3 => {
                let index = read_u64(bytes, &mut cursor, offset)?;
                let unique = match *bytes
                    .get(cursor)
                    .ok_or_else(|| corrupt_error(offset, "missing index uniqueness"))?
                {
                    0 => false,
                    1 => true,
                    _ => return corrupt(offset, "invalid index uniqueness"),
                };
                cursor += 1;
                mutations.push(Mutation::CreateIndex {
                    index: IndexId(index),
                    unique,
                });
            }
            4 | 5 => {
                let index = IndexId(read_u64(bytes, &mut cursor, offset)?);
                let index_key = read_bytes(bytes, &mut cursor, offset)?;
                let primary = read_key(bytes, &mut cursor, offset)?;
                let mutation = if tag == 4 {
                    Mutation::IndexPut {
                        index,
                        index_key,
                        primary,
                    }
                } else {
                    Mutation::IndexDelete {
                        index,
                        index_key,
                        primary,
                    }
                };
                mutations.push(mutation);
            }
            8 => {
                let key = read_bytes(bytes, &mut cursor, offset)?;
                let value = read_bytes(bytes, &mut cursor, offset)?;
                mutations.push(Mutation::BytePut { key, value });
            }
            9 => mutations.push(Mutation::ByteDelete {
                key: read_bytes(bytes, &mut cursor, offset)?,
            }),
            10 | 11 => {
                let index = IndexId(read_u64(bytes, &mut cursor, offset)?);
                let index_key = read_bytes(bytes, &mut cursor, offset)?;
                let primary = read_bytes(bytes, &mut cursor, offset)?;
                let mutation = if tag == 10 {
                    Mutation::ByteIndexPut {
                        index,
                        index_key,
                        primary,
                    }
                } else {
                    Mutation::ByteIndexDelete {
                        index,
                        index_key,
                        primary,
                    }
                };
                mutations.push(mutation);
            }
            6 => {
                let attempt = crate::TransactionAttemptId(read_fixed::<16>(
                    bytes,
                    &mut cursor,
                    offset,
                    "attempt ID",
                )?);
                let digest = read_fixed::<32>(bytes, &mut cursor, offset, "attempt digest")?;
                mutations.push(Mutation::RecordAttempt { attempt, digest });
            }
            7 => {
                let attempt = crate::TransactionAttemptId(read_fixed::<16>(
                    bytes,
                    &mut cursor,
                    offset,
                    "attempt ID",
                )?);
                mutations.push(Mutation::ForgetAttempt { attempt });
            }
            _ => return corrupt(offset, "unknown mutation tag"),
        }
    }
    if cursor != bytes.len() {
        return corrupt(offset, "trailing bytes in transaction envelope");
    }
    Ok(mutations)
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| DbError::ValueTooLarge(value.len()))?;
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

fn frame_checksum(commit: CommitId, payload: &[u8]) -> u32 {
    let mut bytes = Vec::with_capacity(8 + payload.len());
    bytes.extend_from_slice(&commit.0.to_le_bytes());
    bytes.extend_from_slice(payload);
    crc32c::crc32c(&bytes)
}

fn read_bytes(bytes: &[u8], cursor: &mut usize, offset: usize) -> Result<Vec<u8>> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| corrupt_error(offset, "value length overflow"))?;
    let length = u32::from_le_bytes(
        bytes
            .get(*cursor..end)
            .ok_or_else(|| corrupt_error(offset, "missing value length"))?
            .try_into()
            .expect("length width"),
    ) as usize;
    *cursor = end;
    let value_end = cursor
        .checked_add(length)
        .ok_or_else(|| corrupt_error(offset, "value length overflow"))?;
    let value = bytes
        .get(*cursor..value_end)
        .ok_or_else(|| corrupt_error(offset, "truncated value"))?
        .to_vec();
    *cursor = value_end;
    Ok(value)
}

fn read_fixed<const N: usize>(
    bytes: &[u8],
    cursor: &mut usize,
    offset: usize,
    label: &str,
) -> Result<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| corrupt_error(offset, "fixed field length overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| corrupt_error(offset, format!("missing {label}")))?
        .try_into()
        .expect("fixed field width");
    *cursor = end;
    Ok(value)
}

fn read_key(bytes: &[u8], cursor: &mut usize, offset: usize) -> Result<Key> {
    let end = cursor
        .checked_add(16)
        .ok_or_else(|| corrupt_error(offset, "key length overflow"))?;
    let key = Key(bytes
        .get(*cursor..end)
        .ok_or_else(|| corrupt_error(offset, "truncated key"))?
        .try_into()
        .expect("key width"));
    *cursor = end;
    Ok(key)
}

fn read_u64(bytes: &[u8], cursor: &mut usize, offset: usize) -> Result<u64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| corrupt_error(offset, "integer length overflow"))?;
    let value = u64::from_le_bytes(
        bytes
            .get(*cursor..end)
            .ok_or_else(|| corrupt_error(offset, "truncated integer"))?
            .try_into()
            .expect("u64 width"),
    );
    *cursor = end;
    Ok(value)
}

fn io_error(operation: &'static str, source: std::io::Error) -> DbError {
    DbError::Io { operation, source }
}

fn corrupt<T>(offset: usize, reason: &str) -> Result<T> {
    Err(corrupt_error(offset, reason))
}

fn corrupt_error(offset: usize, reason: impl Into<String>) -> DbError {
    DbError::Corruption {
        artifact: "WAL",
        reason: format!("frame at offset {offset}: {}", reason.into()),
    }
}
