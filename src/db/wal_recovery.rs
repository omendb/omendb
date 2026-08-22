//! WAL replay policy for committed prefixes.

use super::mutation::{Mutation, apply as apply_mutation};
use super::{BTree, BlobManager, Error, RecoverySummary, Result};
use crate::btree::MAX_KEY_SIZE;
use crate::recovery::{ParseStatus, RecordType, WalManager, WalRecord};
use crate::storage::format::Manifest;
use std::path::Path;

/// Whether the WAL holds any committed generation ahead of the authority,
/// i.e. content recovery would replay. Retained records at or below the
/// published generation are inert, so their presence alone must not force
/// eager materialization on open.
pub(super) fn wal_has_unpublished_commits(
    wal_path: &Path,
    current_manifest: Option<Manifest>,
) -> Result<bool> {
    let bytes = std::fs::read(wal_path)?;
    let (records, status) = WalManager::parse_records_with_status(&bytes);
    if status == ParseStatus::Corrupt {
        return Err(Error::Corruption("invalid complete WAL record".into()));
    }
    let current_generation = current_manifest
        .map(|manifest| manifest.generation_id.get())
        .unwrap_or(0);
    let current_commit = current_manifest
        .map(|manifest| manifest.commit_id.get())
        .unwrap_or(0);
    for record in &records {
        if let RecordType::Commit = record.record_type {
            let commit = record
                .commit_record()
                .ok_or_else(|| Error::Corruption("invalid WAL commit envelope".into()))?;
            if commit.generation_id.get() > current_generation
                && commit.commit_id.get() > current_commit
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Recover a committed WAL prefix and reject corrupt complete records.
pub(super) fn recover_from_wal(
    wal_path: &Path,
    current_manifest: Option<Manifest>,
    btree: &mut BTree,
    blobs: &mut BlobManager,
) -> Result<RecoverySummary> {
    let wal_data = std::fs::read(wal_path)?;
    let (records, status) = WalManager::parse_records_with_status(&wal_data);
    if status == ParseStatus::Corrupt {
        return Err(Error::Corruption("invalid complete WAL record".into()));
    }

    let mut pending = Vec::new();
    let mut last_commit = None;
    let mut last_commit_offset = 0;
    let mut blob_changed = false;
    let mut offset = 0u64;
    for record in &records {
        let record_len = record.to_bytes().len() as u64;
        match record.record_type {
            RecordType::Put | RecordType::Delete | RecordType::PutV2 | RecordType::DeleteV2 => {
                pending.push(record)
            }
            RecordType::Commit => {
                let commit = record
                    .commit_record()
                    .ok_or_else(|| Error::Corruption("invalid WAL commit envelope".into()))?;
                if commit.mutation_count != pending.len() as u64
                    || commit.digest != digest_records(&pending)
                {
                    return Err(Error::Corruption(
                        "WAL commit does not match its mutation prefix".into(),
                    ));
                }

                if let Some(current) = current_manifest {
                    match commit.generation_id.get().cmp(&current.generation_id.get()) {
                        std::cmp::Ordering::Less => {
                            if commit.commit_id.get() > current.commit_id.get() {
                                return Err(Error::Corruption(
                                    "WAL commit frontier is inconsistent with manifest".into(),
                                ));
                            }
                            pending.clear();
                            offset += record_len;
                            continue;
                        }
                        std::cmp::Ordering::Equal => {
                            if commit.commit_id != current.commit_id {
                                return Err(Error::Corruption(
                                    "WAL commit frontier is inconsistent with manifest".into(),
                                ));
                            }
                            if commit.root_page_id != current.root_page_id
                                || commit.mutation_count != current.mutation_count
                                || commit.digest != current.digest
                            {
                                return Err(Error::Corruption(
                                    "WAL commit disagrees with authoritative manifest".into(),
                                ));
                            }
                            pending.clear();
                            offset += record_len;
                            continue;
                        }
                        std::cmp::Ordering::Greater => {
                            if commit.commit_id <= current.commit_id {
                                return Err(Error::Corruption(
                                    "WAL commit frontier is inconsistent with manifest".into(),
                                ));
                            }
                        }
                    }
                }

                for mutation in pending.drain(..) {
                    let applied = match mutation.record_type {
                        RecordType::Put => {
                            let (key, value) = decode_put_payload(false, &mutation.payload)?;
                            apply_mutation(Mutation::Put { key, value }, btree, blobs)?
                        }
                        RecordType::PutV2 => {
                            let (key, value) = decode_put_payload(true, &mutation.payload)?;
                            apply_mutation(Mutation::Put { key, value }, btree, blobs)?
                        }
                        RecordType::Delete => {
                            let key = decode_delete_payload(false, &mutation.payload)?;
                            apply_mutation(Mutation::Delete { key }, btree, blobs)?
                        }
                        RecordType::DeleteV2 => {
                            let key = decode_delete_payload(true, &mutation.payload)?;
                            apply_mutation(Mutation::Delete { key }, btree, blobs)?
                        }
                        RecordType::Commit => {
                            return Err(Error::Corruption(
                                "commit record appeared in WAL mutation prefix".into(),
                            ));
                        }
                        _ => {
                            return Err(Error::Corruption(
                                "non-mutation passed to WAL applier".into(),
                            ));
                        }
                    };
                    blob_changed |= applied.blob_changed;
                }
                last_commit = Some(commit);
                last_commit_offset = offset;
            }
            _ => {}
        }
        offset += record_len;
    }

    Ok(RecoverySummary {
        last_commit,
        last_commit_offset,
        blob_changed,
    })
}

/// Remove any torn or zeroed tail so future appends land on a record
/// boundary. Retention keeps the WAL across publications; without this,
/// post-crash appends would follow an unparseable partial record.
pub(super) fn truncate_wal_tail(wal_path: &Path) -> Result<()> {
    let bytes = std::fs::read(wal_path)?;
    let (_, status) = WalManager::parse_records_with_status(&bytes);
    if status != ParseStatus::Incomplete {
        return Ok(());
    }
    let (records, _) = WalManager::parse_records_with_status(&bytes);
    let mut end = 0u64;
    for record in &records {
        end += record.to_bytes().len() as u64;
    }
    let file = std::fs::OpenOptions::new().write(true).open(wal_path)?;
    file.set_len(end)?;
    file.sync_data()?;
    crate::storage::record_durability_sync();
    Ok(())
}

pub(super) fn extend_digest(current: u32, record: &WalRecord) -> u32 {
    let bytes = record.to_bytes();
    let mut input = Vec::with_capacity(4 + bytes.len());
    input.extend_from_slice(&current.to_le_bytes());
    input.extend_from_slice(&bytes);
    crc32c::crc32c(&input)
}

pub(super) fn digest_records(records: &[&WalRecord]) -> u32 {
    records
        .iter()
        .fold(0, |digest, record| extend_digest(digest, record))
}

pub(super) fn decode_put_payload(v2: bool, payload: &[u8]) -> Result<(&[u8], &[u8])> {
    if v2 {
        if payload.len() < 4 + 4 {
            return Err(Error::Corruption("WAL v2 put record too small".into()));
        }
        let key_len = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        let value_len_offset = 4usize
            .checked_add(key_len)
            .ok_or_else(|| Error::Corruption("WAL key length overflow".into()))?;
        if payload.len() < value_len_offset + 4 {
            return Err(Error::Corruption("WAL v2 put key is truncated".into()));
        }
        let value_len = u32::from_le_bytes([
            payload[value_len_offset],
            payload[value_len_offset + 1],
            payload[value_len_offset + 2],
            payload[value_len_offset + 3],
        ]) as usize;
        let value_offset = value_len_offset + 4;
        if payload.len() != value_offset + value_len {
            return Err(Error::Corruption("WAL v2 put value is truncated".into()));
        }
        return Ok((&payload[4..value_len_offset], &payload[value_offset..]));
    }

    // Read the pre-v2 u16 layout so an upgrade can recover an older WAL.
    if payload.len() < 4 {
        return Err(Error::Corruption("WAL put record too small".into()));
    }
    let key_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let value_len_offset = 2usize
        .checked_add(key_len)
        .ok_or_else(|| Error::Corruption("WAL key length overflow".into()))?;
    if payload.len() < value_len_offset + 2 {
        return Err(Error::Corruption("WAL put key is truncated".into()));
    }
    let value_len =
        u16::from_le_bytes([payload[value_len_offset], payload[value_len_offset + 1]]) as usize;
    let value_offset = value_len_offset + 2;
    if payload.len() != value_offset + value_len {
        return Err(Error::Corruption("WAL put value is truncated".into()));
    }
    Ok((&payload[2..value_len_offset], &payload[value_offset..]))
}

pub(super) fn decode_delete_payload(v2: bool, payload: &[u8]) -> Result<&[u8]> {
    if v2 {
        if payload.len() < 4 {
            return Err(Error::Corruption("WAL v2 delete record too small".into()));
        }
        let key_len = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        if payload.len() != 4 + key_len {
            return Err(Error::Corruption("WAL v2 delete key is truncated".into()));
        }
        return Ok(&payload[4..]);
    }

    // Read the pre-v2 u16 layout so an upgrade can recover an older WAL.
    if payload.len() < 2 {
        return Err(Error::Corruption("WAL delete record too small".into()));
    }
    let key_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    if payload.len() != 2 + key_len {
        return Err(Error::Corruption("WAL delete key is truncated".into()));
    }
    Ok(&payload[2..])
}

pub(super) fn validate_wal_key_length(key: &[u8]) -> Result<()> {
    if key.len() > MAX_KEY_SIZE {
        return Err(Error::InvalidArgument(
            "key exceeds the maximum B-tree page key size".into(),
        ));
    }
    if u32::try_from(key.len()).is_err() {
        return Err(Error::InvalidArgument(
            "key exceeds the durable WAL length limit".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_wal_put_lengths(key: &[u8], value: &[u8]) -> Result<()> {
    validate_wal_key_length(key)?;
    if u32::try_from(value.len()).is_err() {
        return Err(Error::InvalidArgument(
            "value exceeds the durable WAL length limit".into(),
        ));
    }
    Ok(())
}
