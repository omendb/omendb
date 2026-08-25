//! Logical record versions stored inside the current B-tree value.
//!
//! The generation-based page publication layer remains responsible for
//! physical durability. This module owns the logical MVCC envelope: one
//! ordered chain of committed before-images for one user key. Keeping the
//! chain in the value is deliberately simple for the first physical-MVCC
//! slice; an append-oriented undo store can replace the representation later
//! without changing snapshot visibility rules.

use crate::error::{Error, Result};
use crate::storage::format::{CommitSeq, TxnId};

const MAGIC: &[u8; 4] = b"SVM1";
const HEADER_SIZE: usize = 8;
const VERSION_SIZE: usize = 20;
const TOMBSTONE: u32 = u32::MAX;

/// Maximum encoded version chain accepted by the logical MVCC layer.
///
/// Retention-aware chain compaction is a later storage milestone. Refusing an
/// unbounded chain is safer than allowing one update to exhaust the B-tree or
/// blob admission budget.
pub(crate) const MAX_VERSION_CHAIN_BYTES: usize = 16 * 1024 * 1024;

/// One committed logical version of a value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValueVersion {
    pub(crate) transaction: TxnId,
    pub(crate) commit: CommitSeq,
    pub(crate) value: Option<Vec<u8>>,
}

/// Decode a version chain, treating a non-envelope value as a commit-zero
/// legacy value so an unreleased 0.x database can be read and migrated by its
/// next write.
pub(crate) fn decode(bytes: Option<&[u8]>) -> Result<Vec<ValueVersion>> {
    let Some(bytes) = bytes else {
        return Ok(Vec::new());
    };
    if bytes.len() < MAGIC.len() || bytes[..MAGIC.len()] != *MAGIC {
        return Err(Error::Corruption(
            "MVCC record is missing its versioned envelope".into(),
        ));
    }
    if bytes.len() < HEADER_SIZE {
        return Err(Error::Corruption("truncated MVCC record header".into()));
    }
    if bytes.len() > MAX_VERSION_CHAIN_BYTES {
        return Err(Error::Corruption(
            "MVCC record exceeds the retention limit".into(),
        ));
    }
    let count = u32::from_be_bytes(
        bytes[4..8]
            .try_into()
            .map_err(|_| Error::Corruption("invalid MVCC record version count".into()))?,
    ) as usize;
    let minimum_versions_bytes = count
        .checked_mul(VERSION_SIZE)
        .ok_or_else(|| Error::Corruption("MVCC version count overflows".into()))?;
    if count == 0 {
        return Err(Error::Corruption("MVCC record has no versions".into()));
    }
    if minimum_versions_bytes > bytes.len().saturating_sub(HEADER_SIZE) {
        return Err(Error::Corruption(
            "MVCC record has truncated versions".into(),
        ));
    }
    let mut cursor = HEADER_SIZE;
    let mut versions = Vec::with_capacity(count);
    let mut previous_commit = None;
    for _ in 0..count {
        let header_end = cursor
            .checked_add(VERSION_SIZE)
            .ok_or_else(|| Error::Corruption("MVCC record length overflow".into()))?;
        let header = bytes
            .get(cursor..header_end)
            .ok_or_else(|| Error::Corruption("truncated MVCC version header".into()))?;
        let transaction =
            TxnId::new(u64::from_be_bytes(header[..8].try_into().map_err(
                |_| Error::Corruption("invalid MVCC transaction ID".into()),
            )?));
        let commit =
            CommitSeq::new(u64::from_be_bytes(header[8..16].try_into().map_err(
                |_| Error::Corruption("invalid MVCC commit sequence".into()),
            )?));
        if previous_commit.is_some_and(|previous| commit <= previous) {
            return Err(Error::Corruption(
                "MVCC versions are not in commit order".into(),
            ));
        }
        previous_commit = Some(commit);
        let value_length = u32::from_be_bytes(
            header[16..20]
                .try_into()
                .map_err(|_| Error::Corruption("invalid MVCC value length".into()))?,
        );
        cursor = header_end;
        let value = if value_length == TOMBSTONE {
            None
        } else {
            let length = value_length as usize;
            let end = cursor
                .checked_add(length)
                .ok_or_else(|| Error::Corruption("MVCC value length overflow".into()))?;
            let value = bytes
                .get(cursor..end)
                .ok_or_else(|| Error::Corruption("truncated MVCC value".into()))?
                .to_vec();
            cursor = end;
            Some(value)
        };
        versions.push(ValueVersion {
            transaction,
            commit,
            value,
        });
    }
    if cursor != bytes.len() {
        return Err(Error::Corruption("MVCC record has trailing bytes".into()));
    }
    Ok(versions)
}

/// Encode a non-empty chain of versions in chronological order.
pub(crate) fn encode(versions: &[ValueVersion]) -> Result<Vec<u8>> {
    if versions.is_empty() {
        return Err(Error::InvalidArgument(
            "MVCC record must contain at least one version".into(),
        ));
    }
    let count = u32::try_from(versions.len())
        .map_err(|_| Error::InvalidArgument("too many MVCC versions".into()))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&count.to_be_bytes());
    let mut previous_commit = None;
    for version in versions {
        if previous_commit.is_some_and(|previous| version.commit <= previous) {
            return Err(Error::InvalidArgument(
                "MVCC versions must be in commit order".into(),
            ));
        }
        previous_commit = Some(version.commit);
        bytes.extend_from_slice(&version.transaction.get().to_be_bytes());
        bytes.extend_from_slice(&version.commit.get().to_be_bytes());
        match &version.value {
            Some(value) => {
                let length = u32::try_from(value.len())
                    .map_err(|_| Error::InvalidArgument("MVCC value is too large".into()))?;
                bytes.extend_from_slice(&length.to_be_bytes());
                bytes.extend_from_slice(value);
            }
            None => bytes.extend_from_slice(&TOMBSTONE.to_be_bytes()),
        }
        if bytes.len() > MAX_VERSION_CHAIN_BYTES {
            return Err(Error::InvalidArgument(
                "MVCC version chain exceeds the retention limit".into(),
            ));
        }
    }
    Ok(bytes)
}

/// Return the newest version visible at `snapshot`.
pub(crate) fn visible(versions: &[ValueVersion], snapshot: CommitSeq) -> Option<&ValueVersion> {
    versions
        .iter()
        .rev()
        .find(|version| version.commit <= snapshot)
}

/// Return the newest committed version, if any.
pub(crate) fn latest(versions: &[ValueVersion]) -> Option<&ValueVersion> {
    versions.last()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(commit: u64, value: Option<&[u8]>) -> ValueVersion {
        ValueVersion {
            transaction: TxnId::new(commit + 10),
            commit: CommitSeq::new(commit),
            value: value.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn roundtrip_preserves_versions_and_tombstones() {
        let versions = vec![version(1, Some(b"old")), version(3, None)];
        let encoded = encode(&versions).expect("encode");
        assert_eq!(decode(Some(&encoded)).expect("decode"), versions);
        assert_eq!(
            visible(&versions, CommitSeq::new(2)).unwrap().value,
            Some(b"old".to_vec())
        );
        assert!(
            visible(&versions, CommitSeq::new(3))
                .unwrap()
                .value
                .is_none()
        );
    }

    #[test]
    fn missing_value_is_an_empty_chain() {
        assert!(decode(None).expect("decode").is_empty());
    }

    #[test]
    fn malformed_chain_fails_closed() {
        let mut encoded = encode(&[version(2, Some(b"value"))]).expect("encode");
        encoded.pop();
        assert!(matches!(
            decode(Some(&encoded)),
            Err(Error::Corruption(message)) if message.contains("truncated")
        ));
    }

    #[test]
    fn unversioned_value_is_rejected() {
        assert!(matches!(
            decode(Some(b"legacy")),
            Err(Error::Corruption(message)) if message.contains("missing")
        ));
    }

    #[test]
    fn malformed_count_is_rejected_before_allocation() {
        let mut encoded = MAGIC.to_vec();
        encoded.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            decode(Some(&encoded)),
            Err(Error::Corruption(message)) if message.contains("truncated")
        ));
    }
}
