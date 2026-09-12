//! Transaction-status indirection and compact logical-record MVCC envelope.
//!
//! Physical page versions are deliberately not transaction visibility. A
//! current record is owned either by a live `TxnId` or by a frozen `CommitSeq`,
//! carries its logical value/tombstone, and links to a prior logical version.
//! Publishing one transaction status therefore makes every record owned by that
//! transaction visible together, even when records span multiple access-method
//! objects. The append-oriented version store remains a separate service.

use super::{CommitSeq, TxnId, VersionId};
use std::collections::HashMap;
use std::sync::RwLock;

const RECORD_MAGIC: [u8; 4] = *b"OMV1";
const RECORD_VERSION: u8 = 1;
const RECORD_HEADER_SIZE: usize = 28;
const OWNER_TRANSACTION: u8 = 1;
const OWNER_FROZEN: u8 = 2;
const VALUE_INLINE: u8 = 1;
const VALUE_TOMBSTONE: u8 = 2;
const STATUS_SHARDS: usize = 64;

/// Visibility owner stored with one current or undo record.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RecordOwner {
    Transaction(TxnId),
    Frozen(CommitSeq),
}

/// Logical value stored in a current record or complete before-image.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MvccValue {
    Inline(Vec<u8>),
    Tombstone,
}

/// Current logical record or complete before-image.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MvccRecord {
    owner: RecordOwner,
    undo_head: Option<VersionId>,
    value: MvccValue,
}

impl MvccRecord {
    #[must_use]
    pub const fn new(owner: RecordOwner, undo_head: Option<VersionId>, value: MvccValue) -> Self {
        Self {
            owner,
            undo_head,
            value,
        }
    }

    #[must_use]
    pub const fn owner(&self) -> RecordOwner {
        self.owner
    }

    #[must_use]
    pub const fn undo_head(&self) -> Option<VersionId> {
        self.undo_head
    }

    #[must_use]
    pub const fn value(&self) -> &MvccValue {
        &self.value
    }

    /// Encode a compact fail-closed value envelope for an access-method record.
    pub fn to_bytes(&self) -> Result<Vec<u8>, MvccCodecError> {
        let (owner_kind, owner_id) = match self.owner {
            RecordOwner::Transaction(txn) => (OWNER_TRANSACTION, txn.get()),
            RecordOwner::Frozen(csn) => (OWNER_FROZEN, csn.get()),
        };
        if owner_id == 0 {
            return Err(MvccCodecError::ReservedOwner);
        }
        let (value_kind, value) = match &self.value {
            MvccValue::Inline(value) => (VALUE_INLINE, value.as_slice()),
            MvccValue::Tombstone => (VALUE_TOMBSTONE, &[][..]),
        };
        let value_len = u32::try_from(value.len()).map_err(|_| MvccCodecError::ValueTooLarge)?;
        let total = RECORD_HEADER_SIZE
            .checked_add(value.len())
            .ok_or(MvccCodecError::ValueTooLarge)?;
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(&RECORD_MAGIC);
        bytes.push(RECORD_VERSION);
        bytes.push(owner_kind);
        bytes.push(value_kind);
        bytes.push(0);
        bytes.extend_from_slice(&owner_id.to_le_bytes());
        bytes.extend_from_slice(&self.undo_head.map_or(0, VersionId::get).to_le_bytes());
        bytes.extend_from_slice(&value_len.to_le_bytes());
        bytes.extend_from_slice(value);
        Ok(bytes)
    }

    /// Decode and validate a logical MVCC record envelope.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MvccCodecError> {
        if bytes.len() < RECORD_HEADER_SIZE || bytes[..4] != RECORD_MAGIC {
            return Err(MvccCodecError::Malformed);
        }
        if bytes[4] != RECORD_VERSION || bytes[7] != 0 {
            return Err(MvccCodecError::UnsupportedFormat);
        }
        let owner_raw = read_u64(bytes, 8).ok_or(MvccCodecError::Malformed)?;
        if owner_raw == 0 {
            return Err(MvccCodecError::ReservedOwner);
        }
        let owner = match bytes[5] {
            OWNER_TRANSACTION => RecordOwner::Transaction(TxnId::new(owner_raw)),
            OWNER_FROZEN => RecordOwner::Frozen(CommitSeq::new(owner_raw)),
            _ => return Err(MvccCodecError::UnsupportedFormat),
        };
        let undo_raw = read_u64(bytes, 16).ok_or(MvccCodecError::Malformed)?;
        let value_len = read_u32(bytes, 24).ok_or(MvccCodecError::Malformed)? as usize;
        let expected = RECORD_HEADER_SIZE
            .checked_add(value_len)
            .ok_or(MvccCodecError::Malformed)?;
        if expected != bytes.len() {
            return Err(MvccCodecError::Malformed);
        }
        let value = match bytes[6] {
            VALUE_INLINE => MvccValue::Inline(bytes[RECORD_HEADER_SIZE..].to_vec()),
            VALUE_TOMBSTONE if value_len == 0 => MvccValue::Tombstone,
            VALUE_TOMBSTONE => return Err(MvccCodecError::Malformed),
            _ => return Err(MvccCodecError::UnsupportedFormat),
        };
        Ok(Self {
            owner,
            undo_head: (undo_raw != 0).then_some(VersionId::new(undo_raw)),
            value,
        })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum MvccCodecError {
    #[error("MVCC record envelope is malformed")]
    Malformed,
    #[error("MVCC record uses an unsupported version, owner, value kind, or flags")]
    UnsupportedFormat,
    #[error("MVCC record uses the reserved zero owner identity")]
    ReservedOwner,
    #[error("MVCC inline value exceeds the envelope length domain")]
    ValueTooLarge,
}

/// Recovered/process-local transaction status used for visibility indirection.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TransactionStatus {
    Active,
    Committed(CommitSeq),
    Aborted,
}

/// Result of resolving one record owner for a reader snapshot.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RecordVisibility {
    Visible,
    Active,
    Aborted,
    NewerCommit(CommitSeq),
}

/// Sharded transaction-status table. The WAL decision remains authoritative;
/// this table is its process-local visibility view and is rebuilt on recovery.
pub struct TransactionStatusTable {
    shards: [RwLock<HashMap<TxnId, TransactionStatus>>; STATUS_SHARDS],
}

impl Default for TransactionStatusTable {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionStatusTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(HashMap::new())),
        }
    }

    /// Register one newly active transaction identity.
    pub fn begin(&self, txn: TxnId) -> Result<(), StatusTableError> {
        if txn.get() == 0 {
            return Err(StatusTableError::ReservedTxn);
        }
        let shard = self.shard(txn);
        let mut entries = self.shards[shard]
            .write()
            .map_err(|_| StatusTableError::Poisoned(shard))?;
        if entries.contains_key(&txn) {
            return Err(StatusTableError::DuplicateTxn(txn));
        }
        entries.insert(txn, TransactionStatus::Active);
        Ok(())
    }

    /// Publish one durable transaction decision atomically to all its records.
    pub fn commit(&self, txn: TxnId, csn: CommitSeq) -> Result<(), StatusTableError> {
        if csn.get() == 0 {
            return Err(StatusTableError::ReservedCommitSeq);
        }
        self.transition(txn, TransactionStatus::Committed(csn))
    }

    /// Publish an abort decision for one active transaction.
    pub fn abort(&self, txn: TxnId) -> Result<(), StatusTableError> {
        self.transition(txn, TransactionStatus::Aborted)
    }

    /// Rebuild a committed status from validated WAL recovery.
    ///
    /// Repeating the same recovered decision is idempotent; a conflicting
    /// outcome or CSN fails closed.
    pub fn recover_committed(&self, txn: TxnId, csn: CommitSeq) -> Result<(), StatusTableError> {
        if txn.get() == 0 {
            return Err(StatusTableError::ReservedTxn);
        }
        if csn.get() == 0 {
            return Err(StatusTableError::ReservedCommitSeq);
        }
        self.recover(txn, TransactionStatus::Committed(csn))
    }

    /// Rebuild an abort status from validated WAL recovery.
    pub fn recover_aborted(&self, txn: TxnId) -> Result<(), StatusTableError> {
        if txn.get() == 0 {
            return Err(StatusTableError::ReservedTxn);
        }
        self.recover(txn, TransactionStatus::Aborted)
    }

    pub fn status(&self, txn: TxnId) -> Result<Option<TransactionStatus>, StatusTableError> {
        let shard = self.shard(txn);
        let entries = self.shards[shard]
            .read()
            .map_err(|_| StatusTableError::Poisoned(shard))?;
        Ok(entries.get(&txn).copied())
    }

    /// Resolve visibility without consulting physical page versions.
    ///
    /// An active transaction sees its own writes. Other readers see a
    /// transaction-owned version only after the status entry is committed at or
    /// before their snapshot; aborted/active owners require following undo.
    pub fn visibility(
        &self,
        owner: RecordOwner,
        reader: Option<TxnId>,
        snapshot: CommitSeq,
    ) -> Result<RecordVisibility, StatusTableError> {
        match owner {
            RecordOwner::Frozen(csn) => Ok(if csn <= snapshot {
                RecordVisibility::Visible
            } else {
                RecordVisibility::NewerCommit(csn)
            }),
            RecordOwner::Transaction(txn) if reader == Some(txn) => Ok(RecordVisibility::Visible),
            RecordOwner::Transaction(txn) => {
                match self.status(txn)?.ok_or(StatusTableError::UnknownTxn(txn))? {
                    TransactionStatus::Active => Ok(RecordVisibility::Active),
                    TransactionStatus::Aborted => Ok(RecordVisibility::Aborted),
                    TransactionStatus::Committed(csn) if csn <= snapshot => {
                        Ok(RecordVisibility::Visible)
                    }
                    TransactionStatus::Committed(csn) => Ok(RecordVisibility::NewerCommit(csn)),
                }
            }
        }
    }

    fn transition(&self, txn: TxnId, next: TransactionStatus) -> Result<(), StatusTableError> {
        if txn.get() == 0 {
            return Err(StatusTableError::ReservedTxn);
        }
        let shard = self.shard(txn);
        let mut entries = self.shards[shard]
            .write()
            .map_err(|_| StatusTableError::Poisoned(shard))?;
        let status = entries
            .get_mut(&txn)
            .ok_or(StatusTableError::UnknownTxn(txn))?;
        if *status != TransactionStatus::Active {
            return Err(StatusTableError::InvalidTransition {
                txn,
                current: *status,
                requested: next,
            });
        }
        *status = next;
        Ok(())
    }

    fn recover(&self, txn: TxnId, recovered: TransactionStatus) -> Result<(), StatusTableError> {
        let shard = self.shard(txn);
        let mut entries = self.shards[shard]
            .write()
            .map_err(|_| StatusTableError::Poisoned(shard))?;
        match entries.get(&txn).copied() {
            None => {
                entries.insert(txn, recovered);
                Ok(())
            }
            Some(existing) if existing == recovered => Ok(()),
            Some(existing) => Err(StatusTableError::ConflictingRecovery {
                txn,
                existing,
                recovered,
            }),
        }
    }

    fn shard(&self, txn: TxnId) -> usize {
        (txn.get() as usize) & (STATUS_SHARDS - 1)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum StatusTableError {
    #[error("transaction ID zero is reserved")]
    ReservedTxn,
    #[error("commit sequence zero is reserved for unresolved ownership")]
    ReservedCommitSeq,
    #[error("transaction {0:?} is already registered")]
    DuplicateTxn(TxnId),
    #[error("transaction {0:?} is not present in the status table")]
    UnknownTxn(TxnId),
    #[error("transaction {txn:?} cannot move from {current:?} to {requested:?}")]
    InvalidTransition {
        txn: TxnId,
        current: TransactionStatus,
        requested: TransactionStatus,
    },
    #[error("recovery disagrees for transaction {txn:?}: {existing:?} vs {recovered:?}")]
    ConflictingRecovery {
        txn: TxnId,
        existing: TransactionStatus,
        recovered: TransactionStatus,
    },
    #[error("transaction-status shard {0} is poisoned")]
    Poisoned(usize),
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

    #[test]
    fn record_envelope_round_trips_transaction_frozen_and_tombstone_states() {
        let records = [
            MvccRecord::new(
                RecordOwner::Transaction(TxnId::new(7)),
                Some(VersionId::new(11)),
                MvccValue::Inline(b"value".to_vec()),
            ),
            MvccRecord::new(
                RecordOwner::Frozen(CommitSeq::new(13)),
                None,
                MvccValue::Tombstone,
            ),
        ];
        for record in records {
            let encoded = record.to_bytes().expect("record encodes");
            assert_eq!(
                MvccRecord::from_bytes(&encoded).expect("record decodes"),
                record
            );
        }
    }

    #[test]
    fn malformed_and_unknown_record_envelopes_fail_closed() {
        let record = MvccRecord::new(
            RecordOwner::Transaction(TxnId::new(3)),
            None,
            MvccValue::Inline(b"x".to_vec()),
        );
        let encoded = record.to_bytes().expect("record encodes");
        assert!(matches!(
            MvccRecord::from_bytes(&encoded[..encoded.len() - 1]),
            Err(MvccCodecError::Malformed)
        ));

        let mut version = encoded.clone();
        version[4] = 99;
        assert!(matches!(
            MvccRecord::from_bytes(&version),
            Err(MvccCodecError::UnsupportedFormat)
        ));

        let mut trailing = encoded;
        trailing.push(0);
        assert!(matches!(
            MvccRecord::from_bytes(&trailing),
            Err(MvccCodecError::Malformed)
        ));
    }

    #[test]
    fn duplicate_begin_does_not_overwrite_terminal_status() {
        let statuses = TransactionStatusTable::new();
        let txn = TxnId::new(11);
        statuses.begin(txn).expect("transaction begins");
        statuses
            .commit(txn, CommitSeq::new(5))
            .expect("transaction commits");
        assert!(matches!(
            statuses.begin(txn),
            Err(StatusTableError::DuplicateTxn(id)) if id == txn
        ));
        assert_eq!(
            statuses.status(txn).expect("status"),
            Some(TransactionStatus::Committed(CommitSeq::new(5)))
        );
    }

    #[test]
    fn one_status_publication_makes_multi_object_records_visible_together() {
        let statuses = TransactionStatusTable::new();
        let txn = TxnId::new(17);
        statuses.begin(txn).expect("transaction begins");
        let row = RecordOwner::Transaction(txn);
        let index = RecordOwner::Transaction(txn);
        let snapshot = CommitSeq::new(20);

        assert_eq!(
            statuses
                .visibility(row, None, snapshot)
                .expect("visibility"),
            RecordVisibility::Active
        );
        assert_eq!(
            statuses
                .visibility(index, None, snapshot)
                .expect("visibility"),
            RecordVisibility::Active
        );
        assert_eq!(
            statuses
                .visibility(row, Some(txn), CommitSeq::new(1))
                .expect("own write"),
            RecordVisibility::Visible
        );

        statuses
            .commit(txn, CommitSeq::new(19))
            .expect("commit publishes");
        assert_eq!(
            statuses
                .visibility(row, None, snapshot)
                .expect("visibility"),
            RecordVisibility::Visible
        );
        assert_eq!(
            statuses
                .visibility(index, None, snapshot)
                .expect("visibility"),
            RecordVisibility::Visible
        );
    }

    #[test]
    fn snapshot_and_abort_resolution_require_undo_for_invisible_current_records() {
        let statuses = TransactionStatusTable::new();
        let committed = TxnId::new(23);
        statuses.begin(committed).expect("transaction begins");
        statuses
            .commit(committed, CommitSeq::new(30))
            .expect("transaction commits");
        assert_eq!(
            statuses
                .visibility(
                    RecordOwner::Transaction(committed),
                    None,
                    CommitSeq::new(29)
                )
                .expect("visibility"),
            RecordVisibility::NewerCommit(CommitSeq::new(30))
        );

        let aborted = TxnId::new(29);
        statuses.begin(aborted).expect("transaction begins");
        statuses.abort(aborted).expect("transaction aborts");
        assert_eq!(
            statuses
                .visibility(RecordOwner::Transaction(aborted), None, CommitSeq::new(100))
                .expect("visibility"),
            RecordVisibility::Aborted
        );
        assert_eq!(
            statuses
                .visibility(
                    RecordOwner::Frozen(CommitSeq::new(7)),
                    None,
                    CommitSeq::new(7)
                )
                .expect("frozen visibility"),
            RecordVisibility::Visible
        );
    }

    #[test]
    fn recovered_status_is_idempotent_but_conflicting_outcome_fails() {
        let statuses = TransactionStatusTable::new();
        let txn = TxnId::new(31);
        statuses
            .recover_committed(txn, CommitSeq::new(9))
            .expect("first recovery");
        statuses
            .recover_committed(txn, CommitSeq::new(9))
            .expect("same recovery is idempotent");
        assert!(matches!(
            statuses.recover_aborted(txn),
            Err(StatusTableError::ConflictingRecovery { .. })
        ));
        assert!(matches!(
            statuses.recover_committed(txn, CommitSeq::new(10)),
            Err(StatusTableError::ConflictingRecovery { .. })
        ));
    }
}
