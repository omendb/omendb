//! Sharded logical write-intent ownership for vNext authoritative effects.
//!
//! This is deliberately a nonblocking primitive. It establishes exclusive
//! `(StorageObjectId, key)` ownership and reports conflicts; isolation policy,
//! waiting, timeout, wound/wait, and dependency certification belong above it.
//! Canonical batch acquisition prevents callers from inventing inconsistent
//! lock orders as those policies are added.

use super::{FinalEffect, StorageObjectId, TxnId};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

const INTENT_SHARDS: usize = 64;

#[derive(Debug, Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct IntentKey {
    object: StorageObjectId,
    key: Vec<u8>,
}

impl IntentKey {
    fn from_effect(effect: &FinalEffect) -> Self {
        Self {
            object: effect.object(),
            key: effect.key().to_vec(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct IntentEntry {
    owner: TxnId,
    holders: usize,
}

/// Sharded table of logical key ownership. No shard lock is held across the
/// return boundary; ownership is represented by entries released by the guard.
pub struct WriteIntentTable {
    shards: [Mutex<HashMap<IntentKey, IntentEntry>>; INTENT_SHARDS],
}

impl Default for WriteIntentTable {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteIntentTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    /// Try to acquire a transaction's complete canonical final write set.
    ///
    /// Effects must be strictly ordered by `(object,key)` and owned by `txn`.
    /// On conflict or other failure, every ownership increment made by this call
    /// is rolled back before the error is returned. Re-acquisition by the same
    /// owner is supported through reference counts so nested guards cannot
    /// release each other's ownership prematurely.
    pub fn try_acquire<'a>(
        &'a self,
        txn: TxnId,
        effects: &[FinalEffect],
    ) -> Result<WriteIntentGuard<'a>, WriteIntentError> {
        let mut keys = Vec::with_capacity(effects.len());
        for effect in effects {
            if effect.txn_id() != txn {
                return Err(WriteIntentError::WrongTransaction {
                    expected: txn,
                    actual: effect.txn_id(),
                    ordinal: effect.ordinal(),
                });
            }
            let key = IntentKey::from_effect(effect);
            if keys.last().is_some_and(|previous| previous >= &key) {
                return Err(WriteIntentError::NonCanonicalOrder {
                    object: key.object,
                    key: key.key,
                });
            }
            keys.push(key);
        }

        let mut acquired = 0usize;
        for key in &keys {
            let shard = self.shard(key);
            let mut entries = match self.shards[shard].lock() {
                Ok(entries) => entries,
                Err(_) => {
                    self.rollback(txn, &keys[..acquired]);
                    return Err(WriteIntentError::Poisoned(shard));
                }
            };
            match entries.get_mut(key) {
                None => {
                    entries.insert(
                        key.clone(),
                        IntentEntry {
                            owner: txn,
                            holders: 1,
                        },
                    );
                }
                Some(entry) if entry.owner == txn => {
                    let Some(next) = entry.holders.checked_add(1) else {
                        drop(entries);
                        self.rollback(txn, &keys[..acquired]);
                        return Err(WriteIntentError::HolderCountExhausted);
                    };
                    entry.holders = next;
                }
                Some(entry) => {
                    let owner = entry.owner;
                    drop(entries);
                    self.rollback(txn, &keys[..acquired]);
                    return Err(WriteIntentError::Conflict {
                        object: key.object,
                        key: key.key.clone(),
                        owner,
                    });
                }
            }
            acquired += 1;
        }

        Ok(WriteIntentGuard {
            table: self,
            txn,
            keys,
            released: false,
        })
    }

    /// Return the current owner of one logical key for diagnostics or policy
    /// code. Absence means no transaction currently owns the intent.
    pub fn owner(
        &self,
        object: StorageObjectId,
        key: &[u8],
    ) -> Result<Option<TxnId>, WriteIntentError> {
        let intent = IntentKey {
            object,
            key: key.to_vec(),
        };
        let shard = self.shard(&intent);
        let entries = self.shards[shard]
            .lock()
            .map_err(|_| WriteIntentError::Poisoned(shard))?;
        Ok(entries.get(&intent).map(|entry| entry.owner))
    }

    fn rollback(&self, txn: TxnId, keys: &[IntentKey]) {
        for key in keys.iter().rev() {
            self.release_key(txn, key);
        }
    }

    fn release_key(&self, txn: TxnId, key: &IntentKey) {
        let shard = self.shard(key);
        let mut entries = self.shards[shard]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let remove = match entries.get_mut(key) {
            Some(entry) if entry.owner == txn && entry.holders > 1 => {
                entry.holders -= 1;
                false
            }
            Some(entry) if entry.owner == txn => true,
            _ => false,
        };
        if remove {
            entries.remove(key);
        }
    }

    fn shard(&self, key: &IntentKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & (INTENT_SHARDS - 1)
    }
}

/// RAII ownership of one canonical batch of logical write intents.
pub struct WriteIntentGuard<'a> {
    table: &'a WriteIntentTable,
    txn: TxnId,
    keys: Vec<IntentKey>,
    released: bool,
}

impl WriteIntentGuard<'_> {
    #[must_use]
    pub const fn txn_id(&self) -> TxnId {
        self.txn
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Release this ownership batch before the guard's lexical drop point.
    pub fn release(mut self) {
        self.release_all();
    }

    fn release_all(&mut self) {
        if self.released {
            return;
        }
        self.table.rollback(self.txn, &self.keys);
        self.released = true;
    }
}

impl Drop for WriteIntentGuard<'_> {
    fn drop(&mut self) {
        self.release_all();
    }
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum WriteIntentError {
    #[error(
        "final effect ordinal {ordinal} belongs to transaction {actual:?}, expected {expected:?}"
    )]
    WrongTransaction {
        expected: TxnId,
        actual: TxnId,
        ordinal: u32,
    },
    #[error("final write set is not in strict canonical order at {object:?}/{key:?}")]
    NonCanonicalOrder {
        object: StorageObjectId,
        key: Vec<u8>,
    },
    #[error("write intent for {object:?}/{key:?} is owned by transaction {owner:?}")]
    Conflict {
        object: StorageObjectId,
        key: Vec<u8>,
        owner: TxnId,
    },
    #[error("write-intent shard {0} is poisoned")]
    Poisoned(usize),
    #[error("write-intent nested holder count is exhausted")]
    HolderCountExhausted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{LoggedMutation, normalize_final_effects};

    fn effects(txn: u64, entries: &[(u64, &[u8])]) -> Vec<FinalEffect> {
        let mutations: Vec<_> = entries
            .iter()
            .enumerate()
            .map(|(ordinal, (object, key))| {
                LoggedMutation::ordered_put(
                    TxnId::new(txn),
                    ordinal as u32,
                    StorageObjectId::new(*object),
                    key.to_vec(),
                    b"value".to_vec(),
                )
            })
            .collect();
        normalize_final_effects(TxnId::new(txn), &mutations).expect("normalizes")
    }

    #[test]
    fn guard_holds_and_releases_a_canonical_batch() {
        let table = WriteIntentTable::new();
        let writes = effects(1, &[(1, b"a"), (1, b"b"), (2, b"a")]);
        let guard = table.try_acquire(TxnId::new(1), &writes).expect("acquires");
        assert_eq!(guard.len(), 3);
        assert_eq!(
            table.owner(StorageObjectId::new(1), b"a").expect("owner"),
            Some(TxnId::new(1))
        );
        drop(guard);
        assert_eq!(
            table.owner(StorageObjectId::new(1), b"a").expect("owner"),
            None
        );
    }

    #[test]
    fn conflicting_batch_rolls_back_earlier_acquisitions() {
        let table = WriteIntentTable::new();
        let blocker = effects(2, &[(2, b"b")]);
        let _blocker = table
            .try_acquire(TxnId::new(2), &blocker)
            .expect("blocker acquires");
        let candidate = effects(1, &[(1, b"a"), (2, b"b")]);
        assert!(matches!(
            table.try_acquire(TxnId::new(1), &candidate),
            Err(WriteIntentError::Conflict { owner, .. }) if owner == TxnId::new(2)
        ));
        assert_eq!(
            table.owner(StorageObjectId::new(1), b"a").expect("owner"),
            None,
            "first key from failed batch must roll back"
        );
    }

    #[test]
    fn nested_same_owner_guards_do_not_release_each_other() {
        let table = WriteIntentTable::new();
        let writes = effects(7, &[(3, b"key")]);
        let outer = table
            .try_acquire(TxnId::new(7), &writes)
            .expect("outer acquires");
        let inner = table
            .try_acquire(TxnId::new(7), &writes)
            .expect("same owner reacquires");
        drop(inner);
        assert_eq!(
            table.owner(StorageObjectId::new(3), b"key").expect("owner"),
            Some(TxnId::new(7))
        );
        drop(outer);
        assert_eq!(
            table.owner(StorageObjectId::new(3), b"key").expect("owner"),
            None
        );
    }

    #[test]
    fn same_logical_key_in_different_objects_does_not_conflict() {
        let table = WriteIntentTable::new();
        let first = effects(1, &[(1, b"same")]);
        let second = effects(2, &[(2, b"same")]);
        let _first = table
            .try_acquire(TxnId::new(1), &first)
            .expect("first acquires");
        let _second = table
            .try_acquire(TxnId::new(2), &second)
            .expect("different object acquires");
    }
}
