//! Concurrent logical-page to resident-frame translation baseline.
//!
//! The first vNext implementation deliberately starts with a simple sharded
//! hash table. It is a correctness and end-to-end benchmark baseline, not a
//! declaration that hash translation is the permanent winner. ADR 0013 keeps
//! this boundary explicit so predictive translation, hints/swizzling, direct
//! arrays, and object-local placement can be measured without rewriting access
//! methods.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::{FrameId, PageKey};

/// Default number of independent translation shards.
pub const DEFAULT_TRANSLATION_SHARDS: usize = 64;

/// Failure from the translation baseline itself.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum TranslationError {
    /// Shard count must be a nonzero power of two so lookup uses one mask.
    #[error("translation shard count must be a nonzero power of two")]
    InvalidShardCount,
    /// A thread panicked while mutating one shard; storage must fail closed.
    #[error("translation shard lock is poisoned")]
    Poisoned,
    /// The diagnostic resident-count accumulator overflowed.
    #[error("translation resident count overflowed")]
    CountOverflow,
}

struct TranslationShard {
    entries: RwLock<HashMap<PageKey, FrameId>>,
}

impl TranslationShard {
    fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, HashMap<PageKey, FrameId>>, TranslationError> {
        self.entries.read().map_err(|_| TranslationError::Poisoned)
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, HashMap<PageKey, FrameId>>, TranslationError> {
        self.entries
            .write()
            .map_err(|_| TranslationError::Poisoned)
    }
}

/// Sharded resident-page translation table.
///
/// This table owns only the transient mapping from logical page identity to a
/// process-local frame. It is not a durable page map and never appears in WAL,
/// checkpoints, or on-disk references.
pub struct TranslationTable {
    shards: Box<[TranslationShard]>,
    shard_mask: usize,
}

impl TranslationTable {
    /// Construct the default correctness/performance baseline.
    #[must_use]
    pub fn new() -> Self {
        Self::with_shards(DEFAULT_TRANSLATION_SHARDS)
            .expect("default translation shard count is valid")
    }

    /// Construct a table with an explicit power-of-two shard count.
    pub fn with_shards(shard_count: usize) -> Result<Self, TranslationError> {
        if shard_count == 0 || !shard_count.is_power_of_two() {
            return Err(TranslationError::InvalidShardCount);
        }
        let shards = (0..shard_count)
            .map(|_| TranslationShard::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            shards,
            shard_mask: shard_count - 1,
        })
    }

    /// Return the number of independently locked translation shards.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Look up the resident frame for one logical page.
    pub fn get(&self, key: PageKey) -> Result<Option<FrameId>, TranslationError> {
        let shard = self.shard(key);
        Ok(shard.read()?.get(&key).copied())
    }

    /// Publish or replace one resident mapping, returning the prior frame.
    ///
    /// The buffer manager must publish a mapping only after the target frame is
    /// fully initialized and in `Resident` state.
    pub fn insert(
        &self,
        key: PageKey,
        frame: FrameId,
    ) -> Result<Option<FrameId>, TranslationError> {
        Ok(self.shard(key).write()?.insert(key, frame))
    }

    /// Remove a mapping only if it still names `expected`.
    ///
    /// Conditional removal prevents a delayed eviction from deleting a newer
    /// frame that won a reload/replacement race for the same logical page.
    pub fn remove_if(
        &self,
        key: PageKey,
        expected: FrameId,
    ) -> Result<bool, TranslationError> {
        let mut entries = self.shard(key).write()?;
        if entries.get(&key).copied() != Some(expected) {
            return Ok(false);
        }
        entries.remove(&key);
        Ok(true)
    }

    /// Return the current resident-mapping count for diagnostics/tests.
    ///
    /// This is intentionally not a hot-path operation: it visits every shard.
    pub fn len(&self) -> Result<usize, TranslationError> {
        self.shards.iter().try_fold(0usize, |total, shard| {
            total
                .checked_add(shard.read()?.len())
                .ok_or(TranslationError::CountOverflow)
        })
    }

    /// Return whether no logical pages are currently mapped.
    pub fn is_empty(&self) -> Result<bool, TranslationError> {
        for shard in &self.shards {
            if !shard.read()?.is_empty() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn shard(&self, key: PageKey) -> &TranslationShard {
        &self.shards[self.shard_index(key)]
    }

    fn shard_index(&self, key: PageKey) -> usize {
        // Cheap deterministic mixing keeps the baseline independent from
        // HashMap's randomized hasher while preserving object and page entropy.
        // Translation strategies will be benchmarked end-to-end before this is
        // treated as a permanent fast path.
        let object = key.object().get().wrapping_mul(0x9E37_79B1_85EB_CA87);
        let page = key
            .page()
            .get()
            .rotate_left(31)
            .wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
        let mixed = object ^ page;
        ((mixed ^ (mixed >> 32)) as usize) & self.shard_mask
    }
}

impl Default for TranslationTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vnext::{PageId, StorageObjectId};
    use std::sync::Arc;

    fn key(object: u64, page: u64) -> PageKey {
        PageKey::new(StorageObjectId::new(object), PageId::new(page))
    }

    #[test]
    fn shard_count_must_be_power_of_two() {
        assert!(matches!(
            TranslationTable::with_shards(0),
            Err(TranslationError::InvalidShardCount)
        ));
        assert!(matches!(
            TranslationTable::with_shards(3),
            Err(TranslationError::InvalidShardCount)
        ));
        assert_eq!(
            TranslationTable::with_shards(8)
                .expect("valid shard count")
                .shard_count(),
            8
        );
    }

    #[test]
    fn identical_page_numbers_in_different_objects_do_not_alias() {
        let table = TranslationTable::with_shards(8).expect("translation table");
        table
            .insert(key(1, 7), FrameId::new(3))
            .expect("insert first");
        table
            .insert(key(2, 7), FrameId::new(9))
            .expect("insert second");
        assert_eq!(
            table.get(key(1, 7)).expect("lookup"),
            Some(FrameId::new(3))
        );
        assert_eq!(
            table.get(key(2, 7)).expect("lookup"),
            Some(FrameId::new(9))
        );
        assert_eq!(table.len().expect("length"), 2);
    }

    #[test]
    fn stale_conditional_remove_cannot_delete_newer_mapping() {
        let table = TranslationTable::with_shards(4).expect("translation table");
        let page = key(4, 12);
        table
            .insert(page, FrameId::new(1))
            .expect("initial insert");
        table
            .insert(page, FrameId::new(2))
            .expect("replacement");
        assert!(!table
            .remove_if(page, FrameId::new(1))
            .expect("stale remove"));
        assert_eq!(
            table.get(page).expect("lookup"),
            Some(FrameId::new(2))
        );
        assert!(table
            .remove_if(page, FrameId::new(2))
            .expect("current remove"));
        assert!(table.is_empty().expect("empty"));
    }

    #[test]
    fn independent_shards_accept_parallel_updates() {
        let table = Arc::new(TranslationTable::with_shards(16).expect("translation table"));
        let mut threads = Vec::new();
        for worker in 0..8u64 {
            let table = Arc::clone(&table);
            threads.push(std::thread::spawn(move || {
                for page in 0..128u64 {
                    let frame = FrameId::new((worker as usize * 128) + page as usize);
                    table
                        .insert(key(worker + 1, page), frame)
                        .expect("insert");
                }
            }));
        }
        for thread in threads {
            thread.join().expect("worker");
        }
        assert_eq!(table.len().expect("length"), 8 * 128);
        assert_eq!(
            table.get(key(8, 127)).expect("lookup"),
            Some(FrameId::new((7 * 128) + 127))
        );
    }
}
