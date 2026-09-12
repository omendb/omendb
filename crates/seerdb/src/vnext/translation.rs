//! Concurrent logical-page to resident-frame translation baseline.
//!
//! The implementation is intentionally replaceable. It establishes a measured,
//! correct baseline before predictive translation, swizzling, object-local
//! direct placement, or custom lock-free tables are considered. Crucially, a
//! translation stores a stale-safe `FrameRef`, not a reusable bare slot ID.

use super::{FrameRef, PageKey};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::RwLock;

const DEFAULT_SHARDS: usize = 64;

/// Failure while accessing a poisoned translation shard.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("page translation shard is poisoned")]
pub struct TranslationError;

/// Result of publishing a fully initialized resident page.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PublishResult {
    /// This frame became the authoritative resident translation.
    Published,
    /// Another concurrent loader published first.
    Existing(FrameRef),
}

/// Sharded baseline translation table.
pub struct TranslationTable {
    shards: Box<[RwLock<HashMap<PageKey, FrameRef>>]>,
    mask: usize,
}

impl TranslationTable {
    /// Construct the default 64-shard table.
    #[must_use]
    pub fn new() -> Self {
        Self::with_shards(DEFAULT_SHARDS)
    }

    /// Construct a table with a power-of-two shard count.
    #[must_use]
    pub fn with_shards(shard_count: usize) -> Self {
        assert!(
            shard_count.is_power_of_two(),
            "translation shards must be a nonzero power of two"
        );
        let shards = (0..shard_count)
            .map(|_| RwLock::new(HashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            mask: shard_count - 1,
        }
    }

    /// Look up a resident frame reference.
    pub fn get(&self, key: PageKey) -> Result<Option<FrameRef>, TranslationError> {
        let shard = self.shard(key);
        let guard = self.shards[shard].read().map_err(|_| TranslationError)?;
        Ok(guard.get(&key).copied())
    }

    /// Publish a translation only if no loader has already won for this page.
    ///
    /// Buffer loading happens before this call. Refusing to overwrite an
    /// existing mapping gives duplicate concurrent misses a single winner and a
    /// deterministic loser-cleanup path instead of silently orphaning a live
    /// resident frame.
    pub fn publish_if_absent(
        &self,
        key: PageKey,
        frame: FrameRef,
    ) -> Result<PublishResult, TranslationError> {
        let shard = self.shard(key);
        let mut guard = self.shards[shard].write().map_err(|_| TranslationError)?;
        match guard.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(frame);
                Ok(PublishResult::Published)
            }
            Entry::Occupied(entry) => Ok(PublishResult::Existing(*entry.get())),
        }
    }

    /// Remove a translation only if it still points at the expected frame
    /// incarnation. Delayed eviction therefore cannot delete a newer mapping.
    pub fn remove_if(&self, key: PageKey, expected: FrameRef) -> Result<bool, TranslationError> {
        let shard = self.shard(key);
        let mut guard = self.shards[shard].write().map_err(|_| TranslationError)?;
        if guard.get(&key).copied() == Some(expected) {
            guard.remove(&key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Number of published translations, for diagnostics and tests.
    pub fn len(&self) -> Result<usize, TranslationError> {
        self.shards.iter().try_fold(0usize, |total, shard| {
            let guard = shard.read().map_err(|_| TranslationError)?;
            Ok(total + guard.len())
        })
    }

    /// Whether no page is currently translated.
    pub fn is_empty(&self) -> Result<bool, TranslationError> {
        Ok(self.len()? == 0)
    }

    fn shard(&self, key: PageKey) -> usize {
        // Cheap deterministic mixing. The table is a baseline and this hash is
        // deliberately not elevated into a stable storage/runtime contract.
        let mut value = key.object().get() ^ key.page().get().rotate_left(29);
        value ^= value >> 33;
        value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
        value ^= value >> 33;
        (value as usize) & self.mask
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
    use crate::vnext::{FrameId, FrameIncarnation, PageId, StorageObjectId};
    use std::sync::Arc;

    fn key(object: u64, page: u64) -> PageKey {
        PageKey::new(StorageObjectId::new(object), PageId::new(page))
    }

    fn frame(slot: usize, incarnation: u64) -> FrameRef {
        FrameRef::new(
            FrameId::new(slot),
            FrameIncarnation::new(incarnation).expect("nonzero incarnation"),
        )
    }

    #[test]
    fn page_identity_is_object_scoped() {
        let table = TranslationTable::with_shards(4);
        table
            .publish_if_absent(key(1, 7), frame(3, 1))
            .expect("publish succeeds");
        table
            .publish_if_absent(key(2, 7), frame(4, 1))
            .expect("publish succeeds");
        assert_eq!(table.get(key(1, 7)).expect("lookup"), Some(frame(3, 1)));
        assert_eq!(table.get(key(2, 7)).expect("lookup"), Some(frame(4, 1)));
    }

    #[test]
    fn concurrent_publication_keeps_first_winner() {
        let table = TranslationTable::with_shards(4);
        let page = key(9, 11);
        assert_eq!(
            table.publish_if_absent(page, frame(1, 1)).expect("publish"),
            PublishResult::Published
        );
        assert_eq!(
            table
                .publish_if_absent(page, frame(2, 1))
                .expect("duplicate"),
            PublishResult::Existing(frame(1, 1))
        );
        assert_eq!(table.get(page).expect("lookup"), Some(frame(1, 1)));
    }

    #[test]
    fn stale_removal_checks_incarnation_not_only_slot() {
        let table = TranslationTable::with_shards(4);
        let page = key(3, 5);
        table
            .publish_if_absent(page, frame(7, 2))
            .expect("publish succeeds");
        assert!(
            !table
                .remove_if(page, frame(7, 1))
                .expect("stale removal is safe")
        );
        assert_eq!(table.get(page).expect("lookup"), Some(frame(7, 2)));
        assert!(
            table
                .remove_if(page, frame(7, 2))
                .expect("current removal succeeds")
        );
    }

    #[test]
    fn parallel_updates_across_shards_remain_visible() {
        let table = Arc::new(TranslationTable::with_shards(16));
        let mut threads = Vec::new();
        for worker in 0..8usize {
            let table = Arc::clone(&table);
            threads.push(std::thread::spawn(move || {
                for page in 0..256u64 {
                    let key = key(worker as u64, page);
                    let reference = frame(worker * 256 + page as usize, 1);
                    assert_eq!(
                        table.publish_if_absent(key, reference).expect("publish"),
                        PublishResult::Published
                    );
                    assert_eq!(table.get(key).expect("lookup"), Some(reference));
                }
            }));
        }
        for thread in threads {
            thread.join().expect("worker completes");
        }
        assert_eq!(table.len().expect("len"), 8 * 256);
    }
}
