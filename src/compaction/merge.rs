use crate::compaction::{CompactionFilter, FilterDecision};
use crate::sstable::{SSTable, SSTableError};
use crate::types::InternalKey;
use bytes::Bytes;
use std::sync::Arc;

/// Merged entries from all `SSTables`, sorted by key
pub struct MergeIterator {
    entries: std::vec::IntoIter<(Bytes, Bytes)>,
}

impl MergeIterator {
    /// Create a new merge iterator from multiple `SSTables`
    ///
    /// Collects all entries, sorts by key, and applies MVCC garbage collection.
    /// For L0 compaction, "newest" means from HIGHER `source_id` (later in vector).
    /// L0 `SSTables` are ordered oldest→newest, so higher index = newer data.
    ///
    /// # MVCC Garbage Collection
    /// - `oldest_snapshot`: Sequence number of the oldest active snapshot.
    ///   Pass `u64::MAX` if no snapshots are active (GC everything possible).
    /// - For each `user_key`, keeps:
    ///   - The newest version (always)
    ///   - Any version with seq >= `oldest_snapshot` (visible to active snapshots)
    /// - Drops old versions with seq < `oldest_snapshot` when a newer version exists.
    pub fn new(
        sstables: Vec<SSTable>,
        level: usize,
        filter: Option<Arc<dyn CompactionFilter>>,
    ) -> Result<Self, SSTableError> {
        // Default: no GC (keep all versions)
        Self::with_gc(sstables, level, filter, u64::MAX)
    }

    /// Create a merge iterator with MVCC garbage collection
    ///
    /// # Arguments
    /// * `sstables` - `SSTables` to merge (older first for L0)
    /// * `level` - Target compaction level
    /// * `filter` - Optional compaction filter
    /// * `oldest_snapshot` - Oldest active snapshot seq (`u64::MAX` = no snapshots)
    pub fn with_gc(
        mut sstables: Vec<SSTable>,
        level: usize,
        filter: Option<Arc<dyn CompactionFilter>>,
        oldest_snapshot: u64,
    ) -> Result<Self, SSTableError> {
        let mut all_entries = Vec::new();

        // Collect all entries from all SSTables
        for (source_id, sstable) in sstables.iter_mut().enumerate() {
            let iter = sstable.iter()?;

            for result in iter {
                let (key, value) = result?;
                all_entries.push((key, value, source_id));
            }
        }

        // Sort by encoded key first, then by source_id DESCENDING (higher = newer)
        // With MVCC encoding, this sorts by (user_key ASC, seq DESC, source_id DESC)
        all_entries.sort_by(|a, b| {
            match a.0.cmp(&b.0) {
                std::cmp::Ordering::Equal => b.2.cmp(&a.2), // Higher source_id first (NEWEST)
                other => other,
            }
        });

        let mut finalized_entries = Vec::new();

        if let Some(filter) = filter {
            // Group by ENCODED key and apply filter logic
            // (filter API expects encoded keys, not user keys)
            let mut i = 0;
            while i < all_entries.len() {
                let key = &all_entries[i].0;
                let mut j = i + 1;

                // Find all versions of this ENCODED key
                while j < all_entries.len() && all_entries[j].0 == *key {
                    j += 1;
                }

                // Slice of all versions for this key (already sorted by recency)
                let versions = &all_entries[i..j];

                // 1. Merge phase
                let values: Vec<&[u8]> = versions.iter().map(|(_, v, _)| v.as_ref()).collect();

                // Default: pick newest (first in slice)
                let newest_value = &versions[0].1;
                let mut merged_value_bytes = newest_value.clone();

                // Try custom merge
                if let Some(merged) = filter.merge(level, key, &values) {
                    // If merged, we treat it as a new INLINE value
                    // Prepend FLAG_INLINE (1)
                    let mut with_flag = Vec::with_capacity(1 + merged.len());
                    with_flag.push(crate::sstable::FLAG_INLINE);
                    with_flag.extend_from_slice(&merged);
                    merged_value_bytes = Bytes::from(with_flag);
                }

                // 2. Filter phase
                match filter.filter(level, key, &merged_value_bytes) {
                    FilterDecision::Keep => {
                        finalized_entries.push((key.clone(), merged_value_bytes));
                    }
                    FilterDecision::Remove => {
                        // Skip
                    }
                    FilterDecision::ChangeValue(new_val) => {
                        // Treat as new INLINE value
                        let mut with_flag = Vec::with_capacity(1 + new_val.len());
                        with_flag.push(crate::sstable::FLAG_INLINE);
                        with_flag.extend_from_slice(&new_val);
                        finalized_entries.push((key.clone(), Bytes::from(with_flag)));
                    }
                }

                // Advance to next key
                i = j;
            }
        } else {
            // Fast path with MVCC GC: Group by user_key, apply GC rules
            Self::apply_mvcc_gc(&all_entries, oldest_snapshot, &mut finalized_entries);
        }

        Ok(Self {
            entries: finalized_entries.into_iter(),
        })
    }

    /// Apply MVCC garbage collection to sorted entries
    ///
    /// Groups entries by `user_key` and keeps:
    /// - Newest version (always)
    /// - Versions with seq >= `oldest_snapshot` (visible to snapshots)
    fn apply_mvcc_gc(
        all_entries: &[(Bytes, Bytes, usize)],
        oldest_snapshot: u64,
        finalized_entries: &mut Vec<(Bytes, Bytes)>,
    ) {
        if all_entries.is_empty() {
            return;
        }

        let mut i = 0;
        while i < all_entries.len() {
            let (encoded_key, value, _) = &all_entries[i];

            // Try to decode as InternalKey
            if let Some(ikey) = InternalKey::decode(encoded_key.clone()) {
                let user_key = &ikey.user_key;

                // Find all versions of this user_key
                let mut j = i + 1;
                while j < all_entries.len() {
                    if let Some(next_ikey) = InternalKey::decode(all_entries[j].0.clone()) {
                        if next_ikey.user_key != *user_key {
                            break;
                        }
                    } else {
                        // Not an InternalKey, different key
                        break;
                    }
                    j += 1;
                }

                // Process all versions of this user_key [i..j)
                // First entry (i) is newest due to sort order (seq DESC)
                let mut kept_newest = false;

                for (enc_key, val, _) in &all_entries[i..j] {
                    if let Some(ver_ikey) = InternalKey::decode(enc_key.clone()) {
                        // Keep if:
                        // 1. This is the newest version (first one), OR
                        // 2. seq >= oldest_snapshot (visible to active snapshot)
                        if !kept_newest || ver_ikey.seq >= oldest_snapshot {
                            finalized_entries.push((enc_key.clone(), val.clone()));
                            kept_newest = true;
                        }
                        // else: GC this old version
                    }
                }

                i = j;
            } else {
                // Not an InternalKey (legacy format) - keep with simple dedup
                finalized_entries.push((encoded_key.clone(), value.clone()));

                // Skip duplicates of the same non-MVCC key
                let mut j = i + 1;
                while j < all_entries.len() && all_entries[j].0 == *encoded_key {
                    j += 1;
                }
                i = j;
            }
        }
    }
}

impl Iterator for MergeIterator {
    type Item = Result<(Bytes, Bytes), SSTableError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next().map(Ok)
    }
}
