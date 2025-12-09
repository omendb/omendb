//! Arena-based memtable using SKL skiplist with MVCC support.
//!
//! The memtable uses SKL's `multiple_version` module which provides:
//! - Arena allocation (reduces per-entry heap allocations)
//! - Built-in MVCC with version numbers (maps to our sequence numbers)
//! - Lock-free concurrent access

use bytes::Bytes;
use skl::dynamic::{
    multiple_version::{sync::SkipMap, Map},
    Builder,
};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::sstable::SSTableBuilder;
use crate::types::{InternalKey, ValueType};

/// Value type prefix bytes stored in SKL values
const VALUE_PREFIX: u8 = 0x01;
const TOMBSTONE_PREFIX: u8 = 0x02;
const MERGE_PREFIX: u8 = 0x03;

/// Minimum arena capacity for SKL (internal overhead ~184 bytes + headroom)
const MIN_ARENA_CAPACITY: usize = 4096;

/// Entry type for high-level operations
#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    Value(Bytes),
    Tombstone,
    /// Merge operands with optional base value found in same source
    Merge {
        base: Option<Bytes>,
        operands: Vec<Bytes>,
    },
}

/// In-memory sorted table for recent writes using arena-based skiplist.
pub struct Memtable {
    data: Arc<SkipMap>,
    size: AtomicUsize,
    capacity: usize,
}

impl Memtable {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        // SKL requires minimum arena space for internal structures (~184 bytes overhead).
        // We enforce a minimum of 4KB to ensure basic operations work.
        let arena_capacity = capacity.max(MIN_ARENA_CAPACITY) as u32;

        let data = Builder::new()
            .with_capacity(arena_capacity)
            .alloc::<SkipMap>()
            .expect("failed to allocate memtable arena");

        Self {
            data: Arc::new(data),
            size: AtomicUsize::new(0),
            capacity,
        }
    }

    /// Create memtable with default capacity (64MB)
    #[must_use]
    pub fn with_default_capacity() -> Self {
        Self::new(64 * 1024 * 1024)
    }

    /// Insert a key-value pair with a specific sequence number
    #[inline]
    #[allow(clippy::needless_pass_by_value)] // Bytes is cheap to clone, API clarity
    pub fn put(&self, key: Bytes, value: Bytes, seq: u64) {
        let size_delta = key.len() + value.len() + 1; // +1 for prefix

        // Encode value with prefix
        let mut encoded = Vec::with_capacity(1 + value.len());
        encoded.push(VALUE_PREFIX);
        encoded.extend_from_slice(&value);

        let _ = self.data.insert(seq, key.as_ref(), &encoded);
        self.size.fetch_add(size_delta, Ordering::Relaxed);
    }

    /// Delete a key (insert tombstone) with a specific sequence number
    #[inline]
    #[allow(clippy::needless_pass_by_value)]
    pub fn delete(&self, key: Bytes, seq: u64) {
        let size_delta = key.len() + 1;

        let _ = self.data.insert(seq, key.as_ref(), &[TOMBSTONE_PREFIX]);
        self.size.fetch_add(size_delta, Ordering::Relaxed);
    }

    /// Insert a merge operand
    #[inline]
    #[allow(clippy::needless_pass_by_value)]
    pub fn merge(&self, key: Bytes, operand: Bytes, seq: u64) {
        let size_delta = key.len() + operand.len() + 1;

        // Encode merge operand with prefix
        let mut encoded = Vec::with_capacity(1 + operand.len());
        encoded.push(MERGE_PREFIX);
        encoded.extend_from_slice(&operand);

        let _ = self.data.insert(seq, key.as_ref(), &encoded);
        self.size.fetch_add(size_delta, Ordering::Relaxed);
    }

    /// Get the latest value for a key (Snapshot Isolation)
    /// Returns (Value, Sequence) if found and visible <= `snapshot_seq`
    /// Returns None if not found or deleted
    #[inline]
    pub fn get(&self, key: &[u8], snapshot_seq: u64) -> Option<(Bytes, u64)> {
        // SKL's get returns the latest version <= snapshot_seq
        let entry = self.data.get(snapshot_seq, key)?;
        let value = entry.value();
        let version = entry.version();

        if value.is_empty() {
            return None;
        }

        match value[0] {
            VALUE_PREFIX => Some((Bytes::copy_from_slice(&value[1..]), version)),
            TOMBSTONE_PREFIX => None,
            MERGE_PREFIX => {
                // For simple get, skip merge operands (caller should use get_entry)
                None
            }
            _ => None,
        }
    }

    /// Get Entry (Value, Tombstone, or Merge list) for a key.
    /// This collects all versions/merges visible at the latest seq.
    #[inline]
    pub fn get_entry(&self, key: &[u8]) -> Option<Entry> {
        // Iterate all versions of this key from newest to oldest
        let mut merges = Vec::new();

        // Use iter_all to get all versions, then filter to our key
        for entry in self.data.iter_all(u64::MAX) {
            let entry_key = entry.key();
            if entry_key != key {
                if entry_key > key {
                    // Past our key in sorted order
                    break;
                }
                continue;
            }

            // iter_all returns MaybeTombstone entries, so value() returns Option
            let Some(value) = entry.value() else {
                continue;
            };
            if value.is_empty() {
                continue;
            }

            match value[0] {
                VALUE_PREFIX => {
                    if merges.is_empty() {
                        return Some(Entry::Value(Bytes::copy_from_slice(&value[1..])));
                    }
                    return Some(Entry::Merge {
                        base: Some(Bytes::copy_from_slice(&value[1..])),
                        operands: merges,
                    });
                }
                TOMBSTONE_PREFIX => {
                    if merges.is_empty() {
                        return Some(Entry::Tombstone);
                    }
                    return Some(Entry::Merge {
                        base: None,
                        operands: merges,
                    });
                }
                MERGE_PREFIX => {
                    merges.push(Bytes::copy_from_slice(&value[1..]));
                }
                _ => {}
            }
        }

        if !merges.is_empty() {
            return Some(Entry::Merge {
                base: None,
                operands: merges,
            });
        }

        None
    }

    /// Put an Entry with explicit sequence number (used for WAL recovery/merge resolution)
    pub fn put_entry(&self, key: Bytes, entry: Entry, seq: u64) {
        match entry {
            Entry::Value(v) => self.put(key, v, seq),
            Entry::Tombstone => self.delete(key, seq),
            Entry::Merge { base, operands } => {
                if let Some(v) = base {
                    self.put(key.clone(), v, seq);
                }
                for op in operands {
                    self.merge(key.clone(), op, seq);
                }
            }
        }
    }

    /// Check if key exists (raw check at latest version)
    #[inline]
    pub fn contains_raw(&self, key: &InternalKey) -> bool {
        self.data.get(key.seq, key.user_key.as_ref()).is_some()
    }

    /// Get current size in bytes (approximate)
    #[inline]
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Check if memtable should be flushed
    #[inline]
    pub fn should_flush(&self) -> bool {
        self.size() >= self.capacity
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Iterate over all entries in sorted order (internal representation)
    /// Returns (InternalKey, Bytes) for compatibility with flush
    pub fn iter(&self) -> impl Iterator<Item = (InternalKey, Bytes)> + '_ {
        self.data.iter_all(u64::MAX).filter_map(|entry| {
            let key = entry.key();
            let version = entry.version();

            // iter_all returns MaybeTombstone entries, so value() returns Option
            let value = entry.value()?;

            // Decode value type from prefix
            let (kind, data) = if value.is_empty() {
                (ValueType::Value, Bytes::new())
            } else {
                match value[0] {
                    VALUE_PREFIX => (ValueType::Value, Bytes::copy_from_slice(&value[1..])),
                    TOMBSTONE_PREFIX => (ValueType::Deletion, Bytes::new()),
                    MERGE_PREFIX => (ValueType::Merge, Bytes::copy_from_slice(&value[1..])),
                    _ => (ValueType::Value, Bytes::copy_from_slice(value)),
                }
            };

            let internal_key = InternalKey::new(Bytes::copy_from_slice(key), version, kind);
            Some((internal_key, data))
        })
    }

    /// Iterate over all entries as (`user_key`, Entry) pairs, grouped by user key.
    pub fn iter_entries(&self) -> impl Iterator<Item = (Bytes, Entry)> + '_ {
        self.range_from(&[])
    }

    /// Clone reference to underlying data (for immutable snapshot)
    pub fn snapshot(&self) -> Arc<SkipMap> {
        Arc::clone(&self.data)
    }

    /// Flush memtable to disk as an `SSTable`
    pub fn flush(&self, path: impl AsRef<Path>) -> Result<(), crate::sstable::SSTableError> {
        let mut builder = SSTableBuilder::create(path)?;

        for (ikey, value) in self.iter() {
            let ikey_bytes = ikey.encode();
            match ikey.kind {
                ValueType::Value => builder.add(ikey_bytes, value)?,
                ValueType::Deletion => builder.add_tombstone(ikey_bytes)?,
                ValueType::Merge => builder.add_merge(ikey_bytes, value)?,
                ValueType::Log => {}
            }
        }

        builder.finish()
    }

    // --- Range Iteration Support ---

    /// Forward range scan returning (`user_key`, Entry) pairs.
    ///
    /// SKL's range() at a given version already returns only the latest version
    /// of each key, so we can stream directly without collecting all versions.
    pub fn range(&self, start: &[u8], end: &[u8]) -> impl Iterator<Item = (Bytes, Entry)> + '_ {
        // SKL's range() needs the bounds to outlive the iterator.
        // We collect into a Vec to avoid lifetime issues with the slice references.
        let entries: Vec<_> = self
            .data
            .range(u64::MAX, start..end)
            .filter_map(|entry| {
                let user_key = Bytes::copy_from_slice(entry.key());
                Self::decode_value_to_entry(entry.value()).map(|e| (user_key, e))
            })
            .collect();
        entries.into_iter()
    }

    /// Forward range scan from start key to end.
    pub fn range_from(&self, start: &[u8]) -> impl Iterator<Item = (Bytes, Entry)> + '_ {
        let entries: Vec<_> = self
            .data
            .range(u64::MAX, start..)
            .filter_map(|entry| {
                let user_key = Bytes::copy_from_slice(entry.key());
                Self::decode_value_to_entry(entry.value()).map(|e| (user_key, e))
            })
            .collect();
        entries.into_iter()
    }

    /// Decode a value from SKL format to Entry
    #[inline]
    fn decode_value_to_entry(value: &[u8]) -> Option<Entry> {
        if value.is_empty() {
            return None;
        }

        match value[0] {
            VALUE_PREFIX => Some(Entry::Value(Bytes::copy_from_slice(&value[1..]))),
            TOMBSTONE_PREFIX => Some(Entry::Tombstone),
            MERGE_PREFIX => Some(Entry::Merge {
                base: None,
                operands: vec![Bytes::copy_from_slice(&value[1..])],
            }),
            _ => None,
        }
    }

    /// Reverse range scan returning (`user_key`, Entry) pairs.
    pub fn range_rev(&self, start: &[u8], end: &[u8]) -> impl Iterator<Item = (Bytes, Entry)> + '_ {
        let entries: Vec<_> = self.range(start, end).collect();
        entries.into_iter().rev()
    }

    /// Iterate over all entries in reverse sorted order (internal keys)
    pub fn iter_rev(&self) -> impl Iterator<Item = (InternalKey, Bytes)> + '_ {
        // Collect and reverse since SKL doesn't have direct reverse iter_all
        let entries: Vec<_> = self.iter().collect();
        entries.into_iter().rev()
    }

    /// Internal range scan in reverse (for internal use)
    pub fn range_rev_internal<'a>(
        &'a self,
        start: Option<&'a InternalKey>,
        end: Option<&'a InternalKey>,
    ) -> impl Iterator<Item = (InternalKey, Bytes)> + 'a {
        // Filter the full iteration based on bounds
        let start_key = start.map(|k| k.user_key.as_ref());
        let end_key = end.map(|k| k.user_key.as_ref());
        let start_seq = start.map(|k| k.seq);
        let end_seq = end.map(|k| k.seq);

        self.iter()
            .filter(move |(ikey, _)| {
                let key_ref = ikey.user_key.as_ref();
                let in_start = match (start_key, start_seq) {
                    (Some(sk), Some(ss)) => key_ref > sk || (key_ref == sk && ikey.seq <= ss),
                    _ => true,
                };
                let in_end = match (end_key, end_seq) {
                    (Some(ek), Some(es)) => key_ref < ek || (key_ref == ek && ikey.seq > es),
                    _ => true,
                };
                in_start && in_end
            })
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
    }
}

impl Clone for Memtable {
    fn clone(&self) -> Self {
        // Create new memtable with same capacity and copy data
        let new_mt = Self::new(self.capacity);
        for (ikey, value) in self.iter() {
            match ikey.kind {
                ValueType::Value => new_mt.put(ikey.user_key, value, ikey.seq),
                ValueType::Deletion => new_mt.delete(ikey.user_key, ikey.seq),
                ValueType::Merge => new_mt.merge(ikey.user_key, value, ikey.seq),
                ValueType::Log => {}
            }
        }
        new_mt.size.store(self.size(), Ordering::Relaxed);
        new_mt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_mvcc_put_get() {
        let memtable = Memtable::new(1024 * 1024);

        // Write v1 at seq 10
        memtable.put(Bytes::from("key1"), Bytes::from("val1"), 10);

        // Write v2 at seq 20
        memtable.put(Bytes::from("key1"), Bytes::from("val2"), 20);

        // Get at seq 15 -> Should see v1
        assert_eq!(memtable.get(b"key1", 15), Some((Bytes::from("val1"), 10)));

        // Get at seq 25 -> Should see v2
        assert_eq!(memtable.get(b"key1", 25), Some((Bytes::from("val2"), 20)));
    }

    #[test]
    fn test_memtable_mvcc_delete() {
        let memtable = Memtable::new(1024 * 1024);

        memtable.put(Bytes::from("key1"), Bytes::from("val1"), 10);
        assert_eq!(memtable.get(b"key1", 20), Some((Bytes::from("val1"), 10)));

        // Delete at seq 20
        memtable.delete(Bytes::from("key1"), 20);

        // Get at seq 25 -> Should return None (Deleted)
        assert_eq!(memtable.get(b"key1", 25), None);

        // Get at seq 15 -> Should still see v1
        assert_eq!(memtable.get(b"key1", 15), Some((Bytes::from("val1"), 10)));
    }
}
