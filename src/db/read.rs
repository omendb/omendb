use super::{increment_bytes, partition_for_key, Result, DB};
use crate::memtable::Entry;
use crate::sstable::{SSTable, FLAG_INLINE, FLAG_MERGE, FLAG_POINTER, FLAG_TOMBSTONE};
use crate::types::InternalKey;
use crate::vlog::VLog;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

impl DB {
    /// Get an `SSTable` handle, loading from disk if not cached.
    ///
    /// This is the canonical way to access `SSTables` for reads. Uses a cache-aside
    /// pattern: check cache first, load on miss.
    ///
    /// # Caching behavior
    /// - Cache hit: Returns cached `Arc<Mutex<SSTable>>` (fast path)
    /// - Cache miss: Opens `SSTable`, configures buffer pool/`vLog`, caches it
    ///
    /// # Thread safety
    /// The returned handle is wrapped in `Arc<Mutex<>>` for safe concurrent access.
    /// Multiple readers can hold references; the mutex serializes actual I/O.
    fn get_sstable(&self, path: &PathBuf) -> Result<Arc<Mutex<SSTable>>> {
        self.sstable_cache.get_or_insert_with(path, || {
            let has_vlog = self.has_vlog.load(std::sync::atomic::Ordering::Relaxed);

            // Choose caching strategy: buffer pool (if configured) or global block cache
            let mut sstable = if let Some(ref pool) = self.buffer_pool {
                SSTable::open_with_buffer_pool(path, Some(Arc::clone(pool)))?
            } else {
                let global_cache = Some(Arc::clone(&self.global_block_cache));
                SSTable::open_with_global_cache(path, global_cache)?
            };

            // Attach vLog for value separation if enabled
            if has_vlog {
                let vlog_path = self.options.data_dir.join("values.vlog");
                sstable = sstable.with_vlog(VLog::open(&vlog_path)?);
            }

            Ok(Arc::new(Mutex::new(sstable)))
        })
    }

    /// Get a value by key.
    ///
    /// Returns the value if found, `None` if the key doesn't exist or was deleted.
    /// Automatically handles merge operations if a merge operator is configured.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let start = Instant::now();
        let key = key.as_ref();

        self.read_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Lazy-init operands only when merge ops are encountered (rare case)
        let mut operands: Option<Vec<Bytes>> = None;

        // 1. Check correct partition first
        let partition = partition_for_key(key);
        let mt = self.memtables[partition].load();
        if let Some(entry) = mt.get_entry(key) {
            match entry {
                Entry::Value(v) => {
                    return Ok(self.resolve_merge_opt(key, Some(v), &operands, start))
                }
                Entry::Tombstone => return Ok(self.resolve_merge_opt(key, None, &operands, start)),
                Entry::Merge {
                    base: Some(v),
                    operands: ops,
                } => {
                    // Found base in memtable - resolve immediately
                    operands.get_or_insert_with(Vec::new).extend(ops);
                    return Ok(self.resolve_merge_opt(key, Some(v), &operands, start));
                }
                Entry::Merge {
                    base: None,
                    operands: ops,
                } => {
                    // No base yet - continue searching
                    operands.get_or_insert_with(Vec::new).extend(ops);
                }
            }
        }

        // 2. Check immutable partitions
        let immut_arc = self.immutable_memtables.load();
        if let Some(ref immutable_partitions) = **immut_arc {
            let partition_mt = &immutable_partitions[partition];
            if let Some(entry) = partition_mt.get_entry(key) {
                match entry {
                    Entry::Value(v) => {
                        return Ok(self.resolve_merge_opt(key, Some(v), &operands, start))
                    }
                    Entry::Tombstone => {
                        return Ok(self.resolve_merge_opt(key, None, &operands, start))
                    }
                    Entry::Merge {
                        base: Some(v),
                        operands: ops,
                    } => {
                        operands.get_or_insert_with(Vec::new).extend(ops);
                        return Ok(self.resolve_merge_opt(key, Some(v), &operands, start));
                    }
                    Entry::Merge {
                        base: None,
                        operands: ops,
                    } => {
                        operands.get_or_insert_with(Vec::new).extend(ops);
                    }
                }
            }
        }

        // 3. Check SSTables in LSM tree
        let lsm_arc = self.lsm.load();
        for level_num in 0..lsm_arc.num_levels() {
            if let Some(level) = lsm_arc.level(level_num) {
                // Iterate directly in reverse - no Vec allocation needed
                for sstable_path in level.sstables().iter().rev() {
                    let cached_sstable = self.get_sstable(sstable_path)?;
                    let mut sstable = cached_sstable.lock().expect("SSTable lock poisoned");
                    let result = sstable.get_entry_mvcc(key, u64::MAX)?;

                    if let Some((data, flag)) = result {
                        match flag {
                            FLAG_INLINE | FLAG_POINTER => {
                                return Ok(self.resolve_merge_opt(
                                    key,
                                    Some(data),
                                    &operands,
                                    start,
                                ));
                            }
                            FLAG_TOMBSTONE => {
                                return Ok(self.resolve_merge_opt(key, None, &operands, start));
                            }
                            FLAG_MERGE => {
                                let end_key_vec = increment_bytes(key);
                                let end_key_slice = end_key_vec.as_deref();
                                let iter = sstable.scan_range(key, end_key_slice);

                                for (k, entry) in iter.flatten() {
                                    if k == key {
                                        match entry {
                                            Entry::Value(v) => {
                                                return Ok(self.resolve_merge_opt(
                                                    key,
                                                    Some(v),
                                                    &operands,
                                                    start,
                                                ));
                                            }
                                            Entry::Merge {
                                                base,
                                                operands: ops,
                                            } => {
                                                operands.get_or_insert_with(Vec::new).extend(ops);
                                                if let Some(v) = base {
                                                    return Ok(self.resolve_merge_opt(
                                                        key,
                                                        Some(v),
                                                        &operands,
                                                        start,
                                                    ));
                                                }
                                            }
                                            Entry::Tombstone => {
                                                return Ok(self.resolve_merge_opt(
                                                    key, None, &operands, start,
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                            _ => return Err(crate::sstable::SSTableError::InvalidFormat.into()),
                        }
                    }
                }
            }
        }

        Ok(self.resolve_merge_opt(key, None, &operands, start))
    }

    /// Get a value at a specific sequence number (snapshot isolation).
    pub(crate) fn get_at_seq(&self, key: &[u8], snapshot_seq: u64) -> Result<Option<Bytes>> {
        let partition = partition_for_key(key);
        let mt = self.memtables[partition].load();
        if let Some((value, _seq)) = mt.get(key, snapshot_seq) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        let immut_arc = self.immutable_memtables.load();
        if let Some(ref immutable_partitions) = **immut_arc {
            let partition_mt = &immutable_partitions[partition];
            if let Some((value, _seq)) = partition_mt.get(key, snapshot_seq) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let lsm_arc = self.lsm.load();
        for level_num in 0..lsm_arc.num_levels() {
            if let Some(level) = lsm_arc.level(level_num) {
                for sstable_path in level.sstables().iter().rev() {
                    let cached_sstable = self.get_sstable(sstable_path)?;
                    let mut sstable = cached_sstable.lock().expect("SSTable lock poisoned");
                    if let Ok(Some(value)) = sstable.get_mvcc(key, snapshot_seq) {
                        if value.is_empty() {
                            return Ok(None);
                        }
                        return Ok(Some(value));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Get the latest sequence number for a key.
    pub(crate) fn get_latest_seq(&self, key: &[u8]) -> Result<Option<u64>> {
        let partition = partition_for_key(key);
        let mt = self.memtables[partition].load();
        if let Some((_value, seq)) = mt.get(key, u64::MAX) {
            return Ok(Some(seq));
        }

        let immut_arc = self.immutable_memtables.load();
        if let Some(ref immutable_partitions) = **immut_arc {
            let partition_mt = &immutable_partitions[partition];
            if let Some((_value, seq)) = partition_mt.get(key, u64::MAX) {
                return Ok(Some(seq));
            }
        }

        let lsm_arc = self.lsm.load();
        for level_num in 0..lsm_arc.num_levels() {
            if let Some(level) = lsm_arc.level(level_num) {
                for sstable_path in level.sstables().iter().rev() {
                    let cached_sstable = self.get_sstable(sstable_path)?;
                    let mut sstable = cached_sstable.lock().expect("SSTable lock poisoned");
                    if let Ok(Some((encoded_key, _value))) = sstable.get_raw_entry(key) {
                        if let Some(ikey) = InternalKey::decode(encoded_key) {
                            return Ok(Some(ikey.seq));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Resolve merge with Option<Vec<Bytes>> - avoids allocation when no merges
    #[inline]
    pub(crate) fn resolve_merge_opt(
        &self,
        key: &[u8],
        base: Option<Bytes>,
        operands: &Option<Vec<Bytes>>,
        start: Instant,
    ) -> Option<Bytes> {
        match operands {
            None => {
                // Fast path: no merge operands (common case)
                if !self.options.disable_metrics {
                    self.metrics.record_get(start.elapsed());
                }
                base
            }
            Some(ops) if ops.is_empty() => {
                if !self.options.disable_metrics {
                    self.metrics.record_get(start.elapsed());
                }
                base
            }
            Some(ops) => self.resolve_merge(key, base, ops, start),
        }
    }

    pub(crate) fn resolve_merge(
        &self,
        key: &[u8],
        base: Option<Bytes>,
        operands: &[Bytes],
        start: Instant,
    ) -> Option<Bytes> {
        if operands.is_empty() {
            if !self.options.disable_metrics {
                self.metrics.record_get(start.elapsed());
            }
            return base;
        }

        if let Some(ref op) = self.options.merge_operator {
            let ops_reversed: Vec<&[u8]> = operands
                .iter()
                .rev()
                .map(std::convert::AsRef::as_ref)
                .collect();
            let base_slice = base.as_ref().map(std::convert::AsRef::as_ref);

            if let Some(merged) = op.full_merge(key, base_slice, &ops_reversed) {
                if !self.options.disable_metrics {
                    self.metrics.record_get(start.elapsed());
                }
                Some(Bytes::from(merged))
            } else {
                if !self.options.disable_metrics {
                    self.metrics.record_get(start.elapsed());
                }
                base
            }
        } else {
            if !self.options.disable_metrics {
                self.metrics.record_get(start.elapsed());
            }
            base
        }
    }
}
