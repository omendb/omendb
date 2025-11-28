use super::{increment_bytes, partition_for_key, Result, DB};
use crate::memtable::Entry;
use crate::sstable::{SSTable, FLAG_INLINE, FLAG_MERGE, FLAG_POINTER, FLAG_TOMBSTONE};
use crate::types::InternalKey;
use crate::vlog::VLog;
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::time::Instant;

impl DB {
    /// Get a value by key.
    ///
    /// Returns the value if found, `None` if the key doesn't exist or was deleted.
    /// Automatically handles merge operations if a merge operator is configured.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let start = Instant::now();
        let key = key.as_ref();

        self.read_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut operands: Vec<Bytes> = Vec::new();

        // 1. Check correct partition first
        let partition = partition_for_key(key);
        let mt = self.memtables[partition].load();
        if let Some(entry) = mt.get_entry(key) {
            match entry {
                Entry::Value(v) => return Ok(self.resolve_merge(key, Some(v), &operands, start)),
                Entry::Tombstone => return Ok(self.resolve_merge(key, None, &operands, start)),
                Entry::Merge(ops) => {
                    operands.extend(ops.iter().rev().cloned());
                }
            }
        }

        // 2. Check immutable partitions
        let immut_arc = self.immutable_memtables.load();
        if let Some(ref immutable_partitions) = **immut_arc {
            let partition_mt = &immutable_partitions[partition];
            if let Some(entry) = partition_mt.get_entry(key) {
                match entry {
                    Entry::Value(v) => return Ok(self.resolve_merge(key, Some(v), &operands, start)),
                    Entry::Tombstone => return Ok(self.resolve_merge(key, None, &operands, start)),
                    Entry::Merge(ops) => {
                        operands.extend(ops.iter().rev().cloned());
                    }
                }
            }
        }

        let vlog_path = self.options.data_dir.join("values.vlog");
        let has_vlog = self.has_vlog.load(std::sync::atomic::Ordering::Relaxed);

        // 3. Check SSTables in LSM tree
        let lsm_arc = self.lsm.load();
        for level_num in 0..lsm_arc.num_levels() {
            if let Some(level) = lsm_arc.level(level_num) {
                let sstables: Vec<_> = level.sstables().iter().rev().collect();

                for sstable_path in sstables {
                    let cached_sstable = self.sstable_cache.get_or_insert_with(
                        sstable_path,
                        || -> Result<Arc<Mutex<SSTable>>> {
                            let global_cache = Some(Arc::clone(&self.global_block_cache));
                            let buffer_pool = self.buffer_pool.clone();

                            let mut sstable = if let Some(pool) = buffer_pool {
                                SSTable::open_with_buffer_pool(sstable_path, Some(pool))?
                            } else {
                                SSTable::open_with_global_cache(sstable_path, global_cache)?
                            };

                            if has_vlog {
                                let vlog = VLog::open(&vlog_path)?;
                                sstable = sstable.with_vlog(vlog);
                            }

                            Ok(Arc::new(Mutex::new(sstable)))
                        },
                    )?;

                    let mut sstable = cached_sstable.lock().expect("SSTable lock poisoned");
                    let result = sstable.get_entry_mvcc(key, u64::MAX)?;

                    if let Some((data, flag)) = result {
                        match flag {
                            FLAG_INLINE | FLAG_POINTER => {
                                return Ok(self.resolve_merge(key, Some(data), &operands, start));
                            }
                            FLAG_TOMBSTONE => {
                                return Ok(self.resolve_merge(key, None, &operands, start));
                            }
                            FLAG_MERGE => {
                                let end_key_vec = increment_bytes(key);
                                let end_key_slice = end_key_vec.as_deref();
                                let iter = sstable.scan_range(key, end_key_slice);

                                for (k, entry) in iter.flatten() {
                                    if k == key {
                                        if let Entry::Merge(ops) = entry {
                                            operands.extend(ops.iter().rev().cloned());
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

        Ok(self.resolve_merge(key, None, &operands, start))
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
        let has_vlog = self.options.vlog_threshold.is_some();
        let vlog_path = self.options.data_dir.join("values.vlog");

        for level_num in 0..lsm_arc.num_levels() {
            if let Some(level) = lsm_arc.level(level_num) {
                let sstables: Vec<_> = level.sstables().iter().rev().collect();
                for sstable_path in sstables {
                    let cached_sstable = self.sstable_cache.get_or_insert_with(
                        sstable_path,
                        || -> Result<Arc<Mutex<SSTable>>> {
                            let global_cache = Some(Arc::clone(&self.global_block_cache));
                            let buffer_pool = self.buffer_pool.clone();
                            let mut sstable = if let Some(pool) = buffer_pool {
                                SSTable::open_with_buffer_pool(sstable_path, Some(pool))?
                            } else {
                                SSTable::open_with_global_cache(sstable_path, global_cache)?
                            };
                            if has_vlog {
                                let vlog = VLog::open(&vlog_path)?;
                                sstable = sstable.with_vlog(vlog);
                            }
                            Ok(Arc::new(Mutex::new(sstable)))
                        },
                    )?;

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
        let has_vlog = self.options.vlog_threshold.is_some();
        let vlog_path = self.options.data_dir.join("values.vlog");

        for level_num in 0..lsm_arc.num_levels() {
            if let Some(level) = lsm_arc.level(level_num) {
                let sstables: Vec<_> = level.sstables().iter().rev().collect();
                for sstable_path in sstables {
                    let cached_sstable = self.sstable_cache.get_or_insert_with(
                        sstable_path,
                        || -> Result<Arc<Mutex<SSTable>>> {
                            let global_cache = Some(Arc::clone(&self.global_block_cache));
                            let buffer_pool = self.buffer_pool.clone();
                            let mut sstable = if let Some(pool) = buffer_pool {
                                SSTable::open_with_buffer_pool(sstable_path, Some(pool))?
                            } else {
                                SSTable::open_with_global_cache(sstable_path, global_cache)?
                            };
                            if has_vlog {
                                let vlog = VLog::open(&vlog_path)?;
                                sstable = sstable.with_vlog(vlog);
                            }
                            Ok(Arc::new(Mutex::new(sstable)))
                        },
                    )?;

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
            let ops_reversed: Vec<&[u8]> =
                operands.iter().rev().map(std::convert::AsRef::as_ref).collect();
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
