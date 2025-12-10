use super::{CompactionTask, Result, DB, NUM_PARTITIONS};
use crate::memtable::Memtable;
use crate::types::InternalKey;
use bytes::Bytes;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

impl DB {
    /// Manually flush the memtable to disk
    ///
    /// Forces the in-memory write buffer (memtable) to be written to an `SSTable` on disk.
    /// This operation:
    /// 1. Writes all memtable entries to a new L0 `SSTable`
    /// 2. Clears the WAL (data now safely in `SSTable`)
    /// 3. Replaces memtable with a new empty one
    /// 4. Triggers compaction if L0 has too many `SSTables`
    ///
    /// Flushing normally happens automatically when the memtable is full, but you can
    /// call this method explicitly to control when flushes occur.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success or an error if:
    /// - `SSTable` write fails (disk full, I/O error)
    /// - WAL clear fails
    /// - Compaction fails (if triggered)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use seerdb::{DB, DBOptions};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = DB::open(DBOptions::default())?;
    ///
    /// // Write data
    /// for i in 0..1000 {
    ///     db.put(format!("key{}", i).as_bytes(), b"value")?;
    /// }
    ///
    /// // Force flush before shutdown
    /// db.flush()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - `DBError::Io`: Failed to write `SSTable` or clear WAL
    /// - `DBError::SSTable`: `SSTable` builder error
    /// - `DBError::Compaction`: Compaction failed (if triggered)
    ///
    /// # Performance
    ///
    /// - **Typical latency**: 10-100 milliseconds (depends on memtable size)
    /// - **Disk I/O**: Writes ~64MB `SSTable` (default memtable size)
    /// - **Blocks writes**: Briefly while swapping memtable
    ///
    /// # When to Use
    ///
    /// - **Before shutdown**: Persist final writes
    /// - **Performance tuning**: Avoid flush during hot path
    /// - **Testing**: Deterministic flush timing
    ///
    /// Not typically needed in normal operation (automatic flushing works well).
    pub fn flush(&self) -> Result<()> {
        // **CRITICAL FIX (Bug #10): Wait for background flush to complete BEFORE acquiring mutex
        // If background_flush is enabled AND immutable_memtables is occupied,
        // we must wait for the background flush worker to complete.
        // IMPORTANT: We must wait BEFORE acquiring flush_mutex, otherwise we deadlock:
        //   - flush() holds flush_mutex
        //   - background flush waits for flush_mutex
        //   - flush() waits for background flush to complete
        //   - DEADLOCK!
        if self.options.background_flush {
            // 60 seconds at 10ms sleep. Prevents deadlock if background worker dies.
            // Tested via stress tests under high-contention flush scenarios.
            const MAX_FLUSH_WAIT_ITERATIONS: u32 = 6000;
            let mut iterations = 0;

            // Wait for any in-progress background flush to complete
            loop {
                iterations += 1;
                if iterations > MAX_FLUSH_WAIT_ITERATIONS {
                    warn!(
                        iterations = iterations,
                        "Background flush wait timeout - proceeding anyway"
                    );
                    break;
                }

                let immut_arc = self.immutable_memtables.load();
                if immut_arc.is_none() {
                    // Background flush completed - safe to proceed
                    break;
                }
                // Background flush still in progress - wait briefly and retry
                debug!("Waiting for background flush to complete before explicit flush");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            // immutable_memtables is now None - background flush completed
        }

        // **CRITICAL FIX**: Serialize all flushes to prevent concurrent flush races
        let _flush_lock = self.flush_mutex.lock().expect("Flush mutex poisoned");

        let flush_start = Instant::now();

        // Check total size across all partitions (lock-free with ArcSwap)
        let total_size: usize = self.memtables.iter().map(|mt| mt.load().size()).sum();

        // Early return if all partitions are empty
        if total_size == 0 {
            return Ok(());
        }

        info!(
            total_memtable_size_bytes = total_size,
            partitions = NUM_PARTITIONS,
            "Starting partitioned memtable flush"
        );

        // Handle any failed flush (only when background_flush is disabled)
        if !self.options.background_flush {
            // Background flush disabled - handle synchronously
            // Check if there's a previous failed flush
            // If immutable_memtables is occupied, flush it first to avoid data loss (LOCK-FREE!)
            let pending_immutable_arc = self.immutable_memtables.swap(Arc::new(None));
            let pending_immutable =
                Arc::try_unwrap(pending_immutable_arc).unwrap_or_else(|arc| (*arc).clone());

            if let Some(pending_partitions_arc) = pending_immutable {
                // Previous flush failed - retry flushing the existing immutable partitions
                warn!(
                    partitions = pending_partitions_arc.len(),
                    "Retrying flush of previously failed immutable partitions"
                );

                // Generate filename for pending flush
                let mut counter = self
                    .sstable_counter
                    .lock()
                    .expect("SSTable counter mutex poisoned");
                let pending_flush_sequence = *counter; // Capture sequence for retry flush
                let pending_sstable_path = self
                    .options
                    .data_dir
                    .join(format!("L0_{:06}.sst", *counter));
                *counter += 1;
                drop(counter);

                // Collect and sort entries from all pending partitions (MVCC-aware)
                let mut all_entries: Vec<(InternalKey, Bytes)> = Vec::new();
                for partition_mt in pending_partitions_arc.iter() {
                    for (ikey, value) in partition_mt.iter() {
                        all_entries.push((ikey, value));
                    }
                }

                // Sort by InternalKey (user_key ASC, seq DESC - preserves all versions)
                all_entries.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

                // Build SSTable from sorted entries (MVCC-aware)
                self.build_sstable_from_internal(
                    &pending_sstable_path,
                    all_entries.iter(),
                    pending_flush_sequence,
                )?;
                let pending_size = std::fs::metadata(&pending_sstable_path)?.len();

                // Track physical bytes written to SSTable (retry case)
                if !self.options.disable_metrics {
                    self.metrics.record_physical_bytes(pending_size);
                }

                // CRITICAL FIX (Bug #7c): Serialize LSM tree updates to prevent ABA race
                {
                    let _lsm_lock = self.lsm_mutex.lock().expect("LSM mutex poisoned");

                    // Add to LSM tree (serialized)
                    let mut lsm_clone = (**self.lsm.load()).clone();
                    lsm_clone.add_l0_sstable(pending_sstable_path, pending_size);
                    self.lsm.store(Arc::new(lsm_clone));
                }

                // Now safe to clear WAL (all records written to disk)
                // Note: With PipelinedWAL, we don't need a barrier because put() blocks until WAL write.
                let mut wal = self.wal.lock().expect("WAL mutex poisoned");
                wal.clear()?;
                drop(wal);

                // Update max_flushed_seq for retry flush
                // Use fetch_max to handle out-of-order flush completions (only update if new value is greater)
                self.max_flushed_seq
                    .fetch_max(pending_flush_sequence, Ordering::SeqCst);

                info!("Successfully flushed previously failed immutable partitions");
            }
        }

        // Now check if active partitions need flushing (lock-free with ArcSwap)
        let total_size: usize = self.memtables.iter().map(|mt| mt.load().size()).sum();

        if total_size == 0 {
            return Ok(()); // Nothing to flush
        }

        // Generate SSTable filename for main flush
        let mut counter = self
            .sstable_counter
            .lock()
            .expect("SSTable counter mutex poisoned");
        let flush_sequence = *counter; // Capture sequence for this flush
        let sstable_path = self
            .options
            .data_dir
            .join(format!("L0_{:06}.sst", *counter));
        *counter += 1;
        drop(counter);

        // Swap ALL 16 partitions atomically (lock-free with ArcSwap!)
        // 1. Atomically swap each partition with new empty partition
        // 2. Keep old partitions as Arc<Memtable> (no unwrap needed)
        // 3. Store as immutable partitions
        // No locks needed - ArcSwap.swap() is atomic and lock-free!
        let capacity_per_partition = self.options.memtable_capacity / NUM_PARTITIONS;
        let mut flushing_partitions: Vec<Arc<Memtable>> = Vec::with_capacity(NUM_PARTITIONS);

        // Deref Arc to access the array
        for partition_mt in self.memtables.iter() {
            // Atomic swap: returns Arc<Memtable> of old partition
            // Keep it as Arc<Memtable> - no need to unwrap since immutable_memtables stores Arc
            let old_arc: Arc<Memtable> =
                partition_mt.swap(Arc::new(Memtable::new(capacity_per_partition)));
            flushing_partitions.push(old_arc);
        }

        // Collect entries from ALL partitions (MVCC-aware: preserves all versions)
        let mut all_entries: Vec<(InternalKey, Bytes)> = Vec::new();
        for partition_mt in &flushing_partitions {
            for (ikey, value) in partition_mt.iter() {
                all_entries.push((ikey, value));
            }
        }

        // Store in immutable_memtables so readers can access during flush (LOCK-FREE!)
        self.immutable_memtables
            .store(Arc::new(Some(Arc::new(flushing_partitions))));

        // Sort by InternalKey (user_key ASC, seq DESC - preserves all MVCC versions)
        all_entries.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // Build SSTable from sorted entries (MVCC-aware)
        self.build_sstable_from_internal(&sstable_path, all_entries.iter(), flush_sequence)?;

        // Failpoint: crash after SSTable write but before any metadata updates
        // Test: SSTable orphaned on disk, WAL intact, recovery replays WAL
        crate::fail_point!("flush::after_sstable_write");

        let size = std::fs::metadata(&sstable_path)?.len();

        // Track physical bytes written to SSTable
        if !self.options.disable_metrics {
            self.metrics.record_physical_bytes(size);
        }

        // CRITICAL FIX (Bug #7c): Serialize LSM tree updates to prevent ABA race
        let sstable_path_for_log = sstable_path.clone();
        {
            let _lsm_lock = self.lsm_mutex.lock().expect("LSM mutex poisoned");

            // Add to LSM tree L0 (serialized)
            let mut lsm_clone = (**self.lsm.load()).clone();
            lsm_clone.add_l0_sstable(sstable_path, size);
            self.lsm.store(Arc::new(lsm_clone));
        }

        // Clear immutable partitions + WAL after successful flush (LOCK-FREE!)
        self.immutable_memtables.store(Arc::new(None));

        // Failpoint: crash after SSTable in LSM, before WAL clear
        // Test: data safe in SSTable, WAL replay is idempotent (finds data already exists)
        crate::fail_point!("flush::before_wal_clear");

        // Now safe to clear WAL (all records written to disk)
        // Note: With PipelinedWAL, we don't need a barrier because put() blocks until WAL write.
        // flush() acquires wal lock, so it waits for any concurrent WAL writes to complete.
        let mut wal = self.wal.lock().expect("WAL mutex poisoned");
        wal.clear()?;
        drop(wal);

        // Update max_flushed_seq to allow compaction of this SSTable
        // This MUST happen after immutable_memtables is cleared to prevent data loss
        // Use fetch_max to handle out-of-order flush completions (only update if new value is greater)
        self.max_flushed_seq
            .fetch_max(flush_sequence, Ordering::SeqCst);

        let flush_duration_ms = flush_start.elapsed().as_millis();
        info!(
            duration_ms = flush_duration_ms,
            sstable_path = ?sstable_path_for_log,
            sstable_size_bytes = size,
            partitions_merged = NUM_PARTITIONS,
            "Partitioned memtable flush complete"
        );

        // Check if compaction is needed (LOCK-FREE!)
        if let Some(level_num) = self.lsm.load().needs_compaction() {
            debug!(level = level_num, "Compaction triggered");
            // Arc automatically dropped (lock-free!)

            if let Some(ref tx) = self.compaction_tx {
                // Background compaction: send signal (non-blocking)
                debug!(level = level_num, "Sending background compaction signal");
                let _ = tx.send(CompactionTask::CompactLevel(level_num));
            } else {
                // Synchronous compaction: block until done
                debug!(level = level_num, "Starting synchronous compaction");
                self.compact_level(level_num)?;
            }
        }

        // Record flush
        if !self.options.disable_metrics {
            self.metrics.record_flush();
        }

        // Adjust LSM size ratios based on workload (Dostoevsky adaptive compaction)
        // CRITICAL FIX (Bug #7c): Serialize LSM tree updates to prevent ABA race
        if self.options.adaptive_compaction {
            let writes = self.write_count.load(std::sync::atomic::Ordering::Relaxed);
            let reads = self.read_count.load(std::sync::atomic::Ordering::Relaxed);

            let _lsm_lock = self.lsm_mutex.lock().expect("LSM mutex poisoned");
            let mut lsm_clone = (**self.lsm.load()).clone();
            if lsm_clone.adjust_for_workload(writes, reads) {
                let strategy = lsm_clone.strategy();
                debug!(
                    writes = writes,
                    reads = reads,
                    ratio = strategy.current_ratio(),
                    "Dostoevsky: Adjusted LSM size ratio based on workload"
                );
                self.lsm.store(Arc::new(lsm_clone));
            }
        }

        Ok(())
    }

    /// Helper: Build `SSTable` from iterator of (`InternalKey`, value) pairs (MVCC-aware)
    ///
    /// Preserves all MVCC versions by using `add_internal()` methods.
    /// Handles both normal values and vLog separation.
    pub(crate) fn build_sstable_from_internal<'a, I>(
        &self,
        sstable_path: &Path,
        entries: I,
        _sequence: u64, // Unused: max_sequence derived from entries
    ) -> Result<()>
    where
        I: Iterator<Item = &'a (InternalKey, Bytes)>,
    {
        use crate::sstable::SSTableBuilder;

        let mut vlog_guard = self.vlog.lock().expect("vLog mutex poisoned");

        // Check if we're using cloud storage (feature-gated)
        #[cfg(feature = "object-store")]
        let use_cloud_storage = self.storage_backend.is_some();

        if let (Some(threshold), Some(ref mut vlog)) =
            (self.options.vlog_threshold, vlog_guard.as_mut())
        {
            // KV separation enabled - use vLog for large values
            #[cfg(feature = "object-store")]
            {
                if use_cloud_storage {
                    // Cloud storage + vLog: use buffered builder (MVCC-aware)
                    let mut builder = SSTableBuilder::new_buffered()
                        .with_vlog_threshold(threshold)
                        .with_compression(self.options.compression);

                    for (ikey, value) in entries {
                        builder.add_internal_with_vlog(ikey, value.clone(), vlog)?;
                    }

                    // ALWAYS sync vLog after flush
                    vlog.sync()?;

                    // Build SSTable in memory
                    let bytes = builder.finish_to_bytes()?;

                    // Write to local disk (single syscall)
                    std::fs::write(sstable_path, &bytes)?;

                    // Upload to cloud storage
                    if let Some(ref backend) = self.storage_backend {
                        backend.write_sstable(sstable_path, &bytes)?;
                        debug!(
                            path = ?sstable_path,
                            size_bytes = bytes.len(),
                            "SSTable with vLog uploaded to cloud storage"
                        );
                    }
                } else {
                    // No cloud storage + vLog: use traditional SSTableBuilder (MVCC-aware)
                    let mut builder = SSTableBuilder::create(sstable_path)?
                        .with_vlog_threshold(threshold)
                        .with_compression(self.options.compression);

                    for (ikey, value) in entries {
                        builder.add_internal_with_vlog(ikey, value.clone(), vlog)?;
                    }

                    builder.finish()?;

                    // ALWAYS sync vLog after flush
                    vlog.sync()?;
                }
            }

            // When object-store feature is not enabled, always use traditional SSTableBuilder
            #[cfg(not(feature = "object-store"))]
            {
                let mut builder = SSTableBuilder::create(sstable_path)?
                    .with_vlog_threshold(threshold)
                    .with_compression(self.options.compression);

                for (ikey, value) in entries {
                    builder.add_internal_with_vlog(ikey, value.clone(), vlog)?;
                }

                builder.finish()?;

                // ALWAYS sync vLog after flush
                vlog.sync()?;
            }
        } else {
            // No KV separation - MVCC-aware flush
            drop(vlog_guard);

            // Use buffered builder when cloud storage is enabled (fewer syscalls + upload)
            #[cfg(feature = "object-store")]
            {
                if self.storage_backend.is_some() {
                    let mut builder =
                        SSTableBuilder::new_buffered().with_compression(self.options.compression);

                    for (ikey, value) in entries {
                        builder.add_internal(ikey, value.clone())?;
                    }

                    // Build SSTable in memory
                    let bytes = builder.finish_to_bytes()?;

                    // Write to local disk (single syscall)
                    std::fs::write(sstable_path, &bytes)?;

                    // Upload to cloud storage
                    if let Some(ref backend) = self.storage_backend {
                        backend.write_sstable(sstable_path, &bytes)?;
                        debug!(
                            path = ?sstable_path,
                            size_bytes = bytes.len(),
                            "SSTable uploaded to cloud storage"
                        );
                    }
                } else {
                    // No cloud storage - use traditional SSTableBuilder (MVCC-aware)
                    let mut builder = SSTableBuilder::create(sstable_path)?
                        .with_compression(self.options.compression);

                    for (ikey, value) in entries {
                        builder.add_internal(ikey, value.clone())?;
                    }

                    builder.finish()?;
                }
            }

            // When object-store feature is not enabled, always use traditional SSTableBuilder
            #[cfg(not(feature = "object-store"))]
            {
                let mut builder = SSTableBuilder::create(sstable_path)?
                    .with_compression(self.options.compression);

                for (ikey, value) in entries {
                    builder.add_internal(ikey, value.clone())?;
                }

                builder.finish()?;
            }
        }

        Ok(())
    }

    /// Try to atomically swap all partitions for background flush
    ///
    /// Returns true if partitions were successfully swapped (caller should signal background thread)
    /// Returns false if another thread is already flushing (skip signaling)
    pub(crate) fn try_swap_memtable(&self) -> bool {
        // Try to acquire flush lock - if another thread is flushing, return false
        let Ok(_flush_lock) = self.flush_mutex.try_lock() else {
            return false; // Another thread is flushing
        };

        // Check if immutable_memtables is occupied (LOCK-FREE!)
        let immut_occupied = {
            let immut_arc = self.immutable_memtables.load();
            immut_arc.is_some()
        };

        if immut_occupied {
            // Another thread's flush is still in progress
            return false;
        }

        // Safe to swap - immutable_memtables is None
        // Swap ALL 16 partitions atomically (lock-free with ArcSwap!)
        let capacity_per_partition = self.options.memtable_capacity / NUM_PARTITIONS;
        let mut flushing_partitions = Vec::with_capacity(NUM_PARTITIONS);

        // Deref Arc to access the array
        for partition_mt in self.memtables.iter() {
            // Atomic swap: returns Arc<Memtable> of old partition
            // Keep it as Arc<Memtable> - no need to unwrap since immutable_memtables stores Arc
            let old_arc: Arc<Memtable> =
                partition_mt.swap(Arc::new(Memtable::new(capacity_per_partition)));
            flushing_partitions.push(old_arc);
        }

        // Store in immutable_memtables (LOCK-FREE!)
        self.immutable_memtables
            .store(Arc::new(Some(Arc::new(flushing_partitions))));

        true // Successfully swapped
    }
}
