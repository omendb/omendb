use super::{partition_for_key, DBError, FlushTask, Result, DB};
use crate::wal::Record;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::debug;

impl DB {
    /// Apply WAL records to memtable (used by pipelined WAL)
    ///
    /// It applies all records in the batch to the memtable.
    /// Errors are logged but not propagated since this runs in the commit path.
    pub(crate) fn apply_wal_records(&self, records: &[Record]) {
        for record in records {
            self.apply_single_record(record);
        }
    }

    /// Apply a single WAL record to memtable (used by direct WAL path)
    ///
    /// Logs warnings on failure to aid debugging without crashing the write path.
    #[inline]
    fn apply_single_record(&self, record: &Record) {
        match record {
            Record::Put { key, value, seq } => {
                if let Err(e) = self.put_internal(key.clone(), value.clone(), *seq) {
                    tracing::warn!(seq = seq, error = %e, "Failed to apply WAL Put record");
                }
            }
            Record::Delete { key, seq } => {
                if let Err(e) = self.delete_internal(key.clone(), *seq) {
                    tracing::warn!(seq = seq, error = %e, "Failed to apply WAL Delete record");
                }
            }
            Record::Batch {
                base_seq,
                operations,
            } => {
                let mut current_seq = *base_seq;
                for op in operations {
                    let result = match op {
                        crate::wal::BatchOp::Put { key, value } => {
                            self.put_internal(key.clone(), value.clone(), current_seq)
                        }
                        crate::wal::BatchOp::Delete { key } => {
                            self.delete_internal(key.clone(), current_seq)
                        }
                        crate::wal::BatchOp::Merge { key, operand } => {
                            self.merge_internal(key.clone(), operand.clone(), current_seq)
                        }
                    };
                    if let Err(e) = result {
                        tracing::warn!(seq = current_seq, error = %e, "Failed to apply WAL batch operation");
                    }
                    current_seq += 1;
                }
            }
            Record::Merge { key, operand, seq } => {
                if let Err(e) = self.merge_internal(key.clone(), operand.clone(), *seq) {
                    tracing::warn!(seq = seq, error = %e, "Failed to apply WAL Merge record");
                }
            }
        }
    }

    /// Check if write should be stalled or stopped due to backpressure
    ///
    /// Implements two types of backpressure:
    /// 1. **L0 Backpressure**: Slows down or stops writes when L0 has too many files (compaction lag)
    /// 2. **Memtable Backpressure**: Stops writes when memtables are full and flush is in progress
    ///
    /// Has a timeout to prevent indefinite hangs if background workers fail.
    fn check_write_stall(&self) {
        const MAX_STALL_ITERATIONS: u32 = 6000; // ~60 seconds at 10ms sleep
        let mut iterations = 0;

        // Loop until backpressure is relieved or timeout
        loop {
            iterations += 1;
            if iterations > MAX_STALL_ITERATIONS {
                tracing::error!(
                    iterations = iterations,
                    "Write stall timeout exceeded - proceeding to avoid deadlock"
                );
                break;
            }

            // Check worker health
            if !self.compaction_healthy.load(Ordering::SeqCst) {
                tracing::error!(
                    "Compaction worker is dead - breaking stall loop to avoid deadlock"
                );
                break;
            }
            if !self.flush_healthy.load(Ordering::SeqCst) {
                tracing::error!("Flush worker is dead - breaking stall loop to avoid deadlock");
                break;
            }

            // 1. Check L0 stall (Compaction lag)
            let l0_count = {
                let lsm = self.lsm.load();
                if let Some(level) = lsm.level(0) {
                    level.sstables().len()
                } else {
                    0
                }
            };

            if l0_count >= self.options.l0_stop_writes_trigger {
                // STOP writes: compaction is severely lagging
                // Sleep 10ms and retry
                debug!(
                    "Stalling writes: L0 count {} >= {}",
                    l0_count, self.options.l0_stop_writes_trigger
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
                continue;
            } else if l0_count >= self.options.l0_slowdown_writes_trigger {
                // SLOWDOWN writes: compaction is lagging
                // Sleep 1ms per write to throttle throughput
                std::thread::sleep(std::time::Duration::from_millis(1));
                // Don't loop for slowdown, just delay once per write
            }

            // 2. Check Memtable stall (Flush lag)
            // If immutable memtables exist (flush in progress) AND active memtable is full
            let immut_occupied = self.immutable_memtables.load().is_some();

            if immut_occupied {
                // Flush in progress - check if we are also full
                // We can use a relaxed check for "any partition full"
                let active_full = self.memtables.iter().any(|mt| mt.load().should_flush());

                if active_full {
                    // STOP writes: cannot flush because previous flush is still running
                    // Sleep 1ms and retry
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
            }

            // No blocking conditions met
            break;
        }
    }

    /// Write a key-value pair to the database
    ///
    /// Inserts or updates a key-value pair in the database. The write is:
    /// 1. Written to WAL for durability
    /// 2. Added to memtable (in-memory buffer)
    /// 3. Automatically flushed to disk if memtable is full
    ///
    /// # Arguments
    ///
    /// * `key` - The key to write (can be `&[u8]`, `&str`, etc.)
    /// * `value` - The value to write
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success or an error if:
    /// - WAL write fails (disk full, I/O error)
    /// - Automatic flush fails (`SSTable` write error)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use seerdb::{DB, DBOptions};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = DB::open(DBOptions::default())?;
    ///
    /// // Write string keys
    /// db.put("user:1:name", "Alice")?;
    ///
    /// // Write binary keys
    /// db.put(&[0x00, 0x01], &[0xFF, 0xFE])?;
    ///
    /// // Overwrite existing key
    /// db.put("counter", "1")?;
    /// db.put("counter", "2")?;  // Updates value
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`DBError::Wal`]: WAL write failed (disk full, I/O error)
    /// - [`DBError::Io`]: `SSTable` flush failed during automatic flush
    ///
    /// # Performance
    ///
    /// - Typical latency: 10-100 microseconds
    /// - Latency spikes: 1-10 milliseconds during memtable flush
    /// - Use [`flush()`](Self::flush) explicitly to control flush timing
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        // Apply backpressure (stall writes if system is overloaded)
        self.check_write_stall();

        let start = Instant::now();

        let key = Bytes::copy_from_slice(key.as_ref());
        let value = Bytes::copy_from_slice(value.as_ref());

        // Track logical bytes written (user data)
        if !self.options.disable_metrics {
            let logical_bytes = (key.len() + value.len()) as u64;
            self.metrics.record_logical_bytes(logical_bytes);
        }

        // Memory budget enforcement (if configured)
        if let Some(max_memory) = self.options.max_memory_bytes {
            const MAX_MEMORY_WAIT_ITERATIONS: u32 = 3000; // ~30 seconds at 10ms sleep
            let mut iterations = 0;

            loop {
                iterations += 1;
                if iterations > MAX_MEMORY_WAIT_ITERATIONS {
                    tracing::error!(
                        iterations = iterations,
                        "Memory pressure wait timeout - proceeding to avoid deadlock"
                    );
                    break;
                }

                let current_memory = self.estimate_memory_usage();
                let memory_pressure = (current_memory as f64) / (max_memory as f64);

                if memory_pressure >= 0.95 {
                    // CRITICAL: >95% memory usage - block writes until memory freed
                    // This provides backpressure to prevent OOM
                    debug!(
                        "Memory pressure critical: {:.1}% ({} / {} bytes) - blocking write",
                        memory_pressure * 100.0,
                        current_memory,
                        max_memory
                    );

                    // Try to trigger flush to free memory
                    if let Some(ref tx) = self.flush_tx {
                        let _ = tx.send(FlushTask::Flush);
                    }

                    // Sleep briefly to avoid busy-wait
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue; // Recheck memory after sleep
                } else if memory_pressure >= 0.80 {
                    // WARNING: >80% memory usage - trigger early flush
                    debug!(
                        "Memory pressure high: {:.1}% ({} / {} bytes) - triggering flush",
                        memory_pressure * 100.0,
                        current_memory,
                        max_memory
                    );

                    if let Some(ref tx) = self.flush_tx {
                        let _ = tx.send(FlushTask::Flush);
                    }
                    break; // Flush triggered, proceed with write
                }
                // Memory OK, proceed with write
                break;
            }
        }

        // Disk space check (if configured)
        // Uses periodic caching (10s interval) to avoid performance impact
        self.check_disk_space_cached()?;

        // Assign sequence number
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);

        // Create record - use encoded_len() to avoid double-encode
        let record = Record::Put { key, value, seq };
        let wal_bytes = record.encoded_len() as u64;

        if self.options.skip_wal {
            // Skip WAL entirely: maximum write speed, no durability until flush
            // WARNING: Data loss on crash before flush
            self.apply_single_record(&record);
        } else if self.options.use_direct_wal {
            // Direct WAL path: bypass pipelined WAL for single-threaded workloads
            // This eliminates Arc allocation, channel ops, and thread park/unpark overhead
            {
                let mut wal = self.wal.lock().expect("WAL mutex poisoned");
                wal.write(&record).map_err(DBError::Wal)?;
            }
            // Write to memtable directly
            self.apply_single_record(&record);
        } else {
            // Pipelined Group Commit (WAL + Memtable)
            // The callback is executed by the Leader thread for all records in the batch
            // Memtable write happens inside this callback
            self.pipelined_wal
                .put(record, |batch| {
                    self.apply_wal_records(batch);
                })
                .map_err(DBError::Wal)?;
        }

        // Track physical bytes written (WAL bytes if WAL enabled, else 0)
        if !self.options.disable_metrics {
            let physical_bytes = if self.options.skip_wal { 0 } else { wal_bytes };
            self.metrics.record_physical_bytes(physical_bytes);
            self.metrics.record_put(start.elapsed());
        }

        Ok(())
    }

    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        // Apply backpressure (stall writes if system is overloaded)
        self.check_write_stall();

        let start = Instant::now();
        let key = Bytes::copy_from_slice(key.as_ref());

        // Assign sequence number
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);

        // Write to WAL (durability) - lock-free via channel
        let record = Record::Delete { key, seq };

        // Pipelined Group Commit (WAL + Memtable)
        self.pipelined_wal
            .put(record, |batch| {
                self.apply_wal_records(batch);
            })
            .map_err(DBError::Wal)?;

        // Record latency
        if !self.options.disable_metrics {
            self.metrics.record_delete(start.elapsed());
        }

        Ok(())
    }

    /// Merge a value into the database
    ///
    /// Applies a merge operand to a key. The merge logic is defined by the configured
    /// `MergeOperator`.
    ///
    /// # Arguments
    /// * `key` - The key to merge into
    /// * `operand` - The operand to merge
    pub fn merge(&self, key: impl AsRef<[u8]>, operand: impl AsRef<[u8]>) -> Result<()> {
        self.check_write_stall();
        let start = Instant::now();
        let key = Bytes::copy_from_slice(key.as_ref());
        let operand = Bytes::copy_from_slice(operand.as_ref());

        // Assign sequence number
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);

        let record = Record::Merge { key, operand, seq };

        // Pipelined Group Commit
        self.pipelined_wal
            .put(record, |batch| {
                self.apply_wal_records(batch);
            })
            .map_err(DBError::Wal)?;

        // Metrics (reuse put metric for now or add new one)
        if !self.options.disable_metrics {
            self.metrics.record_put(start.elapsed());
        }

        Ok(())
    }

    /// Create a new write batch
    ///
    /// Batches allow atomic writes of multiple operations with better performance
    /// than individual operations. All operations in a batch are written to WAL
    /// and memtable atomically.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use seerdb::{DB, DBOptions};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = DB::open(DBOptions::default())?;
    ///
    /// let mut batch = db.batch();
    /// batch.put(b"key1", b"value1");
    /// batch.put(b"key2", b"value2");
    /// batch.delete(b"key3");
    /// batch.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    ///
    /// Batching is 2-5x faster than individual operations for batches of 100+ operations.
    pub fn batch(&self) -> crate::batch::Batch<'_> {
        crate::batch::Batch::new(self)
    }

    /// Create a new write batch with preallocated capacity
    ///
    /// Use this when you know the approximate number of operations to avoid reallocations.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use seerdb::{DB, DBOptions};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = DB::open(DBOptions::default())?;
    /// let mut batch = db.batch_with_capacity(1000);
    /// for i in 0..1000 {
    ///     batch.put(format!("key_{}", i).as_bytes(), b"value");
    /// }
    /// batch.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn batch_with_capacity(&self, capacity: usize) -> crate::batch::Batch<'_> {
        crate::batch::Batch::with_capacity(self, capacity)
    }

    /// Begin a new optimistic transaction.
    ///
    /// Transactions provide snapshot isolation with write-write conflict detection
    /// at commit time using Optimistic Concurrency Control (OCC).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use seerdb::{DB, DBOptions};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = DB::open(DBOptions::default())?;
    ///
    /// let mut txn = db.begin_transaction();
    ///
    /// // Read a value (recorded for conflict detection)
    /// let value = txn.get(b"key")?;
    ///
    /// // Buffer writes
    /// txn.put(b"key", b"new_value")?;
    ///
    /// // Commit atomically (validates no conflicts)
    /// txn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Conflict Detection
    ///
    /// If another writer modifies a key that was read by this transaction,
    /// commit will fail with `TransactionConflict`. The transaction can then
    /// be retried with fresh data.
    pub fn begin_transaction(&self) -> crate::transaction::Transaction<'_> {
        let start_seq = self.next_seq.load(Ordering::SeqCst);
        let gc_handle = crate::types::SnapshotHandle::new(
            start_seq,
            std::sync::Arc::clone(&self.snapshot_tracker),
        );
        crate::transaction::Transaction::new(self, start_seq, gc_handle)
    }

    pub(crate) fn put_internal(&self, key: Bytes, value: Bytes, seq: u64) -> Result<()> {
        // Track logical bytes written (user data)
        if !self.options.disable_metrics {
            let logical_bytes = (key.len() + value.len()) as u64;
            self.metrics.record_logical_bytes(logical_bytes);
        }

        // Write to correct partition (lock-free with ArcSwap)
        let partition = partition_for_key(&key);
        let mt = self.memtables[partition].load(); // Lock-free Arc load
        mt.put(key, value, seq); // SkipMap is already lock-free
                                 // Arc automatically dropped, no lock to release!

        // Track write operation for Dostoevsky adaptive compaction
        self.write_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Check if ANY partition should be flushed (lock-free check)
        let should_flush = self.memtables.iter().any(|mt| mt.load().should_flush());
        if should_flush {
            if let Some(ref tx) = self.flush_tx {
                // Background flush: swap memtable immediately (fast), then signal background thread
                if self.try_swap_memtable() {
                    // Successfully swapped - signal background thread to build SSTable
                    debug!("Memtable swapped, signaling background flush");
                    let _ = tx.send(FlushTask::Flush);
                }
                // If swap failed, another thread is already flushing - skip
            } else {
                // Synchronous flush: block until done
                self.flush()?;
            }
        }

        Ok(())
    }

    /// Internal delete method (skips WAL write - used by batch)
    ///
    /// Writes tombstone directly to memtable without WAL logging. This is used by the
    /// batch API which handles WAL writes separately.
    pub(crate) fn delete_internal(&self, key: Bytes, seq: u64) -> Result<()> {
        // Write tombstone to correct partition (lock-free with ArcSwap)
        let partition = partition_for_key(&key);
        let mt = self.memtables[partition].load(); // Lock-free Arc load
        mt.delete(key, seq);
        // Arc automatically dropped, no lock to release!

        // Track write operation for Dostoevsky adaptive compaction
        self.write_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Check if ANY partition should be flushed (lock-free check)
        let should_flush = self.memtables.iter().any(|mt| mt.load().should_flush());
        if should_flush {
            if let Some(ref tx) = self.flush_tx {
                // Background flush: swap memtable immediately (fast), then signal background thread
                if self.try_swap_memtable() {
                    // Successfully swapped - signal background thread to build SSTable
                    debug!("Memtable swapped, signaling background flush");
                    let _ = tx.send(FlushTask::Flush);
                }
                // If swap failed, another thread is already flushing - skip
            } else {
                // Synchronous flush: block until done
                self.flush()?;
            }
        }

        Ok(())
    }

    /// Internal merge method (skips WAL write)
    pub(crate) fn merge_internal(&self, key: Bytes, operand: Bytes, seq: u64) -> Result<()> {
        let partition = partition_for_key(&key);
        let mt = self.memtables[partition].load();

        // Append merge operand to memtable with new sequence number
        // Resolution happens at read time
        mt.merge(key, operand, seq);

        // Check flush logic
        let should_flush = self.memtables.iter().any(|mt| mt.load().should_flush());
        if should_flush {
            if let Some(ref tx) = self.flush_tx {
                if self.try_swap_memtable() {
                    debug!("Memtable swapped, signaling background flush");
                    let _ = tx.send(FlushTask::Flush);
                }
            } else {
                self.flush()?;
            }
        }

        Ok(())
    }
}
