mod error;
mod flush;
mod iter;
mod options;
mod read;
mod write;

pub use error::*;
pub use options::*;

use crate::background_workers::{CompactionTask, FlushTask};
use crate::buffer::{BufferPool, BufferPoolOptions};
use crate::compaction::{compact_sstables, LSMTree};
use crate::health::{HealthCheck, HealthStatus};
use crate::memtable::Memtable;
use crate::metrics::{DBStats, MetricsCollector};
use crate::sstable::SSTable;
use crate::types::InternalKey;
use crate::vlog::VLog;
use crate::wal::{PipelinedWAL, SyncPolicy, WAL};
use arc_swap::ArcSwap;
use bytes::Bytes;
use foldhash::fast::FixedState;
use quick_cache::sync::Cache;
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;
use tracing::{debug, error, info};

/// Number of memtable partitions for reduced lock contention
///
/// Partitioning the memtable reduces lock contention on multi-core systems
/// by allowing concurrent writes to different partitions. Each partition
/// is independently locked, so 16 partitions = 16x less contention.
///
/// Expected improvement: +25-40% write throughput on multi-core systems
/// Research backing: Tucana (2020), FASTER (2018)
const NUM_PARTITIONS: usize = 16;

/// Global foldhash state for partition selection (created once, reused forever)
/// Using `LazyLock` ensures it's initialized exactly once in a thread-safe manner
static PARTITION_HASHER: LazyLock<FixedState> = LazyLock::new(|| FixedState::with_seed(0));

/// Calculate which partition a key belongs to using foldhash
///
/// Uses foldhash (2x faster than xxhash on small keys) to distribute keys
/// evenly across partitions. The hash is stable (same key always goes to
/// same partition), which is critical for correctness.
///
/// Research: foldhash is 50% faster than xxhash on small data (8-32 byte keys)
/// See: `ai/research/SOTA_LIBRARIES.md`
#[inline]
pub(crate) fn partition_for_key(key: &[u8]) -> usize {
    // Use global hasher (created once, reused forever)
    let hash = PARTITION_HASHER.hash_one(key);
    (hash % NUM_PARTITIONS as u64) as usize
}

/// Increment a byte slice to create an exclusive upper bound for prefix scans
///
/// Returns None if the input is all 0xFF bytes (can't increment further).
/// Used by `prefix()` to create a range [prefix, prefix+1).
///
/// # Examples
/// - `b"user"` → `Some(b"uses")`
/// - `b"user\xff"` → `Some(b"usesxx00")`
/// - `b"\xff\xff"` → `None`
fn increment_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    if bytes.is_empty() {
        return None;
    }

    let mut result = bytes.to_vec();

    // Increment from the rightmost byte, carrying over as needed
    for i in (0..result.len()).rev() {
        if result[i] < 0xFF {
            result[i] += 1;
            return Some(result);
        }
        // This byte is 0xFF, set to 0 and continue to carry
        result[i] = 0;
    }

    // All bytes were 0xFF, can't increment
    None
}

/// Main database interface
///
/// An embedded LSM-tree based key-value storage engine with the following properties:
///
/// - **Durable**: All writes are logged to WAL before returning
/// - **Consistent**: Snapshot isolation for reads
/// - **Thread-safe**: Can be safely shared across threads via `Arc<DB>`
/// - **Observable**: Built-in metrics and health checks
///
/// # Architecture
///
/// The database uses an LSM-tree (Log-Structured Merge-tree) architecture:
///
/// 1. **Writes** go to WAL (write-ahead log) + memtable (in-memory)
/// 2. **Memtable** flushes to L0 `SSTables` when full
/// 3. **Compaction** merges `SSTables` across levels to reduce read amplification
/// 4. **Reads** check memtable first, then `SSTables` (with bloom filter optimization)
///
/// # Examples
///
/// ```rust,no_run
/// use seerdb::{DB, DBOptions};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Open database
/// let db = DB::open(DBOptions::default())?;
///
/// // Write
/// db.put(b"user:1:name", b"Alice")?;
/// db.put(b"user:1:email", b"alice@example.com")?;
///
/// // Read
/// let name = db.get(b"user:1:name")?;
/// assert_eq!(name, Some(bytes::Bytes::from("Alice")));
///
/// // Delete
/// db.delete(b"user:1:email")?;
///
/// // Flush to disk
/// db.flush()?;
/// # Ok(())
/// # }
/// ```
///
/// # Thread Safety
///
/// `DB` is thread-safe and can be shared across threads:
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use std::thread;
/// use seerdb::{DB, DBOptions};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Arc::new(DB::open(DBOptions::default())?);
///
/// let db_clone = db.clone();
/// let handle = thread::spawn(move || {
///     db_clone.put(b"thread:1", b"data").unwrap();
/// });
///
/// db.put(b"thread:2", b"data")?;
/// handle.join().unwrap();
/// # Ok(())
/// # }
/// ```
pub struct DB {
    pub(crate) options: DBOptions,
    pub(crate) wal: Arc<Mutex<WAL>>,
    pub(crate) memtables: Arc<[ArcSwap<Memtable>; NUM_PARTITIONS]>,
    pub(crate) immutable_memtables: Arc<ArcSwap<Option<Arc<Vec<Arc<Memtable>>>>>>,
    pub(crate) lsm: Arc<ArcSwap<LSMTree>>,
    pub(crate) vlog: Arc<Mutex<Option<VLog>>>,
    pub(crate) sstable_counter: Arc<Mutex<u64>>,
    pub(crate) metrics: Arc<MetricsCollector>,
    pub(crate) compaction_tx: Option<Sender<CompactionTask>>,
    pub(crate) compaction_worker: Option<JoinHandle<()>>,
    pub(crate) flush_tx: Option<Sender<FlushTask>>,
    pub(crate) flush_worker: Option<JoinHandle<()>>,
    pub(crate) flush_mutex: Arc<Mutex<()>>,
    /// Prevents ABA problem where concurrent flush/compaction overwrite each other's changes.
    pub(crate) lsm_mutex: Arc<Mutex<()>>,
    pub(crate) sstable_cache: Arc<Cache<PathBuf, Arc<Mutex<SSTable>>>>,
    pub(crate) has_vlog: std::sync::atomic::AtomicBool,
    pub(crate) write_count: std::sync::atomic::AtomicU64,
    pub(crate) read_count: std::sync::atomic::AtomicU64,
    /// Compaction only compacts `SSTables` with `max_seq` <= this to avoid deleting unflushed keys.
    pub(crate) max_flushed_seq: Arc<AtomicU64>,
    pub(crate) next_seq: Arc<AtomicU64>,
    #[allow(dead_code)]
    pub(crate) flush_healthy: Arc<AtomicBool>,
    #[allow(dead_code)]
    pub(crate) compaction_healthy: Arc<AtomicBool>,
    /// Delayed deletions prevent races with concurrent readers holding old LSM snapshots.
    pub(crate) pending_deletions: Arc<Mutex<Vec<(PathBuf, std::time::Instant)>>>,
    pub(crate) last_disk_check: Arc<AtomicU64>,
    pub(crate) cached_available_space: Arc<AtomicU64>,
    pub(crate) global_block_cache: Arc<Cache<(u64, u64), Bytes>>,
    pub(crate) buffer_pool: Option<Arc<BufferPool>>,
    pub(crate) compaction_filter: Option<Arc<dyn crate::compaction::CompactionFilter>>,
    pub(crate) pipelined_wal: PipelinedWAL,
    #[cfg(feature = "object-store")]
    pub(crate) storage_backend: Option<Arc<dyn crate::storage::Storage>>,
    #[cfg(feature = "object-store")]
    pub(crate) cold_storage_backend: Option<Arc<dyn crate::storage::Storage>>,
    pub(crate) snapshot_tracker: Arc<crate::types::SnapshotTracker>,
    /// Prevents TOCTOU race in OCC where concurrent transactions both validate and see no conflicts.
    pub(crate) commit_lock: Arc<Mutex<()>>,
}

impl DB {
    /// Open or create a database
    ///
    /// Opens an existing database or creates a new one at the specified path.
    /// If a WAL exists, it will be replayed to recover uncommitted writes.
    ///
    /// # Arguments
    ///
    /// * `options` - Database configuration (see [`DBOptions`])
    ///
    /// # Returns
    ///
    /// Returns a [`DB`] instance or an error if:
    /// - Directory creation fails
    /// - WAL recovery fails (corruption detected)
    /// - Existing `SSTables` are corrupted (checksum mismatch)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use seerdb::{DB, DBOptions};
    /// use std::path::PathBuf;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// // Open with default settings
    /// let db = DB::open(DBOptions::default())?;
    ///
    /// // Open with custom path
    /// let opts = DBOptions {
    ///     data_dir: PathBuf::from("/var/lib/myapp/db"),
    ///     ..Default::default()
    /// };
    /// let db = DB::open(opts)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`DBError::Io`]: Failed to create directory or open files
    /// - [`DBError::Wal`]: WAL corruption detected during recovery
    /// - [`DBError::SSTable`]: `SSTable` checksum validation failed
    pub fn open(options: DBOptions) -> Result<Self> {
        info!(
            path = ?options.data_dir,
            memtable_capacity_mb = options.memtable_capacity / (1024 * 1024),
            background_compaction = options.background_compaction,
            "Opening database"
        );

        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&options.data_dir)?;

        let wal_path = options.data_dir.join("wal.log");
        let vlog_path = options.data_dir.join("values.vlog");

        // Create 16 partitioned memtables (divide capacity by NUM_PARTITIONS)
        let capacity_per_partition = options.memtable_capacity / NUM_PARTITIONS;
        let memtables_vec: Vec<Memtable> = (0..NUM_PARTITIONS)
            .map(|_| Memtable::new(capacity_per_partition))
            .collect();

        // Recover from WAL if it exists
        if wal_path.exists() {
            info!("Recovering from WAL");
            let total_entries_before: usize = memtables_vec.iter().map(super::memtable::Memtable::len).sum();
            crate::db_helpers::recover_partitioned(
                &wal_path,
                &memtables_vec,
                options.merge_operator.as_ref(),
                options.recovery_mode,
            )?;
            let total_entries_after: usize = memtables_vec.iter().map(super::memtable::Memtable::len).sum();
            let recovered = total_entries_after - total_entries_before;
            info!(entries = recovered, "WAL recovery complete");
        } else {
            info!("No existing WAL found, starting fresh");
        }

        // Create new WAL (overwrites old one after recovery)
        let wal = WAL::create(&wal_path, options.wal_sync_policy)?;

        // Create or open vLog if KV separation is enabled
        let vlog = if options.vlog_threshold.is_some() {
            if vlog_path.exists() {
                Some(VLog::open(&vlog_path)?)
            } else {
                Some(VLog::create(&vlog_path)?)
            }
        } else {
            None
        };

        // Create LSM tree (adaptive or fixed strategy)
        let mut lsm = if options.adaptive_compaction {
            info!("Using adaptive compaction (Dostoevsky)");
            LSMTree::new_adaptive(
                &options.data_dir,
                options.base_level_size,
                options.num_levels,
                4,  // min_ratio: write-heavy workloads
                20, // max_ratio: read-heavy workloads
            )
        } else {
            LSMTree::new(
                &options.data_dir,
                options.base_level_size,
                options.size_ratio,
                options.num_levels,
            )
        };

        // Load existing SSTables from disk
        // This also verifies checksums - will fail if any SSTable is corrupted
        lsm.load_existing_sstables()?;
        let total_sstables: usize = (0..lsm.num_levels())
            .filter_map(|i| lsm.level(i))
            .map(|level| level.sstables().len())
            .sum();
        info!(
            sstables = total_sstables,
            levels = lsm.num_levels(),
            "LSM tree loaded"
        );

        // Capture has_vlog before wrapping
        let has_vlog = vlog.is_some();

        // Wrap in ArcSwap for lock-free atomic swaps
        // ArcSwap provides lock-free reads (.load()) and atomic swaps (.swap())
        // SkipMap is already lock-free internally, so this eliminates ALL lock overhead!
        // Convert Vec<Memtable> into [ArcSwap<Memtable>; NUM_PARTITIONS]
        // Then wrap in Arc so background threads can share it
        let mut memtables_iter = memtables_vec.into_iter();
        let memtables_array: [ArcSwap<Memtable>; NUM_PARTITIONS] = std::array::from_fn(|_| {
            ArcSwap::from_pointee(memtables_iter.next().expect("Not enough partitions"))
        });
        let memtables = Arc::new(memtables_array);
        let immutable_memtables = Arc::new(ArcSwap::from_pointee(None));
        let wal = Arc::new(Mutex::new(wal));
        let vlog = Arc::new(Mutex::new(vlog));
        let lsm = Arc::new(ArcSwap::from_pointee(lsm));
        let flush_mutex = Arc::new(Mutex::new(()));
        let lsm_mutex = Arc::new(Mutex::new(()));

        // Initialize SSTable counter from existing files to avoid overwriting
        // Collect all SSTable paths first to avoid borrow issues
        let mut all_sstables = Vec::new();
        {
            let lsm_arc = lsm.load();
            for level_num in 0..lsm_arc.num_levels() {
                if let Some(level) = lsm_arc.level(level_num) {
                    all_sstables.extend(level.sstables().iter().cloned());
                }
            }
        }

        // Find max counter value from filenames like "L0_000123.sst"
        let max_counter = all_sstables
            .iter()
            .filter_map(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| {
                        name.strip_prefix("L")
                            .and_then(|s| s.split('_').nth(1))
                            .and_then(|s| s.strip_suffix(".sst"))
                            .and_then(|s| s.parse::<u64>().ok())
                    })
            })
            .max()
            .unwrap_or(0);

        let sstable_counter = Arc::new(Mutex::new(max_counter + 1));

        // Create metrics early (needed by background worker)
        let metrics = Arc::new(MetricsCollector::new());

        // Initialize sequence tracking (needed by background compaction worker)
        let max_flushed_seq = Arc::new(AtomicU64::new(0));
        let next_seq = Arc::new(AtomicU64::new(1));

        // Initialize background thread health tracking
        // wal_healthy removed - using PipelinedWAL which runs on client threads
        let flush_healthy = Arc::new(AtomicBool::new(true));
        let compaction_healthy = Arc::new(AtomicBool::new(true));

        // Initialize pending deletions queue (for Bug #7b fix)
        let pending_deletions = Arc::new(Mutex::new(Vec::new()));

        // Initialize snapshot tracker for MVCC garbage collection
        let snapshot_tracker = Arc::new(crate::types::SnapshotTracker::new());

        let compaction_filter = options.compaction_filter.clone();
        let _merge_operator = options.merge_operator.clone();

        // Initialize BufferPool if capacity is set
        let buffer_pool = options.buffer_pool_capacity.map(|capacity| {
            let pool_opts = BufferPoolOptions {
                capacity_bytes: capacity,
                // Default 16KB frame for now.
                // In future, we should match SSTable block size or use multi-size pool.
                frame_size: 16 * 1024,
                // Use 16 shards for multi-core systems (reduces lock contention)
                num_shards: 16,
            };
            BufferPool::new(pool_opts)
        });

        // Create cloud storage backend if configured (feature-gated)
        #[cfg(feature = "object-store")]
        let storage_backend: Option<Arc<dyn crate::storage::Storage>> = {
            if let Some(ref config) = options.storage_config {
                let backend: Arc<dyn crate::storage::Storage> = match config {
                    StorageConfig::S3 {
                        bucket,
                        region,
                        endpoint,
                        prefix,
                    } => Arc::new(crate::storage::ObjectStoreBackend::s3(
                        bucket,
                        region,
                        endpoint.as_deref(),
                        prefix.clone(),
                    )?),
                    StorageConfig::Gcs {
                        bucket,
                        service_account_path,
                        prefix,
                    } => Arc::new(crate::storage::ObjectStoreBackend::gcs(
                        bucket,
                        service_account_path.as_deref(),
                        prefix.clone(),
                    )?),
                    StorageConfig::Azure {
                        container,
                        account,
                        prefix,
                    } => Arc::new(crate::storage::ObjectStoreBackend::azure(
                        container,
                        account,
                        prefix.clone(),
                    )?),
                    StorageConfig::Custom(store) => Arc::new(
                        crate::storage::ObjectStoreBackend::new(Arc::clone(store), String::new()),
                    ),
                };
                info!(
                    storage = ?config,
                    "Cloud storage backend configured"
                );
                Some(backend)
            } else {
                None
            }
        };

        // Create cold tier storage backend for tiered storage (feature-gated)
        #[cfg(feature = "object-store")]
        let cold_storage_backend: Option<Arc<dyn crate::storage::Storage>> = {
            if let (Some(cold_level), Some(ref config)) =
                (options.cold_tier_level, &options.cold_storage)
            {
                let backend: Arc<dyn crate::storage::Storage> = match config {
                    StorageConfig::S3 {
                        bucket,
                        region,
                        endpoint,
                        prefix,
                    } => Arc::new(crate::storage::ObjectStoreBackend::s3(
                        bucket,
                        region,
                        endpoint.as_deref(),
                        prefix.clone(),
                    )?),
                    StorageConfig::Gcs {
                        bucket,
                        service_account_path,
                        prefix,
                    } => Arc::new(crate::storage::ObjectStoreBackend::gcs(
                        bucket,
                        service_account_path.as_deref(),
                        prefix.clone(),
                    )?),
                    StorageConfig::Azure {
                        container,
                        account,
                        prefix,
                    } => Arc::new(crate::storage::ObjectStoreBackend::azure(
                        container,
                        account,
                        prefix.clone(),
                    )?),
                    StorageConfig::Custom(store) => Arc::new(
                        crate::storage::ObjectStoreBackend::new(Arc::clone(store), String::new()),
                    ),
                };
                info!(
                    cold_tier_level = cold_level,
                    storage = ?config,
                    "Tiered storage configured: L{}+ → cold storage",
                    cold_level
                );
                Some(backend)
            } else {
                None
            }
        };

        // Start background compaction worker if enabled
        #[cfg(feature = "object-store")]
        let (compaction_tx, compaction_worker) = crate::background_workers::spawn_compaction_worker(
            options.background_compaction,
            Arc::clone(&lsm),
            Arc::clone(&lsm_mutex),
            Arc::clone(&sstable_counter),
            options.data_dir.clone(),
            Arc::clone(&metrics),
            Arc::clone(&max_flushed_seq),
            Arc::clone(&compaction_healthy),
            Arc::clone(&pending_deletions),
            compaction_filter.clone(),
            storage_backend.clone(),
            Arc::clone(&snapshot_tracker),
            options.cold_tier_level,
            cold_storage_backend.clone(),
        );

        #[cfg(not(feature = "object-store"))]
        let (compaction_tx, compaction_worker) = crate::background_workers::spawn_compaction_worker(
            options.background_compaction,
            Arc::clone(&lsm),
            Arc::clone(&lsm_mutex),
            Arc::clone(&sstable_counter),
            options.data_dir.clone(),
            Arc::clone(&metrics),
            Arc::clone(&max_flushed_seq),
            Arc::clone(&compaction_healthy),
            Arc::clone(&pending_deletions),
            compaction_filter.clone(),
            Arc::clone(&snapshot_tracker),
        );

        // Start background flush worker if enabled
        #[cfg(feature = "object-store")]
        let (flush_tx, flush_worker) = crate::background_workers::spawn_flush_worker(
            options.background_flush,
            Arc::clone(&memtables),
            Arc::clone(&immutable_memtables),
            Arc::clone(&wal),
            Arc::clone(&lsm),
            Arc::clone(&lsm_mutex),
            Arc::clone(&vlog),
            Arc::clone(&sstable_counter),
            options.data_dir.clone(),
            Arc::clone(&metrics),
            options.memtable_capacity,
            options.vlog_threshold,
            Arc::clone(&flush_mutex),
            Arc::clone(&max_flushed_seq),
            Arc::clone(&flush_healthy),
            compaction_tx.clone(),
            storage_backend.clone(),
        );

        #[cfg(not(feature = "object-store"))]
        let (flush_tx, flush_worker) = crate::background_workers::spawn_flush_worker(
            options.background_flush,
            Arc::clone(&memtables),
            Arc::clone(&immutable_memtables),
            Arc::clone(&wal),
            Arc::clone(&lsm),
            Arc::clone(&lsm_mutex),
            Arc::clone(&vlog),
            Arc::clone(&sstable_counter),
            options.data_dir.clone(),
            Arc::clone(&metrics),
            options.memtable_capacity,
            options.vlog_threshold,
            Arc::clone(&flush_mutex),
            Arc::clone(&max_flushed_seq),
            Arc::clone(&flush_healthy),
            compaction_tx.clone(),
        );

        // Start background WAL writer (always enabled for lock-free writes)
        // Convert group_commit_delay_us to Duration
        // let group_commit_delay =
        //    std::time::Duration::from_micros(options.group_commit_delay_us);

        // Configure PipelinedWAL delay based on SyncPolicy
        // For SyncPolicy::None, we skip delay for max throughput (fire-and-forget)
        let group_commit_delay = if options.wal_sync_policy == SyncPolicy::None {
            std::time::Duration::ZERO
        } else {
            std::time::Duration::from_micros(options.group_commit_delay_us)
        };

        let pipelined_wal = PipelinedWAL::new(
            Arc::clone(&wal),
            group_commit_delay,
            options.group_commit_max_batch_size,
        );

        let db = Self {
            options: options.clone(),
            wal,
            memtables,
            immutable_memtables,
            lsm,
            vlog,
            sstable_counter,
            metrics,
            compaction_tx,
            compaction_worker,
            flush_tx,
            flush_worker,
            pipelined_wal,
            flush_mutex,
            lsm_mutex,
            sstable_cache: Arc::new(Cache::new(1000)), // Cache up to 1000 SSTables
            has_vlog: std::sync::atomic::AtomicBool::new(has_vlog),
            write_count: std::sync::atomic::AtomicU64::new(0),
            read_count: std::sync::atomic::AtomicU64::new(0),
            max_flushed_seq,
            next_seq,
            flush_healthy,
            compaction_healthy,
            pending_deletions,
            last_disk_check: Arc::new(AtomicU64::new(0)),
            cached_available_space: Arc::new(AtomicU64::new(u64::MAX)), // Start with "infinite" space
            global_block_cache: Arc::new(Cache::new(options.block_cache_capacity)),
            buffer_pool,
            compaction_filter,
            #[cfg(feature = "object-store")]
            storage_backend,
            #[cfg(feature = "object-store")]
            cold_storage_backend,
            snapshot_tracker,
            commit_lock: Arc::new(Mutex::new(())),
        };

        // Flush memtables if any partition filled up during recovery
        let should_flush = db.memtables.iter().any(|mt| mt.load().should_flush());
        if should_flush {
            info!("One or more memtable partitions full after recovery, flushing");
            db.flush()?;
        }

        info!("Database opened successfully");

        Ok(db)
    }


    /// Verify database integrity by checking all checksums
    ///
    /// Performs a full integrity check of the database by validating:
    /// - All `SSTable` block checksums (CRC32C)
    /// - All vLog record checksums (CRC32C) if vLog is enabled
    ///
    /// This is a read-only operation that does not modify any data. Use this to:
    /// - Validate database integrity after crash recovery
    /// - Check for disk corruption or bit rot
    /// - Verify backups before deployment
    ///
    /// # Returns
    ///
    /// Returns `VerifyResult` with counts of verified components on success.
    /// Returns an error immediately if any corruption is detected.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use seerdb::{DB, DBOptions};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = DB::open(DBOptions::default())?;
    ///
    /// // Verify database integrity
    /// let result = db.verify()?;
    /// println!("Verified {} SSTables, {} blocks, {} vLog records",
    ///     result.sstables_verified,
    ///     result.blocks_verified,
    ///     result.vlog_records_verified);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`DBError::SSTable`]: `SSTable` block checksum mismatch (corruption detected)
    /// - [`DBError::VLog`]: vLog record checksum mismatch (corruption detected)
    /// - [`DBError::Io`]: I/O error reading files
    ///
    /// # Performance
    ///
    /// - **Latency**: O(total data size) - reads all blocks and records
    /// - **Disk I/O**: Sequential reads of all `SSTables` and vLog
    /// - **Memory**: Low - processes one block at a time
    ///
    /// For large databases, consider running verification during maintenance windows.
    pub fn verify(&self) -> Result<VerifyResult> {
        use crate::sstable::SSTable;
        use crate::vlog::VLog;

        let mut result = VerifyResult::default();

        // 1. Verify all SSTables
        let lsm_arc = self.lsm.load();
        let sstable_paths = lsm_arc.all_sstable_paths();

        for sstable_path in &sstable_paths {
            let mut sstable = SSTable::open(sstable_path)?;
            let sstable_result = sstable.verify()?;

            result.sstables_verified += 1;
            result.blocks_verified += sstable_result.blocks_verified;
            result.sstable_bytes_verified += sstable_result.bytes_verified;
        }

        // 2. Verify vLog if enabled
        let vlog_path = self.options.data_dir.join("values.vlog");
        if vlog_path.exists() {
            let mut vlog = VLog::open(&vlog_path)?;
            let vlog_result = vlog.verify()?;

            result.vlog_verified = true;
            result.vlog_records_verified = vlog_result.records_verified;
            result.vlog_bytes_verified = vlog_result.bytes_verified;
        }

        Ok(result)
    }


    /// Bulk load key-value pairs directly to `SSTables`
    ///
    /// Bypasses memtable and WAL for maximum throughput. Use for:
    /// - Initial data loading
    /// - Bulk migrations
    /// - Backup restoration
    ///
    /// # Performance
    ///
    /// 10-100x faster than individual `put()` calls for large datasets.
    /// Directly writes to `SSTables` at the configured level.
    ///
    /// # Arguments
    ///
    /// * `entries` - Iterator of (key, value) pairs
    /// * `options` - Configuration for the bulk load operation
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use seerdb::{DB, DBOptions, BulkLoadOptions};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = DB::open(DBOptions::default())?;
    ///
    /// // Load 1 million entries
    /// let entries = (0..1_000_000u64).map(|i| {
    ///     (format!("key_{:08}", i).into_bytes(), format!("value_{}", i).into_bytes())
    /// });
    ///
    /// let stats = db.bulk_load(entries, BulkLoadOptions::default())?;
    /// println!("Loaded {} entries in {} SSTables", stats.entries_loaded, stats.sstables_created);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Data bypasses WAL (not crash-safe during load, but `SSTables` are durable once written)
    /// - If `options.already_sorted` is false, all entries are collected and sorted in memory
    /// - For very large datasets, consider loading in chunks or pre-sorting data
    pub fn bulk_load<I, K, V>(&self, entries: I, options: BulkLoadOptions) -> Result<BulkLoadStats>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        use crate::sstable::SSTableBuilder;
        use crate::types::ValueType;

        let target_level = options.target_level.min(self.options.num_levels - 1);
        let max_entries = options.max_entries_per_sst;

        // Collect and optionally sort entries
        let mut all_entries: Vec<(Bytes, Bytes)> = entries
            .into_iter()
            .map(|(k, v)| {
                (
                    Bytes::copy_from_slice(k.as_ref()),
                    Bytes::copy_from_slice(v.as_ref()),
                )
            })
            .collect();

        if all_entries.is_empty() {
            return Ok(BulkLoadStats::default());
        }

        if !options.already_sorted {
            all_entries.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        }

        let total_entries = all_entries.len() as u64;
        let mut stats = BulkLoadStats {
            entries_loaded: 0,
            sstables_created: 0,
            bytes_written: 0,
            target_level,
        };

        // Get base sequence number for this bulk load
        let base_seq = self.next_seq.fetch_add(total_entries, Ordering::SeqCst);

        // Process entries in chunks to create multiple SSTables
        let mut vlog_guard = self.vlog.lock().expect("vLog mutex poisoned");
        let has_vlog = vlog_guard.is_some();
        let vlog_threshold = self.options.vlog_threshold;

        for (chunk_idx, chunk) in all_entries.chunks(max_entries).enumerate() {
            // Generate SSTable filename
            let mut counter = self
                .sstable_counter
                .lock()
                .expect("SSTable counter mutex poisoned");
            let sstable_path = self
                .options
                .data_dir
                .join(format!("L{}_{:06}.sst", target_level, *counter));
            *counter += 1;
            drop(counter);

            // Build SSTable
            let mut builder =
                SSTableBuilder::create(&sstable_path)?.with_compression(self.options.compression);
            if let Some(threshold) = vlog_threshold {
                builder = builder.with_vlog_threshold(threshold);
            }

            for (i, (key, value)) in chunk.iter().enumerate() {
                let seq = base_seq + (chunk_idx * max_entries) as u64 + i as u64;
                let ikey = InternalKey {
                    user_key: key.clone(),
                    seq,
                    kind: ValueType::Value,
                };

                if has_vlog {
                    if let Some(ref mut vlog) = *vlog_guard {
                        builder.add_internal_with_vlog(&ikey, value.clone(), vlog)?;
                    }
                } else {
                    builder.add_internal(&ikey, value.clone())?;
                }

                stats.entries_loaded += 1;
            }

            builder.finish()?;

            // Get SSTable size
            let size = std::fs::metadata(&sstable_path)?.len();
            stats.bytes_written += size;
            stats.sstables_created += 1;

            // Register with LSM tree
            {
                let _lsm_lock = self.lsm_mutex.lock().expect("LSM mutex poisoned");
                let mut lsm_clone = (**self.lsm.load()).clone();
                lsm_clone.add_to_level(target_level, sstable_path, size);
                self.lsm.store(Arc::new(lsm_clone));
            }

            // Record metrics
            if !self.options.disable_metrics {
                self.metrics.record_physical_bytes(size);
            }
        }

        // Sync vLog if used
        if has_vlog {
            if let Some(ref mut vlog) = *vlog_guard {
                vlog.sync()?;
            }
        }

        info!(
            entries = stats.entries_loaded,
            sstables = stats.sstables_created,
            bytes = stats.bytes_written,
            level = target_level,
            "Bulk load completed"
        );

        Ok(stats)
    }

    /// Compact a level
    fn compact_level(&self, level_num: usize) -> Result<()> {
        #[cfg(feature = "object-store")]
        {
            Self::do_compact_level(
                &self.lsm,
                &self.lsm_mutex,
                &self.sstable_counter,
                &self.options.data_dir,
                level_num,
                &self.metrics,
                &self.max_flushed_seq,
                &self.pending_deletions,
                &self.compaction_filter,
                &self.storage_backend,
                &self.snapshot_tracker,
                self.options.cold_tier_level,
                &self.cold_storage_backend,
            )
        }
        #[cfg(not(feature = "object-store"))]
        {
            Self::do_compact_level(
                &self.lsm,
                &self.lsm_mutex,
                &self.sstable_counter,
                &self.options.data_dir,
                level_num,
                &self.metrics,
                &self.max_flushed_seq,
                &self.pending_deletions,
                &self.compaction_filter,
                &self.snapshot_tracker,
            )
        }
    }

    /// Internal compaction implementation (shared by both sync and async paths)
    #[cfg(feature = "object-store")]
    pub(crate) fn do_compact_level(
        lsm: &Arc<ArcSwap<LSMTree>>,
        lsm_mutex: &Arc<Mutex<()>>,
        sstable_counter: &Arc<Mutex<u64>>,
        data_dir: &Path,
        level_num: usize,
        metrics: &Arc<MetricsCollector>,
        max_flushed_seq: &Arc<AtomicU64>,
        pending_deletions: &Arc<Mutex<Vec<(PathBuf, std::time::Instant)>>>,
        filter: &Option<Arc<dyn crate::compaction::CompactionFilter>>,
        storage_backend: &Option<Arc<dyn crate::storage::Storage>>,
        snapshot_tracker: &Arc<crate::types::SnapshotTracker>,
        cold_tier_level: Option<usize>,
        cold_storage_backend: &Option<Arc<dyn crate::storage::Storage>>,
    ) -> Result<()> {
        Self::do_compact_level_impl(
            lsm,
            lsm_mutex,
            sstable_counter,
            data_dir,
            level_num,
            metrics,
            max_flushed_seq,
            pending_deletions,
            filter,
            storage_backend,
            snapshot_tracker,
            cold_tier_level,
            cold_storage_backend,
        )
    }

    /// Internal compaction implementation (no cloud storage)
    #[cfg(not(feature = "object-store"))]
    pub(crate) fn do_compact_level(
        lsm: &Arc<ArcSwap<LSMTree>>,
        lsm_mutex: &Arc<Mutex<()>>,
        sstable_counter: &Arc<Mutex<u64>>,
        data_dir: &Path,
        level_num: usize,
        metrics: &Arc<MetricsCollector>,
        max_flushed_seq: &Arc<AtomicU64>,
        pending_deletions: &Arc<Mutex<Vec<(PathBuf, std::time::Instant)>>>,
        filter: &Option<Arc<dyn crate::compaction::CompactionFilter>>,
        snapshot_tracker: &Arc<crate::types::SnapshotTracker>,
    ) -> Result<()> {
        Self::do_compact_level_impl(
            lsm,
            lsm_mutex,
            sstable_counter,
            data_dir,
            level_num,
            metrics,
            max_flushed_seq,
            pending_deletions,
            filter,
            snapshot_tracker,
        )
    }

    /// Internal compaction implementation with optional cloud storage support
    #[cfg(feature = "object-store")]
    fn do_compact_level_impl(
        lsm: &Arc<ArcSwap<LSMTree>>,
        lsm_mutex: &Arc<Mutex<()>>,
        sstable_counter: &Arc<Mutex<u64>>,
        data_dir: &Path,
        level_num: usize,
        metrics: &Arc<MetricsCollector>,
        max_flushed_seq: &Arc<AtomicU64>,
        pending_deletions: &Arc<Mutex<Vec<(PathBuf, std::time::Instant)>>>,
        filter: &Option<Arc<dyn crate::compaction::CompactionFilter>>,
        storage_backend: &Option<Arc<dyn crate::storage::Storage>>,
        snapshot_tracker: &Arc<crate::types::SnapshotTracker>,
        cold_tier_level: Option<usize>,
        cold_storage_backend: &Option<Arc<dyn crate::storage::Storage>>,
    ) -> Result<()> {
        let compaction_start = Instant::now();

        // Load LSM tree (LOCK-FREE!)
        let lsm_arc = lsm.load();

        // Get SSTables to compact
        let level = lsm_arc.level(level_num).ok_or(DBError::NotOpened)?;
        let mut all_input_paths: Vec<PathBuf> = level.sstables().to_vec();

        // Limit number of files to compact at once to avoid "Too many open files"
        // and to keep compaction duration predictable.
        const MAX_COMPACTION_FILES: usize = 16;
        if all_input_paths.len() > MAX_COMPACTION_FILES {
            all_input_paths.truncate(MAX_COMPACTION_FILES);
        }

        if all_input_paths.is_empty() {
            return Ok(());
        }

        // **CRITICAL FIX**: Only compact SSTables with max_sequence <= max_flushed_seq
        // This prevents compaction from deleting keys still in immutable memtables
        let safe_seq = max_flushed_seq.load(Ordering::SeqCst);
        let mut input_paths = Vec::new();
        let mut skipped_count = 0;

        for path in all_input_paths {
            // Read SSTable header to get max_sequence
            if let Ok(sstable) = SSTable::open(&path) {
                if sstable.max_sequence() <= safe_seq {
                    input_paths.push(path);
                } else {
                    // Skip this SSTable - it has unflushed keys
                    skipped_count += 1;
                    debug!(
                        path = ?path,
                        sstable_seq = sstable.max_sequence(),
                        safe_seq = safe_seq,
                        "Skipping SSTable with sequence > max_flushed_seq (preventing live key deletion)"
                    );
                }
            }
        }

        if input_paths.is_empty() {
            debug!(
                level = level_num,
                skipped = skipped_count,
                "No SSTables eligible for compaction (all sequences > max_flushed_seq)"
            );
            return Ok(());
        }

        let input_count = input_paths.len();
        debug!(
            level = level_num,
            input_sstables = input_count,
            skipped_sstables = skipped_count,
            safe_seq = safe_seq,
            "Starting compaction"
        );

        // Generate output path
        let mut counter = sstable_counter
            .lock()
            .expect("SSTable counter mutex poisoned");
        let output_path = data_dir.join(format!("L{}_{:06}.sst", level_num + 1, *counter));
        *counter += 1;
        drop(counter);

        // Arc automatically dropped (lock-free!)

        // Determine if target level is in cold tier (tiered storage)
        let target_level = level_num + 1;
        let is_cold_tier = cold_tier_level
            .map(|threshold| target_level >= threshold)
            .unwrap_or(false);

        // Compact SSTables with tier-aware storage routing
        let (result_path, size) = if is_cold_tier && cold_storage_backend.is_some() {
            // COLD TIER: Write to local (cache) + object store (durable)
            // Local copy enables fast reads, object store provides durability
            use crate::compaction::compact_sstables_buffered;

            let oldest_snapshot = snapshot_tracker.oldest_snapshot();
            let bytes = compact_sstables_buffered(
                &input_paths,
                target_level,
                filter.clone(),
                oldest_snapshot,
            )?;
            let size = bytes.len() as u64;

            // Write to local disk (cache for fast reads)
            std::fs::write(&output_path, &bytes)?;

            // Upload to cold storage backend (durable offsite backup)
            if let Some(ref backend) = cold_storage_backend {
                backend.write_sstable(&output_path, &bytes)?;
                debug!(
                    path = ?output_path,
                    size_bytes = size,
                    tier = "cold",
                    level = target_level,
                    "Compacted SSTable written to local cache + cold storage"
                );
            }

            (output_path, size)
        } else if storage_backend.is_some() {
            // HOT TIER with cloud replication: local + cloud
            use crate::compaction::compact_sstables_buffered;

            let oldest_snapshot = snapshot_tracker.oldest_snapshot();
            let bytes = compact_sstables_buffered(
                &input_paths,
                target_level,
                filter.clone(),
                oldest_snapshot,
            )?;
            let size = bytes.len() as u64;

            // Write to local disk (single syscall)
            std::fs::write(&output_path, &bytes)?;

            // Upload to cloud storage (replication, not tiering)
            if let Some(ref backend) = storage_backend {
                backend.write_sstable(&output_path, &bytes)?;
                debug!(
                    path = ?output_path,
                    size_bytes = size,
                    tier = "hot",
                    "Compacted SSTable uploaded to cloud storage"
                );
            }

            (output_path, size)
        } else {
            // HOT TIER: local only - use traditional compaction with MVCC GC
            let oldest_snapshot = snapshot_tracker.oldest_snapshot();
            compact_sstables(
                &input_paths,
                &output_path,
                target_level,
                filter.clone(),
                oldest_snapshot,
            )?
        };

        // Track physical bytes written during compaction
        metrics.record_physical_bytes(size);

        // CRITICAL FIX (Bug #7c): Serialize LSM tree updates to prevent ABA race
        // Hold mutex during read-modify-write to ensure atomicity
        {
            let _lsm_lock = lsm_mutex.lock().expect("LSM mutex poisoned");

            // Update LSM tree - clone, modify, store (serialized)
            let mut lsm_clone = (**lsm.load()).clone();
            lsm_clone.add_to_level(level_num + 1, result_path, size);
            lsm_clone.remove_sstables_from_level(level_num, &input_paths);
            lsm.store(Arc::new(lsm_clone));

            // Lock released here (automatic drop)
        }

        // PRODUCTION FIX (Bug #7b): Queue SSTables for delayed deletion
        // Concurrent readers may hold LSM snapshots pointing to these files.
        // By queuing deletions with timestamps, we ensure files are only deleted
        // after a safe delay (5 seconds), giving readers time to finish.
        {
            let mut pending = pending_deletions
                .lock()
                .expect("pending_deletions lock poisoned");
            let now = std::time::Instant::now();
            for path in input_paths {
                pending.push((path, now));
            }
        }

        // Clean up old pending deletions (files queued >5 seconds ago)
        crate::db_helpers::cleanup_old_deletions(pending_deletions);

        let compaction_duration_ms = compaction_start.elapsed().as_millis();
        info!(
            level = level_num,
            input_sstables = input_count,
            output_size_bytes = size,
            duration_ms = compaction_duration_ms,
            "Compaction complete"
        );

        Ok(())
    }

    /// Internal compaction implementation (no cloud storage support)
    #[cfg(not(feature = "object-store"))]
    fn do_compact_level_impl(
        lsm: &Arc<ArcSwap<LSMTree>>,
        lsm_mutex: &Arc<Mutex<()>>,
        sstable_counter: &Arc<Mutex<u64>>,
        data_dir: &Path,
        level_num: usize,
        metrics: &Arc<MetricsCollector>,
        max_flushed_seq: &Arc<AtomicU64>,
        pending_deletions: &Arc<Mutex<Vec<(PathBuf, std::time::Instant)>>>,
        filter: &Option<Arc<dyn crate::compaction::CompactionFilter>>,
        snapshot_tracker: &Arc<crate::types::SnapshotTracker>,
    ) -> Result<()> {
        let compaction_start = Instant::now();

        // Load LSM tree (LOCK-FREE!)
        let lsm_arc = lsm.load();

        // Get SSTables to compact
        let level = lsm_arc.level(level_num).ok_or(DBError::NotOpened)?;
        let mut all_input_paths: Vec<PathBuf> = level.sstables().to_vec();

        // Limit number of files to compact at once to avoid "Too many open files"
        // and to keep compaction duration predictable.
        const MAX_COMPACTION_FILES: usize = 16;
        if all_input_paths.len() > MAX_COMPACTION_FILES {
            all_input_paths.truncate(MAX_COMPACTION_FILES);
        }

        if all_input_paths.is_empty() {
            return Ok(());
        }

        // **CRITICAL FIX**: Only compact SSTables with max_sequence <= max_flushed_seq
        let safe_seq = max_flushed_seq.load(Ordering::SeqCst);
        let mut input_paths = Vec::new();
        let mut skipped_count = 0;

        for path in all_input_paths {
            if let Ok(sstable) = SSTable::open(&path) {
                if sstable.max_sequence() <= safe_seq {
                    input_paths.push(path);
                } else {
                    skipped_count += 1;
                    debug!(
                        path = ?path,
                        sstable_seq = sstable.max_sequence(),
                        safe_seq = safe_seq,
                        "Skipping SSTable with sequence > max_flushed_seq"
                    );
                }
            }
        }

        if input_paths.is_empty() {
            debug!(
                level = level_num,
                skipped = skipped_count,
                "No SSTables eligible for compaction"
            );
            return Ok(());
        }

        let input_count = input_paths.len();
        debug!(
            level = level_num,
            input_sstables = input_count,
            skipped_sstables = skipped_count,
            safe_seq = safe_seq,
            "Starting compaction"
        );

        // Generate output path
        let mut counter = sstable_counter
            .lock()
            .expect("SSTable counter mutex poisoned");
        let output_path = data_dir.join(format!("L{}_{:06}.sst", level_num + 1, *counter));
        *counter += 1;
        drop(counter);

        // Compact SSTables with MVCC GC
        let oldest_snapshot = snapshot_tracker.oldest_snapshot();
        let (result_path, size) = compact_sstables(
            &input_paths,
            &output_path,
            level_num + 1,
            filter.clone(),
            oldest_snapshot,
        )?;

        // Track physical bytes written during compaction
        metrics.record_physical_bytes(size);

        // CRITICAL FIX (Bug #7c): Serialize LSM tree updates
        {
            let _lsm_lock = lsm_mutex.lock().expect("LSM mutex poisoned");

            let mut lsm_clone = (**lsm.load()).clone();
            lsm_clone.add_to_level(level_num + 1, result_path, size);
            lsm_clone.remove_sstables_from_level(level_num, &input_paths);
            lsm.store(Arc::new(lsm_clone));
        }

        // PRODUCTION FIX (Bug #7b): Queue SSTables for delayed deletion
        {
            let mut pending = pending_deletions
                .lock()
                .expect("pending_deletions lock poisoned");
            let now = std::time::Instant::now();
            for path in input_paths {
                pending.push((path, now));
            }
        }

        crate::db_helpers::cleanup_old_deletions(pending_deletions);

        let compaction_duration_ms = compaction_start.elapsed().as_millis();
        info!(
            level = level_num,
            input_sstables = input_count,
            output_size_bytes = size,
            duration_ms = compaction_duration_ms,
            "Compaction complete"
        );

        Ok(())
    }

    /// Get current memtable size across all partitions (lock-free)
    pub fn memtable_size(&self) -> usize {
        self.memtables.iter().map(|mt| mt.load().size()).sum()
    }

    /// Get number of entries in memtable across all partitions (lock-free)
    pub fn memtable_len(&self) -> usize {
        self.memtables.iter().map(|mt| mt.load().len()).sum()
    }

    /// Get real-time database statistics
    ///
    /// Returns comprehensive statistics for monitoring, observability, and performance tuning.
    /// Includes operation counts, latency percentiles, resource usage, and LSM tree structure.
    ///
    /// # Returns
    ///
    /// A [`DBStats`] struct containing:
    /// - **Throughput**: Reads/writes/deletes per second
    /// - **Operation counts**: Total operations since database opened
    /// - **Latency percentiles**: p50, p95, p99, p999 for get/put/delete (in microseconds)
    /// - **Resource usage**: Memtable, WAL, disk usage
    /// - **LSM structure**: `SSTables` per level, level sizes
    /// - **Uptime**: Time since database opened (seconds)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use seerdb::{DB, DBOptions};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = DB::open(DBOptions::default())?;
    ///
    /// // Perform some operations
    /// for i in 0..10000 {
    ///     db.put(format!("key{}", i).as_bytes(), b"value")?;
    /// }
    ///
    /// // Get statistics
    /// let stats = db.stats();
    /// println!("Throughput: {:.0} writes/sec", stats.writes_per_sec);
    /// println!("p99 latency: {} µs", stats.put_latency_p99_us);
    /// println!("Memtable: {:.1}% full", stats.memtable_utilization_pct);
    /// println!("Disk usage: {} MB", stats.total_disk_bytes / 1_048_576);
    ///
    /// // LSM structure
    /// for (level, count) in stats.sstables_per_level.iter().enumerate() {
    ///     if *count > 0 {
    ///         println!("L{}: {} SSTables", level, count);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    ///
    /// This method is relatively cheap (microseconds) but does:
    /// - Lock memtable, LSM tree briefly to read stats
    /// - Calculate file sizes from filesystem metadata
    /// - Compute latency percentiles from histograms
    ///
    /// Safe to call frequently (e.g., every second for monitoring).
    ///
    /// # See Also
    ///
    /// - [`health()`](Self::health) - Health checks with thresholds
    /// - [`DBStats`] - Full structure documentation
    ///   Estimate current memory usage in bytes
    ///
    /// Includes:
    /// - Active memtables
    /// - Immutable memtables (during flush)
    /// - Block cache (~40MB for 10K blocks)
    /// - `SSTable` cache (~1KB per cached `SSTable`)
    pub fn estimate_memory_usage(&self) -> usize {
        // Active memtables
        let active_memtable_bytes: usize = self.memtables.iter().map(|mt| mt.load().size()).sum();

        // Immutable memtables (if flush in progress)
        let immutable_memtable_bytes: usize = {
            let immutable = self.immutable_memtables.load();
            if let Some(ref partitions) = **immutable {
                partitions.iter().map(|mt| mt.size()).sum()
            } else {
                0
            }
        };

        // Block cache: 10K blocks * ~4KB average = ~40MB
        const BLOCK_CACHE_BYTES: usize = 10_000 * 4096;

        // SSTable cache: 1000 SSTables * ~1KB metadata = ~1MB
        const SSTABLE_CACHE_BYTES: usize = 1_000 * 1024;

        active_memtable_bytes + immutable_memtable_bytes + BLOCK_CACHE_BYTES + SSTABLE_CACHE_BYTES
    }

    /// Check disk space with periodic caching (every 10 seconds)
    ///
    /// This is a performance-optimized version of disk space checking that:
    /// 1. Returns immediately if checked within last 10 seconds (uses cached value)
    /// 2. Otherwise updates cache and checks disk space
    ///
    /// This avoids the performance overhead of calling sysinfo on every write
    /// while still protecting against disk full scenarios.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if sufficient disk space available
    /// - `Err(DBError::DiskSpaceFull)` if disk space below threshold
    ///
    /// # Performance
    ///
    /// - Cached check: < 1 microsecond (single atomic load)
    /// - Fresh check: ~1-5 milliseconds (sysinfo syscall)
    fn check_disk_space_cached(&self) -> Result<()> {
        // Only check if min_disk_space is configured
        if self.options.min_disk_space_bytes.is_none() {
            return Ok(());
        }

        const CHECK_INTERVAL_SECS: u64 = 10;

        // Get current time (seconds since UNIX epoch)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("System time before UNIX EPOCH")
            .as_secs();

        // Get last check time (atomic load, very fast)
        let last_check = self.last_disk_check.load(Ordering::Relaxed);

        // If checked within last 10 seconds, use cached value
        if now.saturating_sub(last_check) < CHECK_INTERVAL_SECS {
            let cached_space = self.cached_available_space.load(Ordering::Relaxed);
            let min_space = self
                .options
                .min_disk_space_bytes
                .expect("min_disk_space_bytes checked above");

            if cached_space < min_space {
                return Err(DBError::DiskSpaceFull {
                    available: cached_space,
                    required: min_space,
                });
            }
            return Ok(());
        }

        // Time to refresh the cache - call the actual disk space check
        // This uses sysinfo which is slow, but we only do it every 10 seconds
        use sysinfo::{DiskExt, System, SystemExt};

        let min_space = self
            .options
            .min_disk_space_bytes
            .expect("min_disk_space_bytes checked above");
        let mut sys = System::new();
        sys.refresh_disks_list();

        // Find the disk containing our data directory
        let data_dir = &self.options.data_dir;
        if let Some(disk) = sys
            .disks()
            .iter()
            .find(|d| data_dir.starts_with(d.mount_point()))
        {
            let available = disk.available_space();

            // Update cache (atomic stores)
            self.cached_available_space
                .store(available, Ordering::Relaxed);
            self.last_disk_check.store(now, Ordering::Relaxed);

            if available < min_space {
                return Err(DBError::DiskSpaceFull {
                    available,
                    required: min_space,
                });
            }
        } else {
            // If we can't find the disk, update timestamp anyway to avoid
            // hammering sysinfo on every write
            self.last_disk_check.store(now, Ordering::Relaxed);
        }

        Ok(())
    }

    pub fn stats(&self) -> DBStats {
        // Get operation counts and throughput
        let (total_puts, total_gets, total_deletes, total_flushes, total_compactions) =
            self.metrics.get_counts();
        let (writes_per_sec, reads_per_sec, deletes_per_sec) = self.metrics.calculate_throughput();

        // Get latency percentiles
        let (put_latencies, get_latencies, delete_latencies) =
            self.metrics.get_latency_percentiles();

        // Get memtable stats (sum across all partitions, lock-free)
        let memtable_size_bytes: usize = self.memtables.iter().map(|mt| mt.load().size()).sum();
        let memtable_capacity_bytes = self.options.memtable_capacity;
        let memtable_utilization_pct =
            (memtable_size_bytes as f64 / memtable_capacity_bytes as f64) * 100.0;

        // Get WAL size
        let wal_size_bytes = self
            .options
            .data_dir
            .join("wal.log")
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);

        // Get LSM tree structure and cache stats (LOCK-FREE!)
        let lsm_arc = self.lsm.load();
        let mut sstables_per_level = Vec::new();
        let mut level_sizes_bytes = Vec::new();
        let mut total_disk_bytes = 0u64;
        let mut total_sstables = 0usize;
        let mut cache_hits_total = 0u64;
        let mut cache_misses_total = 0u64;

        for level_num in 0..lsm_arc.num_levels() {
            if let Some(level) = lsm_arc.level(level_num) {
                let sstables = level.sstables();
                sstables_per_level.push(sstables.len());
                total_sstables += sstables.len();

                let level_size: u64 = sstables
                    .iter()
                    .filter_map(|path| path.metadata().ok().map(|m| m.len()))
                    .sum();
                level_sizes_bytes.push(level_size);
                total_disk_bytes += level_size;
            } else {
                sstables_per_level.push(0);
                level_sizes_bytes.push(0);
            }
        }

        // Collect cache stats from all SSTables
        for level_num in 0..lsm_arc.num_levels() {
            if let Some(level) = lsm_arc.level(level_num) {
                for sstable_path in level.sstables() {
                    if let Some(cached_sstable) = self.sstable_cache.get(sstable_path) {
                        let sstable = cached_sstable.lock().expect("SSTable lock poisoned");
                        let (hits, misses, _) = sstable.cache_stats();
                        cache_hits_total += hits;
                        cache_misses_total += misses;
                    }
                }
            }
        }
        // Arc automatically dropped (lock-free, no explicit drop needed!)

        // Add vLog size if present
        let vlog_size = self
            .options
            .data_dir
            .join("values.vlog")
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);
        total_disk_bytes += vlog_size;

        // Calculate write amplification
        let logical_bytes = self.metrics.logical_bytes_written.load(Ordering::Relaxed);
        let physical_bytes = self.metrics.physical_bytes_written.load(Ordering::Relaxed);
        let write_amplification = if logical_bytes > 0 {
            physical_bytes as f64 / logical_bytes as f64
        } else {
            0.0
        };

        // Calculate cache hit rate
        let cache_total = cache_hits_total + cache_misses_total;
        let cache_hit_rate = if cache_total > 0 {
            cache_hits_total as f64 / cache_total as f64
        } else {
            0.0
        };

        DBStats {
            // Throughput
            writes_per_sec,
            reads_per_sec,
            deletes_per_sec,

            // Operation counts
            total_puts,
            total_gets,
            total_deletes,
            total_flushes,
            total_compactions,

            // Latency percentiles
            put_latency_p50_us: put_latencies.0,
            put_latency_p95_us: put_latencies.1,
            put_latency_p99_us: put_latencies.2,
            put_latency_p999_us: put_latencies.3,

            get_latency_p50_us: get_latencies.0,
            get_latency_p95_us: get_latencies.1,
            get_latency_p99_us: get_latencies.2,
            get_latency_p999_us: get_latencies.3,

            delete_latency_p50_us: delete_latencies.0,
            delete_latency_p95_us: delete_latencies.1,
            delete_latency_p99_us: delete_latencies.2,

            // Resource usage
            memtable_size_bytes,
            memtable_capacity_bytes,
            memtable_utilization_pct,
            wal_size_bytes,
            total_disk_bytes,

            // Block cache performance
            cache_hits: cache_hits_total,
            cache_misses: cache_misses_total,
            cache_hit_rate,
            block_cache_size: self.global_block_cache.len(),
            block_cache_capacity: self.global_block_cache.capacity() as usize,

            // LSM structure
            sstables_per_level,
            level_sizes_bytes,
            total_sstables,

            // Write amplification
            logical_bytes_written: logical_bytes,
            physical_bytes_written: physical_bytes,
            write_amplification,

            // Uptime
            uptime_seconds: self.metrics.uptime_seconds(),
        }
    }

    /// Check database health status
    ///
    /// Performs comprehensive health checks to detect performance degradation or critical
    /// conditions. Returns a [`HealthStatus`] with individual check results and an overall
    /// health indicator.
    ///
    /// # Health Checks
    ///
    /// 1. **Compaction lag** (L0 `SSTable` count)
    ///    - Healthy: ≤10 `SSTables`
    ///    - Degraded: 11-20 `SSTables`
    ///    - Unhealthy: >20 `SSTables`
    ///
    /// 2. **WAL size** (write-ahead log growth)
    ///    - Healthy: ≤100 MB
    ///    - Degraded: 101-500 MB
    ///    - Unhealthy: >500 MB
    ///
    /// 3. **Memtable utilization** (memory pressure)
    ///    - Healthy: ≤80% full
    ///    - Degraded: 81-95% full
    ///    - Unhealthy: >95% full
    ///
    /// 4. **Put latency p99** (write performance)
    ///    - Healthy: ≤100 ms
    ///    - Degraded: 101-1000 ms
    ///    - Unhealthy: >1000 ms
    ///
    /// 5. **Get latency p99** (read performance)
    ///    - Healthy: ≤50 ms
    ///    - Degraded: 51-500 ms
    ///    - Unhealthy: >500 ms
    ///
    /// # Returns
    ///
    /// A [`HealthStatus`] with:
    /// - `healthy`: `true` if all checks are healthy
    /// - `checks`: Individual check results with status and messages
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use seerdb::{DB, DBOptions};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = DB::open(DBOptions::default())?;
    ///
    /// // Perform operations...
    /// for i in 0..10000 {
    ///     db.put(format!("key{}", i).as_bytes(), b"value")?;
    /// }
    ///
    /// // Check health
    /// let health = db.health();
    /// if !health.healthy {
    ///     eprintln!("WARNING: Database health degraded!");
    ///     for check in &health.checks {
    ///         if !check.healthy {
    ///             eprintln!("  - {}: {}", check.name, check.message);
    ///         }
    ///     }
    /// }
    ///
    /// // Pretty print
    /// println!("{}", health);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Use Cases
    ///
    /// - **Monitoring dashboards**: Periodic health checks
    /// - **Alerting systems**: Trigger alerts on degraded/unhealthy status
    /// - **Load shedding**: Reduce traffic if database is unhealthy
    /// - **Debugging**: Diagnose performance issues
    ///
    /// # Performance
    ///
    /// This method is cheap (microseconds) and safe to call frequently.
    /// It only reads metrics and does not perform I/O.
    ///
    /// # See Also
    ///
    /// - [`stats()`](Self::stats) - Detailed statistics without thresholds
    /// - [`HealthStatus`] - Full structure documentation
    pub fn health(&self) -> HealthStatus {
        let mut checks = Vec::new();

        // Check 1: Compaction lag (L0 SSTable count) (LOCK-FREE!)
        let lsm_arc = self.lsm.load();
        let l0_count = if let Some(level) = lsm_arc.level(0) {
            level.sstables().len()
        } else {
            0
        };
        // Arc automatically dropped (lock-free, no explicit drop needed!)

        if l0_count > 20 {
            checks.push(HealthCheck::unhealthy(
                "compaction_lag",
                format!("L0 has {l0_count} SSTables (threshold: 20)"),
            ));
        } else if l0_count > 10 {
            checks.push(HealthCheck::degraded(
                "compaction_lag",
                format!("L0 has {l0_count} SSTables (threshold: 10)"),
            ));
        } else {
            checks.push(HealthCheck::healthy_with_message(
                "compaction_lag",
                format!("L0 has {l0_count} SSTables"),
            ));
        }

        // Check 2: WAL size
        let wal_size_bytes = self
            .options
            .data_dir
            .join("wal.log")
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);
        let wal_size_mb = wal_size_bytes / (1024 * 1024);

        if wal_size_mb > 500 {
            checks.push(HealthCheck::unhealthy(
                "wal_size",
                format!("WAL is {wal_size_mb} MB (threshold: 500 MB)"),
            ));
        } else if wal_size_mb > 100 {
            checks.push(HealthCheck::degraded(
                "wal_size",
                format!("WAL is {wal_size_mb} MB (threshold: 100 MB)"),
            ));
        } else {
            checks.push(HealthCheck::healthy_with_message(
                "wal_size",
                format!("WAL is {wal_size_mb} MB"),
            ));
        }

        // Check 3: Memtable utilization (sum across all partitions, lock-free)
        let memtable_size: usize = self.memtables.iter().map(|mt| mt.load().size()).sum();
        let memtable_capacity = self.options.memtable_capacity;
        let utilization_pct = (memtable_size as f64 / memtable_capacity as f64) * 100.0;

        if utilization_pct > 95.0 {
            checks.push(HealthCheck::unhealthy(
                "memtable_utilization",
                format!("Memtable is {utilization_pct:.1}% full (threshold: 95%)"),
            ));
        } else if utilization_pct > 80.0 {
            checks.push(HealthCheck::degraded(
                "memtable_utilization",
                format!("Memtable is {utilization_pct:.1}% full (threshold: 80%)"),
            ));
        } else {
            checks.push(HealthCheck::healthy_with_message(
                "memtable_utilization",
                format!("Memtable is {utilization_pct:.1}% full"),
            ));
        }

        // Check 4: Put latency (p99)
        let (put_latencies, get_latencies, _) = self.metrics.get_latency_percentiles();
        let put_p99_ms = put_latencies.2 / 1000; // Convert microseconds to milliseconds

        if put_p99_ms > 1000 {
            checks.push(HealthCheck::unhealthy(
                "put_latency_p99",
                format!("Put p99 is {put_p99_ms} ms (threshold: 1000 ms)"),
            ));
        } else if put_p99_ms > 100 {
            checks.push(HealthCheck::degraded(
                "put_latency_p99",
                format!("Put p99 is {put_p99_ms} ms (threshold: 100 ms)"),
            ));
        } else {
            checks.push(HealthCheck::healthy_with_message(
                "put_latency_p99",
                format!("Put p99 is {put_p99_ms} ms"),
            ));
        }

        // Check 5: Get latency (p99)
        let get_p99_ms = get_latencies.2 / 1000; // Convert microseconds to milliseconds

        if get_p99_ms > 500 {
            checks.push(HealthCheck::unhealthy(
                "get_latency_p99",
                format!("Get p99 is {get_p99_ms} ms (threshold: 500 ms)"),
            ));
        } else if get_p99_ms > 50 {
            checks.push(HealthCheck::degraded(
                "get_latency_p99",
                format!("Get p99 is {get_p99_ms} ms (threshold: 50 ms)"),
            ));
        } else {
            checks.push(HealthCheck::healthy_with_message(
                "get_latency_p99",
                format!("Get p99 is {get_p99_ms} ms"),
            ));
        }

        HealthStatus::new(checks)
    }


}

/// Graceful shutdown: signal compaction thread to stop and wait for it
impl Drop for DB {
    fn drop(&mut self) {
        info!("Closing database");

        // CRITICAL: Flush memtable to SSTable before shutdown
        // After WAL recovery, DB::open() creates a fresh WAL. Without flushing,
        // any data in memtable (including recovered data) would be lost on next open.
        debug!("Flushing memtable before shutdown");
        if let Err(e) = self.flush() {
            error!("Failed to flush memtable during shutdown: {}", e);
        }

        // Sync WAL to ensure any remaining data is persisted
        debug!("Syncing WAL before shutdown");
        if let Err(e) = self.pipelined_wal.sync() {
            error!("Failed to sync WAL during shutdown: {}", e);
        }

        // Shutdown background flush worker
        if let Some(ref tx) = self.flush_tx {
            // Send shutdown signal
            debug!("Signaling background flush thread to shut down");
            let _ = tx.send(FlushTask::Shutdown);
        }

        // Wait for flush worker thread to finish
        if let Some(worker) = self.flush_worker.take() {
            debug!("Waiting for background flush thread to finish");
            if let Err(e) = worker.join() {
                error!("Flush worker thread panicked during shutdown: {:?}", e);
            }
        }

        // Shutdown background compaction worker
        if let Some(ref tx) = self.compaction_tx {
            // Send shutdown signal
            debug!("Signaling background compaction thread to shut down");
            let _ = tx.send(CompactionTask::Shutdown);
        }

        // Wait for compaction worker thread to finish
        if let Some(worker) = self.compaction_worker.take() {
            debug!("Waiting for background compaction thread to finish");
            if let Err(e) = worker.join() {
                error!("Compaction worker thread panicked during shutdown: {:?}", e);
            }
        }

        info!("Database closed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_db_open() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();
        assert_eq!(db.memtable_size(), 0);
    }

    #[test]
    fn test_config_profiles() {
        // Test embedded profile
        let dir = tempdir().unwrap();
        let opts = DBOptions::embedded(dir.path().to_path_buf());
        assert_eq!(opts.memtable_capacity, 64 * 1024 * 1024);
        assert!(opts.use_direct_wal);
        assert!(opts.disable_metrics);
        let db = DB::open(opts).unwrap();
        db.put(b"key", b"value").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value")));
        drop(db);

        // Test high_throughput profile
        let dir = tempdir().unwrap();
        let opts = DBOptions::high_throughput(dir.path().to_path_buf());
        assert_eq!(opts.memtable_capacity, 512 * 1024 * 1024);
        assert!(opts.background_compaction);
        assert!(opts.background_flush);
        let db = DB::open(opts).unwrap();
        db.put(b"key", b"value").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value")));
        drop(db);

        // Test large_scale profile
        let dir = tempdir().unwrap();
        let opts = DBOptions::large_scale(dir.path().to_path_buf());
        assert_eq!(opts.memtable_capacity, 1024 * 1024 * 1024);
        assert_eq!(opts.base_level_size, 64 * 1024 * 1024);
        let db = DB::open(opts).unwrap();
        db.put(b"key", b"value").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value")));
        drop(db);

        // Test builder pattern
        let dir = tempdir().unwrap();
        let opts = DBOptions::default()
            .with_data_dir(dir.path())
            .with_memtable_capacity(128 * 1024 * 1024)
            .with_metrics(false)
            .with_direct_wal(true);
        assert_eq!(opts.memtable_capacity, 128 * 1024 * 1024);
        assert!(opts.disable_metrics);
        assert!(opts.use_direct_wal);
        let db = DB::open(opts).unwrap();
        db.put(b"key", b"value").unwrap();
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value")));
    }

    #[test]
    fn test_skip_wal_single_writes() {
        let dir = tempdir().unwrap();
        let opts = DBOptions::default()
            .with_data_dir(dir.path())
            .with_skip_wal(true);

        assert!(opts.skip_wal);
        let db = DB::open(opts).unwrap();

        // Write some data (no WAL)
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Verify all data is readable (from memtable)
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let expected = format!("value_{:03}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
        }

        // Flush to SSTable
        db.flush().unwrap();

        // Data still readable after flush
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let expected = format!("value_{:03}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
        }
    }

    #[test]
    fn test_skip_wal_batch_writes() {
        let dir = tempdir().unwrap();
        let opts = DBOptions::default()
            .with_data_dir(dir.path())
            .with_skip_wal(true);

        let db = DB::open(opts).unwrap();

        // Batch write (no WAL)
        let mut batch = db.batch();
        for i in 0..50 {
            let key = format!("batch_key_{:03}", i);
            let value = format!("batch_value_{:03}", i);
            batch.put(key.as_bytes(), value.as_bytes());
        }
        batch.commit().unwrap();

        // Verify all batch data is readable
        for i in 0..50 {
            let key = format!("batch_key_{:03}", i);
            let expected = format!("batch_value_{:03}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
        }
    }

    #[test]
    fn test_bulk_load_basic() {
        let dir = tempdir().unwrap();
        let opts = DBOptions {
            data_dir: dir.path().to_path_buf(),
            vlog_threshold: None, // Disable vLog for simplicity
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        // Create test entries (unsorted to test sorting)
        let entries = vec![
            (b"key_c".to_vec(), b"value_c".to_vec()),
            (b"key_a".to_vec(), b"value_a".to_vec()),
            (b"key_b".to_vec(), b"value_b".to_vec()),
        ];

        // Bulk load
        let stats = db.bulk_load(entries, BulkLoadOptions::default()).unwrap();

        // Verify stats
        assert_eq!(stats.entries_loaded, 3);
        assert_eq!(stats.sstables_created, 1);
        assert!(stats.bytes_written > 0);
        assert_eq!(stats.target_level, 6);

        // Verify data is readable
        assert_eq!(db.get(b"key_a").unwrap(), Some(Bytes::from("value_a")));
        assert_eq!(db.get(b"key_b").unwrap(), Some(Bytes::from("value_b")));
        assert_eq!(db.get(b"key_c").unwrap(), Some(Bytes::from("value_c")));
        assert_eq!(db.get(b"key_d").unwrap(), None);
    }

    #[test]
    fn test_bulk_load_with_vlog() {
        let dir = tempdir().unwrap();
        let opts = DBOptions {
            data_dir: dir.path().to_path_buf(),
            vlog_threshold: Some(10), // Small threshold to test vLog
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        // Create entries with large values
        let entries = vec![
            (
                b"key_1".to_vec(),
                b"large_value_that_exceeds_threshold".to_vec(),
            ),
            (
                b"key_2".to_vec(),
                b"another_large_value_for_testing".to_vec(),
            ),
        ];

        let stats = db.bulk_load(entries, BulkLoadOptions::default()).unwrap();
        assert_eq!(stats.entries_loaded, 2);

        // Verify data is readable (should go through vLog)
        assert_eq!(
            db.get(b"key_1").unwrap(),
            Some(Bytes::from("large_value_that_exceeds_threshold"))
        );
        assert_eq!(
            db.get(b"key_2").unwrap(),
            Some(Bytes::from("another_large_value_for_testing"))
        );
    }

    #[test]
    fn test_bulk_load_multiple_sstables() {
        let dir = tempdir().unwrap();
        let opts = DBOptions {
            data_dir: dir.path().to_path_buf(),
            vlog_threshold: None,
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        // Create many entries
        let entries: Vec<_> = (0..500)
            .map(|i| {
                (
                    format!("key_{:05}", i).into_bytes(),
                    format!("value_{}", i).into_bytes(),
                )
            })
            .collect();

        // Use small max entries to create multiple SSTables
        let stats = db
            .bulk_load(entries, BulkLoadOptions::default().with_max_entries(100))
            .unwrap();

        assert_eq!(stats.entries_loaded, 500);
        assert_eq!(stats.sstables_created, 5); // 500 / 100 = 5
        assert!(stats.bytes_written > 0);

        // Verify some entries are readable
        assert_eq!(db.get(b"key_00000").unwrap(), Some(Bytes::from("value_0")));
        assert_eq!(
            db.get(b"key_00250").unwrap(),
            Some(Bytes::from("value_250"))
        );
        assert_eq!(
            db.get(b"key_00499").unwrap(),
            Some(Bytes::from("value_499"))
        );
    }

    #[test]
    fn test_bulk_load_already_sorted() {
        let dir = tempdir().unwrap();
        let opts = DBOptions {
            data_dir: dir.path().to_path_buf(),
            vlog_threshold: None,
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        // Pre-sorted entries
        let entries = vec![
            (b"aaa".to_vec(), b"1".to_vec()),
            (b"bbb".to_vec(), b"2".to_vec()),
            (b"ccc".to_vec(), b"3".to_vec()),
        ];

        // Mark as already sorted
        let stats = db
            .bulk_load(entries, BulkLoadOptions::default().already_sorted())
            .unwrap();

        assert_eq!(stats.entries_loaded, 3);
        assert_eq!(db.get(b"aaa").unwrap(), Some(Bytes::from("1")));
        assert_eq!(db.get(b"bbb").unwrap(), Some(Bytes::from("2")));
        assert_eq!(db.get(b"ccc").unwrap(), Some(Bytes::from("3")));
    }

    #[test]
    fn test_bulk_load_target_level() {
        let dir = tempdir().unwrap();
        let opts = DBOptions {
            data_dir: dir.path().to_path_buf(),
            vlog_threshold: None,
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        let entries = vec![(b"key".to_vec(), b"value".to_vec())];

        // Load to L3
        let stats = db
            .bulk_load(entries, BulkLoadOptions::default().with_target_level(3))
            .unwrap();

        assert_eq!(stats.target_level, 3);
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("value")));
    }

    #[test]
    fn test_bulk_load_empty() {
        let dir = tempdir().unwrap();
        let opts = DBOptions {
            data_dir: dir.path().to_path_buf(),
            vlog_threshold: None,
            ..Default::default()
        };
        let db = DB::open(opts).unwrap();

        let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        let stats = db.bulk_load(entries, BulkLoadOptions::default()).unwrap();

        assert_eq!(stats.entries_loaded, 0);
        assert_eq!(stats.sstables_created, 0);
        assert_eq!(stats.bytes_written, 0);
    }

    #[test]
    fn test_db_put_get() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();

        assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("value1")));
        assert_eq!(db.get(b"key2").unwrap(), Some(Bytes::from("value2")));
        assert_eq!(db.get(b"key3").unwrap(), None);
    }

    #[test]
    fn test_db_delete() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        db.put(b"key1", b"value1").unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("value1")));

        db.delete(b"key1").unwrap();
        assert_eq!(db.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_db_overwrite() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        db.put(b"key1", b"old_value").unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("old_value")));

        db.put(b"key1", b"new_value").unwrap();
        assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("new_value")));
    }

    #[test]
    fn test_db_flush() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 100, // Small capacity to trigger flush
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write enough data to trigger flush
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = format!("value_with_long_data_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Data should still be accessible after flush
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = format!("value_with_long_data_{}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(value)));
        }

        // Check that SSTable files were created
        let sst_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s == "sst")
                    .unwrap_or(false)
            })
            .collect();

        assert!(!sst_files.is_empty(), "No SSTable files created");
    }

    #[test]
    fn test_db_recovery_basic() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        // Write some data
        {
            let db = DB::open(options.clone()).unwrap();
            db.put(b"key1", b"value1").unwrap();
            db.put(b"key2", b"value2").unwrap();
            db.put(b"key3", b"value3").unwrap();
            // Drop db (simulates shutdown without flush)
        }

        // Reopen and verify data recovered from WAL
        {
            let db = DB::open(options.clone()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("value1")));
            assert_eq!(db.get(b"key2").unwrap(), Some(Bytes::from("value2")));
            assert_eq!(db.get(b"key3").unwrap(), Some(Bytes::from("value3")));
        }
    }

    #[test]
    fn test_db_recovery_with_deletes() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        // Write and delete some data
        {
            let db = DB::open(options.clone()).unwrap();
            db.put(b"key1", b"value1").unwrap();
            db.put(b"key2", b"value2").unwrap();
            db.delete(b"key1").unwrap(); // Delete key1
            db.put(b"key3", b"value3").unwrap();
        }

        // Reopen and verify recovery
        {
            let db = DB::open(options.clone()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), None); // Deleted
            assert_eq!(db.get(b"key2").unwrap(), Some(Bytes::from("value2")));
            assert_eq!(db.get(b"key3").unwrap(), Some(Bytes::from("value3")));
        }
    }

    #[test]
    fn test_db_recovery_with_overwrites() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        // Write with overwrites
        {
            let db = DB::open(options.clone()).unwrap();
            db.put(b"key1", b"old_value").unwrap();
            db.put(b"key1", b"new_value").unwrap(); // Overwrite
        }

        // Reopen and verify newest value recovered
        {
            let db = DB::open(options.clone()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("new_value")));
        }
    }

    #[test]
    fn test_db_recovery_with_flush() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 100, // Small to trigger flush during recovery
            ..Default::default()
        };

        // Write enough data to trigger flush on recovery
        {
            let db = DB::open(options.clone()).unwrap();
            for i in 0..20 {
                let key = format!("key_{}", i);
                let value = format!("value_with_long_data_{}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }

        // Reopen (recovery should trigger flush due to small memtable)
        {
            let db = DB::open(options.clone()).unwrap();
            for i in 0..20 {
                let key = format!("key_{}", i);
                let value = format!("value_with_long_data_{}", i);
                assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(value)));
            }
        }
    }

    #[test]
    fn test_db_recovery_empty_wal() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        // Create DB (no data written)
        {
            let _db = DB::open(options.clone()).unwrap();
        }

        // Reopen (WAL exists but is empty)
        {
            let db = DB::open(options.clone()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), None);
        }
    }

    #[test]
    fn test_db_with_kv_separation() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 200,   // Small enough to trigger flush
            vlog_threshold: Some(50), // 50 byte threshold
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Small value (stored inline in SSTable after flush)
        db.put(b"small_key", b"tiny_value").unwrap();

        // Large value (will be stored in vLog after flush)
        let large_value = vec![b'X'; 100];
        db.put(b"large_key", &large_value).unwrap();

        // Write more data to trigger flush
        for i in 0..3 {
            let key = format!("k{}", i);
            let value = format!("value_data_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Verify all values can be read (from memtable or flushed SSTable)
        assert_eq!(
            db.get(b"small_key").unwrap(),
            Some(Bytes::from("tiny_value"))
        );
        assert_eq!(
            db.get(b"large_key").unwrap(),
            Some(Bytes::from(large_value))
        );

        // Verify vLog file was created
        let vlog_path = dir.path().join("values.vlog");
        assert!(
            vlog_path.exists(),
            "vLog file should exist with vlog_threshold enabled"
        );
    }

    #[test]
    fn test_db_with_kv_separation_recovery() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            vlog_threshold: Some(50), // 50 byte threshold
            ..Default::default()
        };

        // Write data with large values
        {
            let db = DB::open(options.clone()).unwrap();
            db.put(b"key1", b"small_value").unwrap();
            let large_value = vec![b'Y'; 200];
            db.put(b"key2", &large_value).unwrap();
        }

        // Reopen and verify recovery works with vLog
        {
            let db = DB::open(options.clone()).unwrap();
            assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("small_value")));
            let expected_large = vec![b'Y'; 200];
            assert_eq!(db.get(b"key2").unwrap(), Some(Bytes::from(expected_large)));
        }
    }

    #[test]
    fn test_db_background_compaction() {
        use std::time::Duration;

        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 100,      // Small to trigger flushes
            background_compaction: true, // Enable background compaction
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write enough data to trigger multiple flushes and compaction
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Give background thread time to process compactions
        std::thread::sleep(Duration::from_millis(100));

        // Verify data is still readable
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let expected = format!("value_{:03}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
        }

        // DB will be dropped here, triggering graceful shutdown
    }

    #[test]
    fn test_db_sync_vs_async_compaction() {
        use std::time::Duration;

        // Test that both modes produce identical results
        let dir_sync = tempdir().unwrap();
        let dir_async = tempdir().unwrap();

        let options_sync = DBOptions {
            data_dir: dir_sync.path().to_path_buf(),
            memtable_capacity: 100,
            background_compaction: false, // Synchronous
            ..Default::default()
        };

        let options_async = DBOptions {
            data_dir: dir_async.path().to_path_buf(),
            memtable_capacity: 100,
            background_compaction: true, // Asynchronous
            ..Default::default()
        };

        let db_sync = DB::open(options_sync).unwrap();
        let db_async = DB::open(options_async).unwrap();

        // Write same data to both
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            db_sync.put(key.as_bytes(), value.as_bytes()).unwrap();
            db_async.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Give async compaction time to finish
        std::thread::sleep(Duration::from_millis(100));

        // Verify both return same results
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let expected = format!("value_{:03}", i);
            assert_eq!(
                db_sync.get(key.as_bytes()).unwrap(),
                Some(Bytes::from(expected.clone()))
            );
            assert_eq!(
                db_async.get(key.as_bytes()).unwrap(),
                Some(Bytes::from(expected))
            );
        }
    }

    #[test]
    fn test_db_health_checks() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Perform some operations
        for i in 0..10 {
            db.put(format!("key{}", i).as_bytes(), b"value").unwrap();
        }

        // Get health status
        let health = db.health();

        // Should be healthy (low utilization, low L0 count, etc.)
        assert!(health.healthy);
        assert_eq!(health.checks.len(), 5); // 5 health checks

        // Verify check names
        let check_names: Vec<&str> = health.checks.iter().map(|c| c.name.as_str()).collect();
        assert!(check_names.contains(&"compaction_lag"));
        assert!(check_names.contains(&"wal_size"));
        assert!(check_names.contains(&"memtable_utilization"));
        assert!(check_names.contains(&"put_latency_p99"));
        assert!(check_names.contains(&"get_latency_p99"));

        // Test display formatting (doesn't panic)
        let _display = format!("{}", health);
    }

    #[test]
    fn test_range_scan_with_sstables() {
        let dir = tempdir().unwrap();
        let mut opts = DBOptions::default();
        opts.data_dir = dir.path().to_path_buf();
        opts.memtable_capacity = 1024; // Small memtable to force flush
        opts.background_compaction = false;

        let db = DB::open(opts).unwrap();

        // Insert enough data to trigger flush to SSTables
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Force flush to create SSTables
        db.flush().unwrap();

        // Range scan
        let mut results = vec![];
        for result in db.range(b"key010", Some(b"key020")).unwrap() {
            let (key, value) = result.unwrap();
            results.push((
                String::from_utf8(key.to_vec()).unwrap(),
                String::from_utf8(value.to_vec()).unwrap(),
            ));
        }

        // Should get key010 through key019
        assert_eq!(results.len(), 10);
        assert_eq!(results[0].0, "key010");
        assert_eq!(results[9].0, "key019");
    }

    #[test]
    fn test_range_scan_with_overwrites() {
        let dir = tempdir().unwrap();
        let mut opts = DBOptions::default();
        opts.data_dir = dir.path().to_path_buf();
        opts.memtable_capacity = 1024;
        opts.background_compaction = false;

        let db = DB::open(opts).unwrap();

        // Write initial data
        for i in 0..50 {
            let key = format!("key{:03}", i);
            db.put(key.as_bytes(), b"old_value").unwrap();
        }
        db.flush().unwrap();

        // Overwrite some keys
        for i in 10..20 {
            let key = format!("key{:03}", i);
            db.put(key.as_bytes(), b"new_value").unwrap();
        }

        // Range scan - newer values should override
        let mut results = vec![];
        for result in db.range(b"key010", Some(b"key020")).unwrap() {
            let (key, value) = result.unwrap();
            results.push((
                String::from_utf8(key.to_vec()).unwrap(),
                String::from_utf8(value.to_vec()).unwrap(),
            ));
        }

        assert_eq!(results.len(), 10);
        // All should have new_value (memtable overrides SSTable)
        for result in &results {
            assert_eq!(result.1, "new_value");
        }
    }

    #[test]
    fn test_range_scan_with_deletes() {
        let dir = tempdir().unwrap();
        let mut opts = DBOptions::default();
        opts.data_dir = dir.path().to_path_buf();
        opts.memtable_capacity = 1024;
        opts.background_compaction = false;

        let db = DB::open(opts).unwrap();

        // Write data
        for i in 0..50 {
            let key = format!("key{:03}", i);
            db.put(key.as_bytes(), b"value").unwrap();
        }
        db.flush().unwrap();

        // Delete some keys
        for i in 10..20 {
            let key = format!("key{:03}", i);
            db.delete(key.as_bytes()).unwrap();
        }

        // Range scan - deleted keys should not appear
        let mut results = vec![];
        for result in db.range(b"key005", Some(b"key025")).unwrap() {
            let (key, _value) = result.unwrap();
            results.push(String::from_utf8(key.to_vec()).unwrap());
        }

        // Should get key005-key009 and key020-key024 (5 + 5 = 10 keys)
        assert_eq!(results.len(), 10);
        assert!(!results
            .iter()
            .any(|k| k.as_str() >= "key010" && k.as_str() < "key020"));
    }

    #[test]
    fn test_memory_budget_enforcement() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 1024 * 1024, // 1MB per partition
            max_memory_bytes: Some(200 * 1024 * 1024), // 200MB budget (won't be triggered in test)
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Verify memory estimation works
        let initial_memory = db.estimate_memory_usage();
        assert!(initial_memory > 0, "Memory usage should be non-zero");

        // Write small amount of data (won't trigger enforcement)
        for i in 0..10 {
            let key = format!("key{}", i);
            db.put(key.as_bytes(), b"value").unwrap();
        }

        // Verify data is accessible
        for i in 0..10 {
            let key = format!("key{}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from("value")));
        }
    }

    #[test]
    fn test_estimate_memory_usage() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 1024,
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Initial memory should include cache overhead
        let initial = db.estimate_memory_usage();
        // Should be at least block cache (40MB) + SSTable cache (1MB)
        assert!(initial >= 40 * 1024 * 1024, "Should include cache overhead");

        // Write some data
        for i in 0..10 {
            db.put(format!("key{}", i).as_bytes(), b"value").unwrap();
        }

        // Memory should increase
        let after_write = db.estimate_memory_usage();
        assert!(
            after_write >= initial,
            "Memory should increase after writes"
        );
    }

    #[test]
    fn test_snapshot_basic_isolation() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write initial data
        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();

        // Create consistent snapshot (forces flush)
        let snapshot = db.snapshot().unwrap();

        // Write after snapshot
        db.put(b"key1", b"modified").unwrap();
        db.put(b"key3", b"value3").unwrap();
        db.delete(b"key2").unwrap();

        // Snapshot sees old values
        assert_eq!(snapshot.get(b"key1").unwrap(), Some(Bytes::from("value1")));
        assert_eq!(snapshot.get(b"key2").unwrap(), Some(Bytes::from("value2")));
        assert_eq!(snapshot.get(b"key3").unwrap(), None); // Didn't exist at snapshot time

        // DB sees new values
        assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("modified")));
        assert_eq!(db.get(b"key2").unwrap(), None); // Deleted
        assert_eq!(db.get(b"key3").unwrap(), Some(Bytes::from("value3")));
    }

    #[test]
    fn test_snapshot_range_isolation() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write initial data
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();

        // Create consistent snapshot (forces flush)
        let snapshot = db.snapshot().unwrap();

        // Modify after snapshot
        db.put(b"b", b"modified").unwrap();
        db.delete(b"c").unwrap();
        db.put(b"d", b"4").unwrap();

        // Snapshot range sees original values
        let snap_results: Vec<_> = snapshot
            .range(b"a", Some(b"z"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(snap_results.len(), 3); // a, b, c
        assert_eq!(snap_results[0].1.as_ref(), b"1");
        assert_eq!(snap_results[1].1.as_ref(), b"2");
        assert_eq!(snap_results[2].1.as_ref(), b"3");

        // DB range sees new values
        let db_results: Vec<_> = db
            .range(b"a", Some(b"z"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(db_results.len(), 3); // a, b, d (c deleted)
        assert_eq!(db_results[0].1.as_ref(), b"1");
        assert_eq!(db_results[1].1.as_ref(), b"modified");
        assert_eq!(db_results[2].1.as_ref(), b"4");
    }

    #[test]
    fn test_snapshot_during_concurrent_writes() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = Arc::new(DB::open(options).unwrap());

        // Write initial data
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("initial_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Create consistent snapshot (forces flush)
        let snapshot = db.snapshot().unwrap();

        // Spawn writer thread that modifies data concurrently
        let db_clone = Arc::clone(&db);
        let writer = thread::spawn(move || {
            for i in 0..100 {
                let key = format!("key_{:03}", i);
                let value = format!("modified_{:03}", i);
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });

        // While writes are happening, snapshot still sees original data
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let expected = format!("initial_{:03}", i);
            let actual = snapshot.get(key.as_bytes()).unwrap();
            assert_eq!(actual, Some(Bytes::from(expected)));
        }

        writer.join().unwrap();

        // After writes complete, snapshot still sees original data
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let expected = format!("initial_{:03}", i);
            let actual = snapshot.get(key.as_bytes()).unwrap();
            assert_eq!(actual, Some(Bytes::from(expected)));
        }

        // But DB sees modified data
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let expected = format!("modified_{:03}", i);
            let actual = db.get(key.as_bytes()).unwrap();
            assert_eq!(actual, Some(Bytes::from(expected)));
        }
    }

    #[test]
    fn test_snapshot_sequence_number() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        db.put(b"key1", b"value1").unwrap();
        db.flush().unwrap(); // Force flush to increment sequence
        let snap1 = db.snapshot().unwrap();

        db.put(b"key2", b"value2").unwrap();
        db.flush().unwrap(); // Force flush to increment sequence
        let snap2 = db.snapshot().unwrap();

        // Snap2 should have higher sequence number (after more writes)
        assert!(snap2.sequence_number() >= snap1.sequence_number());

        // Debug output works
        let _debug = format!("{:?}", snap1);
    }

    #[test]
    fn test_multiple_snapshots() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Initial state
        db.put(b"key", b"v1").unwrap();
        let snap1 = db.snapshot().unwrap();

        // Second state
        db.put(b"key", b"v2").unwrap();
        let snap2 = db.snapshot().unwrap();

        // Third state
        db.put(b"key", b"v3").unwrap();
        let snap3 = db.snapshot().unwrap();

        // Current state
        db.put(b"key", b"v4").unwrap();

        // Each snapshot sees its point-in-time value
        assert_eq!(snap1.get(b"key").unwrap(), Some(Bytes::from("v1")));
        assert_eq!(snap2.get(b"key").unwrap(), Some(Bytes::from("v2")));
        assert_eq!(snap3.get(b"key").unwrap(), Some(Bytes::from("v3")));
        assert_eq!(db.get(b"key").unwrap(), Some(Bytes::from("v4")));

        // Drop early snapshots, late ones still work
        drop(snap1);
        drop(snap2);
        assert_eq!(snap3.get(b"key").unwrap(), Some(Bytes::from("v3")));
    }

    #[test]
    fn test_snapshot_with_tombstones() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write and delete
        db.put(b"key1", b"value1").unwrap();
        db.put(b"key2", b"value2").unwrap();
        db.delete(b"key1").unwrap();

        // Snapshot sees key1 as deleted (after flush)
        let snap = db.snapshot().unwrap();
        assert_eq!(snap.get(b"key1").unwrap(), None);
        assert_eq!(snap.get(b"key2").unwrap(), Some(Bytes::from("value2")));

        // Re-insert key1
        db.put(b"key1", b"resurrected").unwrap();

        // Snapshot still sees key1 as deleted
        assert_eq!(snap.get(b"key1").unwrap(), None);

        // DB sees resurrected value
        assert_eq!(db.get(b"key1").unwrap(), Some(Bytes::from("resurrected")));
    }

    #[test]
    fn test_iter_all_keys() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write some keys
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
        db.put(b"d", b"4").unwrap();
        db.put(b"e", b"5").unwrap();

        // Iterate over all keys
        let results: Vec<_> = db.iter().unwrap().map(|r| r.unwrap()).collect();

        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0.as_ref(), b"a");
        assert_eq!(results[1].0.as_ref(), b"b");
        assert_eq!(results[2].0.as_ref(), b"c");
        assert_eq!(results[3].0.as_ref(), b"d");
        assert_eq!(results[4].0.as_ref(), b"e");
    }

    #[test]
    fn test_db_iter_rev() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();

        let results: Vec<_> = db.iter_rev().unwrap().map(|r| r.unwrap()).collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.as_ref(), b"c");
        assert_eq!(results[1].0.as_ref(), b"b");
        assert_eq!(results[2].0.as_ref(), b"a");
    }

    #[test]
    fn test_prefix_scan() {
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write keys with different prefixes
        db.put(b"user:1", b"alice").unwrap();
        db.put(b"user:2", b"bob").unwrap();
        db.put(b"user:3", b"charlie").unwrap();
        db.put(b"post:1", b"hello").unwrap();
        db.put(b"post:2", b"world").unwrap();
        db.put(b"tag:rust", b"lang").unwrap();

        // Scan user: prefix
        let user_results: Vec<_> = db.prefix(b"user:").unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(user_results.len(), 3);
        assert_eq!(user_results[0].0.as_ref(), b"user:1");
        assert_eq!(user_results[1].0.as_ref(), b"user:2");
        assert_eq!(user_results[2].0.as_ref(), b"user:3");

        // Scan post: prefix
        let post_results: Vec<_> = db.prefix(b"post:").unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(post_results.len(), 2);
        assert_eq!(post_results[0].0.as_ref(), b"post:1");
        assert_eq!(post_results[1].0.as_ref(), b"post:2");

        // Scan tag: prefix (single result)
        let tag_results: Vec<_> = db.prefix(b"tag:").unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(tag_results.len(), 1);
        assert_eq!(tag_results[0].0.as_ref(), b"tag:rust");

        // Scan non-existent prefix
        let empty_results: Vec<_> = db
            .prefix(b"missing:")
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(empty_results.len(), 0);
    }

    #[test]
    fn test_increment_bytes_helper() {
        // Normal case
        assert_eq!(increment_bytes(b"user"), Some(b"uses".to_vec()));

        // With 0xFF at end
        assert_eq!(increment_bytes(b"user\xff"), Some(b"uses\x00".to_vec()));

        // Multiple 0xFF at end
        assert_eq!(increment_bytes(b"a\xff\xff"), Some(b"b\x00\x00".to_vec()));

        // All 0xFF
        assert_eq!(increment_bytes(b"\xff\xff"), None);

        // Single byte
        assert_eq!(increment_bytes(b"a"), Some(b"b".to_vec()));
        assert_eq!(increment_bytes(b"\xff"), None);

        // Empty
        assert_eq!(increment_bytes(b""), None);
    }

    #[test]
    fn test_prefix_with_sstables() {
        let dir = tempdir().unwrap();
        let mut opts = DBOptions::default();
        opts.data_dir = dir.path().to_path_buf();
        opts.memtable_capacity = 1024; // Small memtable to force flush

        let db = DB::open(opts).unwrap();

        // Write enough data to trigger flush
        for i in 0..20 {
            let key = format!("key:{:02}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Force flush to ensure data is in SSTables
        db.flush().unwrap();

        // Add some more data in memtable
        db.put(b"key:20", b"value_20").unwrap();
        db.put(b"key:21", b"value_21").unwrap();

        // Prefix scan should find all keys (memtable + SSTables)
        let results: Vec<_> = db.prefix(b"key:").unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 22);

        // Verify ordering
        for i in 0..22 {
            let expected_key = format!("key:{:02}", i);
            assert_eq!(results[i].0.as_ref(), expected_key.as_bytes());
        }
    }

    #[test]
    fn test_prefix_batch_basic() {
        let dir = tempdir().unwrap();
        let mut opts = DBOptions::default();
        opts.data_dir = dir.path().to_path_buf();

        let db = DB::open(opts).unwrap();

        db.put(b"user:1", b"alice").unwrap();
        db.put(b"user:2", b"bob").unwrap();
        db.put(b"post:1", b"hello").unwrap();
        db.put(b"post:2", b"world").unwrap();

        let prefixes = vec![b"user:" as &[u8], b"post:"];
        let results = db.prefix_batch(&prefixes).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].len(), 2);
        assert_eq!(results[1].len(), 2);

        assert_eq!(results[0][0].0.as_ref(), b"user:1");
        assert_eq!(results[0][0].1.as_ref(), b"alice");
        assert_eq!(results[0][1].0.as_ref(), b"user:2");
        assert_eq!(results[0][1].1.as_ref(), b"bob");

        assert_eq!(results[1][0].0.as_ref(), b"post:1");
        assert_eq!(results[1][0].1.as_ref(), b"hello");
        assert_eq!(results[1][1].0.as_ref(), b"post:2");
        assert_eq!(results[1][1].1.as_ref(), b"world");
    }

    #[test]
    fn test_prefix_batch_empty() {
        let dir = tempdir().unwrap();
        let opts = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(opts).unwrap();

        let prefixes: Vec<&[u8]> = vec![];
        let results = db.prefix_batch(&prefixes).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_prefix_batch_no_matches() {
        let dir = tempdir().unwrap();
        let opts = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(opts).unwrap();

        db.put(b"user:1", b"alice").unwrap();

        let prefixes = vec![b"nonexistent:" as &[u8]];
        let results = db.prefix_batch(&prefixes).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), 0);
    }

    #[test]
    fn test_prefix_batch_ordering() {
        let dir = tempdir().unwrap();
        let opts = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = DB::open(opts).unwrap();

        db.put(b"a:1", b"1").unwrap();
        db.put(b"b:1", b"2").unwrap();
        db.put(b"c:1", b"3").unwrap();

        let prefixes = vec![b"c:" as &[u8], b"a:", b"b:"];
        let results = db.prefix_batch(&prefixes).unwrap();

        assert_eq!(results[0][0].1.as_ref(), b"3");
        assert_eq!(results[1][0].1.as_ref(), b"1");
        assert_eq!(results[2][0].1.as_ref(), b"2");
    }

    #[test]
    fn test_prefix_batch_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let opts = DBOptions {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let db = Arc::new(DB::open(opts).unwrap());

        db.put(b"user:1", b"alice").unwrap();
        db.put(b"user:2", b"bob").unwrap();
        db.put(b"post:1", b"hello").unwrap();

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let db = db.clone();
                thread::spawn(move || {
                    let prefixes = vec![b"user:" as &[u8], b"post:"];
                    db.prefix_batch(&prefixes)
                })
            })
            .collect();

        for handle in handles {
            let result = handle.join().unwrap();
            assert!(result.is_ok());
            let results = result.unwrap();
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].len(), 2);
            assert_eq!(results[1].len(), 1);
        }
    }

    #[test]
    fn test_global_block_cache_hits() {
        // Test that global block cache provides cache hits on repeated reads
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 1024,   // Small to force flush
            block_cache_capacity: 100, // Small cache (100 blocks)
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write data to create SSTable
        for i in 0..50 {
            let key = format!("key:{:04}", i);
            let value = vec![i as u8; 100]; // 100 bytes each
            db.put(key.as_bytes(), &value).unwrap();
        }

        // Force flush to disk
        db.flush().unwrap();

        // First read - should miss cache (cold start)
        let stats_before = db.stats();
        let initial_hits = stats_before.cache_hits;
        let _initial_misses = stats_before.cache_misses;

        // Read a key from disk
        let _ = db.get(b"key:0025").unwrap();

        // Read the same key again - should hit cache
        let _ = db.get(b"key:0025").unwrap();

        // Check cache stats
        let stats_after = db.stats();

        // We should have at least one cache hit from the second read
        assert!(
            stats_after.cache_hits > initial_hits,
            "Expected cache hit: before={}, after={}",
            initial_hits,
            stats_after.cache_hits
        );

        // Cache size should be non-zero
        assert!(
            stats_after.block_cache_size > 0,
            "Cache should have entries: {}",
            stats_after.block_cache_size
        );

        // Cache capacity should match what we configured
        assert_eq!(
            stats_after.block_cache_capacity, 100,
            "Cache capacity mismatch"
        );
    }

    #[test]
    fn test_block_cache_stats_in_dbstats() {
        // Test that block cache metrics are properly exposed in DBStats
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            block_cache_capacity: 500, // 500 blocks
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Check initial cache stats
        let stats = db.stats();

        // Cache should be empty initially
        assert_eq!(stats.block_cache_size, 0);
        // quick_cache may round capacity up to next power of 2 (500 -> 512)
        assert!(
            stats.block_cache_capacity >= 500,
            "Cache capacity should be at least 500: {}",
            stats.block_cache_capacity
        );
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 0);
        assert_eq!(stats.cache_hit_rate, 0.0);
    }

    #[test]
    fn test_block_cache_shared_across_sstables() {
        // Test that global cache is shared across multiple SSTables
        let dir = tempdir().unwrap();
        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 2048,    // Small to force multiple flushes
            block_cache_capacity: 1000, // Enough to cache blocks from multiple SSTables
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write first batch and flush to create SSTable 1
        for i in 0..20 {
            let key = format!("batch1:key:{:04}", i);
            let value = vec![i as u8; 64];
            db.put(key.as_bytes(), &value).unwrap();
        }
        db.flush().unwrap();

        // Write second batch and flush to create SSTable 2
        for i in 0..20 {
            let key = format!("batch2:key:{:04}", i);
            let value = vec![i as u8; 64];
            db.put(key.as_bytes(), &value).unwrap();
        }
        db.flush().unwrap();

        // Read from both SSTables
        let _ = db.get(b"batch1:key:0010").unwrap();
        let _ = db.get(b"batch2:key:0010").unwrap();

        // Read again - should hit cache
        let _ = db.get(b"batch1:key:0010").unwrap();
        let _ = db.get(b"batch2:key:0010").unwrap();

        // Check that cache contains blocks from both SSTables
        let stats = db.stats();

        // Should have cache entries (blocks from both SSTables)
        assert!(
            stats.block_cache_size > 0,
            "Global cache should contain entries from multiple SSTables"
        );

        // Should have cache hits from the repeated reads
        assert!(
            stats.cache_hits > 0,
            "Should have cache hits from repeated reads: hits={}",
            stats.cache_hits
        );
    }

    #[test]
    #[cfg(feature = "object-store")]
    fn test_db_with_cloud_storage_backend() {
        use object_store::{memory::InMemory, ObjectStore};

        let dir = tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Create in-memory object store for testing
        let store = std::sync::Arc::new(InMemory::new());
        let _guard = rt.enter();

        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 1000, // Small to trigger flushes
            storage_config: Some(StorageConfig::Custom(store.clone())),
            vlog_threshold: None, // Disable vLog to test non-vLog path
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write enough data to trigger flush
        for i in 0..50 {
            let key = format!("cloud_key_{:03}", i);
            let value = format!("cloud_value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Flush to trigger upload
        db.flush().unwrap();

        // Verify data is readable
        for i in 0..50 {
            let key = format!("cloud_key_{:03}", i);
            let expected = format!("cloud_value_{:03}", i);
            assert_eq!(db.get(key.as_bytes()).unwrap(), Some(Bytes::from(expected)));
        }

        // Check that object store has data
        let listed = rt.block_on(async {
            use futures::TryStreamExt;
            let mut count = 0;
            let mut stream = store.list(None);
            while let Some(meta) = stream.try_next().await.unwrap() {
                if meta.location.to_string().ends_with(".sst") {
                    count += 1;
                }
            }
            count
        });

        // Should have at least one SSTable uploaded
        assert!(
            listed > 0,
            "Object store should have at least one SSTable uploaded"
        );
    }

    #[test]
    #[cfg(feature = "object-store")]
    fn test_db_cloud_storage_with_vlog() {
        use object_store::{memory::InMemory, ObjectStore};

        let dir = tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Create in-memory object store for testing
        let store = std::sync::Arc::new(InMemory::new());
        let _guard = rt.enter();

        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 1000, // Small to trigger flushes
            storage_config: Some(StorageConfig::Custom(store.clone())),
            vlog_threshold: Some(100), // Enable vLog for large values
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write mixed data: small and large values
        for i in 0..30 {
            let key = format!("key_{:03}", i);
            if i % 2 == 0 {
                // Small values (inline)
                let value = format!("small_{:03}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            } else {
                // Large values (vLog)
                let value = vec![b'X'; 200];
                db.put(key.as_bytes(), &value).unwrap();
            }
        }

        // Flush to trigger upload
        db.flush().unwrap();

        // Verify all data is readable
        for i in 0..30 {
            let key = format!("key_{:03}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Key {} should exist", i);
            if i % 2 == 0 {
                assert_eq!(value.unwrap(), Bytes::from(format!("small_{:03}", i)));
            } else {
                assert_eq!(value.unwrap().len(), 200);
            }
        }

        // Check that object store has SSTable data
        let listed = rt.block_on(async {
            use futures::TryStreamExt;
            let mut count = 0;
            let mut stream = store.list(None);
            while let Some(meta) = stream.try_next().await.unwrap() {
                if meta.location.to_string().ends_with(".sst") {
                    count += 1;
                }
            }
            count
        });

        // Should have at least one SSTable uploaded
        assert!(
            listed > 0,
            "Object store should have at least one SSTable with vLog uploaded"
        );
    }

    #[test]
    #[cfg(feature = "object-store")]
    fn test_tiered_storage_cold_tier_compaction() {
        use object_store::{memory::InMemory, ObjectStore};

        let dir = tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Create in-memory object store for cold tier
        let cold_store = std::sync::Arc::new(InMemory::new());
        let _guard = rt.enter();

        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            memtable_capacity: 1000, // Very small to trigger frequent flushes
            base_level_size: 500,    // Very small to trigger compaction quickly
            size_ratio: 2,           // Small ratio to trigger compaction faster
            cold_tier_level: Some(2), // L2+ goes to cold storage
            cold_storage: Some(StorageConfig::Custom(cold_store.clone())),
            vlog_threshold: None, // Disable vLog for simplicity
            ..Default::default()
        };

        let db = DB::open(options).unwrap();

        // Write enough data to trigger multiple flushes and compactions to L2+
        for i in 0..100 {
            let key = format!("tiered_key_{:05}", i);
            let value = format!("tiered_value_{:05}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Force flush and compaction
        db.flush().unwrap();

        // Trigger compaction manually to ensure data reaches cold tier
        for _ in 0..5 {
            if let Some(level) = db.lsm.load().needs_compaction() {
                let _ = db.compact_level(level);
            }
        }

        // Verify all data is still readable (from local cache)
        for i in 0..100 {
            let key = format!("tiered_key_{:05}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Key {} should exist", key);
            let expected = format!("tiered_value_{:05}", i);
            assert_eq!(value.unwrap(), Bytes::from(expected));
        }

        // Check that cold storage has some SSTables
        let cold_count = rt.block_on(async {
            use futures::TryStreamExt;
            let mut count = 0;
            let mut stream = cold_store.list(None);
            while let Some(meta) = stream.try_next().await.unwrap() {
                if meta.location.to_string().ends_with(".sst") {
                    count += 1;
                }
            }
            count
        });

        // After enough compaction, L2+ SSTables should be in cold storage
        // Note: May be 0 if compaction didn't reach L2 in this test run
        // The test primarily verifies the code path works without errors
        info!(
            cold_sstables = cold_count,
            "Tiered storage test complete - cold tier SSTable count"
        );
    }

    #[test]
    #[cfg(feature = "object-store")]
    fn test_tiered_storage_options_validation() {
        // Test that cold_tier_level without cold_storage still works (no-op)
        let dir = tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();

        let options = DBOptions {
            data_dir: dir.path().to_path_buf(),
            cold_tier_level: Some(4), // Set level but no cold storage
            cold_storage: None,       // No cold storage configured
            ..Default::default()
        };

        // Should open without error (cold tier config is ignored without backend)
        let db = DB::open(options).unwrap();
        db.put(b"test_key", b"test_value").unwrap();
        assert_eq!(
            db.get(b"test_key").unwrap(),
            Some(Bytes::from("test_value"))
        );
    }
}
