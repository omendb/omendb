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
/// use seerdb::DB;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Open database
/// let db = DB::open("./my_db")?;
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
/// use seerdb::DB;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Arc::new(DB::open("./my_db")?);
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
///
/// # Panic Policy
///
/// The database uses `expect()` on mutex locks with the intent to panic if a mutex
/// is poisoned. A poisoned mutex indicates a thread panicked while holding the lock,
/// which may have left internal data structures in an inconsistent state.
///
/// **Rationale**: For a storage engine, data integrity is paramount. If a critical
/// section panics, continuing with potentially corrupt data could lead to silent
/// data loss. Crashing immediately allows the database to recover cleanly on restart.
///
/// If you need to handle mutex poisoning gracefully (e.g., in a test harness), you
/// can catch the panic at the thread boundary.
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
    /// Open or create a database with default options.
    ///
    /// This is the simplest way to open a database. For custom configuration,
    /// use [`DBOptions`] with the builder pattern.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use seerdb::DB;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = DB::open("./my_db")?;
    /// db.put(b"key", b"value")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`DBError::Io`]: Failed to create directory or open files
    /// - [`DBError::Wal`]: WAL corruption detected during recovery
    /// - [`DBError::SSTable`]: `SSTable` checksum validation failed
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, DBOptions::default())
    }

    /// Open or create a database with custom options.
    ///
    /// Opens an existing database or creates a new one at the specified path.
    /// If a WAL exists, it will be replayed to recover uncommitted writes.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use seerdb::{DB, DBOptions};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// // Using builder pattern (preferred)
    /// let db = DBOptions::default()
    ///     .memtable_capacity(128 * 1024 * 1024)
    ///     .background_compaction(true)
    ///     .open("./my_db")?;
    ///
    /// // Or using open_with directly
    /// let db = DB::open_with("./my_db", DBOptions::embedded())?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`DBError::Io`]: Failed to create directory or open files
    /// - [`DBError::Wal`]: WAL corruption detected during recovery
    /// - [`DBError::SSTable`]: `SSTable` checksum validation failed
    #[allow(clippy::needless_pass_by_value)] // Options read extensively then stored
    pub fn open_with(path: impl AsRef<Path>, mut options: DBOptions) -> Result<Self> {
        options.data_dir = path.as_ref().to_path_buf();
        info!(
            path = ?options.data_dir,
            memtable_capacity_mb = options.memtable_capacity / (1024 * 1024),
            background_compaction = options.background_compaction,
            "Opening database"
        );

        // =========================================================================
        // Phase 1: Directory and WAL Recovery
        // =========================================================================
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
            let total_entries_before: usize = memtables_vec
                .iter()
                .map(super::memtable::Memtable::len)
                .sum();
            crate::db_helpers::recover_partitioned(
                &wal_path,
                &memtables_vec,
                options.merge_operator.as_ref(),
                options.recovery_mode,
            )?;
            let total_entries_after: usize = memtables_vec
                .iter()
                .map(super::memtable::Memtable::len)
                .sum();
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
            Arc::clone(&immutable_memtables),
            Arc::clone(&wal),
            Arc::clone(&lsm),
            Arc::clone(&lsm_mutex),
            Arc::clone(&vlog),
            Arc::clone(&sstable_counter),
            options.data_dir.clone(),
            Arc::clone(&metrics),
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
            Arc::clone(&immutable_memtables),
            Arc::clone(&wal),
            Arc::clone(&lsm),
            Arc::clone(&lsm_mutex),
            Arc::clone(&vlog),
            Arc::clone(&sstable_counter),
            options.data_dir.clone(),
            Arc::clone(&metrics),
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
    #[allow(clippy::needless_pass_by_value)] // Small struct, convenient API
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

        // Failpoint: crash after compaction output written, before LSM update
        // Test: output orphaned, inputs still in LSM, recovery sees duplicates (idempotent)
        crate::fail_point!("compaction::after_output_write");

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

        // Failpoint: crash after compaction output written, before LSM update
        // Test: output orphaned, inputs still in LSM, recovery sees duplicates (idempotent)
        crate::fail_point!("compaction::after_output_write");

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
mod tests;
