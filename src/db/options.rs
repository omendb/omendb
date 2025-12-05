use crate::merge_operator::MergeOperator;
use crate::wal::{RecoveryMode, SyncPolicy};
use std::path::PathBuf;
use std::sync::Arc;

/// Options for write operations.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    pub sync: bool,
    pub skip_wal: bool,
}

impl WriteOptions {
    #[must_use]
    pub const fn sync() -> Self {
        Self {
            sync: true,
            skip_wal: false,
        }
    }

    #[must_use]
    pub const fn skip_wal() -> Self {
        Self {
            sync: false,
            skip_wal: true,
        }
    }
}

/// Options for read operations.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    pub no_cache: bool,
    pub verify_checksums: bool,
}

impl ReadOptions {
    #[must_use]
    pub const fn no_cache() -> Self {
        Self {
            no_cache: true,
            verify_checksums: false,
        }
    }

    #[must_use]
    pub const fn verify() -> Self {
        Self {
            no_cache: false,
            verify_checksums: true,
        }
    }
}

/// Database configuration options.
///
/// Use the builder pattern to configure options, then call [`open()`](Self::open)
/// to create the database. For simple cases, use [`DB::open()`](super::DB::open).
///
/// # Examples
///
/// ```rust,no_run
/// use seerdb::DBOptions;
///
/// // Simple: use DB::open directly
/// let db = seerdb::DB::open("./my_db")?;
///
/// // Configured: use builder pattern
/// let db = DBOptions::default()
///     .memtable_capacity(64 * 1024 * 1024)
///     .background_compaction(true)
///     .open("./my_db")?;
/// # Ok::<(), seerdb::DBError>(())
/// ```
#[derive(Debug, Clone)]
pub struct DBOptions {
    /// Internal: set by `open()` or `open_with()`, not by users directly.
    pub(crate) data_dir: PathBuf,
    pub memtable_capacity: usize,
    pub wal_sync_policy: SyncPolicy,
    pub recovery_mode: RecoveryMode,
    pub base_level_size: u64,
    pub size_ratio: u64,
    pub num_levels: usize,
    pub vlog_threshold: Option<usize>,
    pub background_compaction: bool,
    pub background_flush: bool,
    pub adaptive_compaction: bool,
    pub max_memory_bytes: Option<usize>,
    pub min_disk_space_bytes: Option<u64>,
    pub max_open_files: Option<usize>,
    pub block_cache_capacity: usize,
    pub buffer_pool_capacity: Option<usize>,
    #[cfg(feature = "object-store")]
    pub storage_config: Option<StorageConfig>,
    pub group_commit_delay_us: u64,
    pub group_commit_max_batch_size: usize,
    pub l0_slowdown_writes_trigger: usize,
    pub l0_stop_writes_trigger: usize,
    pub compaction_filter: Option<Arc<dyn crate::compaction::CompactionFilter>>,
    pub merge_operator: Option<Arc<dyn MergeOperator>>,
    pub disable_metrics: bool,
    pub use_direct_wal: bool,
    pub skip_wal: bool,
    pub compression: crate::sstable::CompressionType,
    #[cfg(feature = "object-store")]
    pub cold_tier_level: Option<usize>,
    #[cfg(feature = "object-store")]
    pub cold_storage: Option<StorageConfig>,
}

impl Default for DBOptions {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::new(), // Set by open()/open_with()
            memtable_capacity: 256 * 1024 * 1024,
            max_open_files: None,
            wal_sync_policy: SyncPolicy::SyncData,
            recovery_mode: RecoveryMode::default(),
            base_level_size: 10 * 1024 * 1024,
            size_ratio: 10,
            num_levels: 7,
            vlog_threshold: Some(4096),
            background_compaction: false,
            background_flush: false,
            adaptive_compaction: false,
            max_memory_bytes: None,
            min_disk_space_bytes: None,
            block_cache_capacity: 16_384,
            #[cfg(feature = "object-store")]
            storage_config: None,
            group_commit_delay_us: 0,
            group_commit_max_batch_size: 1000,
            l0_slowdown_writes_trigger: 20,
            l0_stop_writes_trigger: 36,
            compaction_filter: None,
            buffer_pool_capacity: None,
            merge_operator: None,
            disable_metrics: false,
            use_direct_wal: false,
            skip_wal: false,
            compression: crate::sstable::CompressionType::Lz4,
            #[cfg(feature = "object-store")]
            cold_tier_level: None,
            #[cfg(feature = "object-store")]
            cold_storage: None,
        }
    }
}

impl DBOptions {
    /// Create a new options builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a database with these options at the given path.
    ///
    /// This is the primary way to open a configured database. For simple cases
    /// with default options, use [`DB::open()`](super::DB::open) instead.
    ///
    /// Options can be reused to open multiple databases with the same configuration:
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use seerdb::DBOptions;
    ///
    /// let opts = DBOptions::default()
    ///     .memtable_capacity(128 * 1024 * 1024)
    ///     .background_compaction(true);
    ///
    /// let db1 = opts.open("./db1")?;
    /// let db2 = opts.open("./db2")?;  // Reuse same options
    /// # Ok::<(), seerdb::DBError>(())
    /// ```
    pub fn open(&self, path: impl AsRef<std::path::Path>) -> super::Result<super::DB> {
        super::DB::open_with(path, self.clone())
    }

    /// Configuration profile for embedded/single-process applications.
    ///
    /// Optimized for lower memory usage and simpler operation:
    /// - 64MB memtable
    /// - Smaller block cache
    /// - Direct WAL writes
    /// - Metrics disabled
    /// - Synchronous compaction
    #[must_use]
    pub fn embedded() -> Self {
        Self {
            memtable_capacity: 64 * 1024 * 1024,
            block_cache_capacity: 4_096,
            use_direct_wal: true,
            disable_metrics: true,
            background_compaction: false,
            background_flush: false,
            ..Default::default()
        }
    }

    /// Configuration profile for high-throughput server workloads.
    ///
    /// Optimized for write-heavy workloads:
    /// - 512MB memtable
    /// - Large block cache
    /// - Background compaction and flush
    /// - Adaptive compaction enabled
    #[must_use]
    pub fn high_throughput() -> Self {
        Self {
            memtable_capacity: 512 * 1024 * 1024,
            block_cache_capacity: 65_536,
            background_compaction: true,
            background_flush: true,
            adaptive_compaction: true,
            use_direct_wal: false,
            disable_metrics: false,
            ..Default::default()
        }
    }

    /// Configuration profile for large-scale deployments (1B+ keys).
    ///
    /// Optimized for very large datasets:
    /// - 1GB memtable
    /// - Maximum block cache
    /// - Aggressive vLog threshold for large values
    /// - All background operations enabled
    #[must_use]
    pub fn large_scale() -> Self {
        Self {
            memtable_capacity: 1024 * 1024 * 1024,
            block_cache_capacity: 131_072,
            base_level_size: 64 * 1024 * 1024,
            background_compaction: true,
            background_flush: true,
            adaptive_compaction: true,
            vlog_threshold: Some(1024),
            use_direct_wal: false,
            disable_metrics: false,
            ..Default::default()
        }
    }

    /// Set the memtable capacity in bytes.
    #[must_use]
    pub const fn memtable_capacity(mut self, bytes: usize) -> Self {
        self.memtable_capacity = bytes;
        self
    }

    /// Set the block cache capacity (number of blocks).
    #[must_use]
    pub const fn block_cache_capacity(mut self, num_blocks: usize) -> Self {
        self.block_cache_capacity = num_blocks;
        self
    }

    /// Set the WAL sync policy.
    #[must_use]
    pub const fn sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.wal_sync_policy = policy;
        self
    }

    /// Enable or disable background compaction.
    #[must_use]
    pub const fn background_compaction(mut self, enabled: bool) -> Self {
        self.background_compaction = enabled;
        self
    }

    /// Enable or disable background flush.
    #[must_use]
    pub const fn background_flush(mut self, enabled: bool) -> Self {
        self.background_flush = enabled;
        self
    }

    /// Enable or disable metrics collection.
    #[must_use]
    pub const fn metrics(mut self, enabled: bool) -> Self {
        self.disable_metrics = !enabled;
        self
    }

    /// Enable or disable direct WAL writes.
    #[must_use]
    pub const fn direct_wal(mut self, enabled: bool) -> Self {
        self.use_direct_wal = enabled;
        self
    }

    /// Enable or disable WAL entirely.
    #[must_use]
    pub const fn skip_wal(mut self, enabled: bool) -> Self {
        self.skip_wal = enabled;
        self
    }

    /// Set the value size threshold for vLog separation.
    #[must_use]
    pub const fn vlog_threshold(mut self, threshold: Option<usize>) -> Self {
        self.vlog_threshold = threshold;
        self
    }

    /// Set the compression type for SSTables.
    #[must_use]
    pub const fn compression(mut self, compression: crate::sstable::CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Set the merge operator for read-modify-write operations.
    #[must_use]
    pub fn merge_operator(mut self, operator: Arc<dyn MergeOperator>) -> Self {
        self.merge_operator = Some(operator);
        self
    }

    /// Set the recovery mode for WAL replay.
    #[must_use]
    pub const fn recovery_mode(mut self, mode: RecoveryMode) -> Self {
        self.recovery_mode = mode;
        self
    }

    /// Set the base level size for LSM compaction (bytes).
    #[must_use]
    pub const fn base_level_size(mut self, bytes: u64) -> Self {
        self.base_level_size = bytes;
        self
    }

    /// Set the size ratio between LSM levels.
    #[must_use]
    pub const fn size_ratio(mut self, ratio: u64) -> Self {
        self.size_ratio = ratio;
        self
    }

    /// Set the number of LSM levels.
    #[must_use]
    pub const fn num_levels(mut self, levels: usize) -> Self {
        self.num_levels = levels;
        self
    }

    /// Enable or disable adaptive compaction.
    #[must_use]
    pub const fn adaptive_compaction(mut self, enabled: bool) -> Self {
        self.adaptive_compaction = enabled;
        self
    }

    /// Set the maximum memory budget in bytes.
    #[must_use]
    pub const fn max_memory_bytes(mut self, bytes: Option<usize>) -> Self {
        self.max_memory_bytes = bytes;
        self
    }

    /// Set the minimum disk space threshold in bytes.
    #[must_use]
    pub const fn min_disk_space_bytes(mut self, bytes: Option<u64>) -> Self {
        self.min_disk_space_bytes = bytes;
        self
    }

    /// Set the maximum number of open files.
    #[must_use]
    pub const fn max_open_files(mut self, max: Option<usize>) -> Self {
        self.max_open_files = max;
        self
    }

    /// Set the buffer pool capacity.
    #[must_use]
    pub const fn buffer_pool_capacity(mut self, capacity: Option<usize>) -> Self {
        self.buffer_pool_capacity = capacity;
        self
    }

    /// Set the group commit delay in microseconds.
    #[must_use]
    pub const fn group_commit_delay_us(mut self, us: u64) -> Self {
        self.group_commit_delay_us = us;
        self
    }

    /// Set the maximum batch size for group commits.
    #[must_use]
    pub const fn group_commit_max_batch_size(mut self, size: usize) -> Self {
        self.group_commit_max_batch_size = size;
        self
    }

    /// Set the L0 file count that triggers write slowdown.
    #[must_use]
    pub const fn l0_slowdown_writes_trigger(mut self, count: usize) -> Self {
        self.l0_slowdown_writes_trigger = count;
        self
    }

    /// Set the L0 file count that stops writes entirely.
    #[must_use]
    pub const fn l0_stop_writes_trigger(mut self, count: usize) -> Self {
        self.l0_stop_writes_trigger = count;
        self
    }

    /// Set a compaction filter for custom key filtering during compaction.
    #[must_use]
    pub fn compaction_filter(
        mut self,
        filter: Arc<dyn crate::compaction::CompactionFilter>,
    ) -> Self {
        self.compaction_filter = Some(filter);
        self
    }

    /// Set the storage backend configuration (requires `object-store` feature).
    #[cfg(feature = "object-store")]
    #[must_use]
    pub fn storage_config(mut self, config: StorageConfig) -> Self {
        self.storage_config = Some(config);
        self
    }

    /// Set the LSM level at which data moves to cold storage (requires `object-store` feature).
    #[cfg(feature = "object-store")]
    #[must_use]
    pub const fn cold_tier_level(mut self, level: Option<usize>) -> Self {
        self.cold_tier_level = level;
        self
    }

    /// Set the cold storage backend configuration (requires `object-store` feature).
    #[cfg(feature = "object-store")]
    #[must_use]
    pub fn cold_storage(mut self, config: StorageConfig) -> Self {
        self.cold_storage = Some(config);
        self
    }
}

/// Cloud storage backend configuration.
#[cfg(feature = "object-store")]
#[derive(Debug, Clone)]
pub enum StorageConfig {
    S3 {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        prefix: String,
    },
    Gcs {
        bucket: String,
        service_account_path: Option<PathBuf>,
        prefix: String,
    },
    Azure {
        container: String,
        account: String,
        prefix: String,
    },
    Custom(std::sync::Arc<dyn object_store::ObjectStore>),
}

/// Options for bulk loading data into the database.
#[derive(Debug, Clone)]
pub struct BulkLoadOptions {
    pub target_level: usize,
    pub max_entries_per_sst: usize,
    pub already_sorted: bool,
}

impl Default for BulkLoadOptions {
    fn default() -> Self {
        Self {
            target_level: 6,
            max_entries_per_sst: 1_000_000,
            already_sorted: false,
        }
    }
}

impl BulkLoadOptions {
    #[must_use]
    pub const fn with_target_level(mut self, level: usize) -> Self {
        self.target_level = level;
        self
    }

    #[must_use]
    pub const fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries_per_sst = max;
        self
    }

    #[must_use]
    pub const fn already_sorted(mut self) -> Self {
        self.already_sorted = true;
        self
    }
}

/// Statistics from a bulk load operation.
#[derive(Debug, Clone, Default)]
pub struct BulkLoadStats {
    pub entries_loaded: u64,
    pub sstables_created: u64,
    pub bytes_written: u64,
    pub target_level: usize,
}
