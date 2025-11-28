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
#[derive(Debug, Clone)]
pub struct DBOptions {
    pub data_dir: PathBuf,
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
            data_dir: PathBuf::from("./seerdb_data"),
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
    /// Configuration profile for embedded/single-process applications.
    #[must_use]
    pub fn embedded(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
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
    #[must_use]
    pub fn high_throughput(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
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
    #[must_use]
    pub fn large_scale(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
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

    pub fn with_data_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_dir = path.into();
        self
    }

    #[must_use]
    pub const fn with_memtable_capacity(mut self, bytes: usize) -> Self {
        self.memtable_capacity = bytes;
        self
    }

    #[must_use]
    pub const fn with_block_cache_capacity(mut self, num_blocks: usize) -> Self {
        self.block_cache_capacity = num_blocks;
        self
    }

    #[must_use]
    pub const fn with_sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.wal_sync_policy = policy;
        self
    }

    #[must_use]
    pub const fn with_background_compaction(mut self, enabled: bool) -> Self {
        self.background_compaction = enabled;
        self
    }

    #[must_use]
    pub const fn with_background_flush(mut self, enabled: bool) -> Self {
        self.background_flush = enabled;
        self
    }

    #[must_use]
    pub const fn with_metrics(mut self, enabled: bool) -> Self {
        self.disable_metrics = !enabled;
        self
    }

    #[must_use]
    pub const fn with_direct_wal(mut self, enabled: bool) -> Self {
        self.use_direct_wal = enabled;
        self
    }

    #[must_use]
    pub const fn with_skip_wal(mut self, enabled: bool) -> Self {
        self.skip_wal = enabled;
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
