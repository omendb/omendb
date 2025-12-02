#![cfg_attr(feature = "simd", feature(portable_simd))] // SIMD optimizations (nightly-only)
// Pedantic allows for systems code
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]
#![allow(clippy::cast_possible_truncation)] // Systems code often truncates intentionally
#![allow(clippy::cast_precision_loss)] // f64 calculations are approximate by design
#![allow(clippy::cast_sign_loss)] // Intentional in hashing/indexing
#![allow(clippy::cast_lossless)] // Prefer explicit `as` for clarity in hot paths
#![allow(clippy::cast_possible_wrap)] // Intentional in size calculations
#![allow(clippy::missing_errors_doc)] // Internal errors are self-explanatory
#![allow(clippy::missing_panics_doc)] // Panics indicate bugs, not expected behavior
#![allow(clippy::items_after_statements)] // Common pattern for scoped helpers
#![allow(clippy::too_many_lines)] // Complex DB operations require complex functions
#![allow(clippy::must_use_candidate)] // Builder pattern methods don't need must_use
#![allow(clippy::unused_self)] // Future-proofing for method signatures
#![allow(clippy::ref_option)] // &Option<T> is sometimes clearer than Option<&T>
#![allow(clippy::wildcard_imports)] // Used intentionally in modules
#![allow(clippy::iter_not_returning_iterator)] // iter() returns cursor-like types
#![allow(clippy::assigning_clones)] // clone_from() isn't always better for small types
#![allow(clippy::explicit_iter_loop)] // .iter() is more explicit than &
#![allow(clippy::struct_excessive_bools)] // Config structs need many bools
#![allow(clippy::return_self_not_must_use)] // Builder patterns
#![allow(clippy::needless_lifetimes)] // Sometimes explicit lifetimes are clearer
#![allow(clippy::struct_field_names)] // Prefixing fields is sometimes clearer
#![allow(clippy::manual_let_else)] // let-else isn't always clearer
#![allow(clippy::match_same_arms)] // Explicit match arms can be clearer
#![allow(clippy::missing_fields_in_debug)] // Debug impls don't need all fields
#![allow(clippy::default_trait_access)] // Type::default() is sometimes clearer
#![allow(clippy::unit_arg)] // Matching on () is valid
#![allow(clippy::unnecessary_wraps)] // Wrapping in Option/Result for API consistency

//! seerdb - Research-grade embedded storage engine
//!
//! A modern LSM-tree based key-value storage engine implementing 2018-2024 research
//! on learned data structures, workload-aware optimization, and efficient key-value separation.
//!
//! # Features
//!
//! - **LSM-tree architecture**: Write-optimized with efficient compaction
//! - **Durability**: Write-ahead logging with configurable sync policies
//! - **Concurrency**: Lock-free reads with concurrent writes
//! - **Observability**: Built-in metrics, health checks, and structured logging
//! - **Key-Value Separation**: WiscKey-style vLog for large values (reduces write amplification)
//! - **Background Compaction**: Non-blocking async compaction for better write throughput
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use seerdb::{DB, DBOptions};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Open database with default options
//! let db = DB::open(DBOptions::default())?;
//!
//! // Write data
//! db.put(b"hello", b"world")?;
//!
//! // Read data
//! let value = db.get(b"hello")?;
//! assert_eq!(value, Some(bytes::Bytes::from("world")));
//!
//! // Delete data
//! db.delete(b"hello")?;
//! # Ok(())
//! # }
//! ```
//!
//! # Advanced Configuration
//!
//! ```rust,no_run
//! use seerdb::{DB, DBOptions, SyncPolicy};
//! use std::path::PathBuf;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let opts = DBOptions {
//!     data_dir: PathBuf::from("./my_database"),
//!     memtable_capacity: 64 * 1024 * 1024,  // 64MB memtable
//!     wal_sync_policy: SyncPolicy::SyncData, // fsync data on each write
//!     background_compaction: true,            // Enable async compaction
//!     vlog_threshold: Some(4096),            // Use vLog for values >4KB
//!     ..Default::default()
//! };
//!
//! let db = DB::open(opts)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Architecture
//!
//! seerdb uses an LSM-tree architecture with the following components:
//!
//! - **Memtable**: In-memory buffer using concurrent skiplist
//! - **WAL**: Write-ahead log for durability
//! - **`SSTable`**: Sorted string tables on disk with bloom filters
//! - **LSM Levels**: 7 levels with exponential sizing (10x ratio)
//! - **`VLog`**: Optional value log for key-value separation (large values)
//! - **Compaction**: Background merge of `SSTables` to reduce read amplification
//!
//! # Performance Characteristics
//!
//! - **Writes**: O(log n) in-memory + O(1) WAL append
//! - **Reads**: O(log n) skiplist + O(levels) `SSTable` lookups with bloom filter optimization
//! - **Scans**: Efficient via merge iteration over memtable + `SSTables`
//! - **Space Amplification**: ~2x (typical LSM-tree)
//! - **Write Amplification**: 10-30x (reduced with vLog for large values)
//!
//! # Durability Guarantees
//!
//! seerdb provides configurable durability via [`SyncPolicy`]:
//!
//! - `SyncAll`: fsync both data and metadata (slowest, strongest)
//! - `SyncData`: fsync data only (fast, strong)
//! - `None`: No fsync (fastest, data loss possible on crash)
//!
//! # Observability
//!
//! Built-in metrics and health checks for production deployment:
//!
//! ```rust,ignore
//! # use seerdb::{DB, DBOptions};
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let db = DB::open(DBOptions::default())?;
//! // Get current database statistics
//! let stats = db.stats();
//! println!("Operations: {} reads, {} writes", stats.total_reads, stats.total_writes);
//!
//! // Check database health
//! let health = db.health();
//! println!("Health: {:?}", health);
//! # Ok(())
//! # }
//! ```

// Use jemalloc as the global allocator for better multi-threaded performance
// Tested jemalloc vs mimalloc: jemalloc wins 3/4 workloads (+17-21% improvement)
// Uses disable_initial_exec_tls to work with Python extensions on Linux (glibc TLS limitation)
// Disabled when using dhat profiler (conflicts with #[global_allocator])
#[cfg(not(feature = "dhat-heap"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Internal modules (not re-exported, but accessible for tests)
#[doc(hidden)]
pub mod alex;
mod background_workers;
#[doc(hidden)]
pub mod bloom;
#[doc(hidden)]
pub mod buffer;
#[doc(hidden)]
pub mod compaction;
mod db_helpers;
#[doc(hidden)]
pub mod memtable;
#[doc(hidden)]
pub mod range;
#[doc(hidden)]
pub mod range_merge;
#[doc(hidden)]
pub mod simd;
#[doc(hidden)]
pub mod sstable;
#[doc(hidden)]
pub mod storage;
#[doc(hidden)]
pub mod types;
#[doc(hidden)]
pub mod vlog;
#[doc(hidden)]
pub mod wal;

// Public modules (user-facing API)
pub mod batch;
pub mod db;
pub mod health;
pub mod merge_operator;
pub mod metrics;
pub mod scan;
pub mod snapshot;
pub mod transaction;

// Re-export public API types
// Core database types
#[cfg(feature = "object-store")]
pub use db::StorageConfig;
pub use db::{DBError, DBOptions, ReadOptions, WriteOptions, DB};

// Configuration
pub use sstable::CompressionType;
pub use wal::{RecoveryMode, SyncPolicy};

// Operations
pub use batch::Batch;
pub use scan::{Scan, ScanIterator};
pub use snapshot::Snapshot;
pub use transaction::{Transaction, TransactionConflict};

// Merge operators (user-extensible)
pub use merge_operator::{MergeOperator, StringAppendOperator};

// Observability
pub use health::{CheckStatus, HealthCheck, HealthStatus};
pub use metrics::DBStats;

// Bulk operations
pub use db::{BulkLoadOptions, BulkLoadStats, VerifyResult};
