# Changelog

## [0.0.6] - 2025-12-05

### Fixed

- **Transaction sequence race (P1)**: Concurrent transactions could get overlapping sequence numbers due to load/store race. Fixed with atomic `fetch_add` before writes.
- **Merge operator data loss (P1)**: Merge failure incorrectly converted to tombstone, losing operands. Now preserves operands on merge failure.
- **Memtable snapshot isolation bypass (P1)**: `put_entry` used internal sequence counter, bypassing transaction isolation. Added explicit sequence parameter.
- **Buffer pool eviction race (P2)**: Frame could be evicted between allocation and pin. Now pins frame inside `allocate_frame()`.
- **WAL errors silently ignored (P2)**: WAL record application failures were discarded. Now logs warnings with sequence context.
- **Infinite wait loops (P2)**: Write stall and flush wait could hang forever if workers died. Added 30-60s timeouts.

## [0.0.5] - 2025-12-05

### Changed

- **BREAKING: API redesign following `std::fs` pattern**
  - `DB::open(path)` - simple open with defaults (like `File::open`)
  - `DBOptions::default()...open(path)` - configured open (like `OpenOptions`)
  - Path is now passed to `open()`, not stored in options
  - Builder methods renamed: `with_memtable_capacity()` → `memtable_capacity()`, etc.
  - Profile constructors no longer take path: `DBOptions::embedded()`, `high_throughput()`, `large_scale()`
  - `DBOptions::open(&self, path)` takes `&self` for reusability (matches `OpenOptions::open`)
  - Complete builder API: all options now have builder methods (no struct literals needed)

### Migration

```rust
// Before (0.0.4)
let db = DB::open(DBOptions { data_dir: path, ..Default::default() })?;
let db = DB::open(DBOptions::embedded(path))?;
let db = DB::open(DBOptions::default().with_memtable_capacity(64_MB))?;

// After (0.0.5)
let db = DB::open(path)?;
let db = DBOptions::embedded().open(path)?;
let db = DBOptions::default().memtable_capacity(64_MB).open(path)?;
```

## [0.0.4] - 2025-12-03

### Fixed

- **Merge operator bug**: `Put` + `Merge` operations returned incorrect values. Fixed handling of base values in SSTable reads and merge operand ordering throughout the read path.
- **Tombstone bloom filter bug**: Tombstones and merge operands were incorrectly inserting internal keys (with sequence numbers) into bloom filters, but reads lookup by user key. This caused false negatives where deleted keys could return stale data.
- **L0 SSTable iteration order**: L0 SSTables were iterated in wrong order during scans, causing tombstones in newer SSTables to not shadow values in older ones. Deleted keys could incorrectly return old values.

### Added

- Failpoint infrastructure for deterministic crash testing (`--features failpoints`)
  - `flush::after_sstable_write`, `flush::before_wal_clear`
  - `compaction::after_output_write`, `wal::after_sync`
- Linearizability tests for concurrent operations
- Power failure tests using dm-flakey (Linux)
- Recovery verification tests (metamorphic, differential)

## [0.0.3] - 2025-12-02

### Fixed

- jemalloc TLS: Use `disable_initial_exec_tls` for Python extension compatibility on Linux
  - Fixes "cannot allocate memory in static TLS block" when loading via `dlopen()`

### Changed

- SIMD module always available with internal feature gating
- CI tests both stable (no simd) and nightly (simd)

## [0.0.2] - 2025-11-28

### Added

- Initial public release
- Learned indexes (ALEX) for adaptive key distribution
- Key-value separation (WiscKey) for reduced write amplification
- OCC transactions with snapshot isolation
- Point-in-time snapshots
- Range queries and prefix scans
- Tiered storage with S3/GCS/Azure support
- Compression (ZSTD/LZ4) and SIMD optimizations
