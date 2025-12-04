# Changelog

## [0.0.4] - 2025-12-03

### Fixed

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
