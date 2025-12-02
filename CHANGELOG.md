# Changelog

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
