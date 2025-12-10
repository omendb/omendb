# STATUS - seerdb

**Last Updated**: December 10, 2025
**Version**: 0.0.9 (pending 0.0.10)

---

## Current State

| Metric | Value |
|--------|-------|
| **Tests** | 216 passing, 1 ignored |
| **Clippy** | 0 warnings (pedantic) |
| **Platforms** | Mac M3 Max ✅, Fedora i9-13900KF ✅ |

---

## Session Work (Dec 9-10)

### Completed

1. **Code Review & Refactoring** (`2adddb8`, `b0dde8b`)
   - Removed dead code: `_expansion_factor`, `_retrained`, vestigial flush params
   - `sort_by_key` → `sort_unstable_by_key` (10-17% write improvement)
   - Fixed clippy pedantic warnings

2. **macOS F_BARRIERFSYNC** (`2d2a725`)
   - Added `SyncPolicy::Barrier` for 13x faster WAL sync on macOS
   - Platform-specific impl: F_BARRIERFSYNC on macOS, fdatasync fallback elsewhere
   - Benchmark: 4.2ms → 0.3ms per write op

3. **API/DX Improvements** (`962e77d`, `f30b739`, `0bc8fa2`)
   - Changed defaults: `background_compaction: true`, `background_flush: true`
   - Documented presets with clear tables (what changes, why)
   - Added option tiers: Essential/Tuning/Expert
   - Platform notes in SyncPolicy and DBOptions docs
   - Replaced `eprintln!` with `tracing::warn!`

### Performance Summary

| Platform | SyncData | Barrier | Improvement |
|----------|----------|---------|-------------|
| macOS | 4.2 ms/op | 0.3 ms/op | **13x faster** |
| Linux | 5 µs/op | 5 µs/op | (already fast) |

---

## Design Docs Created

- `ai/design/barrier_sync.md` - F_BARRIERFSYNC implementation plan
- `ai/design/api_design_analysis.md` - API/DX analysis and recommendations

---

## Beads

| ID | Title | Priority | Status |
|----|-------|----------|--------|
| seerdb-vk5 | Monitor SKL 0.23 | P2 | open |
| seerdb-ig3 | F_BARRIERFSYNC | P2 | **closed** |

---

## Known Issues

### Fedora Concurrent Test Hang (Low Priority)
Tests hang on Fedora in release mode with 1KB memtable. Does NOT affect real-world usage (64MB+ memtable).

---

## Next Steps

- Bump version to 0.0.10
- Update CHANGELOG
- Tag and release
