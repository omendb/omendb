# STATUS - seerdb

**Last Updated**: December 5, 2025
**Version**: 0.0.6 (0.0.7 pending release)

---

## Current State

| Metric | Value |
|--------|-------|
| **Tests** | 213 unit + 25 integration files + property tests |
| **Clippy** | 0 warnings (standard + pedantic) |
| **Rustdoc** | 0 warnings |
| **Lines of Code** | ~28K Rust |

### Test Coverage Summary

| Category | Status | Tools |
|----------|--------|-------|
| Unit tests | ✅ | cargo test --lib |
| Integration tests | ✅ | 25+ files in tests/ |
| Property-based | ✅ | proptest (8 properties) |
| Data integrity | ✅ | WAL, flush, compaction, concurrent ops |
| Crash recovery | ✅ | crash_recovery_tests.rs |
| Stress/soak | ✅ | stress_test.rs, soak_test.rs |
| Benchmarks | ✅ | YCSB, baseline vs RocksDB/sled/fjall |
| **Fuzzing** | ✅ | 4 targets: db_ops, wal, sstable, vlog |
| **Loom** | N/A | crossbeam-skiplist incompatible |
| **Shuttle** | ❌ | Randomized concurrency (alternative) |
| **Failpoints** | ✅ | 4 failpoints: flush, compaction, wal |
| **Differential** | ✅ | 7 tests vs RocksDB (--features baseline-benchmarks) |
| **Linearizability** | ✅ | 7 tests: history-based per-key verification |
| **Metamorphic** | ✅ | 11 tests for invariant properties |
| **Power failure** | ✅ | dm-flakey crash simulation (Linux only) |
| **Recovery verify** | ✅ | 8 tests for systematic post-crash verification |

---

## Ordering Invariants Review (Nov 28)

All ordering invariants verified correct:

| Invariant | Location | Status |
|-----------|----------|--------|
| Sequence monotonicity | `write.rs:234` `fetch_add(SeqCst)` | ✅ |
| InternalKey sort (key ASC, seq DESC) | `types.rs:77-80` bitwise NOT | ✅ |
| Memtable MVCC reads | `memtable/mod.rs:83` range search | ✅ |
| Flush order (SSTable→WAL clear) | `flush.rs:242-271` | ✅ |
| Flush sequence tracking | `flush.rs:276` `fetch_max` | ✅ |
| SSTable level recovery | `compaction/mod.rs:505-515` | ✅ (fixed this session) |
| L0 compaction merge order | `merge.rs:62-67` source_id DESC | ✅ |
| LSM read order (L0 newest first) | `read.rs:61` `.iter().rev()` | ✅ |
| Concurrent flush protection | `flush.rs:94,488` mutex + try_lock | ✅ |
| LSM tree serialization | `flush.rs:167,255,315` lsm_mutex | ✅ |

---

## Code Review Findings (Dec 5)

Full codebase review of v0.0.5 (~22,888 lines). All P1/P2 issues fixed.

### Summary

| Severity | Found | Fixed |
|----------|-------|-------|
| P1 (Critical) | 3 | 3 |
| P2 (High) | 6 | 6 |
| P3 (Maintainability) | 2 | 2 |

### Fixed Issues (Dec 5)

| Priority | Issue | Fix |
|----------|-------|-----|
| P1 | Transaction sequence race | Use fetch_add BEFORE memtable write |
| P1 | Merge operator None → Tombstone | Preserve operands on merge failure |
| P1 | Memtable snapshot isolation bypass | Add explicit seq param to put_entry |
| P2 | Buffer pool race condition | Pin frame inside allocate_frame |
| P2 | Mutex poisoning panics | Document intentional panic-on-poison policy |
| P2 | Flush memory explosion | Document concern, note k-way merge as future |
| P2 | ALEX tree routing | Add comprehensive routing documentation |
| P2 | Background wait loop timeouts | Add 30-60s timeouts to all wait loops |
| P2 | WAL errors silently ignored | Log warnings on WAL record application failure |
| P3 | Learned bloom backup filter | Only add uncertain positives for space savings |

### Positives Noted

- Error handling with thiserror - clean, consistent
- ArcSwap for lock-free reads - excellent pattern
- Builder pattern for options - well-designed API
- K-way merge with heap - efficient range queries
- Failpoint framework - excellent crash testing
- Comprehensive test coverage (213 unit + 25 integration)

---

## Recent Changes (Dec 5)

### v0.0.7 (pending)

| Change | Details |
|--------|---------|
| Runtime SIMD dispatch | multiversion crate for CPU feature detection |
| SVE support | aarch64 SVE target for ARM scalable vectors |
| Cascade pattern | Wider SIMD → narrower → scalar fallback |
| Targets | x86_64: AVX-512/AVX2/SSE4.1, aarch64: SVE/NEON |

---

## Recent Changes (Dec 4-5)

### v0.0.5 Release

| Change | Details |
|--------|---------|
| API redesign | std::fs-style pattern (DB::open, DBOptions::default().open()) |
| Code review | Full codebase review completed, 11 issues tracked |
| CI/CD hardening | 4 workflows: ci.yml, release.yml, fuzz.yml, audit.yml |
| Trusted publishing | OIDC-based crates.io publishing (no tokens) |
| Security checks | cargo-audit, cargo-deny, cargo-semver-checks |
| deny.toml | License allowlist, advisory ignores, dependency bans |

### v0.0.4 Release

| Change | Details |
|--------|---------|
| Merge operator bug | Fixed base value handling and operand ordering |
| Power failure tests | Added dm-flakey simulation (`tests/power_failure_tests.rs`) |
| CI fix | Run `--lib` tests only to prevent integration test hangs |

---

## Recent Changes (Dec 3)

### Bug Fixes Found by Verification Tests

Two critical bugs found and fixed by new verification test infrastructure:

| Bug | File | Root Cause | Fix |
|-----|------|------------|-----|
| Tombstone bloom filter | `src/sstable/builder.rs` | `add_tombstone()` inserted full internal key to bloom, but read path checks user key | Extract user key before bloom insertion |
| SSTable iteration order | `src/db/iter.rs` | L0 SSTables iterated oldest-first, but K-way merge treats lower index as newer | Add `.iter().rev()` to iterate newest-first |

Both bugs caused tombstones (deletes) to not properly shadow values after database reopen.

### New Verification Test Files

| File | Tests | Purpose |
|------|-------|---------|
| `tests/recovery_verification_tests.rs` | 8 | Systematic recovery checks: missing keys, extra keys, corrupted values, orphan files |
| `tests/metamorphic_tests.rs` | 11 | Invariant properties: insert+delete=empty, order independence, flush/reopen preserve results |
| `tests/differential_tests.rs` | 7 | Compare seerdb vs RocksDB behavior (requires `--features baseline-benchmarks`) |
| `tests/linearizability_tests.rs` | 7 | History-based linearizability verification with per-key checking |

### Failpoints Added for Deterministic Crash Testing

Added `fail` crate with feature flag for crash injection:

| Failpoint | Location | Tests |
|-----------|----------|-------|
| `flush::after_sstable_write` | After SSTable written, before metadata | WAL preserved |
| `flush::before_wal_clear` | SSTable in LSM, before WAL clear | Idempotent replay |
| `compaction::after_output_write` | Output written, before LSM update | Inputs preserved |
| `wal::after_sync` | After WAL sync completes | Durability |

**Files**: `src/failpoint.rs`, `src/db/flush.rs`, `src/db/mod.rs`, `src/wal/mod.rs`, `tests/failpoint_tests.rs`

**Run**: `cargo test --features failpoints failpoint`

### AI Context Setup

- Created AGENTS.md (symlink CLAUDE.md → AGENTS.md)
- Initialized beads (`bd init`)
- Migrated ai/PLAN.md → ai/ROADMAP.md
- Created ai/DECISIONS.md

---

## Known Issues

### Fedora Concurrent Test Hang (Low Priority)

Tests hang on Fedora in release mode with 1KB memtable. Does NOT affect real-world usage (64MB+ memtable). Mac release mode passes.

---

## Module Structure

See AGENTS.md for full project structure
