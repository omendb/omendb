# TODO - seerdb

**Last Updated**: December 3, 2025

---

## Ready

### Verification Gaps (Dec 2025)

| Task | Priority | Effort | Status |
|------|----------|--------|--------|
| Differential testing vs RocksDB | HIGH | LOW | ✅ Done (7 tests) |
| Metamorphic testing | MEDIUM | LOW | ✅ Done (11 tests) |
| Recovery verification framework | HIGH | MEDIUM | ✅ Done (8 tests) |
| Linearizability checking | HIGH | MEDIUM | Open (beads: seerdb-piw) |
| Power failure testing (lazyfs) | HIGH | MEDIUM | Open (beads: seerdb-17n) |
| Block-level checksum verification | MEDIUM | MEDIUM | Open (beads: seerdb-crp) |
| Shuttle concurrency tests | LOW | MEDIUM | Not started |
| OSS-Fuzz integration | LOW | LOW | Not started |
| Deterministic simulation | VERY HIGH | VERY HIGH | Future |

**Tracked in beads**: `bd list` to see tasks

### How to Run Verification

```bash
# Recovery verification (8 tests)
cargo test --test recovery_verification_tests

# Metamorphic testing (11 tests)
cargo test --test metamorphic_tests

# Differential testing vs RocksDB (7 tests)
cargo test --test differential_tests --features baseline-benchmarks

# Failpoint tests (deterministic crash injection)
cargo test --features failpoints failpoint

# Fuzzing (runs indefinitely, Ctrl+C to stop)
cargo +nightly fuzz run db_operations
cargo +nightly fuzz run wal_parse
cargo +nightly fuzz run sstable_parse
cargo +nightly fuzz run vlog_parse

# Property-based tests
cargo test property

# Stress/soak tests
cargo test stress
cargo test soak
```

---

## In Progress

(none)

---

## Done

### Dec 2025
- [x] Fix tombstone bloom filter bug (user_key extraction in builder.rs)
- [x] Fix SSTable iteration order bug (newest-first for K-way merge)
- [x] Add recovery verification tests (8 tests)
- [x] Add metamorphic tests (11 tests)
- [x] Add differential tests vs RocksDB (7 tests)
- [x] Add failpoints for deterministic crash testing (4 failpoints: flush, compaction, wal)

### Nov 2025
- [x] Split `sstable/mod.rs` (2568 → 1184 lines)
- [x] Add `#[inline]` to hot paths (57 → 71 functions)
- [x] Address pedantic warnings (232 → 9 remaining)
- [x] Extract test modules (db, sstable, compaction)
- [x] Fix P0 data loss bug (SSTable level recovery)
- [x] Fix P0 multi-prefix persistence bug (user_key comparison for MVCC SSTables)

---

## Not Planned

| Feature | Reason |
|---------|--------|
| io_uring | Security CVEs |
| Lock-free WAL | Batch API is the right pattern |
| Column families | Use key prefixes |
