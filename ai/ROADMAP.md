# PLAN - seerdb Data Integrity Hardening

**Created**: November 27, 2025
**Goal**: Ensure no data loss bugs before release

---

## Phase 1: Critical Gap Tests

Priority tests for data loss scenarios not currently covered.

### 1.1 WAL Integrity Tests

| Test | Risk | Description |
|------|------|-------------|
| `test_partial_wal_record_crash` | HIGH | Truncate WAL mid-record, verify recovery skips incomplete record without data loss |
| `test_wal_record_crc_corruption` | HIGH | Corrupt CRC of single record (not header), verify detection |
| `test_wal_batch_partial_write` | HIGH | Batch of 10 records, truncate after record 5, verify first 5 recovered |
| `test_wal_sync_policy_guarantees` | MEDIUM | SyncAll must survive kill -9, SyncData must survive clean crash |

### 1.2 Flush Ordering Tests

| Test | Risk | Description |
|------|------|-------------|
| `test_sstable_write_before_wal_clear` | HIGH | Verify SSTable fully synced before WAL cleared |
| `test_concurrent_flush_sequence_monotonic` | HIGH | Two concurrent flushes, verify max_flushed_seq never decreases |
| `test_flush_crash_leaves_wal_intact` | MEDIUM | Crash during flush, verify WAL still has data for replay |

### 1.3 Compaction Integrity Tests

| Test | Risk | Description |
|------|------|-------------|
| `test_tombstone_shadows_across_levels` | HIGH | Delete in L1, value in L2, verify delete wins after compaction |
| `test_compaction_crash_no_data_loss` | MEDIUM | Crash mid-compaction, verify original SSTables intact |
| `test_compaction_output_atomic` | MEDIUM | Output SSTable either fully written or not present |

### 1.4 Concurrent Operation Tests

| Test | Risk | Description |
|------|------|-------------|
| `test_partition_swap_during_iteration` | HIGH | Iterator active on partition, swap happens, verify no crash/corruption |
| `test_read_during_immutable_clear` | MEDIUM | Reader holds Arc to immutable_memtables, main thread clears it |
| `test_concurrent_put_delete_same_key` | MEDIUM | Race between put and delete for same key |

### 1.5 Edge Case Tests

| Test | Risk | Description |
|------|------|-------------|
| `test_empty_value_full_lifecycle` | MEDIUM | Put empty value, flush, compact, recover, verify preserved |
| `test_key_with_null_bytes` | LOW | Key containing \x00, verify handling |
| `test_value_at_vlog_threshold_boundary` | LOW | Value exactly at threshold, threshold+1, threshold-1 |

---

## Phase 2: Code Review

### 2.1 Ordering Invariants (omendb bug pattern)

Check all places where state is updated in multiple steps:

| File | Location | Check |
|------|----------|-------|
| `db/flush.rs` | `flush_partitioned_memtable` | SSTable write → WAL clear ordering |
| `db/flush.rs` | `try_swap_memtable` | Swap → immutable_memtables store ordering |
| `db/write.rs` | `put_internal` | WAL write → memtable write ordering |
| `compaction/mod.rs` | `compact_sstables` | Output write → input deletion ordering |
| `db/mod.rs` | `open` | Recovery → WAL clear ordering |

### 2.2 Sequence Number Handling

| File | Location | Check |
|------|----------|-------|
| `db/write.rs` | `next_seq` increment | Atomic ordering correct? |
| `db/flush.rs` | `max_flushed_seq` update | Monotonically increasing? |
| `compaction/merge.rs` | MVCC GC | Snapshot seq respected? |

### 2.3 Error Handling Paths

| File | Location | Check |
|------|----------|-------|
| `db/flush.rs` | SSTable write failure | WAL not cleared? |
| `wal/mod.rs` | Sync failure | Data acknowledged but lost? |
| `vlog/mod.rs` | vLog write failure | SSTable pointer valid? |

---

## Phase 3: Refactoring (If Needed)

### 3.1 Potential Issues to Address

| Issue | Impact | Action |
|-------|--------|--------|
| `sstable/mod.rs` size (2561 lines) | Maintainability | Split into reader/writer/builder |
| No WAL record-level CRC | Data integrity | Add per-record CRC (breaking change) |
| Block cache not invalidated on compaction | Stale reads | Add invalidation hook |

### 3.2 Non-Breaking Improvements

| Improvement | Benefit |
|-------------|---------|
| Add `#[must_use]` to Result-returning functions | Prevent ignored errors |
| Add debug assertions for invariants | Catch bugs in dev |
| Add tracing spans for flush/compaction | Debug production issues |

---

## Phase 4: Language Bindings

Expand seerdb beyond Rust-only usage.

### 4.1 Python Bindings (Priority: High)

| Component | Technology | Description |
|-----------|------------|-------------|
| Build system | `maturin` | Python wheel building |
| Bindings | `pyo3` | Rust ↔ Python FFI |
| Package | `seerdb` on PyPI | `pip install seerdb` |

**API Surface**:
```python
from seerdb import DB, DBOptions

# Simple
db = DB.open("./my_db")
db.put(b"key", b"value")
value = db.get(b"key")

# Configured
db = DBOptions().memtable_capacity(64_MB).open("./my_db")

# Context manager
with DB.open("./my_db") as db:
    db.put(b"key", b"value")
```

### 4.2 C API (Priority: Medium)

| Component | Technology | Description |
|-----------|------------|-------------|
| Header gen | `cbindgen` | Generate C header |
| ABI | C-compatible | `extern "C"` functions |
| Memory | Explicit free | `seerdb_db_free()` |

### 4.3 Node.js Bindings (Priority: Low)

| Component | Technology | Description |
|-----------|------------|-------------|
| Bindings | `napi-rs` | Native Node addon |
| Package | `@seerdb/seerdb` | npm package |

---

## Execution Order

1. **Phase 1.1**: WAL tests (highest data loss risk) ✅
2. **Phase 1.2**: Flush ordering tests ✅
3. **Phase 2.1**: Code review ordering invariants ✅
4. **Phase 1.3-1.5**: Remaining tests ✅
5. **Phase 2.2-2.3**: Full code review ✅
6. **Phase 3**: Refactoring if time permits
7. **Phase 4.1**: Python bindings (next priority)

---

## Success Criteria

- [ ] All new tests pass
- [ ] No data loss in 24-hour soak test
- [ ] Code review finds no ordering bugs
- [ ] Benchmarks show no regression

---

## Benchmarks Baseline (Fedora i9-13900KF)

| Benchmark | Nov 27 | Target |
|-----------|--------|--------|
| existing_keys/100K | 447 µs | ≤ 500 µs |
| missing_keys/100K | 12.2 µs | ≤ 15 µs |
| full_scan/10K | 998 µs | ≤ 1100 µs |

---

*Last Updated: November 27, 2025*
