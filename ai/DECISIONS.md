# DECISIONS - seerdb

**Last Updated**: December 3, 2025

---

## Architecture Decisions

### ADR-001: Partitioned Memtable (16 partitions)

**Context**: Need concurrent writes without global lock contention

**Decision**: 16 partitions with foldhash for key distribution

**Rationale**:
- Lock-free reads via ArcSwap
- Scales linearly with cores
- foldhash 2x faster than xxhash for small keys

**Tradeoffs**:
- More complex flush logic (merge 16 partitions)
- Slightly higher memory overhead

---

### ADR-002: crossbeam-skiplist for Memtable

**Context**: Need concurrent sorted data structure

**Decision**: crossbeam-skiplist over BTreeMap+RwLock

**Rationale**:
- Lock-free concurrent reads
- Better cache locality than B-tree
- Proven in production (sled, etc.)

**Tradeoffs**:
- Not compatible with Loom (can't exhaustively test concurrency)
- Higher memory overhead than BTreeMap

---

### ADR-003: WiscKey Value Separation (vLog)

**Context**: Large values cause write amplification in LSM

**Decision**: Values > threshold stored in separate vLog

**Rationale**:
- Reduces SSTable size (faster compaction)
- ~10x reduction in write amplification for 1KB+ values
- Proven in WiscKey paper (FAST'16)

**Tradeoffs**:
- Extra read for large values
- GC complexity for vLog

---

### ADR-004: ALEX Learned Index

**Context**: Binary search O(log n) for SSTable lookup

**Decision**: ALEX learned index for SSTable blocks

**Rationale**:
- O(1) average lookup with learned model
- Adapts to data distribution
- From SIGMOD'20 paper

**Tradeoffs**:
- Training overhead on SSTable build
- Model storage overhead

---

### ADR-005: Failpoints for Crash Testing

**Context**: Crash recovery testing was probabilistic

**Decision**: Add `fail` crate with feature flag

**Rationale**:
- Deterministic crash injection
- Zero overhead without feature flag
- Standard practice (RocksDB uses similar)

**Tradeoffs**:
- Must maintain failpoint locations
- Tests require `--features failpoints`

---

## Not Planned

| Feature | Reason |
|---------|--------|
| io_uring | Security CVEs, unstable ABI |
| Lock-free WAL | Batch API provides same benefits |
| Column families | Use key prefixes instead |
| Loom testing | crossbeam-skiplist incompatible |
