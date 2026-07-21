# seerdb

High-performance, general-purpose embedded storage engine for modern SSDs. Written in Rust.

## What

An embedded key-value storage engine designed from scratch for modern hardware:

- **Out-of-place writes** (LeanStore-inspired): immutable page versions reduce rewrite amplification; SeerDB performance claims remain to be benchmarked
- **KV separation** (WiscKey-inspired): large values stored separately for lower write amplification
- **SSD-native**: designed for NVMe, with optional FDP/ZNS support
- **Snapshots**: immutable root generations with explicit retained historical
  reads; durable data-version MVCC remains a future extension

## Direction

The accepted architecture is a record-aware, out-of-place, versioned B+tree
with WAL, immutable root generations, snapshots, blobs, and generation-safe
reclamation. The first production slice is deliberately a single-writer
durable kernel with concurrent readers. See
[`ai/design/target_architecture.md`](ai/design/target_architecture.md) for the
current design and staged optimization plan.

## Status

The current Rust lane is a single-writer durable kernel with concurrent reads,
root-generation retention, WAL recovery, and crash-safe reclamation baselines.
It is still under production qualification; see [ai/STATUS.md](ai/STATUS.md)
for the open ENOSPC, soak, MVCC, migration, and performance gates.

## Portable qualification

Run the deterministic mixed workload and emit a JSON qualification artifact:

```bash
cargo run --release --all-features --example seerdb_qualification -- \
  --keys 128 --operations 512 --output /tmp/seerdb-qualification.json
```

The harness checks a reference model, retained-root reads, bounded compaction,
periodic close/reopen verification, vacuum, history pruning, offline
`DB::check()`, and final close/reopen.
It reports p50/p95/p99 operation latencies, page-write and space-amplification
observations, separates user-commit work from maintenance/restart work in its
physical counters, and records the final digest. These are portable
qualification measurements, not cross-engine or device-backed performance
claims; the ext4/XFS power-loss runner remains a separate privileged gate.

## References

- LeanStore (VLDB 2024, 2026) — out-of-place B-tree, SSD-aware buffer management
- "How to Write to SSDs" (VLDB 2026) — DB-SSD co-optimization, NoWA pattern
- WiscKey (FAST 2016) — key-value separation for reduced write amplification
- ZLeanStore (GitHub) — C++ implementation of out-of-place B-tree with blob storage

## License

Apache License 2.0
