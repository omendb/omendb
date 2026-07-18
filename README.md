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

## References

- LeanStore (VLDB 2024, 2026) — out-of-place B-tree, SSD-aware buffer management
- "How to Write to SSDs" (VLDB 2026) — DB-SSD co-optimization, NoWA pattern
- WiscKey (FAST 2016) — key-value separation for reduced write amplification
- ZLeanStore (GitHub) — C++ implementation of out-of-place B-tree with blob storage

## License

Apache License 2.0
