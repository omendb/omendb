# OmenDB benchmark results

Measured with `examples/oltp_bench.rs` (`cargo run --release --example
oltp_bench`) on Apple Silicon (APFS), omendb 0.1.0-alpha.1 with the
DirectSeerStore backend. Numbers are single-process; treat them as
engineering baselines, not marketing claims.

## Current numbers

| Workload | Throughput |
|---|---|
| Point insert (single-row commit) | ~70 ops/s |
| Batched insert (100 rows/batch) | ~2,800–3,000 rows/s |
| Concurrent point insert (8 threads) | ~110 ops/s |
| Point read by composite PK | ~300k ops/s |
| Unique secondary-index lookup | ~195k ops/s |
| Full scan (20k rows) | ~1.3M rows/s |
| Read-modify-write update | ~65 ops/s |

## What the numbers mean

- **Write latency is durability-bound.** Each publication wave performs a
  data-device sync, PMT/allocator metadata syncs, and a directory-sync
  barrier (~13 ms wall on APFS). The WAL append itself is <1% of the cost.
  Batched transactions amortize these barriers across many rows — batch
  wherever your workload allows.
- **Concurrent writers do not yet amortize the barrier.** SeerDB's
  group-commit lane stages concurrent commits and publishes them as one
  wave, but staging currently blocks on the database mutex while a wave is
  syncing, so waves collapse to ~1 commit each under contention. Pipelined
  publication (staging against pre-wave state while the previous wave's
  buffers sync) is the identified fix at the engine level.
- **Reads are fast and scale independently of history length** after the
  prefix-bounded index seek landed: unique-index lookups went from 324 to
  ~195k ops/s when exact-key probes replaced whole-tree scans.

## Fixed during this baseline

1. Unique-violation detection and `index_get` used full index-tree scans;
   both now use `[prefix, succ(prefix))` bounded seeks on the encoded entry
   key (entries sharing one value share an exact byte prefix).
2. The uniqueness probe registers its key range as a transactional read
   dependency, so two concurrent inserts of the same unique value can no
   longer both pass from disjoint snapshots; the loser fails with a
   serialization conflict instead of publishing a duplicate.
3. SeerDB phantom validation now seeks change records past the snapshot CSN
   instead of scanning conflict history from the beginning.

## Known follow-ups

- Pipelined publication in SeerDB (staging must not block behind an
  in-flight wave sync) to unlock group-commit write throughput.
- SQL planner: equality predicates do not select secondary indexes yet, so
  point SELECTs through the SQL path scan the table (~60 queries/s at 20k
  rows) while the typed API does ~195k/s.
