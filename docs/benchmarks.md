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
| Concurrent point insert (8 threads, engine tier) | ~511 ops/s |
| Concurrent point insert (16 threads, engine tier) | ~976 ops/s |
| Concurrent point insert (8 threads, SQL facade) | ~146 ops/s |
| Point read by composite PK | ~300k ops/s |
| Unique secondary-index lookup | ~195k ops/s |
| Full scan (20k rows) | ~1.3M rows/s |
| Read-modify-write update | ~65 ops/s |
| SQL point SELECT via secondary index | ~44k queries/s |

## What the numbers mean

- **Write latency is durability-bound.** Each publication wave performs a
  data-device sync, PMT/allocator metadata syncs, and a directory-sync
  barrier (~13 ms wall on APFS). The WAL append itself is <1% of the cost.
  Batched transactions amortize these barriers across many rows — batch
  wherever your workload allows.
- **Concurrent writers amortize the barrier at the engine tier.** After
  pipelined group commit landed (staging no longer blocks behind an
  in-flight wave sync; leader/follower waves coalesce while transactions
  remain pending), 8-thread engine-tier commits went from ~62 to ~511
  ops/s and 16 threads reach ~976 ops/s. Each wave still performs the
  full sync set (data-device, PMT/allocator metadata, directory), so
  single-writer latency is unchanged; throughput scales by amortization
  across concurrent commits.
- **The SQL facade does not yet reach engine-tier write throughput**
  (~146 ops/s at 8 threads vs ~511 direct): the facade serializes read
  paths behind the writer mutex, so facade-level pipelining is the next
  measured follow-up.
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

- Facade-level write pipelining: the SQL facade serializes behind the
  writer mutex (~146 ops/s at 8 threads vs ~511 engine-tier after the
  pipelined group-commit landing); pipelining facade reads/staging is
  the next measured lever.
- WAL-first commit acknowledgment (`Options::wal_first_commits`) to
  collapse the ~4 fsync-class barriers per wave toward PostgreSQL's
  single-WAL-sync pattern; needs crash-matrix qualification through
  the 2 MiB materialization threshold before it can be measured as a
  default.
- Serializable scan transactions: `RelationalDatabaseTransaction::
  scan_serializable` registers the table range as a read dependency, so
  mixed read-write transactions fail on phantom inserts. Plain `scan`
  remains snapshot-isolated by design.
