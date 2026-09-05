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
- **The SQL facade does not reach engine-tier write throughput**
  (~242 ops/s clean, 8 threads, vs ~511 direct). A Mutex-to-RwLock read
  split of the facade's database guard was implemented and measured
  NEUTRAL (242→245 ops/s writes; ~68k reads/s unchanged during write
  waves), so the read-serialization hypothesis is disproven and the
  split was reverted: the gap is per-commit mutation volume (a facade
  insert stages row + 2 index entries + status + change ≈ 7 mutations
  vs the engine probe's 3).
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

## Same-hardware pgbench differential (TPC-B, PostgreSQL wire)

`scripts/pgbench/differential.sh` runs the TPC-B statement mix (point
SELECT, two point UPDATEs, a secondary-key UPDATE, and an INSERT in one
explicit transaction) through the real wire protocol against OmenDB's
pgwire daemon and PostgreSQL 17.11 on the same machine, with identical
schema, script, seed, and retry budget. Measured at scale 1, 4
clients, 30 s. `pgbench_history` is now the stock unkeyed heap shape on
both engines (OmenDB heap tables landed 2026-09-05); the earlier
client-numbered history_id substitute — 37.0 tps — is superseded:

| Engine | TPS | Avg latency | Retried |
|---|---|---|---|
| PostgreSQL 17.11 (fsync on) | 7757 | 0.52 ms | 0% |
| OmenDB (default, heap history) | 41.0 | 96 ms | 38% |
| OmenDB (`--wal-first`, keyed-history era) | 19.8 | 200 ms | 31% |

The stock heap INSERT is ~11% cheaper than the keyed substitute it
replaced (37.0 -> 41.0 tps at the same retry profile) and removes the
one-invocation-per-daemon constraint the keyed client-sequence imposed.

WAL-first commit acks are QUALIFIED for crash correctness (3-mode
process-crash matrix at the real 2 MiB bound;
`crates/seerdb/tests/wal_first_process_crash.rs`) and cut single-writer
commit latency ~44%, but under sustained multi-client load the
deferred 2 MiB materialization stalls the publish lane behind the
checkpoint, so the default stays off. The 40001-class retries in the
differential are honest behavior: OmenDB's optimistic snapshots reject
concurrent same-row writers where PostgreSQL's row locks wait;
`--max-tries` on both engines makes the comparison fair.

Phase timing inside one publication wave explains the gap: the MVCC
version-store sync costs ~4.5 ms and `commit_group_at` (full B-tree
clone + WAL append) another ~4.5 ms, while raw fsync on the same volume
is 0.05 ms — the wave cost is publication-structure CPU, not the
syscall. Collapsing those two phases toward PostgreSQL's single
append+fsync is the measured next lever.

## Known follow-ups

- Publication-wave cost: the ~10 ms wave floor (version-store sync +
  full candidate B-tree clone per wave) is the dominant write latency;
  restructuring publication toward PostgreSQL's single-log append is
  the next measured lever.
- Serializable scan transactions: `RelationalDatabaseTransaction::
  scan_serializable` registers the table range as a read dependency, so
  mixed read-write transactions fail on phantom inserts. Plain `scan`
  remains snapshot-isolated by design.
