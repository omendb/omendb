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
| PostgreSQL 17.11 (fsync on) | 7950 | 0.50 ms | 0% |
| OmenDB (default `--sync-class device`, heap history) | 41.0 | 96 ms | 38% |
| OmenDB (`--sync-class kernel`, heap history) | 88.8 | 45 ms | 42% |
| OmenDB (default `--sync-class device`, wire-COMMIT guard removed, 4 clients) | 88-95 | 43-45 ms | 37-39% |
| OmenDB (`--sync-class kernel`, pending-slot fix, 1 client) | 152 | 6.6 ms | — |
| OmenDB (`--sync-class kernel`, + phantom-scan bound, 1 client) | 152 | ~5 ms | — |
| OmenDB (`--wal-first`, keyed-history era) | 19.8 | 200 ms | 31% |

The stock heap INSERT is ~11% cheaper than the keyed substitute it
replaced (37.0 -> 41.0 tps at the same retry profile) and removes the
one-invocation-per-daemon constraint the keyed client-sequence imposed.

The sync-class rows carry the 2026-09-06 finding: on macOS the default
device barrier (`F_FULLFSYNC`, ~4.2 ms/sync) tripled the wave's sync
cost, while `--sync-class kernel` (plain `fsync`, ~0.03 ms/sync — the
class PostgreSQL's installed build uses via `open_datasync`) doubles
measured TPS (41.0 -> 88.8 at the same 38-42% retry profile). The
kernel class survives process and kernel crash (the crash matrix
passes under both classes) but not power loss on consumer SSDs; the
device class stays the default because this engine never trades
correctness for speed. `wave_cost_probe` / `fsync_dimensions` /
`fsync_latency_probe` (crates/seerdb/examples) reproduce every number
above.

The 2026-09-06 wire-tier attribution closed the gap between the 223 us
engine wave and the ~8 ms full-stack commit: the wire server's describe,
grants, and autocommit-read paths drop active transactions, and
`Drop for Transaction` leaked one pending-committer slot per drop, so
every commit leader saw a phantom "will-stage-soon" transaction and
slept the full 4x750 us coalescing window (~4.6 ms). Releasing the slot
in Drop took single-client TPC-B from 8.18 to 6.6 ms/txn (122 -> 152
tps) with no engine-internal change; `examples/sql_tier_probe.rs` and
`examples/wire_tier_probe.rs` reproduce the tier split (embedded 1.16
ms/txn vs wire 6.7 before the fix, 1.9 after). The 4-client differential
is unchanged by the fix: its ~50 ms latency is the multi-writer
serialization documented below, not the single-client commit path.

The 2026-09-07 phantom-scan bound closed the last unattributed commit
cost: `validate_staged_range_dependencies` scanned the entire
change-record prefix per registered read range per commit (O(total
history), unbounded). Change records sort by commit sequence, so the
scan now starts at snapshot+1 and the B-tree seek skips all history.
Embedded TPC-B 1.16 -> 0.59 ms/txn, COMMIT 1.07 -> 0.44 ms; wire
simple-protocol 1.05 ms/txn. The serializable suite caught the
first cut's exclusive end bound (a phantom at exactly the head must
conflict) — the landed bound is inclusive.

The 2026-09-08 wire-COMMIT guard removal closed the last wire-tier
serialization: the handler's Commit arm held an exclusive outer write
guard on the shared database RwLock across the engine `commit()`
(bound to an unused `_database` variable), serializing the staging
that ADR 0004's group-commit lane is designed to run concurrently —
every publication wave collapsed to a singleton (measured members=1.00
across 3008 waves under `OMENDB_COMMIT_TRACE`; instrumentation since
removed). The guard protects nothing on the commit path: the commit
chain takes only `self`, publication is serialized by the engine's
publish lane, and schema changes are fenced by the catalog-marker
range registered at begin. Removing it restores real wave grouping
(members 2-3 typical, 4 at peak). Interleaved same-database A/B
(baseline first each round, scale 4, TPC-B mix, 30 s): 4 clients
41-43 -> 88-95 tps and 92-97 -> 43-45 ms latency (2.1-2.2x); 8
clients 41 -> 96 tps and 192 -> 78 ms (2.3x). Balance conservation
holds exactly after all runs (branches = tellers = accounts =
history delta = -308,296 across 16,192 committed transactions); the
8-client 1.1% failed transactions are 40001 retry-exhaustion under
contention (8 writers on 4 branches, 32,574 retry attempts), not
corruption — the guarded baseline reaches zero failures only because
its serialization keeps concurrent writers from contending at all.
The wave-singleton abort observed on 2026-09-07 (pgbench abort after 8
transactions, non-retryable) was the pre-overhaul CSN-gap bug
(`e15b7bc`), unmasked by guard removal: a rejected wave member left
a sequence gap that fenced the database; a pre-overhaul worktree
repro confirmed it and the fix holds under the unguarded commit.
Recorded reproduction: `scripts/pgbench/differential.sh 4 4 30` with
and without the guard; wave histograms via the removed
`OMENDB_COMMIT_TRACE` gating.

WAL-first commit acks are QUALIFIED for crash correctness (3-mode
process-crash matrix at the real 2 MiB bound;
`crates/seerdb/tests/wal_first_process_crash.rs`) and cut single-writer
commit latency ~44%, but under sustained multi-client load the
deferred 2 MiB materialization stalls the publish lane behind the
checkpoint, so the default stays off. The 40001-class retries in the
differential are honest behavior: OmenDB's optimistic snapshots reject
concurrent same-row writers where PostgreSQL's row locks wait;
`--max-tries` on both engines makes the comparison fair.

The earlier phase attribution (version-store sync ~4.5 ms +
`commit_group_at` ~4.5 ms vs 0.05 ms raw fsync) pointed at
publication-structure CPU; the 2026-09-06 measurement corrected it —
those phases were each one F_FULLFSYNC device barrier (~4.2 ms), and
the syscall class was the whole story. Under the kernel class the
remaining per-wave costs are the three sync-bearing phases themselves
(collapsing them toward one per wave) and the per-group B-tree clone —
the next measured levers, now visible at ~223 us/txn scale instead of
~13 ms.

## Known follow-ups

- Publication-wave cost: the ~10 ms wave floor (version-store sync +
  full candidate B-tree clone per wave) is the dominant write latency;
  restructuring publication toward PostgreSQL's single-log append is
  the next measured lever.
- Serializable scan transactions: `RelationalDatabaseTransaction::
  scan_serializable` registers the table range as a read dependency, so
  mixed read-write transactions fail on phantom inserts. Plain `scan`
  remains snapshot-isolated by design.
