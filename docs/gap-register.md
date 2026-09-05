# OmenDB gap register

Evidence-backed register of open product gaps, ordered by quality dimension.
This is the input to roadmap prioritization, not a commitment list: items move
into `ai/brief.md` (active work) and `docs/alpha-release-gates.md` (release
evidence) when they become active. Evidence citations refer to tests, source
files, or measured baselines in this repository. Last audited 2026-09-03 (post logical-backup landing).

## Type system — LANDED 2026-08-30

Landed in `feat/types-core` (1bb5b84): Float64, Date, Timestamp,
Decimal (i128 mantissa + u16 scale, 38 digits), and UUID on
`ColumnType`/`Value` (`src/sql_types.rs`), with wire codecs verified
byte-identical to live PostgreSQL (numeric base-10000 groups,
2000-epoch date/timestamp), WHERE literal coercion, cross-numeric
comparison, exact decimal SUM (AVG returns float8), and
describe/probe binding of typed parameters. Per-type storage, wire, and
divergence detail lives in `docs/pgwire-compatibility.md`. Residual
divergences: NUMERIC(p,s) typmod is accepted but not enforced, and U64
dumps as NUMERIC(20,0) (no unsigned 64-bit integer in PostgreSQL).

## Schema evolution — LANDED 2026-08-31

Landed in `feat/schema-evolution`: multi-operation ALTER TABLE (rename
column/table, alter column type with value rewrite through the shared
input grammar, drop column, add nullable column, DROP NOT NULL),
DROP TABLE (refused while referenced by foreign keys), DROP INDEX, and
CREATE INDEX — all one atomic publication where every operation applies
or none do (candidate catalog built before any physical work; row
rewrites and tree drops inside the same SeerDB transaction as the
catalog marker; range-registered scans so concurrent writers conflict
instead of slipping past the publication). Constrained columns
(primary key, secondary index, foreign key) refuse changes; drop the
constraint first. Remaining follow-up work: SET NOT NULL (needs a
validated backfill), ALTER TYPE on constrained columns, and ADD COLUMN
with non-null defaults.

## Backup and restore — LANDED 2026-08-31

Landed in `feat/logical-backup`: `dump_sql`/`restore_sql` (public API +
`omendb-tool dump|restore` CLI). One read-consistent snapshot renders
as plain SQL — tables with inline primary keys, data as multi-row
INSERTs (100 rows per statement) in scan order, named secondary
indexes, foreign keys last as ALTER TABLE ADD CONSTRAINT after data —
restoring into both OmenDB and real PostgreSQL (the live-PG dump
differential runs in the oracle CI job, verifying values, FK
enforcement, and unique constraints after restore). Bytea uses
PostgreSQL hex format; typed literals quote the shared text grammar.
Documented divergence: U64 columns dump as NUMERIC(20,0) (PostgreSQL
has no unsigned 64-bit integer). Engine-level archive/restore
(`crates/seerdb/src/db/archive.rs`) remains the physical-path
primitive.

## Isolation level — medium severity

- Snapshot isolation with write-conflict and cursor-range phantom
  protection is implemented and tested (`scan_serializable`,
  `src/serializable.rs`, ADR 0001).
- Serializable certification (write-skew detection across the full
  dependency graph) is incomplete. No user today can rely on SERIALIZABLE
  semantics.

## SQL breadth — medium severity

- Strong core: joins (inner/non-equi/cross/left/right/full, USING,
  NATURAL), scalar/IN/EXISTS subqueries, aggregates, set operations,
  `RETURNING`, `UPDATE ... FROM`, `DELETE ... USING`
  (`docs/pgwire-compatibility.md`).
- Missing: scalar functions (`CASE`, `COALESCE`, `UPPER`, `LOWER`,
  `LENGTH`, ...), window functions, expression/partial indexes,
  `GROUPING SETS`. Function support blocks on the type system.

## Durability performance — medium severity, root cause re-measured

- Concurrent durable write throughput at the engine: 511 ops/s at 8
  threads (waves of 8; `examples/wave_probe.rs`), 976 ops/s at 16.
  Through the relational facade: ~242 ops/s clean (was reported 146).
- The facade read-serialization hypothesis is DISPROVEN: a
  Mutex→RwLock split of the facade's database guard passed all suites
  but measured neutral (242→245 ops/s; ~68k reads/s during waves
  unchanged), so it was reverted. The facade gap is per-commit mutation
  volume (~7 staged mutations per facade insert vs 3 in the engine
  probe).
- Same-hardware pgbench differential EXISTS now
  (`scripts/pgbench/differential.sh`, TPC-B through the real wire):
  PostgreSQL 17.11 8124 tps / 0.49 ms avg; OmenDB 37.0 tps / 107 ms
  avg (40% serialization retries); OmenDB `--wal-first` 19.8 tps /
  200 ms avg. Wave phase timing: MVCC version-store sync ~4.5 ms +
  `commit_group_at` (full B-tree clone + WAL append) ~4.5 ms per
  wave — publication-structure CPU, not fsync (raw fsync on the same
  volume is 0.05 ms).
- `Options::wal_first_commits` is now crash-QUALIFIED (3-mode
  process-crash matrix at the real 2 MiB bound;
  `crates/seerdb/tests/wal_first_process_crash.rs`) and wired into
  `omendbd --wal-first`; it cuts single-writer commit latency ~44% but
  stalls the publish lane under sustained multi-client load, so the
  default stays off. The measured next lever is collapsing the wave's
  two ~4.5 ms phases toward PostgreSQL's single append+fsync.

## Server UX and operations — medium severity

- Implemented: persistent `omendbd`, SCRAM-SHA-256, table grants,
  cancellation (57014), statement deadlines, result bounds, SIGKILL/reopen.
- Missing: TLS, `EXPLAIN`, slow-statement logging, connection-pooling
  guidance, metrics beyond lifecycle counters.

## DX and ecosystem fit — medium severity

- No published release; `cargo install` path untested end-to-end.
- Untested against real clients: no `psql` session matrix, no major-ORM
  (Prisma/Diesel/SQLAlchemy) compatibility evidence.
- README (113 lines) predates the server-first posture.

## Correctness strengths (for balance)

- Process-level crash matrix across every publication seam, fault
  injection (WAL write/sync failure, authority frame, orphaned versions,
  compaction rename, status replay), and reopen-resolution tests.
- Differential oracles: SQLite overlap suite plus a live PostgreSQL 18.6
  oracle in CI; property tests over row encoding; randomized SQL traces.
- Group-commit, MVCC GC watermarks, retention leases, and the committed-
  change stream were hardened through 2026-08-31 (five correctness slices,
  merged).
