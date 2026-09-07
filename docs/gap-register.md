# OmenDB gap register

Evidence-backed register of open product gaps, ordered by quality dimension.
This is the input to roadmap prioritization, not a commitment list: items move
into the project task tracker and `docs/alpha-release-gates.md` (release
evidence) when they become active. Evidence citations refer to tests, source
files, or measured baselines in this repository. Repository maintenance audit: 2026-09-07.

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

## Isolation level — closed (2026-09-05)

- Serializable certification is implemented and tested: every read a
  transaction performs registers an anti-dependency. Point reads
  (`Transaction::get`) validate against queued writes at stage time and
  against published current records at wave time (O(1) per read); scans
  (`Transaction::scan`) register their exact range and get the same
  phantom protection cursors always had. The classic write-skew and
  doctors-on-call patterns now abort with SQLSTATE 40001.
- `SHOW TRANSACTION_ISOLATION` reports `serializable`. SQL-level selection
  of alternative isolation modes remains outside the current surface.

## SQL breadth — closed (2026-09-06)

- Strong core: joins (inner/non-equi/cross/left/right/full, USING,
  NATURAL), scalar/IN/EXISTS subqueries, aggregates, set operations,
  `RETURNING`, `UPDATE ... FROM`, `DELETE ... USING`
  (`docs/pgwire-compatibility.md`).
- Landed across item 7: `CASE`/`COALESCE`; clock functions
  (`now()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`); the scalar catalog
  (text: `upper`/`lower`/`length`/`btrim`/`ltrim`/`rtrim`; numeric:
  `abs`/`round`/`floor`/`ceil`; datetime: `EXTRACT`/`date_part`/
  `date_trunc`); heap tables (no PRIMARY KEY) with engine-allocated
  durable identities; serializable certification (write-skew aborts
  with SQLSTATE 40001); window functions (ranking, offsets, values,
  running aggregates with PostgreSQL default frames); partial and
  expression indexes (catalog format v6).
- Residual: `GROUPING SETS` remains unsupported; window frames and
  named windows are refused honestly; index expressions cover
  arithmetic only; `nth_value` and explicit `lag`/`lead` offsets are
  refused honestly.

## Durability performance — open

The workload-specific baselines and reproduction commands live in
[`docs/benchmarks.md`](benchmarks.md). Comparisons must match sync class,
transaction size, concurrency, schema, and cache state. Engine-level group
commit and wire-level throughput are separate measurements; a result from
one does not qualify the other. No competitive performance gate is closed
by the correctness or maintenance tests.

## Server UX and operations — medium severity

- Implemented: persistent `omendbd`, SCRAM-SHA-256, table grants,
  cancellation (57014), statement deadlines, result bounds, SIGKILL/reopen,
  `EXPLAIN` (the executor's own access-path decision), slow-statement
  logging (`--slow-statement-ms`, one structured stderr line), static
  describe types (catalog/aggregate/arithmetic-derived, not probe samples).
- TLS: explicitly declined, not silently missing — the server answers
  `SSLRequest` with 'N' (`sslmode=require` fails immediately, `prefer`
  proceeds cleartext). Policy until first-party TLS: loopback/private bind
  or fronting proxy. Documented in `docs/pgwire-compatibility.md`.
- Missing: connection-pooling guidance, metrics beyond lifecycle counters,
  `current_user()` returning the SCRAM identity (trust connections answer
  `omendb`).

## DX and ecosystem fit — medium severity

- No published release; `cargo install` path untested end-to-end.
- Real-client evidence now exists: psql 17 session matrix
  (`tests/project_psql_session.rs`, self-skips without psql) and sqlx
  0.8 (pool, DDL, typed prepared statements, transactions, rollback
  visibility). The sqlx matrix exposed and drove the fix for the
  describe-probe type-inference bug. Untested: Prisma, Diesel,
  SQLAlchemy, ActiveRecord.
- README provides the server/API entry points. Detailed contracts live in
  `docs/pgwire-compatibility.md` and `docs/alpha-release-gates.md`.

## Correctness strengths (for balance)

- Process-level crash matrix across every publication seam, fault
  injection (WAL write/sync failure, authority frame, orphaned versions,
  compaction rename, status replay), and reopen-resolution tests.
- Differential oracles: SQLite overlap suite plus a live PostgreSQL 18.6
  oracle in CI; property tests over row encoding; randomized SQL traces.
- Group-commit, MVCC GC watermarks, retention leases, and the committed-
  change stream were hardened through 2026-08-31 (five correctness slices,
  merged).
