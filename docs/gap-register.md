# OmenDB gap register

Evidence-backed register of open product gaps, ordered by quality dimension.
The [OmenDB roadmap](roadmap.md) owns relational fixes and integration;
[SeerDB's roadmap](../crates/seerdb/docs/roadmap.md) owns kernel work;
[alpha gates](alpha-release-gates.md) own release evidence. This register records
current defects and unsupported behavior, not a second commitment list. Evidence
citations refer to tests, source or measured baselines. The initial source audits
were at `fc98820` (2026-10-11), with the constraint-timing follow-up at `0783097`;
later source checks and reproductions extend this register. "Landed" records
implementation history, not whole-contract qualification; the reopened gates below
supersede broader earlier claims.

## Reopened correctness gates

These defects are in the existing relational product path, not just pending
vNext integration. Fix and qualify them independently of storage-kernel cutover;
incorrect legacy outputs must not become the replacement's oracle.

| Boundary | Open defect | Source |
| --- | --- | --- |
| Authorization | Describe executes protected queries without grant enforcement; expression errors can reveal data. Name-keyed grants can transfer access across rename/recreate, and text-based privilege collection is not a complete binder authority. Transaction controls are incorrectly classified as schema administration. | `src/pgwire_server/{auth,handler}.rs`, `src/sql.rs::statement_access` |
| Prepared binding | Describe/Execute re-infer parameter/result types from live catalog/probes without a frozen advertised signature; token-based cache invalidation misses direct-API DDL. | `src/pgwire_server/handler.rs::{resolved_parameter_types,describe_schema}` |
| Operation failure | A unique insert can return an error while leaving a base row committable without its index entry. Explicit-block result-admission errors can leave writes committable. | `src/seer_direct.rs::DirectTransaction`, `src/relational_database.rs`, `src/pgwire_server/handler.rs` |
| Schema publication | Typed FK creation validates in one transaction and publishes in another, unlike the unified schema-mutation path. | `src/seer_direct.rs::create_foreign_key_with_name` |
| Catalog snapshots | Storage uses a captured catalog while binding/identity paths consult the live catalog; an old transaction can fail after a concurrent column append. | `src/sql_query.rs`, `src/relational_database.rs`, `src/seer_direct.rs` |
| Index equality | Parameterized insertion of typed `Float64(-0.0)` can coexist with `+0.0` in a unique index, while an indexed equality lookup misses the equal negative-zero row. Key encoding preserves zero signs/NaN payloads although scalar equality normalizes them; persisted indexes need an explicit repair/conversion policy. | `src/relational.rs::Value::encode_for_key`, `src/sql_types.rs::F64` |
| Wire statement lists | Simple Query splits raw SQL at semicolons and autocommits fragments, breaking quoted/commented semicolons and the message's implicit transaction boundary. | `src/pgwire_server/handler.rs::do_query` |
| Logical conversion | Dump/restore omits supported FK delete actions; a typed Cascade constraint becomes Restrict. | `src/sql_dump.rs`, `src/sql_schema.rs` |
| Constraint timing | Typed `Immediate` and `DeferredToPublication` both validate FKs at publication, while SQL calls supported FKs immediate; statement-time compatibility and timing-preserving conversion are unqualified. | `src/relational.rs::ConstraintTiming`, `src/seer_direct.rs::validate_referential_integrity`, `src/sql_schema.rs` |

Source audits and out-of-tree direct-API/wire/conversion reproductions establish
these open boundaries; they are not whole-contract qualification. Exercise repairs
through the real entry points and independent semantics, not tests that bless
current output.
The [SeerDB roadmap](../crates/seerdb/docs/roadmap.md) separately owns
load-publication, split-aware admission, runtime failure and recovery gates.

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

## Isolation — legacy evidence (2026-09-05), vNext open

- Serializable certification is implemented and tested: every read a
  transaction performs registers an anti-dependency. Point reads
  (`Transaction::get`) validate against queued writes at stage time and
  against published current records at wave time (O(1) per read); scans
  (`Transaction::scan`) register their exact range and get the same
  phantom protection cursors always had. The classic write-skew and
  doctors-on-call patterns now abort with SQLSTATE 40001.
- `SHOW TRANSACTION_ISOLATION` reports `serializable`. SQL-level selection
  of alternative isolation modes remains outside the current surface.
- This evidence applies to the legacy transactional path. vNext has fixed
  snapshots/write intents without the required read/range/catalog certification.
  The [SeerDB design](../crates/seerdb/docs/architecture.md#serializable-control-and-contentious-writers)
  owns its decision-ordered replacement; no legacy pass closes that gate.

## SQL breadth — supported legacy subset (2026-09-06)

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
  cancellation (57014), statement deadlines, post-materialization result-payload
  checks (not execution-memory quotas), SIGKILL/reopen,
  `EXPLAIN` (the executor's own access-path decision), slow-statement
  logging (`--slow-statement-ms`, one structured stderr line). Describe still
  contains executable sample probes; static result typing is an open binder gate.
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
