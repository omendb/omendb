# OmenDB gap register

Evidence-backed register of open product gaps, ordered by quality dimension.
This is the input to roadmap prioritization, not a commitment list: items move
into `ai/brief.md` (active work) and `docs/alpha-release-gates.md` (release
evidence) when they become active. Evidence citations refer to tests, source
files, or measured baselines in this repository. Last audited 2026-08-31.

## Type system — high severity

- `ColumnType` supports only `Bytes, Bool, I64, U64, Text`
  (`src/relational.rs`). `Value` mirrors it. No Float, Decimal, Timestamp,
  Date, or UUID.
- Consequence: most real schemas cannot be expressed. A `created_at`
  column, a price, or an `id UUID` each block adoption today.
- Prerequisite for: SQL functions (date/text math), ORM compatibility,
  honest benchmark schemas (pgbench uses numeric), and SQL-standard
  semantics (three-valued logic on new types).

## Schema evolution — high severity

- `ALTER TABLE` accepts exactly one nullable `ADD COLUMN`
  (`src/sql_schema.rs`: everything else is rejected as unsupported).
- No `DROP COLUMN`, `RENAME`, `ALTER COLUMN TYPE`, no `CREATE/DROP INDEX`
  DDL through SQL (typed API publishes indexes; SQL path is unverified).
- Consequence: no long-lived deployment can evolve its schema; no
  migrations story. DX blocker of the same class as the type gap.

## Backup and restore — high severity

- Engine-level archive/restore exists in SeerDB
  (`crates/seerdb/src/db/archive.rs`) but is not user-facing.
- No logical dump/restore at the OmenDB layer: no SQL export, no way to
  move data between engines or versions.
- Consequence: "how do I get my data out?" has no answer; upgrade and
  rollback stories are undefined for users. A pg_dump-compatible logical
  dump would double as a differential-test tool.

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

## Durability performance — medium severity, understood

- Concurrent durable write throughput at the engine: 511 ops/s at 8
  threads (waves of 8; `examples/wave_probe.rs`), 976 ops/s at 16.
  Through the relational facade: 146 ops/s — facade data reads still
  serialize behind the wave's database guard.
- Root cause of the remaining gap to PostgreSQL: each wave pays ~4
  fsync-class barriers (data flush 46%, PMT metadata 41%, blobs 8%,
  directory 4%; `docs/benchmarks.md`) while PostgreSQL pays one WAL sync
  per commit group. `Options::wal_first_commits` (ack after one
  group-synced WAL append; 2 MiB materialization threshold) implements
  the PostgreSQL design but is experimental, unqualified at scale, and
  not wired into OmenDB defaults.
- No same-hardware pgbench differential exists yet; the alpha gates
  require one for release evidence.

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
