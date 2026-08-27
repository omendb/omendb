# Changelog

All notable changes to OmenDB are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/) with Cargo's pre-1.0 conventions.

## [Unreleased]

### Added

- Persistent `omendbd` PostgreSQL-wire daemon with durable open/create,
  bounded connection admission, connection and query/describe operation status
  counters, explicit shutdown, and database close on shutdown.
- `pgwire_server::RunningServer` and `ServerConfig` for embedding the same
  persistent server lifecycle in a Tokio application.
- PostgreSQL `CancelRequest` routing into cooperative database operation
  controls, including cancellation-aware autocommit and wire transaction
  checkpoints, cancellable lock waits, and server-tracked blocking workers.
- Bounded embedded SQL batch execution:
  `RelationalDatabase::execute_sql_batch`,
  `execute_sql_batch_with_params`, and the session equivalents, each one
  atomic transaction with a 1,024-statement limit.
- SQLite differential test oracle covering the supported SQL subset,
  including a randomized insert/update/delete trace.
- Property tests for row-envelope and row-identity encodings (roundtrip,
  truncation refusal, corruption detection).
- On-disk format fixtures (`tests/fixtures/format-current/`) that fail CI on
  unintentional incompatible format changes; regenerate deliberately per
  format change via `generate_current_format_fixtures`.
- Operator runbook (`docs/runbook.md`): health checks, verification,
  maintenance, recovery-required reconciliation, platform/filesystem
  assumptions, and current diagnostic/quota semantics.
- Development PostgreSQL wire compatibility matrix
  (`docs/pgwire-compatibility.md`) listing tested protocol/authentication/
  authorization behavior and explicit compatibility gaps.
- Release contract and gates: `docs/alpha-release-gates.md`.
- Reproducible single-client OLTP baseline `examples/alpha_oltp.rs` with
  workload metadata, latency percentiles, peak RSS, and SQLite comparison.
- Peak-RSS reporting in the benchmark JSON output.
- Primary-key fast path for SQL `SELECT`/`UPDATE`/`DELETE` equality
  predicates, including AND-composed composite keys.

### Changed

- PostgreSQL-wire syntax errors now map to SQLSTATE `42601`; undefined
  tables and columns map to `42P01` and `42703`, respectively. Regression
  coverage remains alongside the existing `0A000` unsupported-feature
  assertion.
- Seer read views are cached strongly and invalidated under the publication
  lock; transaction begins capture frontier and view atomically, removing a
  race that surfaced as spurious `StorageSnapshotUnavailable` under
  concurrent writers. Read-only throughput improved roughly an order of
  magnitude on cached workloads.
- SQL three-valued logic fixed for `IN`/`NOT IN` against NULL-containing
  lists; `DISTINCT` now applies before `LIMIT`/`OFFSET`, matching
  PostgreSQL semantics.

### Removed

- Legacy R-milestone diagnostic examples (`r0_*`, `r1_replay`, `r2_replay`,
  `seer_r*`, `runtime_mix`, `project_r2_replay`) and their orphaned
  fixtures; superseded by the integration test suites.

## [0.1.0-alpha.1] - Unreleased

Reserved relational alpha line. It is not yet published or release-ready;
see `docs/alpha-release-gates.md` for the server-first release contract.
