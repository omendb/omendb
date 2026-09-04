# OmenDB relational alpha gates

This is the release contract for the relational OmenDB line. It is a gate, not
an announcement: the current tree is **not alpha-ready** until every required
gate has evidence attached to a release commit.

## Package identity

The crates.io package name remains `omendb`. The existing `0.0.x` releases are
the earlier vector-database line and must remain available to existing users.
The relational line starts at `0.1.0-alpha.1`; it must not be published as
`0.0.38`, which would falsely describe a compatible continuation of the old
product.

The old line is not deleted or recreated. Existing dependents that require
`omendb ^0.0.37` remain on that line. A new alpha dependency must opt into the
`0.1.0-alpha.*` line explicitly.

OmenDB and SeerDB are independently versioned and published, but an OmenDB
release records the exact SeerDB version it was qualified against. During
workspace development OmenDB uses the local path dependency. For a registry
release:

1. publish a compatible SeerDB prerelease, initially `seerdb 0.1.0-alpha.1`;
2. change OmenDB's release manifest from the workspace path dependency to the
   exact compatible registry requirement;
3. run package and install checks for both crates from a clean checkout;
4. publish `omendb 0.1.0-alpha.1` only after the release gate passes.

The crate licenses remain distinct: OmenDB is AGPL-3.0-only and SeerDB is
Apache-2.0.

`cargo publish --dry-run` is required before either publish. No published
version is overwritten. A bad prerelease can be yanked, but its source and
version remain part of the public history.

## Alpha application contract

`0.1.0-alpha.*` is reserved for a future server-first alpha. The current tree
is an internal development preview and does not satisfy this contract merely
because its direct Rust API and bounded SQL tests pass.

A releasable alpha has two surfaces sharing one transaction implementation:

- a persistent single-node OmenDB server with multiple sessions, an explicitly
  documented PostgreSQL protocol subset, authentication, clean lifecycle, and
  operational diagnostics;
- a secondary typed Rust API for integration, testing, and embedded use.

The supported wire and SQL surface is deliberately bounded rather than
PostgreSQL-complete. Unsupported syntax must return `DbError::SqlUnsupported`
rather than silently doing something else. PostgreSQL compatibility claims are
limited to the overlap catalogued in `docs/pgwire-compatibility.md` and backed
by negative tests plus a live PostgreSQL differential oracle.

The alpha does not promise production safety, stable storage-format upgrades,
or competitive OLTP performance until the corresponding storage, server,
correctness, performance, packaging, and operational gates below have passing
evidence. SQLite remains useful as a bounded differential oracle, but it is not
the primary performance target; release evidence must include a reproducible
PostgreSQL-class comparison where the supported workload overlaps.

## Required gates

### Storage and transaction foundation

- [ ] SeerDB provides multi-writer snapshot MVCC with distinct `TxnId`, CSN,
      and LSN identities;
- [ ] first-class trees, ordered cursors, atomic multi-tree transactions, and
      snapshot `{CSN, restart LSN}` export are exercised through OmenDB;
- [ ] committed changes are restartable without reconstructing history by
      rescanning relational tables;
- [ ] crash/recovery tests cover commit, WAL, page-map, checkpoint, GC, and
      retained-snapshot boundaries.

### Server and protocol

The current development tree has a persistent `omendbd` daemon with bounded
multi-session admission, clean shutdown/reopen, cooperative statement
cancellation and deadlines, a bounded result-payload admission check, SCRAM
authentication, table-level grants, and operator-visible server status. Trust
mode is restricted to loopback. A process-level `omendbd` SIGKILL/reopen test
covers clean recovery after daemon loss. The documented wire subset is tested
through `tokio-postgres`, negative SQLSTATE/unsupported paths, and a dedicated
live PostgreSQL 18.6 differential oracle. Broader protocol features and true
execution-memory quotas remain outside the current compatibility claim and are
listed in `docs/pgwire-compatibility.md`; SeerDB's process-level publication
matrix provides the storage-side crash coverage below.

- [x] a persistent daemon opens a durable database and supports multiple
      sessions with clean startup, shutdown, cancellation, and reopen;
- [x] authentication, authorization, resource limits, and operational
      diagnostics have an explicit documented baseline;
- [x] the supported PostgreSQL protocol/SQL subset has differential and
      negative compatibility tests; unsupported behavior fails explicitly.
      Current evidence is catalogued in
      [`docs/pgwire-compatibility.md`](pgwire-compatibility.md), including the
      dedicated live PostgreSQL differential CI job. The SQL scalar type set
      (float8, date, timestamp, numeric, uuid) landed with wire codecs verified
      byte-identical to live PostgreSQL (`feat/types-core`, 1bb5b84;
      `tests/project_typed_values.rs` + the typed-value oracle differential);
      schema evolution (multi-operation ALTER TABLE, DROP TABLE/INDEX) is
      covered by `tests/project_schema_evolution.rs` (54883b8);
- [x] logical dump/restore round-trips: one read-consistent snapshot
      renders as plain SQL that restores into OmenDB and into real
      PostgreSQL, with the differential running in the live-PG oracle CI
      job (`feat/logical-backup`, 7f94fdc;
      `tests/project_dump_restore.rs`).

### Correctness and isolation

- [x] typed and SQL tests pass on the direct SeerDB backend (the
      storage-kernel seam and temporary backend were deleted per
      [ADR 0005](adr/0005-delete-storage-kernel-seem.md));
- [x] SQL three-valued logic tests cover `NULL`, `IN`, `NOT IN`, `BETWEEN`,
      joins, and `DISTINCT` with pagination;
- [x] differential SQL tests compare the overlapping bounded subset with an
      independent SQLite oracle;
- [x] property tests exercise row and row-identity encoding roundtrips,
      truncation refusal, and corruption detection; a randomized SQL trace
      covers transaction sequences against the SQLite oracle;
- [x] concurrent transaction tests cover read/write, write/write, unique,
      foreign-key, snapshot, cancellation, and admission outcomes. Evidence
      includes the wire-level hot-key RMW invariant in
      `tests/project_concurrency_stress.rs`, overlapping unique-value conflicts
      in `src/seer_direct.rs`, concurrent FK and stale-catalog/DDL races in
      `tests/project_referential_actions.rs`, stable read views while writers
      advance in `crates/seerdb/tests/read_view.rs`, cancellation in
      `tests/project_cancellation.rs`, and bounded connection admission in
      `tests/project_pgwire_server.rs`;
- [x] every durable publication seam in SeerDB's current publication protocol
      has process-level kill/reopen coverage. The
      `dbnext_r0_process_crash_publication_matrix` test exits its child with
      code 137 after WAL sync, page write/sync, manifest mirror/sync,
      post-manifest, and final-space faults, and accepts only the documented
      old/new durable outcomes;
- [x] a versioned on-disk fixture (`tests/fixtures/format-current/`,
      regenerated deliberately per format change) proves the documented
      open/upgrade policy.

### Durability and operations

- [x] corruption and fault-atomicity qualification tests exist (seer_direct suite);
- [x] the crash/fault matrix covers WAL write failure, authority-frame sync
      failure, orphaned versions, pre-publication retryability, and status
      replay (SeerDB transactional fault tests + seer_direct qualification);
- [x] recovery-required behavior has an operator-facing runbook
      (`docs/runbook.md`);
- [x] recovery tests assert both the durable outcome and the allowed next
      operation. `crates/seerdb/tests/recovery_next_operation.rs` brackets
      pre-publication old-state recovery and post-authority new-state recovery,
      commits a subsequent mutation, and reopens again to verify that commit.

### Performance

- [ ] a reproducible SQLite OLTP workload reports schema, row count, read /
      write mix, transaction size (`--batch-size`), concurrency, seed, build
      profile, platform, filesystem, p50/p95/p99 latency, throughput, memory,
      and database size;
- [ ] the same workload runs against both OmenDB backends and SQLite without
      changing semantics; batch-size 1 and representative bounded batches are
      reported separately because durability and rollback boundaries differ;
- [ ] CPU, allocation, WAL, fsync, and compaction profiles identify the
      measured bottleneck before an optimization is accepted;
- [x] release CI runs a small regression workload with thresholds and stores
      machine/workload metadata (`perf-smoke` job); no absolute cross-machine
      claim is made from one local run.

### Recorded single-client baselines (macOS aarch64, release, batch-size 1)

From `alpha_oltp --rows 512 --operations 1000 --read-percent 80`, one local
run per backend; these are working baselines for regression tracking, not
competitive claims:

| backend | throughput | p50 | p95 | p99 | db bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| seer | 249 ops/s | 27 us | 17 ms | 20 ms | 338 KB |
| sqlite | 68,591 ops/s | 2.3 us | 55 us | 65 us | 16 KB |

At `--batch-size 32` the same Seer workload reaches ~1,631 ops/s (6.5x),
confirming durable publication as the dominant write cost; SQLite's
autocommit path remains orders of magnitude ahead and closing that gap is
the open performance work.

### Packaging and support

- [x] stable Rust and the documented MSRV (1.89, pinned in CI) pass format,
      tests, clippy, and package checks;
- [x] supported platform and filesystem assumptions are documented
      (`docs/runbook.md`);
- [ ] `cargo package --list` contains only intended files for each package;
- [ ] `cargo publish --dry-run` succeeds for SeerDB first and OmenDB second;
- [ ] README, API docs, changelog, both licenses, and security policy describe
      the alpha contract and known non-goals.

## Version progression

Use `0.1.0-alpha.1`, `alpha.2`, and so on for prerelease iterations that keep
the alpha contract and persistence policy compatible. Use `0.1.0` only after
that contract is stable enough for ordinary application adoption. Use `0.2.0`
for the next intentionally incompatible pre-1.0 line, and `1.0.0` only when
API, persistence, and operational compatibility are commitments rather than
intentions.
