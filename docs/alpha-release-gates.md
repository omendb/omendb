# OmenDB relational alpha gates

This is the currently planned **native relational** release gate. PostgreSQL
wire compatibility is optional (see the [current design](architecture.md)).
It is a gate, not
an announcement: the current tree is **not alpha-ready** until every required
gate has evidence attached to a release commit. The 2026-10-11 audit reopened
product correctness gates in the [gap register](gap-register.md). Checked items
below describe bounded historical evidence, not qualification of the uncovered
cases or the replacement kernel.

## Package identity

The crates.io package name remains `omendb`. The existing `0.0.x` releases are
the earlier vector-database line and must remain available to existing users.
The relational line starts at `0.1.0-alpha.1`; it must not be published as
`0.0.38`, which would falsely describe a compatible continuation of the old
product.

The old line is not deleted or recreated. Existing dependents that require
`omendb ^0.0.37` remain on that line. A new alpha dependency must opt into the
`0.1.0-alpha.*` line explicitly.

OmenDB and SeerDB are currently independently versioned, but ADR 0013 makes
that a packaging choice rather than an architectural requirement. An OmenDB
release records the exact SeerDB workspace/package revision it was qualified
against.

If SeerDB is published separately for an alpha, publish and qualify
`durable-fs` and SeerDB before OmenDB, then run package/install checks from a
clean checkout. If the integrated architecture no longer benefits from a
separate SeerDB release, do not preserve one merely because earlier alpha plans
assumed it.

The crate licenses remain distinct while both crates exist: OmenDB is
AGPL-3.0-only and SeerDB is Apache-2.0. `cargo publish --dry-run` is required
for every package that will actually be published. No published version is
overwritten; a bad prerelease can be yanked but remains part of public history.

## Alpha application contract

`0.1.0-alpha.*` is reserved for a future server-first alpha. The current tree
is an internal development preview and does not satisfy this contract merely
because its direct Rust API and bounded SQL tests pass.

A releasable native relational alpha has **two deployment surfaces sharing
one transaction implementation**:

- a persistent single-node native OmenDB SQL server with multiple sessions,
  documented supported dialect/semantics, authentication, lifecycle and diagnostics;
- a secondary typed Rust API for integration, testing and embedded use.

A PostgreSQL-wire adapter remains a useful, optional interoperability surface,
not a requirement shaping native SQL, catalog or storage. **If shipped**, its
advertised subset must retain honest unsupported-syntax behavior, negative tests,
and matched live PostgreSQL differential evidence documented in
[the adapter matrix](pgwire-compatibility.md). The current preview includes
this adapter; existing checked tests below are historical evidence for it,
not a mandatory dependency of every future native SQL deployment.

The alpha does not promise production safety, stable storage-format upgrades,
or competitive OLTP performance until the corresponding storage, server,
correctness, performance, packaging, and operational gates below have passing
evidence. SQLite remains useful as a bounded differential oracle, but it is not
the primary performance target; release evidence must include a reproducible
PostgreSQL-class comparison where the supported workload overlaps.

## Required gates

### Storage and transaction foundation

- [ ] the shared transaction/storage kernel provides multi-writer MVCC with
      distinct `TxnId`, CSN and LSN identities, preserving the advertised
      SERIALIZABLE point/absence/range dependencies as well as snapshot visibility;
- [ ] authoritative ordered/record participants and atomic multi-object decisions
      are exercised through OmenDB; retained snapshot/change export includes source
      identity and atomically registered leases, not only `{CSN, restart LSN}`;
- [ ] decision-ordered certification sees earlier accepted uninstalled effects;
      resource/history pressure cannot discard evidence and permit success;
- [ ] runtime-owned completion fences before unresolved ownership release, and
      protected reservations cover original WAL, future undo and structural/overflow
      work before possible decision append; pre-decision pressure refusal is recovery-free,
      and accepted decisions have protected installation capacity rather than pressure-driven
      refusal. Device/corruption failure fences and reports unresolved/recovery-required,
      not a clean abort;
- [ ] every supported ordered record has actual fenced split/internal/root and
      replay representation; overflow, retention and retry-space are qualified;
- [ ] committed changes are restartable without reconstructing history by
      rescanning relational tables;
- [ ] crash/recovery tests cover commit, WAL, page-map, checkpoint, GC, and
      retained-snapshot boundaries, including non-reusable identity/resume high-water
      marks and reachable owner/history/outcome closure before log reclamation;

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
- [x] the **current optional adapter's** supported PostgreSQL protocol/SQL
      subset has differential and
      negative compatibility tests; unsupported behavior fails explicitly.
      Current evidence is catalogued in
      [`docs/pgwire-compatibility.md`](pgwire-compatibility.md), including the
      dedicated live PostgreSQL differential CI job. The SQL scalar type set
      (float8, date, timestamp, numeric, uuid) landed with wire codecs verified
      byte-identical to live PostgreSQL (`feat/types-core`, 1bb5b84;
      `tests/project_typed_values.rs` + the typed-value oracle differential);
      schema evolution (multi-operation ALTER TABLE, DROP TABLE/INDEX) is
      covered by `tests/project_schema_evolution.rs` (54883b8);
- [x] bounded logical dump/restore fixtures round-trip one read-consistent
      snapshot through OmenDB and real PostgreSQL in the live-PG oracle CI job
      (`feat/logical-backup`, 7f94fdc; `tests/project_dump_restore.rs`);
- [ ] explicit offline preview conversion preserves supported logical rows and
      catalog semantics, including FK actions/timing, or refuses incompatible input.
      Current Cascade-to-Restrict conversion does not satisfy this contract;
- [ ] authorization applies to Describe as well as Execute, Describe performs no
      user-query execution, and restricted roles can roll back without DDL grants;
- [ ] explicit-block errors include binding, authorization and precommit result
      admission, while post-decision delivery errors do not fabricate an abort;
- [ ] current-grant checks on bound principal/object incarnations at Describe/execution
      prevent cached-plan grant reuse and rename/recreate privilege transfer;
      revocation alone does not cancel prior work or reauthorize commit, while a
      later authorization error makes an explicit block rollback-only;
- [ ] prepared statements preserve advertised parameter/result signatures across
      DDL/replanning; incompatible rebinding fails before execution, and parameter
      shapes are validated without fallback decoding or truncation;
- [ ] parser-driven wire statement lists preserve the advertised implicit/explicit
      transaction boundaries, including semicolons in strings/comments;
- [ ] durable retry/outcome handles distinguish refused/aborted, committed, unresolved
      and expired/foreign requests without duplicate execution across reconnect/reopen;
- [ ] concurrent direct/wire execution has bounded workers and transaction-grain
      admission, with protected completion capacity and byte/work-bounded batches.

### Correctness and isolation

- [ ] failed direct/SQL mutations cannot commit partial row/index/catalog state;
- [ ] typed and SQL FK creation validate and publish in one schema transaction
      with dependencies retained across concurrent parent/child writes;
- [ ] binding, identity, access-path choice and decoding share one retained
      transaction/catalog view; enforcement/name/lifecycle dependencies prevent old
      writers bypassing newly activated indexes or constraints;
- [ ] immediate constraints enforce their declared mutation/statement boundary;
      FK index bindings preserve ordered column pairs, CHECK/NULL and index equality
      semantics agree, deferred semantics are explicit, and conversion preserves timing;
- [x] direct exact-index traversal does not duplicate continuation-boundary rows,
      and direct parameterized batches reject unequal statement/parameter-list
      lengths before work (`tests/project_api.rs`);
- [x] typed and SQL tests pass on the direct SeerDB backend (the
      storage-kernel seam and temporary backend were deleted per
      [ADR 0005](adr/0005-delete-storage-kernel-seam.md));
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

- [x] a reproducible SQLite OLTP workload reports schema, row count, read /
      write mix, transaction size (`--batch-size`), concurrency, seed, build
      profile, platform, filesystem, p50/p95/p99 latency, throughput, memory,
      and database size (`examples/alpha_oltp.rs`, one JSON object per run
      with every field above; recorded rows live in
      [`docs/benchmarks.md`](benchmarks.md) and the baselines table below);
- [x] the same workload runs against the OmenDB backend (direct SeerDB
      storage, [ADR 0005](adr/0005-delete-storage-kernel-seam.md); the
      earlier second backend was the deleted temporary seam) and SQLite
      without changing semantics — each run verifies its engine's final
      state against an independent expected-state oracle and the recorded
      runs agree; batch-size 1 and representative bounded batches are
      reported separately in the baselines table below;
- [x] CPU, allocation, WAL, fsync, and compaction profiles identify the
      measured bottleneck before an optimization is accepted
      (`crates/seerdb/examples/cpu_alloc_probe.rs`: counting global allocator
      + `getrusage` CPU + publication-phase/byte diffs over the TPC-B-shaped
      stream; 2026-09-08 profile recorded in [`docs/benchmarks.md`](benchmarks.md)
      — device barrier commits are 96.8% sync-wait, kernel-barrier commits
      become 76% CPU-bound at 393 allocations/18.8 KB per transaction);
- [x] release CI runs a small regression workload with thresholds and stores
      machine/workload metadata (`perf-smoke` job); no absolute cross-machine
      claim is made from one local run.

### Recorded single-client baselines (macOS aarch64, release)

From `alpha_oltp --rows 512 --read-percent 80`, one local run per
configuration, refreshed 2026-09-08; these are working baselines for
regression tracking, not competitive claims. Single-run numbers move
with OS/SQLite versions and filesystem state (SQLite's batch-1 figure
was ~68,591 ops/s on the 2026-09-05 refresh):

| configuration | backend | throughput | p50 | p95 | p99 | db bytes |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| batch-size 1, 1,000 ops | seer | 267 ops/s | 12 us | 18.9 ms | 25.7 ms | 717 KB |
| batch-size 1, 1,000 ops | sqlite | 14,059 ops/s | 3.9 us | 0.37 ms | 1.28 ms | 16 KB |
| batch-size 32, 4,000 ops | seer | 2,236 ops/s | 12.9 ms | 26.5 ms | 28.7 ms | 1,153 KB |
| batch-size 32, 4,000 ops | sqlite | 362,982 ops/s | 88 us | 0.10 ms | 0.15 ms | 16 KB |

Batch-size 32 multiplies Seer throughput ~8x, confirming durable
publication as the dominant write cost; SQLite's in-transaction batching
remains orders of magnitude ahead and closing that gap is the open
performance work.

### Packaging and support

- [ ] stable Rust and the documented MSRV pass the current CI matrix. Historical
      passes do not close current store-ownership test and stable Clippy failures;
      [CI configuration](../.github/workflows/ci.yml) owns toolchains and checks;
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
