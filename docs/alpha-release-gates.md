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

OmenDB and its storage dependency are released together:

1. publish a compatible SeerDB prerelease, initially `seerdb 0.1.0-alpha.1`;
2. change OmenDB's release manifest from the development git revision to the
   exact compatible registry requirement;
3. run package and install checks from a clean checkout;
4. publish `omendb 0.1.0-alpha.1` only after the release gate passes.

`cargo publish --dry-run` is required before either publish. No published
version is overwritten. A bad prerelease can be yanked, but its source and
version remain part of the public history.

## Alpha application contract

The alpha supports two application-facing surfaces:

- the typed Rust API, including transactions, snapshots, schema, indexes,
  constraints, recovery state, and backend diagnostics;
- the documented, bounded embedded SQL subset.

The SQL subset is intentionally not PostgreSQL-compatible. Unsupported syntax
must return `DbError::SqlUnsupported` rather than silently doing something
else. PostgreSQL wire support remains experimental and is not part of the
alpha compatibility promise.

The alpha does not promise production safety, stable storage-format upgrades,
or performance parity with SQLite until the corresponding gates below have
passing fixtures and published evidence.

## Required gates

### Correctness and isolation

- [x] typed and SQL tests pass on both temporary and SeerDB backends;
- [x] SQL three-valued logic tests cover `NULL`, `IN`, `NOT IN`, `BETWEEN`,
      joins, and `DISTINCT` with pagination;
- [x] differential SQL tests compare the overlapping bounded subset with an
      independent SQLite oracle;
- [x] property tests exercise row and row-identity encoding roundtrips,
      truncation refusal, and corruption detection; a randomized SQL trace
      covers transaction sequences against the SQLite oracle;
- [ ] concurrent transaction tests cover read/write, write/write, unique,
      foreign-key, snapshot, cancellation, and admission outcomes;
- [ ] every durable publication seam has a process-level kill/reopen test;
- [x] a versioned on-disk fixture (`tests/fixtures/format-current/`,
      regenerated deliberately per format change) proves the documented
      open/upgrade policy.

### Durability and operations

- [x] corruption, archive, compaction, and external fault tests exist;
- [x] the crash matrix runs from a clean subprocess for create, commit,
      checkpoint, compaction, archive restore, reopen, and the SQL batch
      boundary;
- [x] `verify`, diagnostics, support bundles, and recovery-required behavior
      have an operator-facing runbook (`docs/runbook.md`);
- [ ] recovery tests assert both the durable outcome and the allowed next
      operation, not only that reopening succeeds.

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
- [ ] release CI runs a small regression workload with thresholds and stores
      machine/workload metadata; no absolute cross-machine claim is made from
      one local run.

### Recorded single-client baselines (macOS aarch64, release, batch-size 1)

From `alpha_oltp --rows 512 --operations 1000 --read-percent 80`, one local
run per backend; these are working baselines for regression tracking, not
competitive claims:

| backend | throughput | p50 | p95 | p99 | db bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| temporary | 964 ops/s | 13 us | 5.3 ms | 6.1 ms | 85 KB |
| seer | 249 ops/s | 27 us | 17 ms | 20 ms | 338 KB |
| sqlite | 68,591 ops/s | 2.3 us | 55 us | 65 us | 16 KB |

At `--batch-size 32` the same Seer workload reaches ~1,631 ops/s (6.5x),
confirming durable publication as the dominant write cost; SQLite's
autocommit path remains orders of magnitude ahead and closing that gap is
the open performance work.

### Packaging and support

- [x] stable Rust and the documented MSRV (1.89, pinned in CI) pass format,
      tests, clippy, and package checks;
- [ ] supported platform and filesystem assumptions are documented;
- [ ] `cargo package --list` contains only intended files;
- [ ] `cargo publish --dry-run` succeeds for SeerDB first and OmenDB second;
- [ ] README, API docs, changelog, license, and security policy describe the
      alpha contract and known non-goals.

## Version progression

Use `0.1.0-alpha.1`, `alpha.2`, and so on for prerelease iterations that keep
the alpha contract and persistence policy compatible. Use `0.1.0` only after
that contract is stable enough for ordinary application adoption. Use `0.2.0`
for the next intentionally incompatible pre-1.0 line, and `1.0.0` only when
API, persistence, and operational compatibility are commitments rather than
intentions.
