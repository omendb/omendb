# seerdb

Generic durable ordered byte-KV storage engine for Rust consumers. Written in Rust and maintained as an independently versioned crate in the OmenDB workspace. SeerDB is a storage foundation, not a SQL engine or an embedded-only product.

## What

SeerDB provides a generic durable kernel for applications and database
products that need ordered byte keys and values. It can be embedded by a Rust
consumer, but its long-term role in OmenDB is the transactional storage
foundation beneath a database server:

- **Durable generations**: pages, checkpoints, commit metadata, manifests, and
  WAL cleanup publish in a checked order. Two independently valid manifest slots
  provide a fallback root when the newest publication is damaged.
- **Out-of-place pages**: immutable page versions support generation-aware
  reclamation, compaction, vacuum, and retained historical reads.
- **WAL recovery**: committed-prefix replay, torn-tail handling, corruption
  refusal, offline checks, and destination-only repair are part of the baseline.
- **KV separation**: large values use the compatibility whole-image blob format;
  the segmented catalog layout is available as an opt-in format under separate
  qualification.
- **Reader/writer contract**: the low-level `DB` uses one serialized
  publication lane with concurrent reads, root-bound read views, retained
  snapshots, and expected-base conflict refusal.
- **Transactional ordered-KV facade**: `TransactionDatabase` provides first-class
  `TreeId`s, fixed snapshots resolved through current records and an
  append-oriented before-image version store, durable transaction-status
  indirection, ordered scans, atomic multi-tree batches, snapshot-isolation
  conflict checking, ordered cursor handles with read-range phantom
  protection, and bounded `gc_versions` maintenance that respects
  active snapshots, which also freezes resolved commit sequences into current
  records and prunes status entries nothing references anymore. Its durable
  conflict records and version history survive
  reopen. Commits pipeline through a group-commit lane (ADR 0004 in the
  repository docs): staging and validation run concurrently, and each publish
  wave installs many logical commits under one authority frame with a single
  sync pair.
- **Committed-change stream**: every commit sequence number has exactly one
  durable change record, so `read_changes` walks gap-free history from any
  retained position. Durable retention leases (`acquire_change_lease`,
  advance, release) pin ranges across restarts; `gc_changes` prunes only
  below the minimum active floor and keeps everything when no lease exists.
  `snapshot_export` yields the `{CSN, restart LSN}` resume point.
- **Capacity handling**: typed ENOSPC refusal and preflight admission cover
  ordinary commits and maintenance, with same-handle retry and reopen tests.
- **SSD-aware paths**: Linux O_DIRECT, page-aligned buffers, and optional FDP/ZNS
  feature flags exist, but device-specific optimization is not required by the
  baseline contract.

The current development line uses a fixed 4 KiB page format, exposed as
`seerdb::PAGE_SIZE`. Page sizing is not an ignored tuning knob: changing it
requires a separately versioned format and matching buffer/device
implementation.

SeerDB does not yet claim physical per-record page MVCC, parallel physical
publication, SQL, HA, or cross-device performance parity. The transactional
facade is a qualified development surface, not a release guarantee; the
OmenDB server contract remains active architecture work.

## Basic use

Create a database once, then reopen the same directory on later process starts:

```rust
use seerdb::{DB, Options};

let mut db = DB::create("./my_db", Options::default())?;
db.put(b"key", b"value")?;
db.flush()?;
assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
db.close()?;

let mut db = DB::open("./my_db", Options::default())?;
assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
db.close()?;
```

Use `commit_batch` for a low-level atomic group of byte-oriented puts and
deletes. Use `TransactionDatabase` when you need first-class trees, fixed
snapshots, ordered scans, and concurrent transaction conflict checking. Use
`begin_read_view`, `retain_commit`, or `begin_snapshot` when a read must stay
bound to a published historical generation. Call `verify`, `check`, and
`durability_status` in operational tooling rather than treating a successful
open as a complete integrity audit.

## Architecture and implementation state

SeerDB currently exposes the generation-based ordered-KV development API above.
The future general-purpose transaction/storage runtime is experimental and is
not a qualified multi-writer or restart-safe production engine. It should be
usable independently of OmenDB without forcing typed rows or specialized
vector/columnar extents through opaque byte-KV storage. OmenDB is the primary
integration/performance consumer, not a required dependency of standalone KV.

Read the maintained [SeerDB architecture](docs/architecture.md),
[SeerDB roadmap](docs/roadmap.md), and [qualification procedures](docs/qualification.md).
The OmenDB [product architecture](../../docs/architecture.md) and
[relational roadmap](../../docs/roadmap.md) describe the consumer's requirements.
Older ADRs and benchmarks are evidence, not accepted replacements for these
contracts. No storage format, API stability or competitive performance claim is
implied by these documents.

## License

Apache License 2.0
