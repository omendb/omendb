# OmenDB architecture

**Status:** accepted direction; implementation is incomplete and the current
alpha line is not a release of this architecture.

OmenDB is a **server-first PostgreSQL-class relational OLTP database written in
Rust**. PostgreSQL compatibility belongs at deliberate external boundaries
(wire protocol, SQL behavior, drivers, and tooling); OmenDB does not copy
PostgreSQL's internal page layout, executor, WAL, or process-per-connection
model.

The direct Rust API remains a useful embedded and testing surface. It must use
the same transaction and storage semantics as the server, not become a second
database product.

## Product shape

```text
PostgreSQL clients / future native clients
                    |
             OmenDB server
                    |
       session, auth, protocol, SQL
                    |
        planner and typed execution
                    |
       transaction and catalog layer
                    |
                 SeerDB
                    |
              local durable storage
```

The first serious deployment target is a single excellent node with WAL-based
replication and operational tooling. Distributed SQL is not a prerequisite for
the first server architecture.

OLTP and OLAP use separate physical engines. OmenDB exports a consistent
snapshot plus an ordered committed-change stream to a future `omen-olap`
consumer instead of making integrated HTAP storage a prerequisite.

## Workspace and ownership

The repository is a Cargo workspace whose root package is OmenDB:

```text
omendb/
├── Cargo.toml       # root package and workspace
├── src/             # OmenDB server and relational engine
├── crates/
│   └── seerdb/      # independent generic storage crate
├── docs/
├── benchmarks/
└── tests/
```

OmenDB remains `AGPL-3.0-only`. SeerDB remains an independently versioned and
publishable `Apache-2.0` crate. The repository is the single writable source
for both projects; SeerDB's former standalone repository is not a second
implementation source.

The current development dependency is a workspace path dependency. Registry
releases remain independent: publish and qualify a SeerDB prerelease first,
then release OmenDB against that exact SeerDB version. Neither package version
is inherited from the workspace.

## OmenDB and SeerDB boundary

SeerDB is a generic OLTP-oriented transactional ordered-KV engine. Its logical
model is deliberately small:

```text
TreeId + unsigned-lexicographically ordered key bytes + opaque value bytes
```

SeerDB owns:

- ordered B-tree access and cursors;
- transactional tree lifecycle and atomic multi-tree mutation;
- MVCC visibility and write conflicts;
- page, buffer, blob, and physical mapping management;
- WAL, checkpoint, crash recovery, durability, and storage pressure;
- generic committed changes and snapshot/restart positions.

OmenDB owns:

- SQL and PostgreSQL-facing behavior;
- catalogs, schema, row codecs, NULL/type semantics;
- primary and secondary index meaning;
- constraints, DDL, optimizer, and execution;
- relational CDC interpretation.

OmenDB encodes relational keys and rows into SeerDB's byte boundary. SeerDB
must not acquire SQL schema IDs, NULL bitmaps, column directories, or index
semantics. OmenDB must not bypass SeerDB's transaction/MVCC machinery.

A generic storage-plugin matrix is not the product architecture. OmenDB should
call SeerDB through a capability-rich Rust API. A future alternate engine can
integrate through a deliberate adapter, but OmenDB will not hard-code a list of
RocksDB, Fjall, redb, or other first-party backends. The transaction, crash,
and snapshot invariants are recorded in
[`docs/adr/0001-seerdb-transaction-contract.md`](adr/0001-seerdb-transaction-contract.md).

## Transaction and durability identities

These identities are distinct:

```text
TxnId = transaction identity
CSN   = logical committed visibility order
LSN   = durable log position
```

The storage transaction API must eventually provide:

- snapshot-isolated multi-writer transactions;
- point reads, conditional insert/put/delete, and ordered cursors;
- atomic writes across multiple trees;
- point and range dependency events for serializable certification;
- short-lived borrowed record access with explicit page-pin lifetimes;
- a durability result containing CSN and durable LSN;
- storage pressure and retention visibility.

A physical WAL is for recovery and physical replication. It is not the public
long-term CDC format. A generic committed change stream preserves transaction
boundaries and TreeId/key operations without exposing page-layout details.

Snapshot export must atomically return:

```text
snapshot CSN X + restart LSN Y
```

A consumer copies snapshot X and then consumes committed changes after Y with
no gap. This is the contract for backups, CDC, and `omen-olap` bootstrap.

## Execution and server direction

The relational engine will share one typed planner/type system across two
execution paths:

- **OLTP micro-plans** for prepared point lookups, simple DML, and short
  transactions; these avoid generic `Vec<Row>`/`Vec<Value>` hot paths.
- **Batch pipelines** for scans, joins, aggregates, sorts, and index builds;
  these use typed vectors, selection representations, bounded work, and
  cancellation.

The server target is one multi-threaded process with bounded resources and
home-worker execution for short transactions. Protocol I/O, CPU scheduling,
deadlines, cancellation, memory quotas, admission, and observability are
first-class concerns. A future io_uring or native transport path must be able
to fit behind explicit runtime boundaries rather than leak into SQL code.

## Storage direction

SeerDB's existing B-tree, out-of-place pages, checksums, blob separation, and
fault/recovery testing are useful foundations. The target storage architecture
keeps these physical concerns separate from logical transaction versions:

```text
logical key/value MVCC
        |
current record + undo/version chain
        |
ordered B-trees and optimistic page latches
        |
buffer residency and durable PageId mapping
        |
out-of-place segments, WAL, checkpoint, and GC
```

The current serialized-writer/generation API is a correctness prototype, not
the final OmenDB transaction seam. The redesign proceeds from a simple correct
multi-writer snapshot-isolation implementation, then measures serializable
certification, buffer translation, WAL commit modes, and device-specific
optimizations. No disk-format stability promise should force v1 architecture
into permanence.

## Current status and roadmap

The current tree has a direct Rust relational API, durable SeerDB integration,
SQL/catalog/index/constraint tests, fault and recovery coverage, and a
feature-gated PostgreSQL wire server. `src/pgwire_server.rs` now also exposes a
persistent `RunningServer` and the `omendbd` binary: one process owns one
opened database, bounds admitted connection tasks, derives trust/SCRAM policy
from the durable auth catalog, reports connection and query/describe lifecycle
counters, and closes the handle on explicit shutdown. This is a server foundation, not yet the complete
alpha contract: protocol coverage, authorization policy, resource quotas, and
the complete crash-level daemon matrix remain open. Wire cancellation now
routes `CancelRequest` to the transaction API's cooperative checkpoints;
representative lock-wait and shutdown-time wire tests cover SQLSTATE `57014`
and worker drain before reopen. A daemon-level SIGKILL/reopen test covers
recovery after process loss. The direct
SeerDB qualification path (`src/seer_direct.rs`) remains test-only evidence for
catalog/table/index-to-tree mapping and is not a second relational backend.

The roadmap is intentionally dependency-ordered, but OmenDB follows each
SeerDB milestone immediately so the storage contract is validated by a real
relational consumer:

1. **Contracts and model — substantially complete:** workspace ownership,
   generic ordered-KV boundary, TxnId/CSN/LSN rules, tree lifecycle,
   snapshot/change positioning, and crash-state invariants.
2. **SeerDB transactional foundation — in progress:** the first vertical
   slice now provides `TxnId`, `CommitSeq`, `TreeId`, fixed-snapshot
   transactions, current-record MVCC with append-oriented before-images,
   ordered scans, atomic multi-tree mutation, and durable exact write-conflict
   records. It also returns an explicit `{CSN, LSN}` commit position and
   persists that position through manifest/WAL recovery. The implementation is
   design-gated by [ADR 0002](adr/0002-seerdb-mvcc-version-storage.md) and
   [ADR 0003](adr/0003-seerdb-commit-recovery-state-machine.md); the initial
   transaction-status table and active-snapshot registry now exist, with
   bounded logical version GC respecting active snapshots. Ordered cursors
   with read-range phantom protection, durable retention leases, the zero-gap
   `{CSN, restart LSN}` committed-change stream, group-commit publication
   ([ADR 0004](adr/0004-group-commit-publication-lane.md)), and durable status
   freezing (GC rewrites current records to carry their resolved CSN and prunes
   status entries no retained reference needs) are implemented.
   Page-level multi-writer installation remains open.
3. **OmenDB direct integration — complete:**
   `src/seer_direct.rs` is the only production backend ([ADR 0005](adr/0005-delete-storage-kernel-seam.md)):
   one catalog tree, one tree per table, one tree per index, typed
   transactions over SeerDB snapshots, immediate foreign-key validation,
   and ON DELETE CASCADE/SET NULL referential actions at commit time.
   The storage-kernel seam, attempt machinery, coalesced publication,
   archive/restore, and the temporary backend were deleted outright.
4. **Server alpha foundation — in progress:** persistent multi-session
   lifecycle, bounded connection admission, configuration, lifecycle
   diagnostics, and clean shutdown/reopen behavior are implemented. The
   deliberate PostgreSQL protocol subset, authorization baseline, resource
   quotas, and the complete process-level compatibility/crash matrix remain
   open; wire cancellation and tracked worker draining are implemented.
5. **Correctness and scale:** serializable certification, snapshot plus
   zero-gap logical CDC, physical replication, typed OLTP micro-plans, batch
   execution, and benchmark-led storage/runtime optimization.

A test-only SeerDB reference model exercises snapshot visibility, disjoint and
conflicting writers, atomic multi-tree mutation, tree-ID burn on abort, and
retained snapshot visibility. It is a semantic oracle, not a production
backend. The alpha release gates define evidence requirements; the current
`0.1.0-alpha.*` line remains unreleased until the server-first storage and
server criteria are met.
