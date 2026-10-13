# Storage-kernel vNext roadmap

**Branch:** `storage-kernel-vnext`

**Goal:** replace generation-COW and universal opaque-KV storage with one qualified
transaction/storage kernel, native ordered/record access, and integrated OmenDB.

**Design owners:** [ADR 0013](../adr/0013-storage-kernel-and-access-methods.md)
(boundary), [ADR 0014](../adr/0014-vnext-installation-and-recovery.md)
(decision/lifecycle/recovery), [ADR 0009](../adr/0009-buffered-btree-and-materialization.md)
(ordered layout), [ADR 0007](../adr/0007-runtime-execution-and-contention.md)
(relational/runtime), [ADR 0010](../adr/0010-row-layout-and-column-families.md)
(rows), [ADR 0011](../adr/0011-htap-and-analytical-representations.md)
(analytics), [ADR 0012](../adr/0012-unified-modalities-and-rewrite-strategy.md)
(products/repositories).

## Strategy and checkpoint

The 2026-10-13 review inspected source at `92e2ac8eed6af89d164af4b44189146a68ce9864`.
It is a design/targeted source audit, not a new test run or benchmark. No milestone
is closed by the resulting documentation. Historical tests remain evidence at their
recorded revision; known legacy defects require independent semantics, not parity
with incorrect output. This file is the sole OmenDB implementation sequence.

| Milestone | Component evidence | Remaining qualification |
| --- | --- | --- |
| A — identities/frames | Object/PageId and exact-incarnation frames | Integrated lifetime/pressure ownership |
| B — buffer | Sharded translation, CLOCK, single-flight Loading, dependency-checked spill | Protected progress, complete persistent lifecycle/capture |
| C — ordered pages | v4 slotted B-link tree, atomic upsert and split dependencies | Bounded cells/overflow, actual fenced feasibility, structural finish |
| D — decisions | Ordered WAL append, grouped undo, readiness/contiguous completion | Mandatory runtime responsibility, certification, deterministic reservations, complete-log target |
| E — MVCC | Logical owner/history, effect identity, snapshots/private overlay | Registered leases/dependencies, bounded work/history/reclamation |
| F — persistence | Directory/component exclusion, checksummed images and placement map | Complete checkpoint/manifest, restart, bounded retry-space and crash qualification |
| G — relational | Existing product adapter/oracles | Binder/statement/catalog correctness, canonical records and qualified cutover |

Entry points are `crates/seerdb/src/vnext/{ordered_commit,commit_append,
runtime_admission,ordered_mvcc,ordered_recovery,buffer,page_store,page_map,
undo_store,segment_log}.rs` and `vnext/btree/`.

### Known blockers, not completed foundations

- `OrderedCommitCoordinator` still optionally attaches runtime admission and page
  dependencies. Its fixed snapshots/write intents are not the target serializable
  point/absence/range/catalog certifier.
- `CommitAdmission::drop` releases drain membership without owning commit outcome.
  Caught unwind after possible append can therefore escape the required failure fence.
- Empty-leaf preflight does not prove a record fits future fenced/split pages or
  that an admitted installer has sufficient allocation/pin/scratch capacity.
- Original WAL batches can exceed segment capacity inside device append. A known
  deterministic size refusal must not become an uncertain/fenced outcome.
- `CommitAppender` encodes the batch and performs append under its ordering mutex.
  The coordinator synchronizes WAL and then required undo before installing.
  These are verified structural costs, not newly measured bottleneck magnitudes.
- Placement-map duplicate validation, abandoned-image scanning and retained-history
  indexes/replay output need bounded/nonquadratic behavior before scale claims.
- Legacy Describe/auth, statement failure, catalog/constraint and cursor/conversion
  issues remain on the serving path; the [gap register](../gap-register.md) owns them.

## Immediate sequence

### 1. Own lifecycle, certification and resources before checkpoint

Construct one persistent runtime; remove optional-service composition from real entry
points. Arm commit responsibility before possible append and retain intents, resources
and drain membership until explicit completion, clean refusal or terminal fencing.
Qualify unwind/cancel/disconnect, visibility gaps and checkpoint drain through the real
runtime. All read/snapshot/direct/server and structural/GC/allocation paths use its
failure, history and capture authority.

Reserve transaction resources before snapshot/intents and bounded completion resources
before decision. Include original log bytes, required history, chain/split/root IDs,
peak pins, scratch and bookkeeping. Keep progress capacity under foreground/analytical
saturation; add a coordinated installer slow path rather than unbounded retries.
Move whole-batch representability refusal ahead of device entry.

Implement decision-ordered certification now, not an adapter callback at cutover.
Include point/absence/range/gap, name/catalog/enforcement and mandatory constraint
observations against earlier accepted but uninstalled writers. Acceptance and conflict
summary publication share the sequencer. Budget retained summaries and qualify
pressure/refusal without discarding evidence.

Correct the maintained relational boundary in parallel: one non-executing binder,
private statement deltas, transaction-owned rollback-only state, retained catalog
and one DDL/schema transaction. Current authorization/enforcement semantics follow
ADR 0007. Security/integrity work need not wait for a new row format.

### 2. Replace record representation and complete installer progress

Implement bounded key/fence/separator/value descriptors and immutable logical overflow
in a new experimental format. Choose splits from successfully built actual fenced
candidates, then balance. Preserve useful internal fanout and finish parent/root work
before readiness. One logical admission policy covers current values, future history,
original WAL, decode memory and configuration limits.

Cover long/common-prefix keys, replacement/delete/replay, leaf/internal/root splits,
corrupt or spliced chains and retained readers. Preserve the legitimate 180-byte
replacement on a 384-byte page and real inline-growth cases where relevant. Do not
shrink test records to retain an obsolete size expectation or fabricate a split after
overflow legitimately removes it. Genuine refusal must preserve old state.

Resolve the complete-log/history representation from ADR 0014 before stabilizing
checkpoint formats. Prove reconstruction from checkpoint plus committed suffix,
including retained history and logical version identity. Retain the current two-barrier
path only as a temporary comparison/proof baseline until replacement passes; deleting
undo synchronization without replacing its dependency is not an optimization.
Prepare bodies outside the decision lane, then preserve ordered certification/append.
Measure log-byte cost, barrier count and lane occupancy before selecting scheduling.

### 3. Capture and recover complete authority — Milestone F

Drain committing mutations through contiguous visibility and capture a structurally
complete prefix under structural/freezing/GC/allocation quiescence. Bind roots, object
lifecycle, child/right/overflow closure, allocation state, placement map, owner/history
outcomes and retention floors to store identity. Snapshot registration shares capture
and retention authority. Use stable bounded image capture and strict nonquadratic
reference validation; preserve prior authority until publication succeeds.

Reopen checkpoint plus synchronized retained committed suffix through the common
applicator. Working spill images and maximum page LSNs are not logical restart authority.
Complete-log qualification kills after decision sync but before history/page flush.
Qualify crash during recovery, two consecutive reopens and a subsequent normal operation.
Disposable working epochs or another proved retry-space policy must bound abandoned
undo/image growth without invalidating older checkpoints or leases.

### 4. Bound retention, recovery and work

Checkpoint owner outcomes or safely freeze before reclaiming decisions. Respect
snapshot/catalog/object, CDC, replica, backup and prior-checkpoint floors. No ad hoc
undo truncation, chain reclamation or PageId reuse. Bound recovery memory/time against
retained distance using streaming/indexing and policy, not only frame-sized I/O.

Cursor batches have row, byte and physical-work budgets, including invisible slots,
history/overflow and I/O. Preserve continuation, snapshot/catalog/overlay identity;
no prefix rescans or guards across yield. Bound checkpoint pauses versus dirty fraction
and database size; replace blocking capture if it misses agreed cutover budgets.

### 5. Measure the actual qualified vNext path

Agree hardware, durability/isolation, data sizes, concurrency and latency/throughput/
memory/checkpoint/recovery budgets before measuring. No thresholds chosen after results.
Use Linux device-backed storage, not tmpfs; report macOS sync-class differences rather
than compare stronger barriers with weaker acknowledgements as an engine ranking.

The first comparisons are:

| Experiment | Alternatives | Decision evidence |
| --- | --- | --- |
| Durable KV/OLTP | vNext, legacy, RocksDB, Fjall, SurrealKV; PostgreSQL at the SQL layer | Matched atomicity/isolation/durability, correctness, p50/p95/p99, CPU/memory |
| Commit/history | Current WAL+undo versus complete recoverable log; bounded batching, then autonomous NVMe | Barriers, bytes, lane occupancy, visibility waits, recovery/retention |
| Ordered/row layout | Clustered typed records versus record pages + primary index | Point/range/update, secondary indexes, scans, histories, write amplification |
| Buffer/placement | Correct sharded baseline versus validated translation/layout policies | Cycles, LLC/TLB/NUMA, read amplification, host/device writes |
| HTAP | Rows + selected derived columns versus single-copy hybrid and isolated analytics | OLTP tails, scan throughput, total bytes/memory, lag and checkpoint/recovery |
| Vector | Exact, resident ANN and appropriate filtered/SSD candidates | Recall plus exact eligibility, restrictive/correlated filters, churn, OOD, tail/memory/I/O |

Use hot and larger-than-memory data, uniform/skewed keys, 1/4/16+ committers, long
snapshots, large records/transactions and ongoing checkpoints/GC. Sustain enough
mutation to expose reclamation/compaction debt. Product perf smoke is not isolated
vNext evidence. Paper/vendor numbers select experiments, not winners.

### 6. Canonical records and product integration — Milestone G

Integrate schema-driven rows with ordered/constraint indexes and catalog under one
decision and lifetime. Compare clustered records with heap/row pages; do not invent
a redundant physical structure solely to satisfy a two-access-method slogan.

Require binder-owned types/effects/privileges/dependencies; one retained catalog with
enforcement membership; private statement deltas and fixed DML input; immediate versus
deferred constraint timing; mutation-driven unique/FK probes; current-grant checks at
Describe/execution; rollback-only explicit blocks; concurrent direct/wire runtime
without an outer database lock; and bounded native batches/typed guarded rows.

Qualify write skew, absence/phantoms, disjoint-row unique conflicts, parent deletion
versus child insertion, old writers versus added constraints/DDL, and an earlier
decision paused before install. Run PostgreSQL SQL/wire and retained SQLite oracles,
conversion/schema/constraint, dump/restore, crashes and pgbench/TPC-B plus YCSB/TPC-C-
shaped workloads on the new path. Preserve supported logical preview data or refuse
conversion explicitly. Cut over only after semantic/crash and agreed performance gates;
delete superseded engines, adapters, flags, fixtures and documentation in that scope.

## Format policy and later work

Current WAL/undo/MVCC framing is v2 and ordered pages v4. New overflow/history bytes
require new versions; only provably incomplete final appends are repairable. Preview
conversion does not promise stable-format upgrades.

Native batch analytics and exact vector scoring are the first integration probes;
specialized acceleration follows workload value and ADR 0012's repository boundaries.
The standalone OLAP companion remains separate; integrated retrieval components have
one source owner after migration, not duplicated stores. No private source publication
or repository archival follows automatically. Regional HA precedes general distribution;
object storage is a deliberate cold/archive or separately qualified disaggregated profile.

Reassess applicable CI against replaced contracts; obsolete tests are not a prerequisite
to redesign, but implementation delivery still requires the relevant gates and honest
failed/unrun reporting. Historical handoffs/research do not create another backlog.
