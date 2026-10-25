# Native relational engine and storage-kernel roadmap

**Branch:** `main` — vNext remains experimental until qualified product cutover.

**Goal:** replace the generation-COW serving path with a qualified, concurrent,
**server-first** native relational OLTP engine over one SeerDB transaction/storage
authority. Embedded use is secondary and shares the same engine. PostgreSQL
compatibility is an optional surface, not a storage/SQL architecture constraint.
Object storage is a secondary tier for backups, archived WAL, snapshots and
immutable cold extents; local RAM/NVMe remains the primary transaction path.

**Current design:** [ADR 0015](../adr/0015-native-relational-engine.md)
(native relational target and rejected alternatives). Supporting owners: [ADR 0013](../adr/0013-storage-kernel-and-access-methods.md)
(boundary), [ADR 0014](../adr/0014-vnext-installation-and-recovery.md)
(decision/lifecycle/recovery), [ADR 0009](../adr/0009-buffered-btree-and-materialization.md)
(ordered layout), [ADR 0007](../adr/0007-runtime-execution-and-contention.md)
(relational/runtime), [ADR 0010](../adr/0010-row-layout-and-column-families.md)
(rows), [ADR 0011](../adr/0011-htap-and-analytical-representations.md)
(analytics), [ADR 0012](../adr/0012-unified-modalities-and-rewrite-strategy.md)
(products/repositories).

## Strategy and checkpoint

The 2026-10-25 design review used current main at 1590011, selected the
native relational-first direction in ADR 0015, and performed no implementation
tests or performance qualification. Subsequent October 25 scope clarification
makes server concurrency primary, retains SeerDB's separately useful KV façade
without letting it dictate OmenDB internals, and provisionally favors SERIALIZABLE
for native SQL while READ COMMITTED remains a required distinct supported mode.
Do not finalize that default without matched retry/latency/client-UX evidence. The prior 2026-10-13 review inspected source at `92e2ac8eed6af89d164af4b44189146a68ce9864`.
It is a design/targeted source audit, not a new test run or benchmark. No milestone
is closed by the resulting documentation. Historical tests remain evidence at their
recorded revision; known legacy defects require independent semantics, not parity
with incorrect output. This file is the sole OmenDB implementation sequence.

| Milestone | Component evidence | Remaining qualification |
| --- | --- | --- |
| A — identities/frames | Object/PageId and exact-incarnation frames | Integrated lifetime/pressure ownership |
| B — buffer | Sharded translation, CLOCK, single-flight Loading, dependency-checked spill | Protected progress, complete persistent lifecycle/capture |
| C — ordered pages | v4 slotted B-link tree, atomic upsert and split dependencies | Bounded cells/overflow, actual fenced feasibility, structural finish |
| D — decisions | Ordered WAL, grouped undo, owned attempts and gated contiguous completion | Persistent runtime integration, certification, deterministic reservations, complete-log target |
| E — MVCC | Logical owner/history, effect identity, snapshots/private overlay | Registered leases/dependencies, bounded work/history/reclamation |
| F — persistence | Directory/component exclusion, checksummed images and placement map | Complete checkpoint/manifest, restart, bounded retry-space and crash qualification |
| G — relational | Existing product adapter/oracles | Binder/statement/catalog correctness, canonical records and qualified cutover |

Entry points are `crates/seerdb/src/vnext/{ordered_commit,commit_append,
runtime_admission,ordered_mvcc,ordered_recovery,buffer,page_store,page_map,
undo_store,segment_log}.rs` and `vnext/btree/`.

### Known blockers, not completed foundations

- Ordered coordinators and MVCC readers require shared runtime admission; armed
  commit attempts fence unresolved exits before intents/drain membership drop, and
  publication/ACK order against that fence. A persistent owner joining these
  services, mandatory page dependencies, snapshot/history leases and structural
  paths remains unimplemented. Fixed snapshots/write intents are not the target
  serializable point/absence/range/catalog certifier.
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

## Implementation sequence

This is the single active plan. Milestones are correctness/progress gates, not a
requirement to preserve vNext module boundaries. Reuse proved components, rewrite
weak ones, and remove transitional facades once a replacement is qualified. Develop
on main; keep the legacy path only as a *fallible* comparison/oracle until cutover.

### 1. Establish a real end-to-end runtime and fix SQL integrity in parallel

Build one persistent SeerDB owner spanning buffer, ordered objects, transaction and
status registry, catalog retention, resource admission, WAL, redo/history, recovery,
commit/visibility, checkpoint and GC. The native server and concurrent SQL sessions
are the main qualification path; embedded remains an optional deployment surface. No persistent optional coordinator assembly,
independent failure boundaries or nested commits. Keep the exact-incarnation
single-flight Loading repair and owned commit-attempt guard; qualify them through
the actual runtime. Preserve responsibility from before possible append through
acknowledged contiguous visibility, clean pre-append refusal or terminal fencing.

The first integration fixture is **a real relational transaction**, not another
component-only KV test: create a table and unique/secondary index, insert/update/
delete rows transactionally, read a fixed snapshot, commit and reopen. Start with
the simplest safe storage/checkpoint mode; add multiple concurrent sessions,
conflicting writers, long snapshots and fault injection to the *same* path.

In parallel, correct the serving SQL boundary before exposing untrusted clients:
one non-executing binder, current-grant authorization on Describe/execution,
statement-private row/index/catalog delta, rollback-only explicit transactions,
retained catalog identity, one schema publication transaction, exact constraints,
typed SQL results and correct cursor continuation. Fix the gap register's FK timing,
batch-length and conversion errors. Oracle tests must reflect supported native SQL
semantics, not assume full PostgreSQL compatibility or bless legacy defects.

**Gate:** one persistent runtime and one binder/statement contract own actual API
and SQL entry points; a failed statement cannot commit partial effects; no global
RelationalDatabase lock is needed for independent reads/transactions.

### 2. Finish representable native rows, indexes and commit certification

Implement schema-driven clustered records and ordered primary/secondary/constraint
indexes, with stable logical row identity for primary-key-less tables. Eliminate
mandatory generic Value/Row decoding on primary-key probes. Ordered pages need
bounded key/fence/separator/value descriptors and logical overflow. Validate
*actual fenced leaf/internal/root split candidates* and completion capacity before
commit. Preserve legitimate 180-byte replacements on 384-byte test pages and
long/common-prefix keys, updates, deletes, replay and corrupt chains. Allocation
refusal must leave the prior logical value intact; don't weaken tests to fit an
obsolete layout.

Implement decision-ordered serializable certification against accepted (including
uninstalled) earlier point, absence, gap/range, namespace, catalog/enforcement and
constraint effects. It is the initial correctness reference and provisional
server default. Provide explicit READ COMMITTED statement snapshots and safe
wait/recheck rather than an alias for weaker write-write conflict checks; test
mixed-isolation histories, not only homogeneous isolation. Retain full evidence under pressure; provide explicit refusal
when resources are exhausted. Introduce queued hot-key waiting only after the
low-contention path is correct, and separately model statement-refreshing isolation.

Resolve complete WAL-plus-checkpoint recoverability for current rows *and required
predecessor history* before eliminating synchronous undo. Reserve log, history,
overflow, pages, pins, scratch, conflict summaries and installation capacity before
possibly durable decision; move whole-batch/segment representability refusal before
device entry. Prepare and encode outside the short ordered certification/append lane.
The old WAL+undo barrier path remains a proof baseline until the new format passes;
do not delete the history barrier by assertion.

**Gate:** a real SQL transaction spanning canonical row, catalog and required
indexes certifies correctly, recovers its retained versions and completes structurally
after faults. An accepted decision cannot become impossible to install or replay.

### 3. Checkpoint, reopen and bound every resource lifetime

Capture one complete structurally closed authority cut (visible CSN + decision
LSN), initially with a quiesced correctness baseline. Include roots, row/index/
catalog lifecycles, fences/right links, child/overflow/extents, allocation/page
map, owner outcomes and all required history and retention floors. Validate
references nonquadratically, with exact store identity; publish a checksummed
manifest only after dependent bytes are durable. Preserve previous checkpoint
authority on failed publication. No working spill image or maximum page LSN is
complete logical replay evidence.

Recover checkpoint plus validated committed suffix through the *same* logical
effect reducer/install identity as live commits; close incomplete final appends
only where proven safe. Test SIGKILL or injected failures after decision durability
but before history/page writes, checkpoint publication and GC. Crash during recovery,
reopen twice, then execute an ordinary mutation. Repeated failed recoveries cannot
accumulate unbounded abandoned pages/images; old readers/consumers must not lose
leased history or trigger unsafe PageId reuse.

Snapshot/export and CDC registrations atomically capture source incarnation,
compatible catalog, snapshot CSN, restart LSN and leases for copied state and
transaction-complete change suffix. Budget snapshot, old checkpoint, CDC, backup,
future replica and certification histories *separately*. Implement stable bounded
image-capture/epoch checkpointing if whole-database quiescence violates the
predeclared pause/size budgets. This is a pre-production scale requirement, not
a speculative separate storage engine.

**Gate:** deterministic crash/failure matrix passes across actual server/direct
entry points; checkpoint, replay, retained snapshots and space reclamation converge
under churn and bounded RAM/SSD budgets.

### 4. Replace the SQL and scheduling hot paths, then cut over

Replace source-text semantic guesses, probe-based Describe, generic row-materializing
point plans and offset-rescanning morsels with one binder/optimizer/typed IR and
a native micro-plan plus typed-batch execution path. Storage cursor batches bound
visible rows, output bytes, raw/invisible slots, undo hops, overflow/I/O and CPU
work, with exclusive logical continuation across yield. No held frame latch
survives output suspension. Shared expression/type/NULL/collation semantics across
both execution modes are mandatory.

Change Database into a cloneable concurrent handle, replace global outer
RwLock serialization and unbounded blocking work with bounded/lazy workers and
transaction-grain resource admission. Progress-critical WAL/commit/reclaim capacity
cannot be starved by scans. Benchmark and qualify mixed reads/writes, schema
changes, timeouts, cancellations, disconnections and high contention through
native/direct and server interfaces. One transaction/catalog/log authority must
span all authoritative objects.

Cut over *only* after independent consistency/recovery/operational gates and
agreed performance floors. Convert supported developer-preview logical state
offline or refuse it; don't guarantee stable physical-format compatibility. Delete
legacy B-tree/COW generation publishers, compatibility adapters, old flags and
stale tests/documents after the new path is qualified. Don't retain a permanent
storage-backend selector.

**Gate:** genuine multi-writer SQL server and embedded client run one native
engine, with no known authorization/data-integrity defect and no hidden old
publication path.

### 5. Optimize with matched device-backed workloads

Declare hardware, filesystem, key/value sizes, memory, durability/isolation,
concurrency and budget thresholds **before** measuring. Linux on real NVMe
and production sync semantics is the main comparison; macOS APFS is an explicit
different durability class, never ranked against a weaker Linux ACK. Report
p50/p95/p99, throughput, aborts/retries, CPU/allocations, LLC/TLB and latch/
certification/translation/frontier waits, WAL bytes, host/device write
amplification, RSS, GC/checkpoint pauses and recovery time/space.

| Experiment | Real alternatives and question |
| --- | --- |
| Durable indexed OLTP | Native OmenDB vs PostgreSQL and SQLite (overlapping native SQL); RocksDB, Fjall, SurrealKV at comparable KV semantics only |
| Commit and visibility | Complete log vs two-barrier baseline; natural grouped commits, parallel encoding/installation, optional autonomous NVMe |
| Primary layout | Clustered schema-typed rows vs bounded heap/PAX prototype; total indexed update/scan/space/recovery economics |
| Ordered pages | Current slotted B-link node vs compact/inline-prefix/SIMD and BBTree-inspired batch search; 4/8/16 KiB as applicable |
| Buffer residency | Safe sharded translation baseline vs validated optimistic guards, hints/predictive mapping and prefetch; no unsafe stale load/eviction |
| Mixed OLTP/analytics | Concurrent short transactions plus long selective/full scans, indexes, checkpoint and GC; resident/larger-than-RAM and cold starts |
| Hot contention | Ordered OCC plus waiting vs alternatives at declared isolation; observe retries and tail latency, not throughput only |

Cover uniform/skewed hot keys, 1/4/16+ writers, large transactions and records,
long readers, mixed schema/constraint work and sustained updates that expose
reclamation debt. User-visible SQL performance, not only a storage microbenchmark,
selects the architecture. Vendor/paper speedups suggest candidates only.

**Gate:** no priority workload suffers unacceptable p99/space/recovery collapse
or unbounded maintenance pressure. Promote a fancier algorithm only after a
representative matched improvement and equivalent failure proofs.

### 6. Qualify optional object storage and introduce other engines when earned

After the server's primary local durable runtime works, support S3-compatible
snapshot/backup export, manifest verification and restore, archived complete WAL
segments and selectively cold immutable extents. Keep per-commit WAL sync and
mutable OLTP pages local; object-store primary/disaggregated persistence is a
separate later design with its own measured and fault-qualified recovery protocol.
Test interrupted multi-object uploads, manifest publish, snapshot leases, missing
objects, caching, egress/request cost and safe eventual GC. Do not mistake strong
single-key read-after-write for an atomic multi-object database commit.

Future regional replication/distribution should learn from PlanetScale Neki's
**query-driven shard groups**, staged snapshot + transactional change-stream
catch-up, topology epochs, and lazy routing. Never add mandatory router/control
plane overhead to the single-node fast path. Do not ship non-atomic cross-range
SQL transactions as ordinary commits merely because Neki's current preview has
that limitation.

### Native extensions

After relational core qualification, expose snapshot-correct native typed
batch/selection access and source-complete transactional change streams to
text/ANN/graph/columnar engines. Required constraint indexes remain
authoritative, while derived structures must prove source identity, snapshot
retention, complete deltas and exact/explicitly approximate fallback. Begin
with exact vector scoring and native row/batch analytics. Compare selective
columnar acceleration and search strategies only on filtered/churn-heavy mixed
workloads with measured unused-feature cost. A separate independently ingested
OLAP or vector product is not a participant in an OmenDB SQL commit.

Regional HA precedes general distribution. Do not impose physical B-tree page
or universal KV ranges as a future placement API; select logical partition/range
keys from the chosen data model at distribution design time.

## Format policy and later work

Current WAL/undo/MVCC framing is v2 and ordered pages v4; both are
experimental and may be replaced directly. ADR 0015 is the new native target. New overflow/history bytes
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
