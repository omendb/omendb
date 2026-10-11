# Storage-kernel vNext roadmap

**Branch:** `storage-kernel-vnext`

**Goal:** replace generation-COW and the universal opaque-KV physical boundary
with one transaction/storage kernel and concrete access methods.

**Design owners:** [ADR 0013](../adr/0013-storage-kernel-and-access-methods.md)
(boundary), [ADR 0014](../adr/0014-vnext-installation-and-recovery.md)
(decisions/lifecycle/recovery), [ADR 0009](../adr/0009-buffered-btree-and-materialization.md)
(ordered layout), [ADR 0007](../adr/0007-runtime-execution-and-contention.md)
(relational/runtime semantics), [ADR 0010](../adr/0010-row-layout-and-column-families.md)
(canonical rows).

## Strategy and checkpoint

This is a replacement, not a permanent backend matrix. Keep the legacy engine as
a semantic/fault reference until cutover; known defects in the
[gap register](../gap-register.md) require independent PostgreSQL/SQLite semantics,
not parity with incorrect output. No SOTA or competitive-performance claim is
qualified. Research selects experiments, not implementation by reputation.

Source reviewed at `0783097` on 2026-10-11. The single-flight buffer repair is
implemented; the accepted design below is not thereby implemented. These milestones
retain their names for navigation, not an assumption that each foundation is closed.

| Milestone | Current evidence | Remaining qualification |
| --- | --- | --- |
| A — identities/frames | Object/PageId and exact-incarnation frame lifecycle | Lifetime/pressure and integrated ownership |
| B — buffer | Sharded translation, CLOCK, single-flight Loading, dirty creation and dependency-checked spill | Protected progress, guarded checkpoint capture, persistent lifecycle |
| C — ordered pages | v4 slotted B-link tree, atomic upsert, split/dependency propagation | Bounded cells/overflow, actual fenced split feasibility and structural completion |
| D — decisions | Ordered append, exact WAL sync, grouped undo, status/readiness and contiguous completion | Runtime-owned attempts, deterministic reservations and decision-ordered certification |
| E — MVCC | Logical owner/undo resolution, install identity, point/range fixed snapshots and private overlay | Registered leases/dependencies, work-bounded reads and retention/reclamation |
| F — persistence | Store incarnation/component leases, OS directory lock, checksummed image arena and caller-selected placement map | Complete capture/manifest, restart authority, retry-space and crash qualification |
| G — relational integration | Legacy product adapter only | Coherent binder/transaction/catalog semantics, canonical-record participant, concurrent runtime, product cutover |

Implementation entry points are under `crates/seerdb/src/vnext`: `frame.rs`,
`buffer.rs`, `btree/`, `commit_append.rs`, `ordered_commit.rs`, `ordered_mvcc*.rs`,
`ordered_txn_read.rs`, `runtime_admission.rs`, `ordered_recovery.rs`,
`{store,page_store,page_map,page_image,undo_store,segment_log}.rs`.

### Known blockers, not completed foundations

- Empty unfenced-leaf preflight accepts a durable decision that cannot install or
  replay after adding split/fence space. Byte-balanced pivot choice does not test
  actual candidate pages. Overflow/layout work replaces this assumption.
- Caught commit unwinding can release checkpoint membership without fencing.
  Explicit returned-error handling does not discharge all completion responsibility.
- Full original WAL batches can exceed segment capacity inside device append,
  turning deterministic refusal into an uncertain/fenced outcome.
- Optional coordinator admission does not own all reads, snapshot leases,
  structural/object mutations or the persistent runtime.
- Fixed snapshots and exact write intents do not implement SERIALIZABLE. There
  is no vNext point/absence/range/catalog certifier ordered with decisions.
- Placement-map duplicate detection is quadratic; reopen scans abandoned arena
  images; recovery/history indexes and replay output grow with retained history.
- Relational Describe/auth, statement failure, catalog consistency, typed FK
  publication, cursor progress and conversion defects remain on the serving path.

Exclusive store ownership remains a required contract. Diagnose surviving
ownership failures against clean source; eventual retry is not proof of prompt
lease release. No unvalidated mitigation closes that gate.

## Immediate sequence

### 1. Own lifecycle, certification and resources before checkpoint

Reassess CI against this design before further implementation: distinguish
surviving contract failures from checks tied to paths being replaced. Do not
require a green obsolete implementation before redesign; implementation delivery
still requires applicable gates and candid reporting.

Replace persistent optional-service composition with one runtime owner. Implement
commit-attempt responsibility before possible append; hold intents, reservations
and checkpoint membership until explicit completion, clean refusal or terminal
fencing. Qualify unwind/cancel/disconnect, frontier gaps and drain failure through
the real runtime. All read/snapshot/direct/server capabilities share failure and
history authority; structural/freezing/GC/allocation paths share capture authority.

Add transaction-grain workload reservations distinct from checkpoint drain.
Reserve original WAL bytes, undo before-images, chain/split/root IDs and storage,
peak installer pins/scratch and bookkeeping before a possible decision. Keep
protected completion/recovery capacity under foreground and analytical saturation.
Declare configuration/progress bounds and implement a coordinated slow path;
healthy admitted decisions cannot rely on unlimited `NoVictim`/allocation retries.
Move deterministic whole-batch refusal ahead of device entry.

Implement decision-ordered optimistic certification now, not as an adapter callback
in G: point/absence/range/gap, name/catalog/enforcement and lifecycle dependencies,
mandatory constraint observations, acceptance plus conflict-summary publication
under the same sequencer as CSN/append. Include earlier accepted but uninstalled
writers. Pin/budget summary history and refuse/expire abortable work explicitly
under pressure. The prefix exposed as SERIALIZABLE must consist of certified writers.

Start relational correctness in parallel at the maintained execution boundary:
replace executable Describe/type inference with one binder; introduce
transaction-owned failed state/private statement deltas and one retained catalog
view; funnel DDL through one schema transaction. Reuse these owners at cutover,
not a second binder for vNext. Security/integrity repairs need not wait for a new
physical row format. Preserve supported behavior with independent oracles; do not
patch obsolete structures merely to make their old tests green.

### 2. Replace record representation and complete installer progress

Implement ADR 0009's bounded physical key/fence/separator/value descriptors and
immutable object-local logical chains in a new experimental page format. Preserve
logical WAL/MVCC/undo identity. Derive codec/page-size bounds from actual layouts,
then choose splits from successfully built fenced candidates before balancing.
Maintain useful internal fanout and finish parent/root obligations before readiness.
Reserve sufficient work despite concurrent structural growth; no tree pins/latches
span durability waits or overflow resolution.

Use one logical admission policy across current values, future undo, original WAL
batch and decode memory. Current undo's 16 MiB encoded-record cap is a limiting
component, not a permanent general-purpose requirement; extend framing before
exposing larger support. Reject impossible configurations before decisions.

Qualification includes:

- accepted long/common-prefix keys and values through leaf/internal/root splits,
  replay, replacement and delete;
- the legitimate 180-byte replacement on a 384-byte page and its neighbors;
- separate real inline-growth split/dependency coverage when overflow eliminates
  a formerly incidental physical split;
- a genuine resource/allocation refusal preserving an old value, not shrinking
  test records to retain an obsolete oversize expectation;
- corrupt/missing/spliced chains, cached-prefix ordering and retained readers;
- allocation-free repeated retained completed effects, distinct from unfinished
  structural work and recovery restarted from prior authority.

### 3. Capture and recover complete authority — Milestone F

Close new committing mutations, drain admitted completion without starving it,
then capture the exact contiguous `(CSN, decision LSN)` prefix under structural,
freezing, GC and allocation quiescence. Include roots/object lifecycle, all
child/right and overflow references/successors, allocation high-water marks,
placement map, reachable undo/owner outcomes and retention floors.

Snapshot/lease registration and retention-floor capture share authority. Readers
may continue, but capture must not wait indefinitely for all pins. Implement stable
guarded image capture with bounded pin lifetimes. Consolidate nonquadratic strict
reference/map validation. Candidate publication binds store identity and satisfies
all file/directory/WAL/undo/image barriers while retaining prior complete authority.

Reopen the selected checkpoint plus synchronized retained committed WAL suffix
through the existing applicator. Publish visibility only after ordered replay and
reference validation. A page maximum LSN is never logical-redo completeness;
uncheckpointed spill never selects restart state.

Give recovery working allocations an explicit retry-space lifecycle. Disposable
working epochs or a qualified bounded policy must prevent repeated crashes from
unbounded undo/image growth and abandoned-image scanning. Reclamation cannot
truncate shared history or invalidate prior checkpoints/leases. Qualify candidate
failure, crash during recovery, two consecutive reopens and a subsequent normal
operation at every transaction/structural/publication boundary.

### 4. Bound retention, recovery and work

Checkpoint durable owner outcomes or safe freezing before WAL reclamation.
Reclaim only with explicit snapshot/catalog/object/CDC/replica/backup/prior-checkpoint
horizons. No ad-hoc undo front truncation, PageId reuse or chain reclamation.
Bound recovery memory versus checkpoint/log distance and retained history through
streaming/indexing and retention policy, not merely frame-at-a-time payload reads.

Cursor batches have row, byte and **physical-work** budgets, including invisible
slots, history traversal, chain reads and I/O. Preserve strict logical continuation,
snapshot/catalog/overlay identity and bounded guards across yields; no prefix rescans.

After design replacements are identified, inspect current CI and diagnose each
surviving failure. Retire obsolete implementation assertions with their deleted
paths; retain replacement protection for ownership, durability, concurrency and
SQL semantics. A green old-engine baseline is not a prerequisite to redesign.
Implementation delivery still requires the applicable repository gates and current
CI matrix, with failed/unrun checks reported accurately.

### 5. Measure the actual qualified vNext path

Before a passing performance claim, agree hardware, durability/isolation, datasets,
concurrency and throughput/latency/memory/checkpoint/recovery budgets. Do not choose
thresholds after seeing results. Product perf smoke is not an isolated-vNext benchmark.

Compare hot/cached and larger-than-memory workloads, uniform/skewed keys, 1/4/16+
committers, long snapshots, large records/transactions and concurrent checkpointing.
Report throughput/p50/p95/p99, CPU/cycles, allocations/residency, translation/latch/
conflict/admission waits, WAL/undo barrier count/latency, ready-to-visible wait,
history-read cost, logical/host/device writes, checkpoint pause/size and recovery
memory/time. Include LLC/TLB/page walks/bandwidth/NUMA where available.

Address measured architectural costs before cutover: serial WAL/undo barriers,
history metadata, certification retention/aborts, full-envelope overflow reads,
frontier head-of-line waiting and checkpoint pauses. Compare inline MVCC metadata
plus immutable payloads, resident recent history with bounded fallback, adaptive/
autonomous durability scheduling and coherent nonblocking checkpoints only with
a correctness proof for changed lifetimes/ordering. Translation hints, custom
latches, finer SMO, SIMD/JIT and device-specific placement are conditional experiments.

### 6. Canonical records and product integration — Milestone G

After crash-qualified kernel authority, implement schema-driven canonical rows and
compare clustered primary B-tree against row/heap pages plus primary index. Prove
an actual record access-method installation/lifetime/recovery participant, not
only a row codec in several B-trees. Secondary and constraint structures share
one decision/catalog authority.

The relational contract in ADR 0007 requires:

- binder-owned parameter/result types, effects, privileges and dependencies;
- retained catalog/layout/object views including enforcement membership and name
  resolution; stale writers cannot omit newly activated indexes/constraints;
- private statement deltas, transaction-owned failed-block lifecycle and fixed
  statement input views for multirow DML/cascades;
- statement-time immediate constraints, explicit deferred semantics, mutation-driven
  unique/FK probes and authoritative child-side lookup, all certified with decisions;
- current-grant checks at each Describe/execution boundary; revocation alone does
  not cancel work or reauthorize commit, but a later authorization error makes an
  explicit block rollback-only; prepared plans never act as grants;
- concurrent direct/wire execution through one bounded runtime without an outer
  database-wide lock; guarded typed rows and byte/work-bounded native batches;
- explicit offline preview conversion preserving supported logical rows/catalog/
  constraints/actions/timing or refusing input, without silently importing physical
  state or retaining a permanent second engine.

Qualify row/index/constraint/catalog activation and retirement under one durable
decision, retained readers and kill/reopen boundaries. Exercise write skew, absence/
phantom, disjoint-row unique values, child insert versus parent deletion, old writers
versus added constraints and incompatible DDL. Pause an earlier accepted decision
before install to prove the certifier does not rely on current pages.

Run typed relational, live PostgreSQL SQL/wire and retained SQLite differential
oracles, schema/constraint/conversion tests, process crashes and pgbench/TPC-B plus
YCSB/TPC-C-shaped workloads on the new path. Cut over only after semantic/crash
and agreed representative-performance gates; delete superseded engines, adapters,
flags, dependencies, fixtures and documentation in the same cutover scope.

## Format policy and later work

WAL/undo outer frames and MVCC envelope are currently v2; ordered pages are v4.
New overflow bytes need a new version. Unknown versions/kinds/flags, complete bad
checksums and invalid references fail closed. Only provably incomplete final
appends are repairable. Preview conversion is explicit; stable physical-format
upgrade compatibility is not promised.

After cutover, choose specialized text/vector/graph/JSON or HTAP work from product
value. Preserve exact snapshot/authorization fallback and retained representation
coverage, not merely an applied frontier. Canonical-row families/arenas, encoded
analytical chunks, prefetch/async I/O and placement follow measurements. Regional
HA precedes general distribution; object storage remains an explicit cold/archive/
disaggregated profile, not the mandatory local OLTP substrate.

This is the sole implementation sequence. Historical handoffs, research records
and ADR migration sketches do not create a parallel backlog or authorize a merge,
release, publication or deployment.
