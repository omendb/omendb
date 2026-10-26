# SeerDB implementation roadmap

**Maintained storage-kernel execution plan, 2026-10-25.** This plan owns
SeerDB's transaction, physical-storage and reusable-library work. It does
not duplicate the [OmenDB relational roadmap](../../../docs/roadmap.md),
which owns binder, SQL, catalog semantics and product cutover. The target
contract is [architecture](architecture.md); [qualification](qualification.md)
retains test procedures and scoped historical evidence.

The implementation under `src/vnext/` remains experimental. The serving
`DB` / `TransactionDatabase` generation-COW engine is not the intended
multi-writer kernel. Preserve useful failure oracles until the new path is
qualified, then remove superseded code and APIs rather than keeping a
permanent backend matrix. No test, benchmark or milestone is claimed complete
by this document.

## S1. One owned persistent runtime and recoverable decision

Integrate store incarnation/ownership, WAL append and durability, transaction
statuses, visibility frontier, resource admission, ordered objects, buffer
pool, page materialization and recovery into *one persistent constructor*.
Separate individual module constructors may remain internal test seams, not
public persistent databases. Remove optional composition of independent failure
boundaries from real entry points.

Prove atomic transactional writes across multiple logical ordered objects and
committed reads through the public SeerDB KV facade. Refuse deterministic
record/log/space infeasibility before irreversible device append, and own every
attempt from before possible append through completion or terminal fencing.
A cancellation, caught panic or uncertain I/O cannot lose installer resources
or silently abort an appended decision.

**Gate:** create, open, commit, crash, reopen and continue one multi-object
transaction on real storage through one runtime, initially replaying the
complete WAL from a safe genesis state without checkpoint truncation. Exercise
conflicting writers and ambiguous acknowledgements. Full checkpoint publication,
retained-history closure and reclamation belong to **S3**, not this early gate.

## S2. Correct concurrent MVCC and complete record representability

Build a bounded current-version/history model with logical version identities
and explicit leases. Decision-ordered serializable certification must cover
point/absence/gap/range and external caller-supplied catalog/constraint
dependencies against *accepted, not-yet-installed* writers. Retention pressure
cannot discard certification evidence and permit success. A distinct
READ COMMITTED statement-snapshot/wait-and-recheck mode is a later required
API, not a name for fixed-snapshot write conflict checking.

Implement bounded fully fenced B-link leaves/internal/root page layouts and
logical overflow so every admitted supported record can be installed and
replayed, including updates, deletes, long/common-prefix keys and history.
Prepare bodies outside the sequencer; keep ordered decision/append short.
Replace the two-barrier WAL+undo format only after checkpoint plus the complete
committed log can reconstruct retained predecessor history.

**Gate:** multi-writer isolation and mixed snapshot histories pass independent
serializability/constraint oracles; real post-decision failures cannot strand
uninstallable records or permanently lose an acknowledged version.

## S3. Bounded materialization, checkpoint, recovery and GC

Complete the durable, checksummed checkpoint graph (object roots, right links,
fences, overflow, page map, allocation high-water marks, outcomes and history).
A validated complete manifest plus committed logical WAL suffix is the sole
restart authority. Working spills/max page LSN alone do not prove replay
completeness. Preserve prior complete checkpoints on failed publication.

Remove known unbounded paths: copy short stable frame images before device I/O,
avoid frame byte latches during writeback, bound/incrementalize page mapping,
stream WAL replay with spill-capable bookkeeping, bound historical version
indexes and positional reads, and account for abandoned recovery allocations.
Respect active snapshot, CDC, backup and prior checkpoint floors. Use a
quiesced correct checkpoint as an initial proof but qualify incremental/epoch
capture against declared pause/size budgets before scale claims.

**Gate:** arbitrary cuts around log, history, image, checkpoint, recovery and
GC; two reopens and a new ordinary mutation; no foreign reference adoption,
stale resurrection, permanent lost versions, unbounded memory or space leak
under long readers, churn and ENOSPC. Complete snapshot/change bootstrap
has an atomically retained handoff, not just CSN/LSN numbers.

## S4. Performance and general-purpose library interface

Benchmark the actual persistent runtime and transaction lifecycle on matched
device-backed workloads: hot and bigger-than-RAM ordered KV; varied record
sizes, 1/4/16+ writers, skew, long snapshots, GC/checkpoint pressure and
steady churn. Compare RocksDB, Fjall and SurrealKV only at equivalent
KV semantics and durability; the OmenDB SQL benchmark belongs to OmenDB.
Measure p50/p95/p99, throughput, allocations, CPU, cache/TLB misses,
WAL barriers, write/space amplification, read I/O, recovery time and
full memory/retention overhead.

After core qualification, compare physical batch reads with per-key
operations and current cursors: exclusive key continuation, repeat prefix
descent, owned-value decoding/copying, snapshot/guard lifetimes, selection,
read bytes, cache locality and parallel writers. Only expose reusable
encoded-block metadata when a concrete independent storage consumer benefits;
SQL, BM25 and ANN operations must remain outside SeerDB.

Only then benchmark optimistic guards, inline/SIMD and BBTree-inspired search,
node/page sizing, batched seeks, predictive translation, placement hints,
autonomous NVMe writes, preconstructed invisible installation, LSM/PAX/heap
alternatives and special SSD features. Choose workload-specific mechanisms
with equivalent failure/lifetime proofs, not a blanket SOTA claim.

The kernel must compile and function as an independent Rust dependency,
without OmenDB types, schemas, SQL or server features. Accept generic
physical/storage primitives here; leave relational codecs, constraints,
catalog and query scheduling with OmenDB. Test standalone SeerDB and the
OmenDB integration surface independently.

A standalone Rust ordered transactional KV interface should be practical for
other projects, support explicitly documented transaction/snapshot outcomes,
batch/range access, durable backup/change APIs and useful resource settings,
and not require OmenDB SQL dependency or separately maintained commit code.

**Gate:** one kernel passes both standalone KV and OmenDB end-to-end
correctness/recovery/performance budgets. Any generalization that materially
hurts OmenDB must be redesigned or rejected.

## S5. Consumer cutover and post-core tiers

The [OmenDB roadmap](../../../docs/roadmap.md) owns transactional typed-row/
catalog/index integration, binder/executor migration, server semantics and
release qualification. SeerDB provides the complete authoritative storage
and snapshot/CDC boundary and removes legacy implementations after consumers
and old logical-data conversion are qualified.

Initially support S3-compatible backup, archived log/checkpoint and cold
immutable extents without remote dependency on local commit. Remote-primary
or disaggregated storage, regional quorum logging and global distributed
transactions are separate deployment protocols, not default kernel requirements.

Cross-cutting rule: do not close a gate from a codec/unit test, pre-existing
preview benchmark, or a design document. Source review and comparisons are
dated evidence; release gates demand actual persistent runtime behavior.
