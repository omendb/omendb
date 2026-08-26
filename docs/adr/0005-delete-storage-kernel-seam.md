# ADR 0005: Delete the storage-kernel seam; DirectSeerStore becomes the backend

- **Status:** accepted; implemented
- **Scope:** OmenDB relational facade, SQL integration, kernel modules
- **Depends on:** [ADR 0004](0004-group-commit-publication-lane.md)

## Context

OmenDB's public relational facade (`RelationalDatabase`, `seer_relational.rs`,
`kernel.rs`, `seer_kernel.rs`, `temporary_kernel.rs`, `archive.rs`,
`session.rs`, `store.rs`; ~15k lines together) is architected around the
transitional `StorageKernel` seam: attempt records, prepared/certified
transactions, coalesced publication, retained historical leases, archive and
restore, and physical status projections. The direct qualification path
(`DirectSeerStore`) proves the same logical model over `TransactionDatabase`
with none of that machinery — SeerDB's own transaction pipeline subsumes it.

Nothing has shipped. There is no compatibility obligation in either
direction.

## Decision

Delete the seam rather than porting it. The target shape:

1. **`DirectSeerStore` is the only production backend.** It grows an explicit
   transactional surface (`DirectTransaction`: get/scan/index_get within one
   fixed snapshot, staged writes, one commit) so multi-statement SQL batches
   map onto single SeerDB transactions.
2. **Deleted:** `StorageKernel`, `SeerKernel`, `TemporaryKernel`, attempt
   records, coalesced publication plumbing, archive/restore, retained-snapshot
   capture, physical status projections, and `seer_relational.rs`. The
   temporary backend remains only if its differential-conformance role is
   preserved by instantiating tests against `DirectSeerStore` instead;
   otherwise it goes too.
3. **Kept:** catalog model, row encoding, index key codec, the SQL layer's
   parsing/planning (re-plumbed from attempts to `DirectTransaction`),
   capability reporting trimmed to what the backend actually supports.

## Staged execution (each stage lands green)

1. Grow `DirectSeerStore`'s transactional surface — **done** (`4c2844b`).
2. Re-plumb the facade onto SeerDB transactions; delete attempt machinery,
   coalescing, group-commit pipeline, parallel preparation, replication
   scaffolding, archive/restore, snapshot capture/leases, checkpoint/
   compaction/verify/diagnostics surfaces — **done** (`7114f66`).
3. Adapt integration tests and examples; delete feature-dead test files —
   **done** (`ea0ffa7`).
4. Documentation cleanup — **done** (this change).

## Consequences

- The facade loses features the product never promised anyone.
- SeerDB owns durability semantics end to end; there is exactly one
  publication path (group-commit lane).
- Historical leases and snapshot capture return later as first-class SeerDB
  features, not facade emulations.
