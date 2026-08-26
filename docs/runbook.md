# OmenDB operator runbook (development preview)

This runbook covers the current operational surface of one `RelationalDatabase`
or `RelationalDatabaseSession` handle: health checks, integrity verification,
maintenance, and recovery after an ambiguous durable write. It is a
qualification aid for the unreleased development line, not the final
server-operations contract; behavior may change before the server-first alpha.

## Platform and filesystem assumptions

OmenDB's durability contract relies on standard POSIX durability semantics:
`fsync`/`fdatasync` on file descriptors plus directory-entry syncs after
artifact creation. It is tested on APFS (macOS) and ext4 (Linux) through CI
and local runs.

- Network filesystems (NFS, SMB), FUSE layers, and block devices that ignore
  sync requests void the crash contract; do not place a database directory
  there.
- Multiple processes must not share one database directory; SeerDB enforces
  a writer lock per directory and the Temporary backend expects exclusive
  ownership.
- Case-insensitive filesystems work, but artifact names are lowercase and
  fixed; renaming or hand-editing files inside the directory is unsupported
  and will be refused as corruption on open.

## Health checks

Session admission exposes `admission_status()` — active operations, writer
counts, admission waits, and rejections. `commit_id()` returns the visible
commit frontier (the SeerDB commit sequence number). There is no separate
lifecycle-status or diagnostic-report surface; a failed durable write fences
writes until reopen, and reopen failure surfaces as an open error.

## Routine maintenance

SeerDB owns reclamation: MVCC version-store GC and change-stream GC run
against retention leases inside the engine; there is no operator-facing
`checkpoint()`/`compact()` surface. Long-lived retention leases keep history
alive and block reclamation — release them when the consumer finishes.

## Recovery-required handling

An ambiguous publication (`RecoveryRequired`, or errors classified
`TransactionErrorClass::ReopenRequired`) leaves exactly two possible durable
outcomes: the transaction committed, or it did not. There is no partial
state. The reconciliation procedure is:

1. Stop issuing operations on the handle; close it (a close error still
   consumes the handle).
2. Reopen with the same `RelationalBackendConfig`.
3. Inspect application-visible state (for example, re-read the rows the
   transaction would have written) and reapply only if absent.

Transactions publish atomically through SeerDB's group-commit lane, so an
ambiguous outcome is exactly "committed" or "not committed"; application-level
idempotency keys are the caller's reconciliation tool.

## Backups

Copy the database directory only while no process holds it closed; SeerDB
enforces a writer lock per directory. Snapshot export through SeerDB's
`snapshot_export()` (`{CSN, restart LSN}`) identifies a consistent position
for logical exports; physical archive/restore is future work.

## Escalation

Collect for a bug report: the redacted support bundle, `diagnose()` output,
the exact error variant (all errors are typed, not strings), the last
successful operation class, and whether the directory can be preserved
unmodified for inspection.
