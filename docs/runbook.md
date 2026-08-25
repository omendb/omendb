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

`status()` returns backend-neutral lifecycle state without I/O:

- `state: Ready` — the handle accepts ordinary work.
- `state: RecoveryRequired` — an earlier durable outcome is ambiguous. Close
  and reopen before relying on or extending this handle's state.
- `commit` — the visible commit frontier.
- `pending_mutations` — journaled mutations not yet in a published
  generation. Non-zero means work exists beyond the last published
  checkpoint; it is normal between publications and drains on the next
  commit or `checkpoint()`.
- `write_fenced` — true means the writer refused further publications after
  an error; reopen is mandatory before more writes.

`diagnose()` correlates status, metrics, storage identity, and findings into
one report. Findings map to actions through
`RelationalDiagnosticCode::message()` and `recommended_action()`; treat
`Error`-severity findings as stop conditions for the current handle.

`support_bundle()` returns a redacted, bounded event snapshot for bug
reports. It performs no verification and never contains row data.

## Integrity verification

`verify()` runs a read-only integrity pass and returns logical counts
(tables, indexes, rows, index entries). SeerDB backends additionally report
physical pages, data/blob bytes, and WAL bytes. Run it:

- after reopening following a crash or ambiguous error;
- before taking a snapshot/archive;
- periodically as a scheduled check — it does not repair anything.

A failing verify is corruption evidence: stop using the handle, preserve the
directory as-is, and recover from a verified archive
(`SeerKernel::snapshot` output) if one exists. Do not delete files from the
database directory to "clean up".

## Routine maintenance

- `checkpoint()` publishes the current state through the backend checkpoint
  protocol and reports before/after frontiers. Use it after large import
  batches to bound recovery time; it is idempotent at an unchanged frontier.
- `compact()` reclaims dead versions/pages. On SeerDB it reports reclaimed
  physical pages; on Temporary it reports reclaimed row/index fragments.
  Compaction respects active readers; retained snapshots pin their history
  until released via `release(lease)`.

Retention discipline matters: every `retain`/`retain_current` lease keeps its
snapshot's history alive and blocks reclamation of that root. Release leases
when the read completes; long-lived leases are the most common cause of
unbounded growth.

## Recovery-required handling

An ambiguous publication (`RecoveryRequired`, or errors classified
`TransactionErrorClass::ReopenRequired`) leaves exactly two possible durable
outcomes: the transaction committed, or it did not. There is no partial
state. The reconciliation procedure is:

1. Stop issuing operations on the handle; close it (a close error still
   consumes the handle).
2. Reopen with the same `RelationalBackendConfig`.
3. Check application-level effects: for attempt-aware transactions call
   `resolve_attempt(attempt)` — `Some` means published (do not rerun), `None`
   means not published (safe to retry with a fresh attempt).
4. For plain transactions, inspect application-visible state (for example,
   re-read the rows the transaction would have written) and reapply only if
   absent.
5. Run `verify()` before resuming ordinary traffic.

Never retry a possibly-committed transaction under the same attempt identity
without resolving first; idempotency records exist precisely to make that
check cheap and definitive.

## Backups

Use `SeerKernel::snapshot(archive_path)` (verified, immutable archive) rather
than copying live files. Restore with `SeerKernel::restore` into a fresh
directory; a failed restore never creates the destination. Copying a running
database directory is not supported.

## Escalation

Collect for a bug report: the redacted support bundle, `diagnose()` output,
the exact error variant (all errors are typed, not strings), the last
successful operation class, and whether the directory can be preserved
unmodified for inspection.
