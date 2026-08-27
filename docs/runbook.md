# OmenDB operator runbook (development preview)

This runbook covers the current operational surface of one persistent
`omendbd` process and its `RelationalDatabase` handle: startup, lifecycle,
health checks, maintenance, and recovery after an ambiguous durable write. It
is a qualification aid for the unreleased development line, not a final
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

## Startup and lifecycle

Start the daemon with an explicit durable path:

```bash
cargo run --features pgwire --bin omendbd -- \
  --path ./omendb-data --bind 127.0.0.1:5432 --max-connections 128
```

The daemon creates a missing path by default, binds the listener, and keeps one
owning database handle for its lifetime. Ctrl-C requests shutdown; the accept
loop stops, admitted connections are cancelled, connection-local transaction
blocks are dropped, and the durable handle is closed before the process exits.
Call `RunningServer::shutdown` instead of dropping the handle when embedding the
server and the close result must be observed. A dropped running handle still
signals shutdown but cannot report a close error.

Empty `pgwire_auth` catalogs enable trust authentication only on loopback. A
provisioned user switches startup to SCRAM-SHA-256; provision users while the
database is closed, then restart the daemon. `--max-connections` bounds
connection tasks; rejected sockets are counted in `RunningServer::status()`.
PostgreSQL `CancelRequest` messages cancel the active database operation at
its cooperative checkpoints; synchronous query work runs in server-tracked
blocking workers so awaited shutdown drains it before closing the database.
Schema publication remains a non-interruptible operation and checks
cancellation before it starts.

## Health checks

An embedded `RunningServer` exposes `status()` with active, accepted, and
rejected connection counts, the configured admission bound, shutdown state,
and tracked query/describe operation counters. `active_operations` is the
current worker count; `completed_operations` counts terminal worker results;
`failed_operations` counts wire errors; and `cancelled_operations` counts
workers whose cancellation token was observed. The failed and cancelled
counters may overlap. A `RelationalDatabaseSession` exposes
`admission_status()` with active operations, writer counts, admission waits,
and rejections. `commit_id()` returns the visible commit frontier (the SeerDB
commit sequence number). A failed durable write fences writes until reopen,
and reopen failure surfaces as an open error.

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
