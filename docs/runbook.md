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
  a writer lock per directory.
- Case-insensitive filesystems work, but artifact names are lowercase and
  fixed; renaming or hand-editing files inside the directory is unsupported
  and will be refused as corruption on open.

## Startup and lifecycle

Start the daemon with an explicit durable path:

```bash
cargo run --features pgwire --bin omendbd -- \
  --path ./omendb-data --bind 127.0.0.1:5432 --max-connections 128 \
  --statement-timeout-ms 30000
```

The daemon creates a missing path by default, binds the listener, and keeps one
owning database handle for its lifetime. Ctrl-C requests shutdown; the accept
loop stops, admitted connections are cancelled, connection-local transaction
blocks are dropped, and the durable handle is closed before the process exits.
Call `RunningServer::shutdown` instead of dropping the handle when embedding the
server and the close result must be observed. A dropped running handle still
signals shutdown but cannot report a close error.

New databases store an explicit loopback-only bootstrap capability. Creating the
first native principal disables it permanently: removing the last user or
credential does not restore trust. Each new connection selects its authentication
mode from the current committed catalog. Existing bootstrap connections lose
permission for later queries when bootstrap is disabled.

`--max-connections` bounds connection tasks; rejected sockets are counted in
`RunningServer::status()`.
`--statement-timeout-ms` applies a cooperative deadline to each statement and
`Describe`; zero rejects execution immediately. `--max-result-bytes` rejects
an estimated materialized result payload above its bound with SQLSTATE `54000`;
it does not bound execution-time memory. PostgreSQL `CancelRequest` messages
cancel the active database operation at its cooperative checkpoints;
synchronous query work runs in server-tracked blocking workers so awaited
shutdown drains it before closing the database. Schema publication remains a
non-interruptible operation and checks cancellation and the statement deadline
before it starts.

## Principals and grants

The native catalog owns principals, opaque credentials, table grants and an
explicit database-administration capability. A new principal has no privileges.
Write access includes reads, but does not permit DDL. A table named `*` is not an
administration grant. SQL `CREATE ROLE` and `GRANT` are not implemented; use the
trusted Rust API. There is no provisioning CLI.

For a persistent database, stop the daemon and open its existing path from a
trusted Rust program built with the `pgwire` feature. For example, provision an
operator with explicit administration:

```rust
use omendb::{RelationalBackendConfig, RelationalDatabase, pgwire_server};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut database =
        RelationalDatabase::open(RelationalBackendConfig::new("./omendb-data"))?;
    let password = std::env::var("OMENDB_PASSWORD")?;
    pgwire_server::provision_wire_user(&mut database, "operator", &password)?;
    pgwire_server::provision_wire_admin(&mut database, "operator", true)?;
    database.close()?;
    Ok(())
}
```

For table-only access, use
`provision_wire_grant(&mut database, "app", "accounts", true, false)` after creating
that principal and table. Password rotation preserves the principal's identity
and grants. The transaction APIs can publish principal, credential, grant and row
changes together. Direct native database access is a privileged host interface;
do not expose arbitrary native operations to untrusted callers.

Grants follow principal and table incarnations: rename preserves them, but
reusing a dropped principal name or table name/ID does not transfer access.
Describe and execution check current committed authorization even inside an
older data transaction. Revocation does not cancel already-admitted work or
reauthorize its commit. A later authorization error aborts the block's staged
changes; rollback and failed-block cleanup remain available.

Histories created before native authorization remain readable through the
trusted native API, but serving refuses missing authorization state. The old
`pgwire_auth` and `pgwire_grants` tables are not imported: their name-based grants
cannot identify the intended object incarnations safely. Inspect the intended
policy, explicitly remove those legacy tables through `apply_schema_mutations`,
then call `initialize_authorization()` on an uninitialized history and reprovision
credentials and grants. Initialization refuses existing or malformed state and
never silently deletes legacy tables. Close the native handle before restarting
the daemon. Keep the database on loopback until provisioning is complete.

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
commit sequence number). The current hard wire-server quotas are admitted
connection count, cooperative statement time, and estimated result payload
bytes. Query execution-memory quotas are not yet implemented. A failed durable
write fences writes until reopen, and reopen failure surfaces as an open error.

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
3. Reconcile using application-owned idempotency evidence written atomically
   with the operation. Re-reading ordinary rows is not generally proof of the
   outcome: later updates/deletes can hide a committed effect. Do not retry an
   unresolved operation merely because an expected row is absent.

Transactions publish atomically through SeerDB's group-commit lane, but a lost
acknowledgement does not tell the caller which outcome occurred. The preview has
no general reconnect-safe outcome token; application-level idempotency and
explicit reconciliation remain the caller's responsibility.

## Backups

Logical dump and restore are the supported path (`omendb-tool`):

```bash
omendb-tool dump --path ./omendb-data > backup.sql
omendb-tool restore --path ./restored-db --input backup.sql
```

The dump renders one read-consistent snapshot as plain SQL: tables with
inline primary keys, data as multi-row INSERTs in scan order, secondary
indexes after data, foreign keys last as `ALTER TABLE ADD CONSTRAINT`.
It restores into OmenDB and into PostgreSQL (within the documented
divergences in `docs/gap-register.md`), so the same tool verifies a
backup by differential restore. Native principals, credentials and grants are
not included in logical dumps; explicitly reprovision authorization on a restored
database before serving it. Restore is not resumable: it stops at
the first error and earlier committed chunks remain — drop and re-run
into a fresh directory after correcting the script. U64 columns dump as
`NUMERIC(20,0)` (PostgreSQL has no unsigned 64-bit integer).

Physical copies of the database directory require no process holding it
open; SeerDB enforces a writer lock per directory. Snapshot export
through SeerDB's `snapshot_export()` (`{CSN, restart LSN}`) identifies a
consistent position for logical exports; physical archive/restore is
future work.

## Escalation

Collect for a bug report: the redacted support bundle, `diagnose()` output,
the exact error variant (all errors are typed, not strings), the last
successful operation class, and whether the directory can be preserved
unmodified for inspection.
