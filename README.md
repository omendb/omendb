# OmenDB

Native, server-first relational database in Rust, built on a shared
[SeerDB](crates/seerdb) transaction/storage kernel. The goal is a general-purpose
alternative to PostgreSQL/MySQL rather than a PostgreSQL implementation. The
current developer preview exposes a bounded PostgreSQL-wire adapter and Rust
API; these do not define the native engine's long-term SQL or storage semantics.
Specialized search/analytics and Neki-class horizontal scale are later goals.
See the [OmenDB architecture](docs/architecture.md) and
[SeerDB kernel boundaries](crates/seerdb/docs/architecture.md) for product direction,
not implementation claims.

**Developer preview.** The server alpha is still in development. SQL coverage,
APIs, persistence formats, and supported platforms are subject to change.
Known integrity and resource-boundary gaps make the current preview
unsuitable for untrusted clients or irreplaceable data. See the
[known limitations](docs/gap-register.md).

## Run the server

From a checkout with Rust installed:

```sh
cargo run --features pgwire --bin omendbd -- \
  --path ./omendb-data --bind 127.0.0.1:5432
```

The daemon opens or creates the database and closes it on Ctrl-C. New databases
have an explicit loopback-only bootstrap capability. Creating the first native
principal disables bootstrap; deleting users does not restore it. SCRAM users
need explicit table grants or database administration. See the
[runbook](docs/runbook.md) for provisioning and legacy-database initialization.
The server does not terminate TLS.

The current interface supports a bounded SQL subset, transaction blocks,
multiple client sessions, cancellation, and configurable connection, statement,
and result limits. PostgreSQL wire support does not imply full PostgreSQL SQL
or client compatibility. Check the [compatibility matrix](docs/pgwire-compatibility.md)
and [SQL gap register](docs/gap-register.md) before integrating a client.

## Use from Rust

OmenDB is not currently published to crates.io. Use a Git dependency or this
workspace to access the direct API:

```rust
use omendb::{RelationalBackendConfig, RelationalDatabase};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut database =
        RelationalDatabase::create(RelationalBackendConfig::new("./omendb-data"))?;
    database.execute_sql(
        "CREATE TABLE accounts (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    )?;
    database.execute_sql("INSERT INTO accounts VALUES (1, 'Alice'), (2, 'Bob')")?;
    let result = database.execute_sql("SELECT id, name FROM accounts ORDER BY id")?;
    println!("{:?}", result.rows);
    database.close()?;
    Ok(())
}
```

`create` requires a directory that does not already exist. Each direct SQL
write is one transaction; use `execute_sql_batch` or a typed transaction to
commit several statements together. A failed row, SQL, or analytical operation
aborts the entire typed transaction, even if its error is caught. Later operations
and `commit()` return `DbError::TransactionAborted`; begin a new transaction to
continue. Statement-level recovery and savepoints are not supported.

Typed transactions retain their catalog alongside their row/index snapshot;
`transaction.catalog()` exposes that layout even after later DDL. Old writers
conflict with schema publication rather than omitting new constraints or indexes.
SQL inside an existing transaction conservatively rejects catalog-generation
changes; start a new transaction.

Float equality keys normalize signed zeros and all NaN payloads while stored rows
retain their original bits. Affected legacy histories require explicit conversion;
see the [format upgrade procedure](docs/runbook.md#equality-key-format-upgrades).

## Storage and tools

OmenDB owns SQL, catalog and relational semantics plus the physical meaning of
rows and specialized access paths. SeerDB is the shared transaction/storage
kernel: transaction state, MVCC, durability, recovery, buffer/page management,
and physical lifetime services. Its ordered-KV surface is one access
method/facade, not OmenDB's universal storage model. Both are developed in this
workspace; SeerDB remains Apache-2.0 and may remain independently publishable
where that packaging boundary is useful. Shared filesystem durability primitives
live in the Apache-2.0 crate [`durable-fs`](crates/durable-fs).

Logical SQL dump and restore are available through the bundled tool:

```sh
cargo run --bin omendb-tool -- dump --path ./omendb-data > backup.sql
cargo run --bin omendb-tool -- restore --path ./fresh-db --input backup.sql
```

Dumps preserve supported foreign-key delete actions and refuse constraint timing
that SQL restore cannot represent. Restore creates a missing destination or adds
to an existing one; use a fresh directory for a complete backup.

See the [architecture](docs/architecture.md), [operational runbook](docs/runbook.md),
and [kernel qualification procedures](crates/seerdb/docs/qualification.md) for
design, recovery and available checks.

## Development

```sh
cargo fmt --all -- --check
cargo test --all-features --all-targets
cargo clippy --all-features --all-targets -- -D warnings
```

OmenDB is [AGPL-3.0-only](LICENSE). SeerDB has its own license under
[`crates/seerdb`](crates/seerdb). Report vulnerabilities according to
[SECURITY.md](SECURITY.md).
