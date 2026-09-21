# OmenDB

Relational database server in Rust, built on the shared transaction/storage
kernel in [SeerDB](crates/seerdb). OmenDB targets single-node OLTP first, with
PostgreSQL ecosystem integration through an experimental PostgreSQL wire
interface and a path to integrated search, analytics, HA, and distribution. A
direct Rust API is also available.

**Developer preview.** The server alpha is still in development. SQL coverage,
APIs, persistence formats, and supported platforms are subject to change.
See the [release gates](docs/alpha-release-gates.md) for the release contract.

## Run the server

From a checkout with Rust installed:

```sh
cargo run --features pgwire --bin omendbd -- \
  --path ./omendb-data --bind 127.0.0.1:5432
```

The daemon opens or creates the database and closes it on Ctrl-C. An empty
authentication catalog permits trust authentication on loopback only. Provision
SCRAM users before enabling authenticated access; see the [runbook](docs/runbook.md).
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
commit several statements together.

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

See the [architecture](docs/architecture.md), [operational runbook](docs/runbook.md),
and [benchmark documentation](docs/benchmarks.md) for design, recovery, and
measurement details.

## Development

```sh
cargo fmt --all -- --check
cargo test --all-features --all-targets
cargo clippy --all-features --all-targets -- -D warnings
```

OmenDB is [AGPL-3.0-only](LICENSE). SeerDB has its own license under
[`crates/seerdb`](crates/seerdb). Report vulnerabilities according to
[SECURITY.md](SECURITY.md).
