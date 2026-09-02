# PostgreSQL wire compatibility matrix

This is the current development evidence for OmenDB's deliberately bounded
PostgreSQL wire surface. It is a compatibility record, not a claim that OmenDB
implements PostgreSQL. The ordinary behavior and negative-path evidence lives
in `tests/project_pgwire_server.rs`:

```bash
cargo test --features pgwire --test project_pgwire_server
```

A separate live differential in `tests/project_postgres_oracle.rs` compares a
representative documented subset against PostgreSQL. It is ignored in ordinary
local test runs because it requires a PostgreSQL server; CI runs it against
PostgreSQL 18.6 in the dedicated `postgres-oracle` job. To run it manually:

```bash
POSTGRES_ORACLE_URL='host=127.0.0.1 port=5432 user=postgres password=postgres dbname=postgres' \
  cargo test --features pgwire --test project_postgres_oracle -- --ignored --nocapture
```

## Server, startup, and lifecycle

| Surface | Current behavior | Evidence |
| --- | --- | --- |
| Persistent ownership | One `omendbd` process owns one durable database and listener; shutdown drains connections and workers before close. | `persistent_server_reopens_durable_database_after_shutdown`, `persistent_server_shutdown_cancels_query_before_database_close` |
| Process loss | A daemon killed with `SIGKILL` can be followed by durable reopen with committed data intact. | `omendbd_process_kill_reopens_durable_database` (Unix) |
| Connection admission | `ServerConfig::max_connections` is a hard bound; excess sockets are rejected and counted. | `persistent_server_rejects_connections_over_bound` |
| Statement deadline | `ServerConfig::statement_timeout` and `--statement-timeout-ms` apply a cooperative deadline to each statement and describe; expiry maps to `57014`. Schema publication remains non-interruptible after its preflight. | `persistent_server_statement_timeout_cancels_before_execution` |
| Result payload bound | `ServerConfig::max_result_bytes` and `--max-result-bytes` reject an estimated materialized result payload above the configured bound with `54000`. This limits response payload admission, not execution memory. | `persistent_server_rejects_result_over_byte_bound` |
| Trust startup | Empty auth catalogs use trust only on loopback. | `wire_trust_mode_refuses_non_loopback_listener` |
| SCRAM startup | Provisioned users authenticate with SCRAM-SHA-256; wrong and unknown credentials fail with `28P01`. Repeated failures receive bounded delay. | `wire_scram_auth_accepts_provisioned_user_and_rejects_bad_password`, `wire_auth_failure_delays_repeat_attempts` |
| Authorization | Provisioned grants distinguish read, write, and schema-admin access; ungranted tables, including tables referenced by nested subqueries, deny access. | `wire_grant_enforcement_reader_writer_admin` |
| Diagnostics | `RunningServer::status()` reports connection admission and tracked query/describe worker outcomes. | `persistent_server_reopens_durable_database_after_shutdown`, `query_worker_tracker_records_terminal_operation_outcomes` (unit) |

## Wire protocol

| Surface | Current behavior | Evidence |
| --- | --- | --- |
| Startup and authentication | PostgreSQL startup packets use trust or SCRAM according to the durable auth catalog. | `wire_scram_auth_accepts_provisioned_user_and_rejects_bad_password`, `pgwire_sasl_initial_response_decode_probe` |
| Simple queries | Semicolon-separated statements, transaction control, and ordinary SQL execute through the wire handler. | `wire_transaction_block_rollback_discards_writes`, `wire_transaction_block_commit_persists_and_crosses_connections` |
| Extended queries | Parse, parameter type resolution, bind, describe, and execute are exercised through `tokio-postgres`; repeated parameterized execution is covered. Parameter inference includes the supported `BETWEEN` form so bound values inherit the compared column's logical type. | `wire_hot_statement_repeats_across_params`, `sql_parameter_types_infer_between_bounds_from_column`, all `Client::query`/`Client::execute` tests |
| Result encoding | Typed integers, text, NULL values, projections, aggregates, and `RETURNING` results are decoded by `tokio-postgres`. OmenDB's logical text type is described as PostgreSQL `TEXT`; supported DML `RETURNING` uses static target-table metadata rather than depending on a probe row. | `wire_client_selects_seeds_and_reads_typed_rows`, `wire_insert_returning`, `wire_update_delete_returning`, `wire_left_outer_join_null_extension`, live PostgreSQL oracle |
| Transactions | `BEGIN`/`START`, `COMMIT`/`END`, and `ROLLBACK`/`ABORT` map to connection-local transaction blocks. Failed blocks reject later statements with `25P02` until rollback. | `wire_transaction_block_commit_persists_and_crosses_connections`, `wire_aborted_block_rejects_until_rollback` |
| Cancellation | `CancelRequest` routes through the server-owned `(pid, secret)` registry to cooperative checkpoints and maps to `57014`. | `wire_cancel_request_aborts_lock_wait_before_publication`, `persistent_server_shutdown_cancels_query_before_database_close` |
| Clean statement failure | Errors do not tear down a usable connection; syntax errors map to `42601`, parameter-shape errors to protocol violation `08P01`, undefined tables to `42P01`, undefined columns to `42703`, datatype mismatches to `42804`, division by zero to `22012`, numeric overflow to `22003`, not-null violations to `23502`, and unsupported statements to `0A000`. | `wire_client_gets_clean_errors_and_rejects_unsupported_sql`, `sql_parameter_errors_use_protocol_violation_state` (unit) |

## Scalar value types

The typed scalars below ride the standard binary result and parameter
encodings. Each encoder was pinned byte-for-byte against PostgreSQL `COPY
BINARY` probes (see `/tmp/pgprobe` methodology: probe first, then implement),
and the live oracle compares decoded values and type OIDs for a representative
typed workload in every CI run.

| Type | Storage | Wire encoding | Semantics | Evidence |
| --- | --- | --- | --- | --- |
| `FLOAT8` / `DOUBLE PRECISION` | IEEE-754 bits | PostgreSQL float8 binary; NaN/Infinity text forms | SQL float total order: NaN sorts greatest and equals itself, `-0 == 0` | `typed_scalar_subset_matches_live_postgres`, `project_typed_values.rs` |
| `DATE` | days since Unix epoch | i32 days since 2000-01-01 | Proleptic Gregorian, no timezone | `typed_scalar_subset_matches_live_postgres`, date parse/format unit tests |
| `TIMESTAMP` (without time zone) | microseconds since Unix epoch | i64 microseconds since 2000-01-01 | Microsecond precision, no timezone | `typed_scalar_subset_matches_live_postgres` |
| `NUMERIC` / `DECIMAL` | i128 mantissa + scale | PostgreSQL base-10000 groups, decimal-point-aligned weight, u16 dscale | Exact decimal arithmetic (PG divide rule: result scale = dscale + divisor digits + 4); stored rows keep dscale (`1.50` round-trips), equality keys normalize trailing zeros (`1.50 == 1.5`) | `typed_scalar_subset_matches_live_postgres`, `tests/project_typed_values.rs` |
| `UUID` | 128 raw bits | PostgreSQL uuid binary | Canonical hex text parse/format | `typed_scalar_subset_matches_live_postgres` |

Known divergence: `NUMERIC(p,s)` typmods are accepted in DDL but not stored
or enforced (values track their own scale, like unconstrained `NUMERIC`).

## SQL workload coverage

The wire suite exercises the supported bounded SQL overlap, including:

- single-table projection, aliases, arithmetic, parameters, `IN`, `BETWEEN`,
  `NULL`, `DISTINCT`, ordering, and pagination, including typed scalar
  comparisons, ranges, and aggregates over the typed columns;
- inner, non-equi, cross, left, right, and full joins, including `USING` and
  `NATURAL JOIN`;
- scalar, `IN`, and `EXISTS` subqueries; grouping and aggregates; set
  operations; and schema-qualified public names;
- `INSERT`, `INSERT ... SELECT`, `UPDATE`, `DELETE`, `RETURNING`, `UPDATE ...
  FROM`, and `DELETE ... USING`; and
- schema publication and schema-qualified public names exercised against the
  seeded durable catalog.

The embedded SQL suite owns the differential SQLite oracle for the bounded
engine-level overlap. The live PostgreSQL oracle independently exercises a
representative wire-level overlap and compares prepared-statement field names,
PostgreSQL type OIDs, decoded row values, and transaction-visible results. The
oracle is intentionally narrower than the ordinary pgwire suite: its purpose is
to catch semantic or metadata drift in behavior OmenDB already documents, not
to imply PostgreSQL-complete protocol or SQL coverage.

## Explicit gaps

The following are outside the current compatibility claim and remain release
or later compatibility work:

- SSL negotiation, COPY, replication, notifications, cursors, portals with
  incremental `max_rows`, and the wider PostgreSQL session-parameter surface;
- exhaustive SQLSTATE mapping; remaining execution-time `InvalidState` errors
  still report a generic internal wire error rather than their
  PostgreSQL-specific SQLSTATE;
- query-result execution-memory quotas; the result-payload bound is an
  estimated pre-encoding check and is not a full memory quota; and
- process-level kill/reopen coverage at each durable publication seam.
