# PostgreSQL wire compatibility matrix

This is the current development evidence for OmenDB's deliberately bounded
PostgreSQL wire surface. It is a compatibility record, not a claim that OmenDB
implements PostgreSQL. The executable evidence is in
`tests/project_pgwire_server.rs`; run it with:

```bash
cargo test --features pgwire --test project_pgwire_server
```

## Server, startup, and lifecycle

| Surface | Current behavior | Evidence |
| --- | --- | --- |
| Persistent ownership | One `omendbd` process owns one durable database and listener; shutdown drains connections and workers before close. | `persistent_server_reopens_durable_database_after_shutdown`, `persistent_server_shutdown_cancels_query_before_database_close` |
| Process loss | A daemon killed with `SIGKILL` can be followed by durable reopen with committed data intact. | `omendbd_process_kill_reopens_durable_database` (Unix) |
| Connection admission | `ServerConfig::max_connections` is a hard bound; excess sockets are rejected and counted. | `persistent_server_rejects_connections_over_bound` |
| Trust startup | Empty auth catalogs use trust only on loopback. | `wire_trust_mode_refuses_non_loopback_listener` |
| SCRAM startup | Provisioned users authenticate with SCRAM-SHA-256; wrong and unknown credentials fail with `28P01`. Repeated failures receive bounded delay. | `wire_scram_auth_accepts_provisioned_user_and_rejects_bad_password`, `wire_auth_failure_delays_repeat_attempts` |
| Authorization | Provisioned grants distinguish read, write, and schema-admin access; ungranted tables deny access. | `wire_grant_enforcement_reader_writer_admin` |
| Diagnostics | `RunningServer::status()` reports connection admission and tracked query/describe worker outcomes. | `persistent_server_reopens_durable_database_after_shutdown`, `query_worker_tracker_records_terminal_operation_outcomes` (unit) |

## Wire protocol

| Surface | Current behavior | Evidence |
| --- | --- | --- |
| Startup and authentication | PostgreSQL startup packets use trust or SCRAM according to the durable auth catalog. | `wire_scram_auth_accepts_provisioned_user_and_rejects_bad_password`, `pgwire_sasl_initial_response_decode_probe` |
| Simple queries | Semicolon-separated statements, transaction control, and ordinary SQL execute through the wire handler. | `wire_transaction_block_rollback_discards_writes`, `wire_transaction_block_commit_persists_and_crosses_connections` |
| Extended queries | Parse, parameter type resolution, bind, describe, and execute are exercised through `tokio-postgres`; repeated parameterized execution is covered. | `wire_hot_statement_repeats_across_params`, all `Client::query`/`Client::execute` tests |
| Result encoding | Typed integers, text, NULL values, projections, aggregates, and `RETURNING` results are decoded by `tokio-postgres`. | `wire_client_selects_seeds_and_reads_typed_rows`, `wire_insert_returning`, `wire_update_delete_returning`, `wire_left_outer_join_null_extension` |
| Transactions | `BEGIN`/`START`, `COMMIT`/`END`, and `ROLLBACK`/`ABORT` map to connection-local transaction blocks. Failed blocks reject later statements with `25P02` until rollback. | `wire_transaction_block_commit_persists_and_crosses_connections`, `wire_aborted_block_rejects_until_rollback` |
| Cancellation | `CancelRequest` routes through the server-owned `(pid, secret)` registry to cooperative checkpoints and maps to `57014`. | `wire_cancel_request_aborts_lock_wait_before_publication`, `persistent_server_shutdown_cancels_query_before_database_close` |
| Clean statement failure | Errors do not tear down a usable connection; syntax errors map to `42601` and unsupported statements map to `0A000`. | `wire_client_gets_clean_errors_and_rejects_unsupported_sql` |

## SQL workload coverage

The wire suite exercises the supported bounded SQL overlap, including:

- single-table projection, aliases, arithmetic, parameters, `IN`, `BETWEEN`,
  `NULL`, `DISTINCT`, ordering, and pagination;
- inner, non-equi, cross, left, right, and full joins, including `USING` and
  `NATURAL JOIN`;
- scalar, `IN`, and `EXISTS` subqueries; grouping and aggregates; set
  operations; and schema-qualified public names;
- `INSERT`, `INSERT ... SELECT`, `UPDATE`, `DELETE`, `RETURNING`, `UPDATE ...
  FROM`, and `DELETE ... USING`; and
- schema publication and schema-qualified public names exercised against the
  seeded durable catalog.

The embedded SQL suite, rather than the wire suite, owns the differential
SQLite oracle. Wire tests currently prove OmenDB behavior and negative paths;
they do not constitute a differential run against a live PostgreSQL server.

## Explicit gaps

The following are outside the current compatibility claim and remain release
gates:

- SSL negotiation, COPY, replication, notifications, cursors, portals with
  incremental `max_rows`, and the wider PostgreSQL session-parameter surface;
- a live-PostgreSQL differential matrix for the documented subset;
- exhaustive SQLSTATE mapping (for example, an unknown SQL table currently
  reports a generic internal wire error rather than PostgreSQL's `42P01`);
- query-result memory, statement-time, and bytes-per-query quotas; and
- process-level kill/reopen coverage at each durable publication seam.
