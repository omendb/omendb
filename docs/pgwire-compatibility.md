# PostgreSQL wire compatibility matrix

This is the current development evidence for OmenDB's deliberately bounded
PostgreSQL wire surface. It is a compatibility record, not a claim that OmenDB
implements PostgreSQL. The ordinary behavior and negative-path evidence lives
in `tests/project_pgwire_server.rs` and `tests/project_transaction_controls.rs`:

```bash
cargo test --features pgwire --test project_pgwire_server --test project_transaction_controls
```

The [gap register](gap-register.md) qualifies this evidence. Integrity and
resource-boundary limitations remain open; the intended
[product architecture](architecture.md) is not a compatibility claim.

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
| Bootstrap startup | Fresh databases explicitly enable loopback-only bootstrap. Creating the first principal disables it; later connections use current native state and existing bootstrap connections lose future query permission. | `wire_trust_mode_refuses_non_loopback_listener`, `wire_live_bootstrap_transition_uses_native_default_deny` |
| SCRAM startup | Native credentials authenticate with SCRAM-SHA-256 and pin the principal incarnation used for proof. Startup errors are fatal; wrong and unknown credentials fail with `28P01`. Repeated failures receive bounded delay. | `wire_scram_auth_accepts_provisioned_user_and_rejects_bad_password`, `wire_auth_failure_delays_repeat_attempts`, `tests/project_pgwire_authorization.rs` |
| Authorization | Native grants bind principal and table incarnations; administration is explicit. Preparation, Describe, and every Execute check current committed authority, including nested access. Older data snapshots do not retain revoked grants; already-authorized commit and cleanup do not reauthorize. Missing authority fails closed. | `wire_grant_enforcement_reader_writer_admin`, `retained_describe_checks_revocation_and_aborts_staged_writes`, `tests/project_authorization.rs` |
| Diagnostics | `RunningServer::status()` reports connection admission and tracked query/describe worker outcomes. | `persistent_server_reopens_durable_database_after_shutdown`, `query_worker_tracker_records_terminal_operation_outcomes` (unit) |
| Slow statement log | `ServerConfig::slow_statement_threshold` / `--slow-statement-ms N` log statements (execution plus commit publication) above the threshold to stderr as one structured line: duration, authenticated user, single-line statement. Default off. | `omendbd_logs_slow_statements_when_threshold_set` |
| EXPLAIN | `EXPLAIN <SELECT/INSERT/UPDATE/DELETE>` reports the same access path the executor takes: primary-key lookup, index scan, NULL short-circuit, or seq scan. `EXPLAIN ANALYZE` is refused. Grant analysis recurses into the explained statement. | `explain_names_the_access_path_execution_takes`, `explain_reports_access_path_through_wire` |
| Session functions | `version()` answers `OmenDB <version> (on PostgreSQL wire protocol)`; `current_user()`, `current_schema()`, `current_database()` answer for capability detection. | `psql_session_matrix_matches_alpha_contract`, `tests/project_sql.rs` |
| SHOW | The compatibility GUCs clients probe: `server_version`, `server_encoding`, `client_encoding`, `transaction_isolation` (reports `serializable` — read-write transactions certify first-committer-wins with SQLSTATE 40001 aborts), `standard_conforming_strings`, `max_connections`, `search_path`. Others are refused with the supported list. | `psql_session_matrix_matches_alpha_contract` |
| Real client matrix | Interactive psql 17 runs the alpha contract: plain SQL in one session, capability detection, honest `feature not supported` failures for pg_catalog meta-commands (`\dt`, `\d`, `\l`) with the session staying usable, and explicit transaction blocks. | `tests/project_psql_session.rs` (self-skips without psql on PATH) |

## Wire protocol

| Surface | Current behavior | Evidence |
| --- | --- | --- |
| Startup and authentication | PostgreSQL startup packets use explicit bootstrap or SCRAM according to the native authorization catalog. | `wire_scram_auth_accepts_provisioned_user_and_rejects_bad_password`, `pgwire_sasl_initial_response_decode_probe` |
| Simple queries | Statement splitting respects literals, identifiers and comments. Multiple statements outside explicit blocks still commit individually, not as one implicit transaction. | `plain_controls_and_lexical_boundaries_are_preserved`, `wire_transaction_block_rollback_discards_writes` |
| Extended queries | Parse binds supported SQL without execution. Describe and execution share fixed parameter/result signatures. Catalog changes, including direct-API DDL, require reprepare (`0A000`). Incremental Execute row limits are refused (`0A000`). | `describe_binds_without_evaluating_rows_or_benign_parameter_values`, `prepared_signature_cannot_drift_after_direct_api_ddl`, `wire_portal_execute_checks_failed_blocks_and_refuses_incremental_fetch`, `wire_hot_statement_repeats_across_params` |
| Result encoding | Typed integers, text, NULL values, projections, aggregates, and `RETURNING` results are decoded by `tokio-postgres`. OmenDB's logical text type is described as PostgreSQL `TEXT`; all supported result signatures come from binding, not observed rows. | `wire_client_selects_seeds_and_reads_typed_rows`, `wire_insert_returning`, `wire_update_delete_returning`, `wire_left_outer_join_null_extension`, live PostgreSQL oracle |
| Transactions | Plain `BEGIN`/`START TRANSACTION`, `COMMIT`/`END`, and `ROLLBACK`/`ABORT` map to connection-local blocks. Complete controls are parsed: characteristics (including read-only/isolation), savepoints and chaining are rejected rather than ignored. Block errors and idle expiry retain a rollback-only boundary (`25P02`); COMMIT then reports ROLLBACK. Consumed commit errors report idle, not a phantom aborted block. | `tests/project_transaction_controls.rs`, `wire_grant_enforcement_reader_writer_admin`, `idle_expiry_releases_transactions_without_restoring_autocommit` |
| Cancellation | `CancelRequest` routes through the server-owned `(pid, secret)` registry to cooperative checkpoints and maps to `57014`. | `wire_cancel_request_aborts_lock_wait_before_publication`, `persistent_server_shutdown_cancels_query_before_database_close` |
| Clean statement failure | Errors do not tear down a usable connection; syntax errors map to `42601`, parameter-shape errors to protocol violation `08P01`, undefined tables to `42P01`, undefined columns to `42703`, datatype mismatches to `42804`, division by zero to `22012`, numeric overflow to `22003`, not-null violations to `23502`, and unsupported statements to `0A000`. | `wire_client_gets_clean_errors_and_rejects_unsupported_sql`, `sql_parameter_errors_use_protocol_violation_state` (unit) |

## Scalar value types

The typed scalars below ride the standard binary result and parameter
encodings. Each encoder was checked byte-for-byte against PostgreSQL `COPY BINARY`
probes; maintained typed-value tests and the live oracle compare decoded values
and type OIDs for a representative typed workload in every CI run.

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

## TLS stance

OmenDB does not terminate TLS. The server answers PostgreSQL `SSLRequest`
negotiation with 'N' (no SSL), which is the honest protocol response:

- `sslmode=require` fails immediately with `server does not support SSL`;
- `sslmode=prefer` (the libpq default) falls back to the cleartext session;
- `sslmode=disable` connects without negotiation.

Until TLS termination exists, deployment policy is: bind to loopback or a
private interface, or terminate TLS in front of the daemon (for example a
local `stunnel`/HAProxy sidecar). Trust mode is already restricted to
loopback listeners; a non-loopback bind requires provisioned SCRAM users
first. Recording a client's TLS rejection in the daemon log is open work.

## Explicit gaps

The following are outside the current compatibility claim and remain release
or later compatibility work:

- TLS termination (see the stance above), COPY, replication, notifications,
  cursors, portals with incremental `max_rows`, and the wider PostgreSQL
  session-parameter surface;
- `version()` and `current_user` answer generic values: `version()` names
  OmenDB honestly rather than spoofing PostgreSQL, and `current_user`
  reports `omendb` for trust-mode connections; SCRAM-authenticated roles
  need their real identity threaded into session functions;
- exhaustive SQLSTATE mapping; remaining execution-time `InvalidState` errors
  still report a generic internal wire error rather than their
  PostgreSQL-specific SQLSTATE;
- query-result execution-memory quotas; the result-payload bound is an
  estimated pre-encoding check and is not a full memory quota;
- full serving-path and replacement-kernel process-level kill/reopen coverage;
  existing SeerDB legacy publication tests do not qualify vNext;
- statement-time immediate FK semantics and action/timing-preserving conversion:
  legacy typed FK validation occurs at publication even when labelled immediate.

## Parameter and window semantics

Parameter and result signatures are fixed at preparation. Describe rechecks
current committed grants without evaluating queries or invented parameter values.
Any schema change—even unrelated direct-API DDL—invalidates prepared statements
other than transaction controls and requires reprepare (`0A000`). Parameters
whose types remain unresolved are refused with `42P18`.

Positional parameters are discovered from parsed SQL, so comments, string
literals, and quoted identifiers do not contribute parameter positions. The
maximum position is 65,535, matching the wire parameter-count field; invalid
or larger positions return a SQL error before allocating a type vector.

Window functions evaluate the full filtered partition before LIMIT/OFFSET.
Without window ORDER BY, the default frame is the whole partition. With
ORDER BY, aggregates and value functions use the prefix through the current
row's final ordering peer, following the
[PostgreSQL default frame](https://www.postgresql.org/docs/current/tutorial-window.html).
The statement's deterministic primary-key tie-breaker does not split window
peer groups. Explicit frames and named windows remain unsupported.

Regressions live in `tests/project_sql_parameters.rs`,
`tests/project_sql_windows.rs`, and the PostgreSQL wire and live-oracle suites.

At session capacity, the server still accepts cancellation through a separate
pool of at most eight readers. Each reader has a one-second deadline and
accepts only a fixed-size cancellation packet; ordinary startup cannot bypass
the session limit. Completed session and control tasks are reaped while serving.
