# Known limitations of the OmenDB preview

**Source-reviewed through 2026-10-28; not release qualification.** The preview is
unsuitable for untrusted clients or irreplaceable data. Earlier component tests
and feature demonstrations do not establish correctness at the boundaries below.
The [compatibility matrix](pgwire-compatibility.md) describes tested overlap, not
complete PostgreSQL compatibility. [Architecture](architecture.md) describes the
target, not current implementation.

## Open integrity and authorization boundaries

These concern the serving relational path, independently of the experimental
storage replacement. Fixes must preserve independent semantic/security oracles,
not treat current erroneous output as the contract.

| Boundary | Known limitation | Source |
| --- | --- | --- |
| Authorization | Describe executes protected queries without grant enforcement. Name-keyed grants can transfer across rename/recreate; text-based access collection is incomplete. | `src/pgwire_server/{auth,handler}.rs`, `src/sql.rs::statement_access` |
| Prepared statements | Parameter/result signatures can be inferred again from live catalog/probes; token-based invalidation misses direct-API DDL. | `src/pgwire_server/handler.rs::{resolved_parameter_types,describe_schema}` |
| Schema publication | Typed FK creation validates and publishes in separate transactions. | `src/seer_direct.rs::create_foreign_key_with_name` |
| Catalog snapshots | Storage captures a catalog while some binding/identity paths consult the live catalog, permitting mixed-generation behavior after concurrent DDL. | `src/sql_query.rs`, relational database and direct storage adapter |
| Index equality | Typed negative and positive float zero can occupy separate unique-index entries despite scalar equality; indexed lookup can omit an equal row. Persisted indexes require explicit conversion/rebuild or refusal. | `src/relational.rs::Value::encode_for_key`, `src/sql_types.rs::F64` |
| Wire statement lists | Multiple statements outside explicit blocks commit individually rather than sharing the simple message's implicit transaction boundary. Lexical splitting now respects quoted/commented semicolons. | `src/pgwire_server/handler.rs::do_query` |
| Logical conversion | Dump/restore omits supported FK delete actions, converting Cascade to Restrict. | `src/sql_dump.rs`, `src/sql_schema.rs` |
| Constraint timing | Immediate and deferred typed FKs both validate at publication; advertised statement-time FK behavior is unqualified. | `src/relational.rs::ConstraintTiming`, `src/seer_direct.rs::validate_referential_integrity` |

## Resource and execution limits

Connection limits, cooperative deadlines and estimated result-payload checks are
not execution-memory quotas or tenant isolation. Public `serve()` does not apply
the configured persistent server's connection bound. Queries use blocking workers
and database-wide locking rather than the experimental resource governor.

Scans materialize whole tables before truncation; analytical morsels repeatedly
scan increasing prefixes. SQL aggregation separately materializes rows and grows
unaccounted group state. Cancellation checks above these operations cannot bound
the work already performed. Result limits are checked after materialization.
Sources: `src/relational_database.rs::scan`, `src/morsel.rs::next_morsel`,
`src/sql_query.rs::execute_aggregate_query`, `src/pgwire_server/{lifecycle,encoding}.rs`.

The standalone governor's `TerminateLargest` policy can release in-flight accounting
before the associated reactor dispatch completes. It is not the live server's
admission authority and must not be treated as a hard resource bound.

## Supported-subset and operational gaps

- `NUMERIC(p,s)` modifiers are not enforced. U64 dumps as `NUMERIC(20,0)` for
  PostgreSQL, which has no matching unsigned integer type.
- SET NOT NULL, ALTER TYPE on constrained columns and appended non-null defaults
  lack qualified schema-evolution support.
- GROUPING SETS, explicit/named window frames, `nth_value`, explicit `lag`/`lead`
  offsets and general index expressions are outside the supported subset;
  expression indexes currently cover arithmetic.
- SQL isolation alternatives are not selectable. Legacy serializability tests do
  not qualify the replacement's read/range/catalog certification.
- TLS is not terminated by OmenDB. Use loopback/private deployment or a TLS proxy;
  SCRAM does not encrypt subsequent traffic. See the [runbook](runbook.md).
- Session identity functions, broader metrics and ORM compatibility remain limited;
  tested clients and protocol gaps are listed in the compatibility matrix.
- No released relational product or qualified replacement runtime is claimed.
  Whole-engine durability, performance, packaging and recovery evidence remain
  required; a group-commit or component benchmark is not server qualification.
