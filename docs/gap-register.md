# Known limitations of the OmenDB preview

**Source-reviewed through 2026-10-28; not release qualification.** The preview is
unsuitable for untrusted clients or irreplaceable data. Earlier component tests
and feature demonstrations do not establish correctness at the boundaries below.
The [compatibility matrix](pgwire-compatibility.md) describes tested overlap, not
complete PostgreSQL compatibility. [Architecture](architecture.md) describes the
target, not current implementation.

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
- Transactional DDL is unsupported. Send DDL alone in simple Query messages,
  outside explicit blocks; mixed or compound DDL messages are refused.
  Multi-table drops do not reorder dependencies; drop referencing tables first.
- SQL isolation alternatives are not selectable. Legacy serializability tests do
  not qualify the replacement's read/range/catalog certification.
- TLS is not terminated by OmenDB. Use loopback/private deployment or a TLS proxy;
  SCRAM does not encrypt subsequent traffic. See the [runbook](runbook.md).
- Session identity functions, broader metrics and ORM compatibility remain limited;
  tested clients and protocol gaps are listed in the compatibility matrix.
- No released relational product or qualified replacement runtime is claimed.
  Whole-engine durability, performance, packaging and recovery evidence remain
  required; a group-commit or component benchmark is not server qualification.
