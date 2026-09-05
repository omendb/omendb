-- pgbench differential schema, run identically on OmenDB and
-- PostgreSQL. Shape mirrors pgbench -i scale :scale:
--
--   pgbench_branches  :scale rows        (keyed bid)
--   pgbench_tellers   10 * :scale rows   (keyed tid)
--   pgbench_accounts  100000 * :scale rows (keyed aid)
--   pgbench_history   append-only heap log (no key, exactly like stock)
--
-- Differences from stock pgbench DDL are stated here so the comparison
-- stays honest:
--   * keyed tables declare their PRIMARY KEY inline (stock pgbench
--     backfills them with ALTER TABLE, which is not in the alpha
--     surface; the physical schema is the same after -i)
--   * pgbench_history is the stock shape: no key at all. OmenDB now
--     supports heap tables, so the earlier keyed-substitute
--     (client-numbered BIGINT history_id) is gone; both engines run
--     the identical unkeyed INSERT.
--   * mtime is supplied by the script as a timestamp literal, not
--     CURRENT_TIMESTAMP: identical statements on both engines is the
--     controlled comparison; clock values would only add skew between
--     engines.
--
-- :scale is substituted by psql -v scale=N.

DROP TABLE IF EXISTS pgbench_history, pgbench_tellers, pgbench_branches, pgbench_accounts;

CREATE TABLE pgbench_branches (
    bid      INT         PRIMARY KEY,
    bbalance INT         NOT NULL,
    filler   CHAR(88)    NOT NULL
);

CREATE TABLE pgbench_tellers (
    tid      INT         PRIMARY KEY,
    bid      INT         NOT NULL,
    tbalance INT         NOT NULL,
    filler   CHAR(84)    NOT NULL
);

CREATE TABLE pgbench_accounts (
    aid      BIGINT      PRIMARY KEY,
    bid      INT         NOT NULL,
    abalance INT         NOT NULL,
    filler   CHAR(84)    NOT NULL
);

CREATE TABLE pgbench_history (
    tid        INT        NOT NULL,
    bid        INT        NOT NULL,
    aid        BIGINT     NOT NULL,
    delta      INT        NOT NULL,
    mtime      TIMESTAMP  NOT NULL,
    filler     CHAR(22)   NOT NULL
);
