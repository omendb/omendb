-- pgbench differential schema, run identically on OmenDB and
-- PostgreSQL. Shape mirrors pgbench -i scale :scale:
--
--   pgbench_branches  :scale rows        (keyed bid)
--   pgbench_tellers   10 * :scale rows   (keyed tid)
--   pgbench_accounts  100000 * :scale rows (keyed aid)
--   pgbench_history   append-only log     (keyed client sequence)
--
-- Differences from stock pgbench DDL are stated here so the comparison
-- stays honest:
--   * every table declares its PRIMARY KEY inline (stock pgbench
--     backfills them with ALTER TABLE, which is not in the alpha
--     surface; the physical schema is the same after -i)
--   * pgbench_history gains a BIGINT history_id as client-numbered
--     sequence (:client_id * 2^40 + per-client counter). Stock
--     pgbench_history has no key at all; a keyed log table with the
--     same insert cost per row is the closest keyed shape.
--   * mtime is supplied by the script as a timestamp literal, not
--     CURRENT_TIMESTAMP, which is outside the alpha SQL surface.
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
    history_id BIGINT     PRIMARY KEY,
    tid        INT        NOT NULL,
    bid        INT        NOT NULL,
    aid        BIGINT     NOT NULL,
    delta      INT        NOT NULL,
    mtime      TIMESTAMP  NOT NULL,
    filler     CHAR(22)   NOT NULL
);
