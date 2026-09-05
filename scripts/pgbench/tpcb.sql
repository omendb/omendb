-- pgbench differential TPC-B script, run identically on OmenDB and
-- PostgreSQL via pgbench -f. One transaction per execution: the same
-- five statements, in the same order, as the stock builtin:
--
--   1. SELECT abalance FROM pgbench_accounts WHERE aid = :aid
--   2. UPDATE pgbench_tellers SET tbalance = tbalance + :delta
--        WHERE tid = :tid
--   3. UPDATE pgbench_accounts SET abalance = abalance + :delta
--        WHERE aid = :aid
--   4. UPDATE pgbench_branches SET bbalance = bbalance + :delta
--        WHERE bid = :bid
--   5. INSERT INTO pgbench_history (tid, bid, aid, delta, mtime, filler)
--
-- pgbench_history is unkeyed (the stock shape, now supported by
-- OmenDB's heap tables), so no client-side sequence is needed and the
-- script can run any number of times against one database.
--
-- Variables (pgbench metacommands run client-side and never touch the
-- engine):
--   :scale       from -D, matches schema scale
--   :mtime       from -D, a fixed timestamp literal
--   :aid :tid :bid :delta random per execution, exactly like builtin
--                TPC-B

\set aid random(1, 100000 * :scale)
\set bid random(1, 1 * :scale)
\set tid random(1, 10 * :scale)
\set delta random(-5000, 5000)

BEGIN;

SELECT abalance FROM pgbench_accounts WHERE aid = :aid;

UPDATE pgbench_tellers
   SET tbalance = tbalance + :delta
 WHERE tid = :tid;

UPDATE pgbench_accounts
   SET abalance = abalance + :delta
 WHERE aid = :aid;

UPDATE pgbench_branches
   SET bbalance = bbalance + :delta
 WHERE bid = :bid;

INSERT INTO pgbench_history
       (tid, bid, aid, delta, mtime, filler)
VALUES (:tid, :bid, :aid, :delta, :mtime, '');

COMMIT;
