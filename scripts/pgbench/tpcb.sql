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
--   5. INSERT INTO pgbench_history VALUES (...)
--
-- Variables (pgbench metacommands run client-side and never touch the
-- engine):
--   :client_id   builtin, unique per pgbench client
--   :scale       from -D, matches schema scale
--   :mtime       from -D, a fixed timestamp literal
--   :hid         per-client counter, starts at 0 from -D, incremented
--                each execution; history_id = :hid * 2^40 + :client_id
--                keeps client streams unique (each client's iteration
--                count maps to its own 2^40 range, seeded by client
--                id, so -D hid=0 initializes every client the same)
--   :aid :tid :bid :delta random per execution, exactly like builtin
--                TPC-B

\set hid :hid + 1
\set history_id :hid * 1099511627776 + :client_id
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
       (history_id, tid, bid, aid, delta, mtime, filler)
VALUES (:history_id, :tid, :bid, :aid, :delta, :mtime, '');

COMMIT;
