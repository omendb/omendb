#!/usr/bin/env bash
# Same-hardware pgbench differential: OmenDB (pgwire) vs PostgreSQL.
#
# The alpha gates require release evidence in the form of a reproducible
# PostgreSQL-class comparison on the supported workload overlap. pgbench
# is PostgreSQL's own TPC-B-shaped workload: a point SELECT, two point
# UPDATEs, a point UPDATE by secondary key, and an INSERT, wrapped in an
# explicit transaction, executed through the real PostgreSQL wire
# protocol.
#
# The stock pgbench schema needs two capabilities outside the alpha SQL
# surface (tables without any PRIMARY KEY, and the CURRENT_TIMESTAMP
# function). Rather than skewing the comparison, this differential runs
# the SAME custom schema and the SAME custom script file on both
# engines: the history log table gets a client-sequence BIGINT key
# (pgbench \set variables persist per client, so each client numbers its
# own inserts 1, 2, 3, ...; the key prefix keeps client streams unique),
# and mtime is a script-side parameter. Every statement below is
# wire-identical on both engines; the schema and script live next to
# this script so the run is reproducible from source.
#
# Usage:
#   scripts/pgbench/differential.sh [scale] [clients] [duration]
#
# Environment:
#   OMENDB_BIN      omendbd binary (default: cargo build output)
#   PGBENCH         pgbench binary (default: postgresql@17 keg)
#   POSTGRES_URL    libpq connection string for the live PostgreSQL
#                   (default: local socket, current user, db postgres)

set -euo pipefail

SCALE="${1:-1}"
CLIENTS="${2:-4}"
DURATION="${3:-30}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST="127.0.0.1"
PORT="${OMENDB_PORT:-15432}"
DATA="$(mktemp -d)/omen-pgbench"
OMENDB_BIN="${OMENDB_BIN:-$(cargo metadata --format=gnu --no-deps 2>/dev/null \
  | tr ' ' '\n' | grep -m1 target/release/omendbd || echo target/release/omendbd)}"
PGBENCH="${PGBENCH:-/opt/homebrew/opt/postgresql@17/bin/pgbench}"
PSQL="${PSQL:-/opt/homebrew/opt/postgresql@17/bin/psql}"
POSTGRES_URL="${POSTGRES_URL:-host=$HOST user=$USER dbname=postgres}"
# Seed mtime as a script variable: identical literals on both engines.
MTIME="'1970-01-01 00:00:00'"

command -v "$PGBENCH" >/dev/null || { echo "pgbench not found at $PGBENCH" >&2; exit 1; }
command -v "$OMENDB_BIN" >/dev/null || { echo "omendbd not found at $OMENDB_BIN" >&2; exit 1; }
[ -f "$SCRIPT_DIR/schema.sql" ] || { echo "schema.sql not found" >&2; exit 1; }
[ -f "$SCRIPT_DIR/tpcb.sql" ] || { echo "tpcb.sql not found" >&2; exit 1; }

echo "== pgbench differential: scale=$SCALE clients=$CLIENTS duration=${DURATION}s =="
echo "omendbd: $OMENDB_BIN"
echo "pgbench: $PGBENCH"
echo

# ---------------------------------------------------------------------------
# 1. OmenDB: start the daemon, initialize the schema, seed, run.
# ---------------------------------------------------------------------------
# omendbd creates the database directory itself; pre-creating it would
# make the daemon try to open an empty (corrupt-looking) database.
# OMENDB_ARGS adds daemon flags, e.g. OMENDB_ARGS=--wal-first to measure
# WAL-first commit acknowledgement.
"$OMENDB_BIN" --path "$DATA" --bind "$HOST:$PORT" ${OMENDB_ARGS:-} &
OMENDAEMON=$!
trap 'kill $OMENDAEMON 2>/dev/null || true; rm -rf "$(dirname "$DATA")"; rm -f "${SEED:-}"' EXIT

for _ in $(seq 1 50); do
  "$PSQL" -h "$HOST" -p "$PORT" -U omendb -d omendb -c "SELECT 1" >/dev/null 2>&1 && break
  sleep 0.2
done

echo "-- OmenDB: initialize (scale $SCALE) --"
"$PSQL" -h "$HOST" -p "$PORT" -U omendb -d omendb \
  -v "scale=$SCALE" -v "ON_ERROR_STOP=1" -f "$SCRIPT_DIR/schema.sql" >/dev/null

# Seed: pgbench's own generators use COPY (client-side g) or
# generate_series (server-side G), both outside the supported surface.
# Generate the same rows as multi-row INSERTs and run the identical file
# on both engines: branches 1..scale, tellers 1..10*scale
# (bid = (tid-1)/10 + 1), accounts 1..100000*scale
# (bid = (aid-1)/100000 + 1), balances 0, empty fillers.
SEED="$(mktemp -t pgbench-seed).sql"
awk -v scale="$SCALE" 'BEGIN {
  q = "\047\047";
  print "INSERT INTO pgbench_branches (bid, bbalance, filler) VALUES";
  for (i = 1; i <= scale; i++)
    printf "(%d, 0, %s)%s\n", i, q, (i < scale ? "," : ";");
  print "INSERT INTO pgbench_tellers (tid, bid, tbalance, filler) VALUES";
  n = 10 * scale;
  for (i = 1; i <= n; i++)
    printf "(%d, %d, 0, %s)%s\n", i, int((i - 1) / 10) + 1, q, (i < n ? "," : ";");
  # Accounts seed splits into one INSERT per 500 rows: the change
  # record per commit is bounded (MAX_CHANGE_RECORD_BYTES), so a single
  # million-row INSERT would exceed it.
  n = 100000 * scale;
  for (i = 1; i <= n; i++) {
    if (i % 500 == 1) print "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES";
    printf "(%d, %d, 0, %s)%s\n", i, int((i - 1) / 100000) + 1, q, (i % 500 == 0 || i == n ? ";" : ",");
  }
}' > "$SEED"

echo "-- OmenDB: seed (scale $SCALE) --"
"$PSQL" -h "$HOST" -p "$PORT" -U omendb -d omendb -v "ON_ERROR_STOP=1" -f "$SEED" >/dev/null

echo "-- OmenDB: run ($CLIENTS clients, ${DURATION}s) --"
# -n skips pgbench's automatic pre-run VACUUM, which is outside the
# supported surface; it is not part of the measured transaction mix.
# --max-tries gives both engines the same retry budget for 40001-class
# serialization conflicts: OmenDB's optimistic snapshots reject and the
# client retries, PostgreSQL's row locks wait; without retries OmenDB's
# hot-row conflicts would count as failures instead of work.
"$PGBENCH" -h "$HOST" -p "$PORT" -U omendb -n -c "$CLIENTS" -T "$DURATION" -P 10 \
  --max-tries 100 -D "scale=$SCALE" -D "mtime=$MTIME" \
  -f "$SCRIPT_DIR/tpcb.sql" omendb 2>&1 | tail -12

kill $OMENDAEMON 2>/dev/null || true
wait $OMENDAEMON 2>/dev/null || true
echo

# ---------------------------------------------------------------------------
# 2. PostgreSQL: same machine, same schema shape, same seed, same script.
# ---------------------------------------------------------------------------
PSQLDB="pgbench_diff"
echo "-- PostgreSQL: initialize + seed (scale $SCALE) --"
PGTARGET="$POSTGRES_URL dbname=$PSQLDB"
"$PSQL" -d "$POSTGRES_URL" -c "DROP DATABASE IF EXISTS $PSQLDB" >/dev/null
"$PSQL" -d "$POSTGRES_URL" -c "CREATE DATABASE $PSQLDB" >/dev/null
"$PSQL" -d "$PGTARGET" \
  -v "scale=$SCALE" -v "ON_ERROR_STOP=1" -f "$SCRIPT_DIR/schema.sql" >/dev/null
"$PSQL" -d "$PGTARGET" -v "ON_ERROR_STOP=1" -f "$SEED" >/dev/null

echo "-- PostgreSQL: run ($CLIENTS clients, ${DURATION}s) --"
"$PGBENCH" -d "$PGTARGET" -n -c "$CLIENTS" -T "$DURATION" -P 10 \
  --max-tries 100 -D "scale=$SCALE" -D "mtime=$MTIME" \
  -f "$SCRIPT_DIR/tpcb.sql" 2>&1 | tail -12

PGTARGET="$POSTGRES_URL dbname=$PSQLDB"
"$PSQL" -d "$POSTGRES_URL" -c "DROP DATABASE IF EXISTS $PSQLDB" >/dev/null
echo
echo "== done =="
