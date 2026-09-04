#!/usr/bin/env bash
# Same-hardware pgbench differential: OmenDB (pgwire) vs PostgreSQL.
#
# The alpha gates require release evidence in the form of a reproducible
# PostgreSQL-class comparison on the supported workload overlap. pgbench
# is PostgreSQL's own TPC-B-shaped workload, so it defines the supported
# overlap: simple SELECT/UPDATE point statements inside explicit
# transactions, executed through the real PostgreSQL wire protocol.
#
# This script initializes the standard pgbench schema on both engines
# with identical scale and runs the same client/transaction mix on the
# same machine, then prints both results side by side.
#
# Usage:
#   scripts/pgbench/differential.sh [scale] [clients] [duration]
#
# Environment:
#   OMENDB_BIN      omendbd binary (default: cargo build output)
#   PGBENCH         pgbench binary (default: postgresql@17 keg)
#   POSTGRES_URL    libpq connection string for the live PostgreSQL

set -euo pipefail

SCALE="${1:-1}"
CLIENTS="${2:-4}"
DURATION="${3:-30}"
HOST="127.0.0.1"
PORT="${OMENDB_PORT:-15432}"
DATA="$(mktemp -d)/omen-pgbench"
OMENDB_BIN="${OMENDB_BIN:-$(cargo metadata --format=gnu --no-deps 2>/dev/null \
  | tr ' ' '\n' | grep -m1 target/release/omendbd || echo target/release/omendbd)}"
PGBENCH="${PGBENCH:-/opt/homebrew/opt/postgresql@17/bin/pgbench}"
PSQL="${PSQL:-/opt/homebrew/opt/postgresql@17/bin/psql}"
POSTGRES_URL="${POSTGRES_URL:-host=$HOST user=$USER dbname=postgres}"

command -v "$PGBENCH" >/dev/null || { echo "pgbench not found at $PGBENCH" >&2; exit 1; }

echo "== pgbench differential: scale=$SCALE clients=$CLIENTS duration=${DURATION}s =="
echo "omendbd: $OMENDB_BIN"
echo "pgbench: $PGBENCH"
echo

# ---------------------------------------------------------------------------
# 1. OmenDB: start the daemon, initialize the pgbench schema, run.
# ---------------------------------------------------------------------------
mkdir -p "$DATA"
"$OMENDB_BIN" --path "$DATA" --bind "$HOST:$PORT" &
OMENDAEMON=$!
trap 'kill $OMENDAEMON 2>/dev/null || true; rm -rf "$(dirname "$DATA")"' EXIT

for _ in $(seq 1 50); do
  "$PSQL" -h "$HOST" -p "$PORT" -U omendb -d omendb -c "SELECT 1" >/dev/null 2>&1 && break
  sleep 0.2
done

echo "-- OmenDB: initialize (scale $SCALE) --"
# pgbench -i runs through the same wire path it later measures; DDL and
# COPY-approximate multi-row INSERTs both count as supported surface.
"$PGBENCH" -h "$HOST" -p "$PORT" -U omendb -i -s "$SCALE" omendb 2>&1 | tail -2

echo "-- OmenDB: run ($CLIENTS clients, ${DURATION}s) --"
"$PGBENCH" -h "$HOST" -p "$PORT" -U omendb -c "$CLIENTS" -T "$DURATION" -P 10 omendb 2>&1 | tail -8

kill $OMENDAEMON 2>/dev/null || true
wait $OMENDAEMON 2>/dev/null || true
echo

# ---------------------------------------------------------------------------
# 2. PostgreSQL: same machine, same schema shape, same mix.
# ---------------------------------------------------------------------------
PSQLDB="pgbench_diff"
echo "-- PostgreSQL: initialize (scale $SCALE) --"
"$PSQL" "$POSTGRES_URL" -c "DROP DATABASE IF EXISTS $PSQLDB" >/dev/null
"$PSQL" "$POSTGRES_URL" -c "CREATE DATABASE $PSQLDB" >/dev/null
"$PGBENCH" "$POSTGRES_URL" -i -s "$SCALE" -d "$PSQLDB" 2>&1 | tail -2

echo "-- PostgreSQL: run ($CLIENTS clients, ${DURATION}s) --"
"$PGBENCH" "$POSTGRES_URL" -c "$CLIENTS" -T "$DURATION" -P 10 -d "$PSQLDB" 2>&1 | tail -8

"$PSQL" "$POSTGRES_URL" -c "DROP DATABASE IF EXISTS $PSQLDB" >/dev/null
echo
echo "== done =="
