#!/usr/bin/env bash
# Bootstrap ingest tables on each QuestDB shard (docker network hostnames).
set -euo pipefail

SHARDS="${QUESTDB_SHARDS:-questdb-0:8812 questdb-1:8812}"
PG_USER="${QUESTDB_USER:-admin}"
PG_PASSWORD="${QUESTDB_PASSWORD:-quest}"
PG_DATABASE="${QUESTDB_DATABASE:-qdb}"

TRADE_DDL="CREATE TABLE IF NOT EXISTS trade_ticks (
  symbol SYMBOL,
  venue SYMBOL,
  price DOUBLE,
  size DOUBLE,
  aggressor_side SYMBOL,
  trade_id STRING,
  timestamp TIMESTAMP
) timestamp(timestamp) PARTITION BY DAY;"

QUOTE_DDL="CREATE TABLE IF NOT EXISTS quote_ticks (
  symbol SYMBOL,
  venue SYMBOL,
  bid DOUBLE,
  ask DOUBLE,
  bid_size DOUBLE,
  ask_size DOUBLE,
  timestamp TIMESTAMP
) timestamp(timestamp) PARTITION BY DAY;"

run_ddl() {
  local hostport="$1"
  local host="${hostport%%:*}"
  local port="${hostport##*:}"
  echo "Waiting for ${host}:${port}..."
  for _ in $(seq 1 60); do
    if PGPASSWORD="$PG_PASSWORD" psql -h "$host" -p "$port" -U "$PG_USER" -d "$PG_DATABASE" -c "SELECT 1" >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
  PGPASSWORD="$PG_PASSWORD" psql -h "$host" -p "$port" -U "$PG_USER" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 -c "$TRADE_DDL"
  PGPASSWORD="$PG_PASSWORD" psql -h "$host" -p "$port" -U "$PG_USER" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 -c "$QUOTE_DDL"
  echo "  ok  ingest tables on ${host}:${port}"
}

for shard in $SHARDS; do
  run_ddl "$shard"
done

echo "QuestDB ingest table init complete"
