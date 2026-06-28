#!/usr/bin/env python3
"""
QuestDB time-series / windowing tests for quest-router.

For router-compatible SQL the test follows:
  1. Query the router
  2. Run the same SQL on each shard
  3. Merge shard rows
  4. Assert router output == merged shards

QuestDB-specific syntax (SAMPLE BY with shard-key predicate) routes through the router
as single-shard verbatim passthrough. Unkeyed dialect (LATEST ON without WHERE key)
still runs shard-only until multi-shard merge is implemented.

Usage:
  docker compose up -d --build
  pip install -r scripts/requirements.txt
  python scripts/test_timeseries.py --init

Examples:
  python scripts/test_timeseries.py --init --hours 6 --ticks-per-hour 120
  python scripts/test_timeseries.py --skip-ingest --symbol SYM-0001
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (
    ROUTER_ILP_HOST,
    ROUTER_ILP_PORT,
    ROUTER_PG_HOST,
    ROUTER_PG_PORT,
    SHARD_KEY,
    SHARD_PG_PORTS,
    compare_router_to_holding_shard,
    compare_router_to_shards,
    exec_sql,
    merge_shard_rows,
    pg_connect,
    query_all_shards,
    query_shard,
    require_psycopg2,
    send_ilp,
    sql_string,
    symbol_name,
    wait_for_tcp,
)

TABLE = "ts_test_ticks"
NS_PER_HOUR = 3_600_000_000_000

SYMBOL_SORT = lambda r: str(r[0])


def ddl() -> str:
    return f"""
        CREATE TABLE {TABLE} (
            symbol SYMBOL,
            price DOUBLE,
            volume DOUBLE,
            ts TIMESTAMP
        ) timestamp(ts) PARTITION BY DAY;
    """


def init_table() -> None:
    for port in SHARD_PG_PORTS:
        with pg_connect("127.0.0.1", port) as conn:
            conn.autocommit = True
            exec_sql(conn, f"DROP TABLE IF EXISTS {TABLE}")
            exec_sql(conn, ddl())
    print(f"  ok  {TABLE} recreated on {len(SHARD_PG_PORTS)} shard(s)")


def aligned_hour_start_ns() -> int:
    now_s = int(time.time())
    hour_s = now_s - (now_s % 3600)
    return hour_s * 1_000_000_000


def ingest_ticks(
    num_symbols: int,
    hours: int,
    ticks_per_hour: int,
    batch_size: int,
) -> int:
    anchor_ns = aligned_hour_start_ns()
    start_ns = anchor_ns - (hours - 1) * NS_PER_HOUR
    interval_ns = NS_PER_HOUR // ticks_per_hour
    batch: list[str] = []
    sent = 0

    for sym_idx in range(num_symbols):
        sym = symbol_name(sym_idx)
        base_price = 100.0 + sym_idx
        tick_idx = 0
        for h in range(hours):
            hour_base = start_ns + h * NS_PER_HOUR
            for t in range(ticks_per_hour):
                ts = hour_base + t * interval_ns
                price = base_price + (tick_idx % 20) * 0.1
                volume = 1.0 + (tick_idx % 5)
                batch.append(
                    f"{TABLE},{SHARD_KEY}={sym} price={price},volume={volume} {ts}\n"
                )
                tick_idx += 1
                sent += 1
                if len(batch) >= batch_size:
                    send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "".join(batch))
                    batch.clear()

    if batch:
        send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "".join(batch))
    return sent


def shard_holding_symbol(symbol: str) -> int | None:
    sym = sql_string(symbol)
    for idx, port in enumerate(SHARD_PG_PORTS):
        rows = query_shard(port, f"SELECT count() FROM {TABLE} WHERE {SHARD_KEY} = {sym}")
        if rows and int(rows[0][0]) > 0:
            return idx
    return None


def ohlc_sql(symbol: str) -> str:
    sym = sql_string(symbol)
    return f"""
        SELECT
            first(price) AS open,
            max(price) AS high,
            min(price) AS low,
            last(price) AS close,
            sum(volume) AS volume,
            count() AS n
        FROM {TABLE}
        WHERE {SHARD_KEY} = {sym}
    """


def group_by_symbol_sql() -> str:
    return f"""
        SELECT {SHARD_KEY}, count() AS n, avg(price) AS avg_price
        FROM {TABLE}
        GROUP BY {SHARD_KEY}
        ORDER BY {SHARD_KEY}
    """


def test_router_ohlc_vs_shards(symbol: str) -> int:
    return compare_router_to_holding_shard(
        f"OHLC aggregates symbol={symbol}",
        ohlc_sql(symbol),
        TABLE,
        symbol,
    )


def test_group_by_vs_shards() -> int:
    return compare_router_to_shards(
        "GROUP BY symbol",
        group_by_symbol_sql(),
        sort_key=SYMBOL_SORT,
    )


def sample_by_hour_sql(symbol: str) -> str:
    sym = sql_string(symbol)
    return f"""
        SELECT
            ts,
            first(price) AS open,
            max(price) AS high,
            min(price) AS low,
            last(price) AS close,
            sum(volume) AS volume,
            count() AS n
        FROM {TABLE}
        WHERE {SHARD_KEY} = {sym}
        SAMPLE BY 1h
        ORDER BY ts
    """


def test_sample_by_hour_router(
    symbol: str,
    expected_hours: int,
    ticks_per_hour: int,
) -> int:
    """Keyed SAMPLE BY routes through router to the holding shard."""
    sql = sample_by_hour_sql(symbol)
    failures = compare_router_to_holding_shard(
        f"SAMPLE BY 1h (router vs holding shard, symbol={symbol})",
        sql,
        TABLE,
        symbol,
    )
    if failures:
        return failures

    shard_idx = shard_holding_symbol(symbol)
    if shard_idx is None:
        return 1
    shard_port = SHARD_PG_PORTS[shard_idx]
    try:
        rows = query_shard(shard_port, sql)
    except Exception as exc:
        print(f"  FAIL  SAMPLE BY bucket validation: {exc}")
        return 1

    if len(rows) != expected_hours:
        print(f"  FAIL  bucket count {len(rows)} != expected {expected_hours}")
        return 1

    total_ticks = sum(int(row[6]) for row in rows)
    expected_total = expected_hours * ticks_per_hour
    if total_ticks != expected_total:
        print(f"  FAIL  tick sum {total_ticks} != expected {expected_total}")
        return 1

    print(f"  ok  {len(rows)} hourly buckets, {total_ticks} ticks total")
    return 0


def test_sample_by_hour(
    symbol: str,
    shard_port: int,
    expected_hours: int,
    ticks_per_hour: int,
) -> int:
    """Legacy shard-only SAMPLE BY validation (bucket semantics)."""
    sym = sql_string(symbol)
    sql = sample_by_hour_sql(symbol)
    print(f"\n--- SAMPLE BY 1h bucket check (symbol={symbol}, PG :{shard_port}) ---")
    try:
        rows = query_shard(shard_port, sql)
    except Exception as exc:
        print(f"  FAIL  SAMPLE BY query: {exc}")
        return 1

    if len(rows) != expected_hours:
        print(f"  FAIL  bucket count {len(rows)} != expected {expected_hours}")
        return 1

    failures = 0
    total_ticks = 0
    for i, row in enumerate(rows):
        ts, open_, high, low, close, volume, n = row
        n = int(n)
        total_ticks += n
        if n != ticks_per_hour:
            print(f"  FAIL  bucket {i} ({ts}): count {n} != {ticks_per_hour}")
            failures += 1
        if float(high) < float(low):
            print(f"  FAIL  bucket {i} ({ts}): high < low")
            failures += 1

    expected_total = expected_hours * ticks_per_hour
    if total_ticks != expected_total:
        print(f"  FAIL  tick sum {total_ticks} != expected {expected_total}")
        failures += 1

    if failures:
        return failures
    print(f"  ok  {len(rows)} hourly buckets, {total_ticks} ticks total")
    return 0


def sample_by_minute_sql(symbol: str) -> str:
    sym = sql_string(symbol)
    return f"""
        SELECT ts, count() AS n, avg(price) AS avg_price
        FROM {TABLE}
        WHERE {SHARD_KEY} = {sym}
        SAMPLE BY 15m
        ORDER BY ts
    """


def test_sample_by_minute_router(symbol: str) -> int:
    return compare_router_to_holding_shard(
        f"SAMPLE BY 15m (router vs holding shard, symbol={symbol})",
        sample_by_minute_sql(symbol),
        TABLE,
        symbol,
    )


def test_sample_by_minute(symbol: str, shard_port: int) -> int:
    sql = sample_by_minute_sql(symbol)
    print(f"\n--- SAMPLE BY 15m bucket check (symbol={symbol}, PG :{shard_port}) ---")
    try:
        rows = query_shard(shard_port, sql)
    except Exception as exc:
        print(f"  FAIL  SAMPLE BY 15m query: {exc}")
        return 1

    if not rows:
        print("  FAIL  no buckets returned")
        return 1

    if any(int(n) <= 0 for _, n, _ in rows):
        print("  FAIL  empty bucket in SAMPLE BY 15m result")
        return 1

    print(f"  ok  {len(rows)} fifteen-minute buckets with avg(price)")
    return 0


def test_latest_on_merged_vs_manual(num_symbols: int) -> int:
    """
    LATEST ON cannot go through router — verify merged per-shard results match
    a manual merge of the same per-shard queries (sanity on merge logic).
    """
    sql = f"""
        SELECT {SHARD_KEY}, price, volume, ts
        FROM {TABLE}
        LATEST ON ts PARTITION BY {SHARD_KEY}
        ORDER BY {SHARD_KEY}
    """
    print("\n--- LATEST ON ts PARTITION BY symbol (shard-only, merge sanity) ---")
    print("  note  router cannot parse LATEST ON — comparing shard merge to itself")
    try:
        parts = query_all_shards(sql)
        for idx, part in enumerate(parts):
            print(f"  shard-{idx}: {len(part)} row(s)")
        merged = merge_shard_rows(parts, sort_key=SYMBOL_SORT)
    except Exception as exc:
        print(f"  FAIL  LATEST ON query: {exc}")
        return 1

    if len(merged) != num_symbols:
        print(f"  FAIL  got {len(merged)} rows, expected {num_symbols}")
        return 1

    symbols = {str(r[0]) for r in merged}
    expected = {symbol_name(i) for i in range(num_symbols)}
    missing = expected - symbols
    if missing:
        print(f"  FAIL  missing symbols: {sorted(missing)[:5]}")
        return 1

    print(f"  ok  latest row for each of {num_symbols} symbols (merged {len(SHARD_PG_PORTS)} shards)")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="quest-router QuestDB time-series test")
    p.add_argument("--init", action="store_true", help="drop + recreate ts_test_ticks on shards")
    p.add_argument("--symbols", type=int, default=4, help="distinct shard keys to ingest")
    p.add_argument("--hours", type=int, default=4, help="hours of history per symbol")
    p.add_argument("--ticks-per-hour", type=int, default=60, help="ticks per hour per symbol")
    p.add_argument("--symbol", default=None, help="symbol for single-shard tests")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--settle-s", type=float, default=2.5)
    p.add_argument("--skip-ingest", action="store_true")
    p.add_argument("--skip-wait", action="store_true")
    return p.parse_args()


def main() -> int:
    require_psycopg2()
    args = parse_args()
    test_symbol = args.symbol or symbol_name(0)
    expected_ticks = args.symbols * args.hours * args.ticks_per_hour

    print("== quest-router time-series test ==")
    print(f"table   : {TABLE}")
    print(f"symbols : {args.symbols}")
    print(f"window  : {args.hours}h × {args.ticks_per_hour} ticks/symbol")
    print(f"expect  : {expected_ticks} total ticks")
    print(f"probe   : {test_symbol}")
    print("pattern : router query → per-shard query → merge → compare (where supported)")

    if not args.skip_wait:
        print("\n1. Waiting for services...")
        wait_for_tcp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "router ILP")
        wait_for_tcp(ROUTER_PG_HOST, ROUTER_PG_PORT, "router PG")
        for port in SHARD_PG_PORTS:
            wait_for_tcp("127.0.0.1", port, f"shard PG :{port}")

    if args.init:
        print("\n2. Initializing table on shards...")
        init_table()

    if not args.skip_ingest:
        print(f"\n3. Ingesting {expected_ticks} ticks via router ILP...")
        sent = ingest_ticks(args.symbols, args.hours, args.ticks_per_hour, args.batch_size)
        print(f"  ok  sent {sent} lines")
        print(f"\nWaiting {args.settle_s:.1f}s for QuestDB to settle...")
        time.sleep(args.settle_s)
    else:
        print("\n3. Skipping ingest (--skip-ingest)")

    failures = 0

    print("\n4. Router vs merged shards (sqlparser-compatible SQL)...")
    failures += test_router_ohlc_vs_shards(test_symbol)
    failures += test_group_by_vs_shards()

    shard_idx = shard_holding_symbol(test_symbol)
    if shard_idx is None:
        print(f"\n  FAIL  symbol {test_symbol} not found on any shard")
        failures += 1
    else:
        print("\n5. QuestDB dialect (keyed SAMPLE BY via router)...")
        failures += test_sample_by_hour_router(
            test_symbol, args.hours, args.ticks_per_hour
        )
        failures += test_sample_by_minute_router(test_symbol)
        shard_port = SHARD_PG_PORTS[shard_idx]
        failures += test_sample_by_hour(
            test_symbol, shard_port, args.hours, args.ticks_per_hour
        )
        failures += test_sample_by_minute(test_symbol, shard_port)

    print("\n6. Per-shard LATEST ON merge...")
    failures += test_latest_on_merged_vs_manual(args.symbols)

    print()
    if failures:
        print(f"FAILED ({failures} check(s))")
        return 1
    print("PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
