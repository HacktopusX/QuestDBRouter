#!/usr/bin/env python3
"""
Federated JOIN tests for quest-router.

For each JOIN query the test:
  1. Runs the query through the router
  2. Runs the same SQL on each shard
  3. Merges shard result sets
  4. Asserts router output == merged shards (identical rows)

Tables must be registered in routing.tables (see config/docker-quest-router.toml).

Usage:
  docker compose up -d --build
  pip install -r scripts/requirements.txt
  python scripts/test_joins.py --init

Examples:
  python scripts/test_joins.py --init --symbols 8 --trades-per-symbol 5
  python scripts/test_joins.py --skip-ingest
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (
    ROUTER_ILP_HOST,
    ROUTER_ILP_PORT,
    ROUTER_PG_HOST,
    ROUTER_PG_PORT,
    SHARD_KEY,
    SHARD_PG_PORTS,
    compare_router_to_shards,
    compare_row_sets,
    count_table_on_shard,
    exec_sql,
    pg_connect,
    query_router,
    query_shard,
    require_psycopg2,
    send_ilp,
    sql_string,
    symbol_name,
    wait_for_tcp,
)

TRADES_TABLE = "join_test_trades"
ORDERS_TABLE = "join_test_orders"

JOIN_SORT = lambda r: (str(r[0]), int(r[1]))


@dataclass(frozen=True)
class TradeRow:
    symbol: str
    trade_id: int
    price: float


@dataclass(frozen=True)
class OrderRow:
    symbol: str
    trade_id: int
    qty: float


def ddl_trades() -> str:
    return f"""
        CREATE TABLE {TRADES_TABLE} (
            symbol SYMBOL,
            trade_id LONG,
            price DOUBLE,
            ts TIMESTAMP
        ) timestamp(ts) PARTITION BY DAY;
    """


def ddl_orders() -> str:
    return f"""
        CREATE TABLE {ORDERS_TABLE} (
            symbol SYMBOL,
            trade_id LONG,
            qty DOUBLE,
            ts TIMESTAMP
        ) timestamp(ts) PARTITION BY DAY;
    """


def init_tables() -> None:
    for port in SHARD_PG_PORTS:
        with pg_connect("127.0.0.1", port) as conn:
            conn.autocommit = True
            exec_sql(conn, f"DROP TABLE IF EXISTS {TRADES_TABLE}")
            exec_sql(conn, f"DROP TABLE IF EXISTS {ORDERS_TABLE}")
            exec_sql(conn, ddl_trades())
            exec_sql(conn, ddl_orders())
    print(f"  ok  {TRADES_TABLE} + {ORDERS_TABLE} recreated on {len(SHARD_PG_PORTS)} shard(s)")


def build_dataset(num_symbols: int, trades_per_symbol: int) -> tuple[list[TradeRow], list[OrderRow]]:
    trades: list[TradeRow] = []
    orders: list[OrderRow] = []
    for sym_idx in range(num_symbols):
        sym = symbol_name(sym_idx)
        for t in range(trades_per_symbol):
            trade_id = sym_idx * 1000 + t
            price = 100.0 + sym_idx + t * 0.5
            qty = 10.0 + t
            trades.append(TradeRow(sym, trade_id, price))
            orders.append(OrderRow(sym, trade_id, qty))
    return trades, orders


def ingest_rows(
    trades: list[TradeRow],
    orders: list[OrderRow],
    batch_size: int,
) -> None:
    base_ts = time.time_ns()
    batch: list[str] = []
    seq = 0

    def flush() -> None:
        nonlocal batch
        if batch:
            send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "".join(batch))
            batch = []

    for trade in trades:
        ts = base_ts + seq
        batch.append(
            f"{TRADES_TABLE},{SHARD_KEY}={trade.symbol} "
            f"trade_id={trade.trade_id}i,price={trade.price} {ts}\n"
        )
        seq += 1
        if len(batch) >= batch_size:
            flush()

    for order in orders:
        ts = base_ts + seq
        batch.append(
            f"{ORDERS_TABLE},{SHARD_KEY}={order.symbol} "
            f"trade_id={order.trade_id}i,qty={order.qty} {ts}\n"
        )
        seq += 1
        if len(batch) >= batch_size:
            flush()

    flush()


def inner_join_sql(symbol_filter: str | None = None) -> str:
    where = f"WHERE t.symbol = {sql_string(symbol_filter)}" if symbol_filter else ""
    order = "t.trade_id" if symbol_filter else "t.symbol, t.trade_id"
    return f"""
        SELECT t.symbol, t.trade_id, t.price, o.qty
        FROM {TRADES_TABLE} t
        JOIN {ORDERS_TABLE} o
          ON t.symbol = o.symbol AND t.trade_id = o.trade_id
        {where}
        ORDER BY {order}
    """


def left_join_unmatched_sql() -> str:
    return f"""
        SELECT o.symbol, o.trade_id, t.price, o.qty
        FROM {ORDERS_TABLE} o
        LEFT JOIN {TRADES_TABLE} t
          ON t.symbol = o.symbol AND t.trade_id = o.trade_id
        WHERE t.price IS NULL
        ORDER BY o.symbol, o.trade_id
    """


def shard_holding_symbol(symbol: str) -> int | None:
    sym = sql_string(symbol)
    for idx, port in enumerate(SHARD_PG_PORTS):
        rows = query_shard(port, f"SELECT count() FROM {TRADES_TABLE} WHERE symbol = {sym}")
        if rows and int(rows[0][0]) > 0:
            return idx
    return None


def verify_shard_totals(expected_trades: int, expected_orders: int) -> int:
    failures = 0
    trade_counts = [count_table_on_shard(p, TRADES_TABLE) for p in SHARD_PG_PORTS]
    order_counts = [count_table_on_shard(p, ORDERS_TABLE) for p in SHARD_PG_PORTS]
    print("\n=== shard row counts ===")
    for i, (tc, oc) in enumerate(zip(trade_counts, order_counts)):
        print(f"  shard-{i}: trades={tc}  orders={oc}")
    if sum(trade_counts) != expected_trades:
        print(f"  FAIL  trade total {sum(trade_counts)} != {expected_trades}")
        failures += 1
    if sum(order_counts) != expected_orders:
        print(f"  FAIL  order total {sum(order_counts)} != {expected_orders}")
        failures += 1
    return failures


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="quest-router federated JOIN test")
    p.add_argument("--init", action="store_true", help="drop + recreate join tables on shards")
    p.add_argument("--symbols", type=int, default=16, help="distinct shard keys")
    p.add_argument("--trades-per-symbol", type=int, default=10, help="trade/order pairs per symbol")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--settle-s", type=float, default=2.0)
    p.add_argument("--skip-ingest", action="store_true")
    p.add_argument("--skip-wait", action="store_true")
    return p.parse_args()


def main() -> int:
    require_psycopg2()
    args = parse_args()
    expected_rows = args.symbols * args.trades_per_symbol

    print("== quest-router JOIN test ==")
    print(f"tables  : {TRADES_TABLE}, {ORDERS_TABLE}")
    print(f"symbols : {args.symbols}")
    print(f"pairs   : {args.trades_per_symbol} per symbol ({expected_rows} join rows)")
    print("pattern : router query → per-shard query → merge → compare")

    if not args.skip_wait:
        print("\n1. Waiting for services...")
        wait_for_tcp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "router ILP")
        wait_for_tcp(ROUTER_PG_HOST, ROUTER_PG_PORT, "router PG")
        for port in SHARD_PG_PORTS:
            wait_for_tcp("127.0.0.1", port, f"shard PG :{port}")

    if args.init:
        print("\n2. Initializing join tables on shards...")
        init_tables()

    trades, orders = build_dataset(args.symbols, args.trades_per_symbol)

    if not args.skip_ingest:
        print(f"\n3. Ingesting {len(trades) + len(orders)} rows via router ILP...")
        ingest_rows(trades, orders, args.batch_size)
        print(f"  ok  sent {len(trades)} trades + {len(orders)} orders")
        print(f"\nWaiting {args.settle_s:.1f}s for QuestDB to settle...")
        time.sleep(args.settle_s)
    else:
        print("\n3. Skipping ingest (--skip-ingest)")

    failures = 0
    failures += verify_shard_totals(len(trades), len(orders))

    print("\n4. Federated INNER JOIN (router vs merged shards)...")
    failures += compare_router_to_shards(
        "INNER JOIN",
        inner_join_sql(),
        sort_key=JOIN_SORT,
    )

    print("\n5. INNER JOIN with shard-key filter (router vs single shard)...")
    filter_sym = symbol_name(0)
    filter_sql = inner_join_sql(filter_sym)
    shard_idx = shard_holding_symbol(filter_sym)
    if shard_idx is None:
        print(f"  FAIL  symbol {filter_sym} not found on any shard")
        failures += 1
    else:
        shard_port = SHARD_PG_PORTS[shard_idx]
        print(f"\n--- INNER JOIN symbol={filter_sym} (shard-{shard_idx} :{shard_port}) ---")
        try:
            router_rows = query_router(filter_sql)
            shard_rows = query_shard(shard_port, filter_sql)
            print(f"  router: {len(router_rows)} row(s), shard-{shard_idx}: {len(shard_rows)} row(s)")
            failures += compare_row_sets(
                f"INNER JOIN symbol={filter_sym}",
                router_rows,
                shard_rows,
            )
        except Exception as exc:
            print(f"  FAIL  filtered JOIN: {exc}")
            failures += 1

    print("\n6. LEFT JOIN unmatched rows (router vs merged shards)...")
    failures += compare_router_to_shards(
        "LEFT JOIN unmatched",
        left_join_unmatched_sql(),
        sort_key=JOIN_SORT,
    )

    print()
    if failures:
        print(f"FAILED ({failures} check(s))")
        return 1
    print("PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
