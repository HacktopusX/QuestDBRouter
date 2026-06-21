#!/usr/bin/env python3
"""
Smoke test for quest-router against docker-compose (or local stack).

Writes ILP lines through the router, then reads via PostgreSQL wire protocol.
Optionally verifies rows landed on the expected shard by querying shard PG ports directly.

Usage:
  docker compose up -d --build
  pip install -r scripts/requirements.txt
  python scripts/test_router.py

Environment overrides:
  ROUTER_ILP_HOST, ROUTER_ILP_PORT   (default localhost:9009)
  ROUTER_PG_HOST, ROUTER_PG_PORT     (default localhost:8812)
  SHARD0_PG_PORT, SHARD1_PG_PORT     (default 18812, 18822)
"""

from __future__ import annotations

import os
import socket
import sys
import time
from dataclasses import dataclass

try:
    import psycopg2
except ImportError:
    print("Install dependencies: pip install -r scripts/requirements.txt", file=sys.stderr)
    sys.exit(1)

ROUTER_ILP_HOST = os.environ.get("ROUTER_ILP_HOST", "127.0.0.1")
ROUTER_ILP_PORT = int(os.environ.get("ROUTER_ILP_PORT", "9009"))
ROUTER_PG_HOST = os.environ.get("ROUTER_PG_HOST", "127.0.0.1")
ROUTER_PG_PORT = int(os.environ.get("ROUTER_PG_PORT", "8812"))

SHARD0_PG_PORT = int(os.environ.get("SHARD0_PG_PORT", "18812"))
SHARD1_PG_PORT = int(os.environ.get("SHARD1_PG_PORT", "18822"))

PG_USER = os.environ.get("QUESTDB_USER", "admin")
PG_PASSWORD = os.environ.get("QUESTDB_PASSWORD", "quest")
PG_DATABASE = os.environ.get("QUESTDB_DATABASE", "qdb")

TABLE = "router_test_trades"


@dataclass(frozen=True)
class Sample:
    symbol: str
    price: float


SAMPLES = [
    Sample("BTC-USD", 42000.5),
    Sample("ETH-USD", 2200.25),
]


def wait_for_tcp(host: str, port: int, label: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"  ok  {label} ({host}:{port})")
                return
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"{label} not reachable at {host}:{port} within {timeout}s")


def send_ilp(host: str, port: int, lines: list[str]) -> None:
    payload = "".join(lines)
    with socket.create_connection((host, port), timeout=10) as sock:
        sock.sendall(payload.encode("utf-8"))


def pg_connect(host: str, port: int):
    return psycopg2.connect(
        host=host,
        port=port,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE,
        connect_timeout=10,
    )


def exec_sql(conn, query: str, params: tuple | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        if cur.description:
            return cur.fetchall()
    return []


def setup_table_on_shards() -> None:
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            symbol SYMBOL,
            price DOUBLE,
            ts TIMESTAMP
        ) timestamp(ts) PARTITION BY DAY;
    """
    for port in (SHARD0_PG_PORT, SHARD1_PG_PORT):
        with pg_connect("127.0.0.1", port) as conn:
            conn.autocommit = True
            exec_sql(conn, ddl)
    print("  ok  table ready on both shards")


def write_samples_via_router() -> None:
    now_ns = time.time_ns()
    lines = []
    for i, sample in enumerate(SAMPLES):
        ts = now_ns + i
        lines.append(f"{TABLE},symbol={sample.symbol} price={sample.price} {ts}\n")
    send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, lines)
    print(f"  ok  wrote {len(lines)} ILP lines via router")


def read_via_router(symbol: str) -> list[tuple]:
    with pg_connect(ROUTER_PG_HOST, ROUTER_PG_PORT) as conn:
        conn.autocommit = True
        return exec_sql(
            conn,
            f"SELECT symbol, price FROM {TABLE} WHERE symbol = %s ORDER BY price",
            (symbol,),
        )


def count_on_shard(port: int, symbol: str) -> int:
    with pg_connect(ROUTER_PG_HOST if port == ROUTER_PG_PORT else "127.0.0.1", port) as conn:
        conn.autocommit = True
        rows = exec_sql(
            conn,
            f"SELECT count() FROM {TABLE} WHERE symbol = %s",
            (symbol,),
        )
    return int(rows[0][0])


def main() -> int:
    print("== quest-router smoke test ==")

    print("\n1. Waiting for services...")
    wait_for_tcp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "router ILP")
    wait_for_tcp(ROUTER_PG_HOST, ROUTER_PG_PORT, "router PG")
    wait_for_tcp("127.0.0.1", SHARD0_PG_PORT, "shard-0 PG")
    wait_for_tcp("127.0.0.1", SHARD1_PG_PORT, "shard-1 PG")

    print("\n2. Creating table on each shard (direct PG)...")
    setup_table_on_shards()

    print("\n3. Writing samples via router (ILP)...")
    write_samples_via_router()
    time.sleep(2)

    print("\n4. Reading back via router (PG wire)...")
    failures = 0
    for sample in SAMPLES:
        rows = read_via_router(sample.symbol)
        if not rows:
            print(f"  FAIL  no rows for {sample.symbol}")
            failures += 1
            continue
        got_symbol, got_price = rows[0][0], float(rows[0][1])
        if got_symbol != sample.symbol or abs(got_price - sample.price) > 1e-6:
            print(f"  FAIL  {sample.symbol}: expected price {sample.price}, got {rows}")
            failures += 1
        else:
            print(f"  ok  {sample.symbol} price={got_price}")

    print("\n5. Shard placement (direct PG to each QuestDB node)...")
    for sample in SAMPLES:
        c0 = count_on_shard(SHARD0_PG_PORT, sample.symbol)
        c1 = count_on_shard(SHARD1_PG_PORT, sample.symbol)
        total = c0 + c1
        if total != 1:
            print(f"  FAIL  {sample.symbol}: shard counts shard0={c0} shard1={c1} (expected total 1)")
            failures += 1
        else:
            shard = "questdb-0" if c0 else "questdb-1"
            print(f"  ok  {sample.symbol} -> {shard} (counts: {c0}/{c1})")

    print()
    if failures:
        print(f"FAILED ({failures} check(s))")
        return 1
    print("PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
