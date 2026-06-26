#!/usr/bin/env python3
"""
Verify BitUnix trade ticks ingested through quest-router.

Run while ``bitunix_quest_router_sink.py`` is streaming, or after it has written rows.

Usage (from rust-something repo root)::

    docker compose up -d --build quest-router
    python scripts/verify_bitunix_ticks.py

Environment overrides::

    BITUNIX_SYMBOL=BTCUSDT
    ROUTER_PG_HOST, ROUTER_PG_PORT   (default localhost:8812)
    VERIFY_TIMEOUT_SECS=120            wait for first row
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from common import ROUTER_PG_HOST
from common import ROUTER_PG_PORT
from common import compare_router_to_holding_shard
from common import query_router
from common import sql_string
from common import wait_for_tcp

TABLE = "trade_ticks"
SYMBOL = os.environ.get("BITUNIX_SYMBOL", "BTCUSDT")
TIMEOUT_SECS = float(os.environ.get("VERIFY_TIMEOUT_SECS", "120"))


def wait_for_rows(min_count: int = 1) -> int:
    sym = sql_string(SYMBOL)
    deadline = time.monotonic() + TIMEOUT_SECS
    while time.monotonic() < deadline:
        try:
            rows = query_router(f"SELECT count() FROM {TABLE} WHERE symbol = {sym}")
            count = int(rows[0][0]) if rows else 0
            if count >= min_count:
                return count
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(
        f"No rows for symbol={SYMBOL} in {TABLE} within {TIMEOUT_SECS}s "
        f"(is bitunix_quest_router_sink.py running?)",
    )


def main() -> int:
    print(f"Verifying {TABLE} for symbol={SYMBOL} via quest-router PG")
    wait_for_tcp(ROUTER_PG_HOST, ROUTER_PG_PORT, "quest-router PG")

    count = wait_for_rows()
    print(f"  ok  router reports {count} row(s) for {SYMBOL}")

    sym = sql_string(SYMBOL)
    recent_sql = (
        f"SELECT symbol, price, size FROM {TABLE} "
        f"WHERE symbol = {sym} ORDER BY timestamp DESC LIMIT 1"
    )
    recent = query_router(recent_sql)
    if recent:
        symbol, price, size = recent[0]
        print(f"  ok  most recent tick price={price} size={size}")
    else:
        print("  warn  recent tick query returned no rows")

    keyed_sql = (
        f"SELECT symbol, price, size FROM {TABLE} "
        f"WHERE symbol = {sym} ORDER BY timestamp DESC LIMIT 5"
    )
    return compare_router_to_holding_shard(
        "keyed trade_ticks",
        keyed_sql,
        TABLE,
        SYMBOL,
    )


if __name__ == "__main__":
    raise SystemExit(main())
