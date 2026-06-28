#!/usr/bin/env python3
"""Create ingest target tables on every QuestDB shard (ILP only writes one shard per key)."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from common import SHARD_PG_PORTS, exec_sql, pg_connect, wait_for_tcp

TRADE_TICKS_DDL = """
CREATE TABLE IF NOT EXISTS trade_ticks (
    symbol SYMBOL,
    venue SYMBOL,
    price DOUBLE,
    size DOUBLE,
    aggressor_side SYMBOL,
    trade_id STRING,
    timestamp TIMESTAMP
) timestamp(timestamp) PARTITION BY DAY;
"""

QUOTE_TICKS_DDL = """
CREATE TABLE IF NOT EXISTS quote_ticks (
    symbol SYMBOL,
    venue SYMBOL,
    bid DOUBLE,
    ask DOUBLE,
    bid_size DOUBLE,
    ask_size DOUBLE,
    timestamp TIMESTAMP
) timestamp(timestamp) PARTITION BY DAY;
"""


def init_tables(shard_ports: list[int] | None = None) -> None:
    ports = shard_ports or SHARD_PG_PORTS
    for port in ports:
        wait_for_tcp("127.0.0.1", port, f"questdb shard PG {port}", timeout=120.0)
        with pg_connect("127.0.0.1", port) as conn:
            conn.autocommit = True
            exec_sql(conn, TRADE_TICKS_DDL)
            exec_sql(conn, QUOTE_TICKS_DDL)
        print(f"  ok  ingest tables on shard PG {port}")


def main() -> int:
    print("Initializing trade_ticks / quote_ticks on all QuestDB shards")
    init_tables()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
