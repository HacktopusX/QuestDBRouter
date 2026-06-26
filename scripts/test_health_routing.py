#!/usr/bin/env python3
"""
Health-aware routing smoke test.

Requires docker-compose stack with quest-router and two QuestDB shards.
Stops one shard container, verifies router still accepts ILP for keys that
hash to the remaining healthy shard.

Usage:
  docker compose up -d --build
  python scripts/test_health_routing.py

Environment:
  HEALTH_TEST_STOP_SHARD   container to stop (default questdb-1)
  HEALTH_TEST_SETTLE_S     seconds to wait after ILP (default 2)
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.request

from common import (
    METRICS_URL,
    ROUTER_ILP_HOST,
    ROUTER_ILP_PORT,
    ROUTER_PG_HOST,
    ROUTER_PG_PORT,
    exec_sql,
    pg_connect,
    require_psycopg2,
    setup_table_on_shards,
    sql_string,
    wait_for_tcp,
)

SHARD_CONTAINER = os.environ.get("HEALTH_TEST_STOP_SHARD", "questdb-1")
SETTLE_S = float(os.environ.get("HEALTH_TEST_SETTLE_S", "2"))
TABLE = "router_test_trades"

# Consistent-hash placement for the default 2-shard ring (weight=1, vnodes=128).
# Pick a symbol on the shard that stays up when the paired container is stopped.
SYMBOL_ON_SHARD: dict[int, str] = {
    0: "btc-usdt",
    1: "HEALTH-TEST-SYM",
}
STOPPED_CONTAINER_TO_SHARD = {
    "questdb-0": 0,
    "questdb-1": 1,
}


def ilp_send(lines: list[str]) -> None:
    with socket.create_connection((ROUTER_ILP_HOST, ROUTER_ILP_PORT), timeout=10) as sock:
        sock.sendall("".join(lines).encode("utf-8"))


def router_pg_query(sql: str) -> list[tuple]:
    with pg_connect(ROUTER_PG_HOST, ROUTER_PG_PORT) as conn:
        conn.autocommit = True
        return exec_sql(conn, sql)


def stopped_shard_id(container: str) -> int:
    shard_id = STOPPED_CONTAINER_TO_SHARD.get(container)
    if shard_id is None:
        raise ValueError(
            f"unknown HEALTH_TEST_STOP_SHARD={container!r}; "
            f"expected one of {sorted(STOPPED_CONTAINER_TO_SHARD)}"
        )
    return shard_id


def healthy_shard_id(stopped: int) -> int:
    return 1 - stopped


def symbol_for_healthy_shard(stopped_container: str) -> str:
    healthy = healthy_shard_id(stopped_shard_id(stopped_container))
    return SYMBOL_ON_SHARD[healthy]


def wait_for_shard_marked_unhealthy(stopped_shard: int, timeout: float = 30.0) -> None:
    """Poll router metrics until the stopped shard is excluded."""
    needle = f'quest_router_shard_healthy{{shard="{stopped_shard}",protocol="ilp"}} 0'
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            body = urllib.request.urlopen(METRICS_URL, timeout=3).read().decode()
            if needle in body:
                print(f"  ok  shard-{stopped_shard} marked unhealthy in metrics")
                return
        except OSError:
            pass
        time.sleep(1)
    print(f"  WARN  timed out waiting for shard-{stopped_shard} unhealthy gauge")


def main() -> int:
    require_psycopg2()
    stopped = stopped_shard_id(SHARD_CONTAINER)
    healthy = healthy_shard_id(stopped)
    symbol = symbol_for_healthy_shard(SHARD_CONTAINER)

    print("== quest-router health routing test ==")
    print(f"stop shard container : {SHARD_CONTAINER} (shard-{stopped})")
    print(f"healthy shard          : shard-{healthy}")
    print(f"test symbol            : {symbol}")

    wait_for_tcp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "router ILP")
    wait_for_tcp(ROUTER_PG_HOST, ROUTER_PG_PORT, "router PG")

    print(f"\nCreating {TABLE} on each shard (direct PG)...")
    setup_table_on_shards(TABLE)

    print(f"\nStopping {SHARD_CONTAINER} to simulate unhealthy shard...")
    subprocess.run(["docker", "compose", "stop", SHARD_CONTAINER], check=True)
    wait_for_shard_marked_unhealthy(stopped)

    failures = 0
    try:
        print("Writing ILP through router...")
        ts = int(time.time() * 1_000_000_000)
        ilp_send([f"{TABLE},symbol={symbol} price=1.0 {ts}\n"])

        print(f"Waiting {SETTLE_S:.1f}s for QuestDB to settle...")
        time.sleep(SETTLE_S)

        print("Querying via router PG...")
        sym = sql_string(symbol)
        rows = router_pg_query(
            f"SELECT count() FROM {TABLE} WHERE symbol = {sym}"
        )
        count = int(rows[0][0]) if rows else 0
        if count >= 1:
            print(f"  ok  router returned count={count} after shard stop")
        else:
            print(f"  FAIL  expected rows for {symbol}, got count={count}")
            failures += 1
    except Exception as exc:
        print(f"  FAIL  {exc}")
        failures += 1
    finally:
        print(f"Restarting {SHARD_CONTAINER}...")
        subprocess.run(["docker", "compose", "start", SHARD_CONTAINER], check=True)

    return 1 if failures else 0


if __name__ == "__main__":
    rc = main()
    if rc == 0:
        print("PASSED")
    else:
        print("FAILED")
    sys.exit(rc)
