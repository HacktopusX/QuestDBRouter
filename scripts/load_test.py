#!/usr/bin/env python3
"""
Load test for quest-router: ILP writes + PG wire reads (simple & extended).

Exercises:
  - ILP line routing by shard tag (symbol)
  - PG simple queries with literal WHERE (sqlparser routing)
  - PG parameterized queries via extended protocol (Parse/Bind with $1)

Usage:
  docker compose up -d --build
  pip install -r scripts/requirements.txt
  python scripts/load_test.py --setup --mode all

Examples:
  python scripts/load_test.py --setup --mode ilp --ilp-workers 16 --ilp-lines 100000
  python scripts/load_test.py --mode pg-extended --pg-workers 8 --pg-queries 5000
  python scripts/load_test.py --mode all --symbols 64 --metrics
"""

from __future__ import annotations

import argparse
import random
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (
    METRICS_URL,
    ROUTER_ILP_HOST,
    ROUTER_ILP_PORT,
    ROUTER_PG_HOST,
    ROUTER_PG_PORT,
    SHARD_KEY,
    send_ilp,
    setup_table_on_shards,
    wait_for_tcp,
    pg_connect,
    exec_sql,
    require_psycopg2,
)

from dataclasses import dataclass, field

DEFAULT_TABLE = "load_test_trades"


@dataclass
class LoadResult:
    name: str
    operations: int = 0
    errors: int = 0
    duration_s: float = 0.0
    latencies_ms: list[float] = field(default_factory=list)

    def throughput(self) -> float:
        if self.duration_s <= 0:
            return 0.0
        return self.operations / self.duration_s

    def latency_summary(self) -> dict[str, float]:
        if not self.latencies_ms:
            return {}
        sorted_ms = sorted(self.latencies_ms)
        n = len(sorted_ms)

        def pct(p: float) -> float:
            idx = min(int(n * p), n - 1)
            return sorted_ms[idx]

        return {
            "p50": pct(0.50),
            "p95": pct(0.95),
            "p99": pct(0.99),
            "max": sorted_ms[-1],
        }


def symbol_name(i: int) -> str:
    return f"SYM-{i:04d}"


def make_ilp_line(table: str, symbol: str, price: float, ts_ns: int) -> str:
    return f"{table},{SHARD_KEY}={symbol} price={price} {ts_ns}\n"


def seed_ilp(table: str, symbols: list[str], lines_per_symbol: int) -> int:
    """Seed data through router ILP for read benchmarks."""
    now = time.time_ns()
    batch: list[str] = []
    count = 0
    for sym in symbols:
        for j in range(lines_per_symbol):
            batch.append(make_ilp_line(table, sym, 100.0 + j, now + count))
            count += 1
            if len(batch) >= 500:
                send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "".join(batch))
                batch.clear()
    if batch:
        send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "".join(batch))
    time.sleep(1.5)
    return count


def run_ilp_load(
    table: str,
    workers: int,
    total_lines: int,
    batch_size: int,
    num_symbols: int,
) -> LoadResult:
    result = LoadResult(name="ilp-write")
    lines_per_worker = total_lines // workers
    lock = threading.Lock()

    def worker(worker_id: int) -> None:
        local_latencies: list[float] = []
        local_ops = 0
        local_errors = 0
        try:
            sock = __import__("socket").create_connection(
                (ROUTER_ILP_HOST, ROUTER_ILP_PORT), timeout=30
            )
        except OSError:
            with lock:
                result.errors += lines_per_worker
            return

        try:
            sent = 0
            batch: list[str] = []
            base_ts = time.time_ns() + worker_id * 1_000_000_000

            while sent < lines_per_worker:
                sym = symbol_name((worker_id + sent) % num_symbols)
                line = make_ilp_line(
                    table,
                    sym,
                    50.0 + (sent % 100),
                    base_ts + sent,
                )
                batch.append(line)
                sent += 1

                if len(batch) >= batch_size or sent >= lines_per_worker:
                    payload = "".join(batch)
                    batch.clear()
                    start = time.perf_counter()
                    try:
                        sock.sendall(payload.encode("utf-8"))
                        line_count = payload.count("\n")
                        elapsed_ms = (time.perf_counter() - start) * 1000
                        per_line = elapsed_ms / max(1, line_count)
                        local_latencies.extend([per_line] * line_count)
                        local_ops += line_count
                    except OSError:
                        local_errors += 1
                        break
        finally:
            sock.close()

        with lock:
            result.operations += local_ops
            result.errors += local_errors
            result.latencies_ms.extend(local_latencies)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(worker, range(workers)))
    result.duration_s = time.perf_counter() - started
    return result


def run_pg_simple_load(
    table: str,
    workers: int,
    queries_per_worker: int,
    symbols: list[str],
) -> LoadResult:
    result = LoadResult(name="pg-simple (literal WHERE)")
    lock = threading.Lock()

    def worker(_: int) -> None:
        local_latencies: list[float] = []
        local_ops = 0
        local_errors = 0
        try:
            conn = pg_connect(ROUTER_PG_HOST, ROUTER_PG_PORT)
            conn.autocommit = True
        except Exception:
            with lock:
                result.errors += queries_per_worker
            return

        try:
            for i in range(queries_per_worker):
                sym = random.choice(symbols)
                sql = (
                    f"SELECT count() FROM {table} "
                    f"WHERE {SHARD_KEY} = '{sym}'"
                )
                start = time.perf_counter()
                try:
                    exec_sql(conn, sql)
                    local_latencies.append((time.perf_counter() - start) * 1000)
                    local_ops += 1
                except Exception:
                    local_errors += 1
        finally:
            conn.close()

        with lock:
            result.operations += local_ops
            result.errors += local_errors
            result.latencies_ms.extend(local_latencies)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(worker, range(workers)))
    result.duration_s = time.perf_counter() - started
    return result


def run_pg_extended_load(
    table: str,
    workers: int,
    queries_per_worker: int,
    symbols: list[str],
) -> LoadResult:
    """Parameterized reads — exercises Parse/Bind re-route on shard key param."""
    result = LoadResult(name="pg-extended (parameterized)")
    lock = threading.Lock()

    def worker(_: int) -> None:
        local_latencies: list[float] = []
        local_ops = 0
        local_errors = 0
        try:
            conn = pg_connect(ROUTER_PG_HOST, ROUTER_PG_PORT)
            conn.autocommit = True
        except Exception:
            with lock:
                result.errors += queries_per_worker
            return

        sql = f"SELECT count() FROM {table} WHERE {SHARD_KEY} = %s"
        try:
            for i in range(queries_per_worker):
                sym = random.choice(symbols)
                start = time.perf_counter()
                try:
                    exec_sql(conn, sql, (sym,))
                    local_latencies.append((time.perf_counter() - start) * 1000)
                    local_ops += 1
                except Exception:
                    local_errors += 1
        finally:
            conn.close()

        with lock:
            result.operations += local_ops
            result.errors += local_errors
            result.latencies_ms.extend(local_latencies)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(worker, range(workers)))
    result.duration_s = time.perf_counter() - started
    return result


def run_pg_table_route_load(
    table: str,
    workers: int,
    queries_per_worker: int,
) -> LoadResult:
    """Global aggregate without shard key — federated broadcast."""
    result = LoadResult(name="pg-table-route (no WHERE)")
    lock = threading.Lock()

    def worker(_: int) -> None:
        local_latencies: list[float] = []
        local_ops = 0
        local_errors = 0
        try:
            conn = pg_connect(ROUTER_PG_HOST, ROUTER_PG_PORT)
            conn.autocommit = True
        except Exception:
            with lock:
                result.errors += queries_per_worker
            return

        sql = f"SELECT count() FROM {table}"
        try:
            for _ in range(queries_per_worker):
                start = time.perf_counter()
                try:
                    exec_sql(conn, sql)
                    local_latencies.append((time.perf_counter() - start) * 1000)
                    local_ops += 1
                except Exception:
                    local_errors += 1
        finally:
            conn.close()

        with lock:
            result.operations += local_ops
            result.errors += local_errors
            result.latencies_ms.extend(local_latencies)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(worker, range(workers)))
    result.duration_s = time.perf_counter() - started
    return result


def run_pg_scan_load(
    table: str,
    workers: int,
    queries_per_worker: int,
) -> LoadResult:
    """Full table scan via federated broadcast (`SELECT *`)."""
    result = LoadResult(name="pg-scan (SELECT *)")
    lock = threading.Lock()

    def worker(_: int) -> None:
        local_latencies: list[float] = []
        local_ops = 0
        local_errors = 0
        try:
            conn = pg_connect(ROUTER_PG_HOST, ROUTER_PG_PORT)
            conn.autocommit = True
        except Exception:
            with lock:
                result.errors += queries_per_worker
            return

        sql = f"SELECT {SHARD_KEY}, price FROM {table}"
        try:
            for _ in range(queries_per_worker):
                start = time.perf_counter()
                try:
                    rows = exec_sql(conn, sql)
                    local_latencies.append((time.perf_counter() - start) * 1000)
                    local_ops += 1
                    if not rows:
                        local_errors += 1
                except Exception:
                    local_errors += 1
        finally:
            conn.close()

        with lock:
            result.operations += local_ops
            result.errors += local_errors
            result.latencies_ms.extend(local_latencies)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(worker, range(workers)))
    result.duration_s = time.perf_counter() - started
    return result


def fetch_metrics_snippet(url: str) -> str | None:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except OSError as e:
        return f"(metrics unavailable: {e})"

    lines = []
    for line in body.splitlines():
        if line.startswith("quest_router_requests_total"):
            lines.append(line)
    return "\n".join(lines) if lines else "(no quest_router_requests_total in response)"


def print_result(result: LoadResult) -> None:
    print(f"\n=== {result.name} ===")
    print(f"operations : {result.operations}")
    print(f"errors     : {result.errors}")
    print(f"duration   : {result.duration_s:.2f}s")
    print(f"throughput : {result.throughput():.1f} ops/s")
    lat = result.latency_summary()
    if lat:
        print(
            f"latency ms : p50={lat['p50']:.2f} "
            f"p95={lat['p95']:.2f} p99={lat['p99']:.2f} max={lat['max']:.2f}"
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="quest-router load test")
    p.add_argument(
        "--mode",
        choices=["ilp", "pg-simple", "pg-extended", "pg-table", "scan", "all"],
        default="all",
        help="which workload to run",
    )
    p.add_argument("--table", default=DEFAULT_TABLE, help="QuestDB table name")
    p.add_argument("--setup", action="store_true", help="create table on shards + seed ILP data")
    p.add_argument("--symbols", type=int, default=32, help="distinct shard keys")
    p.add_argument("--seed-lines", type=int, default=5, help="ILP lines per symbol when --setup")
    p.add_argument("--ilp-workers", type=int, default=8)
    p.add_argument("--ilp-lines", type=int, default=20000, help="total ILP lines to write")
    p.add_argument("--ilp-batch", type=int, default=200, help="lines per socket send batch")
    p.add_argument("--pg-workers", type=int, default=4)
    p.add_argument("--pg-queries", type=int, default=1000, help="queries per PG worker")
    p.add_argument("--metrics", action="store_true", help="print prometheus counters after run")
    p.add_argument("--metrics-url", default=METRICS_URL)
    p.add_argument("--skip-wait", action="store_true", help="skip TCP readiness checks")
    return p.parse_args()


def main() -> int:
    require_psycopg2()
    args = parse_args()
    symbols = [symbol_name(i) for i in range(args.symbols)]

    print("== quest-router load test ==")
    print(f"router ILP {ROUTER_ILP_HOST}:{ROUTER_ILP_PORT}")
    print(f"router PG  {ROUTER_PG_HOST}:{ROUTER_PG_PORT}")
    print(f"shard key  {SHARD_KEY}")
    print(f"symbols    {args.symbols}")

    if not args.skip_wait:
        print("\nWaiting for router...")
        wait_for_tcp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "router ILP")
        wait_for_tcp(ROUTER_PG_HOST, ROUTER_PG_PORT, "router PG")

    if args.setup:
        print("\nSetup: tables on shards...")
        setup_table_on_shards(args.table)
        print("  ok  tables created")
        print(f"Seeding {len(symbols)} symbols x {args.seed_lines} lines via ILP...")
        seeded = seed_ilp(args.table, symbols, args.seed_lines)
        print(f"  ok  seeded {seeded} lines")

    results: list[LoadResult] = []
    modes = (
        ["ilp", "pg-simple", "pg-extended", "pg-table", "scan"]
        if args.mode == "all"
        else [args.mode]
    )

    for mode in modes:
        if mode == "ilp":
            results.append(
                run_ilp_load(
                    args.table,
                    args.ilp_workers,
                    args.ilp_lines,
                    args.ilp_batch,
                    args.symbols,
                )
            )
        elif mode == "pg-simple":
            if not args.setup and args.mode != "all":
                print("\nNote: pg modes benefit from --setup to seed read data.")
            results.append(
                run_pg_simple_load(
                    args.table, args.pg_workers, args.pg_queries, symbols
                )
            )
        elif mode == "pg-extended":
            results.append(
                run_pg_extended_load(
                    args.table, args.pg_workers, args.pg_queries, symbols
                )
            )
        elif mode == "pg-table":
            results.append(
                run_pg_table_route_load(args.table, args.pg_workers, args.pg_queries)
            )
        elif mode == "scan":
            if not args.setup and args.mode != "all":
                print("\nNote: scan mode benefits from --setup to seed read data.")
            results.append(
                run_pg_scan_load(args.table, args.pg_workers, max(1, args.pg_queries // 10))
            )

    for r in results:
        print_result(r)

    total_ops = sum(r.operations for r in results)
    total_errors = sum(r.errors for r in results)
    print("\n=== summary ===")
    print(f"total operations : {total_ops}")
    print(f"total errors     : {total_errors}")

    if args.metrics:
        print("\n=== prometheus (quest_router_requests_total) ===")
        print(fetch_metrics_snippet(args.metrics_url))

    return 1 if total_errors else 0


if __name__ == "__main__":
    sys.exit(main())
