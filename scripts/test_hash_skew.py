#!/usr/bin/env python3
"""
Measure consistent-hash skew after ingesting N symbols × M records via the router.

Writes ILP lines through quest-router, then counts rows on each shard directly.
Reports per-shard record/symbol distribution and skew metrics (CV, max/min ratio,
chi-square vs uniform).

Usage:
  docker compose up -d --build
  pip install -r scripts/requirements.txt
  python scripts/test_hash_skew.py --init --symbols 256 --records 100

Examples:
  python scripts/test_hash_skew.py --init --symbols 1024 --records 50
  python scripts/test_hash_skew.py --symbols 64 --records 10 --skip-ingest
  python scripts/test_hash_skew.py --init --symbols 32 --records 1 --show-symbols
"""

from __future__ import annotations

import argparse
import math
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
    count_table_on_shard,
    exec_sql,
    pg_connect,
    require_psycopg2,
    send_ilp,
    symbol_name,
    wait_for_tcp,
)

DEFAULT_TABLE = "hash_skew_trades"


@dataclass(frozen=True)
class SkewReport:
    shard_counts: list[int]
    symbol_counts: list[int]
    total_records: int
    total_symbols: int

    @property
    def num_shards(self) -> int:
        return len(self.shard_counts)

    def mean_records(self) -> float:
        return self.total_records / self.num_shards if self.num_shards else 0.0

    def mean_symbols(self) -> float:
        return self.total_symbols / self.num_shards if self.num_shards else 0.0

    def coefficient_of_variation(self, counts: list[int]) -> float:
        if not counts:
            return 0.0
        mean = sum(counts) / len(counts)
        if mean == 0:
            return 0.0
        variance = sum((c - mean) ** 2 for c in counts) / len(counts)
        return math.sqrt(variance) / mean

    def max_min_ratio(self, counts: list[int]) -> float:
        if not counts:
            return 0.0
        low = min(counts)
        if low == 0:
            return float("inf")
        return max(counts) / low

    def chi_square_uniform(self, counts: list[int]) -> float:
        """Chi-square statistic vs equal expected counts."""
        n = len(counts)
        total = sum(counts)
        if n == 0 or total == 0:
            return 0.0
        expected = total / n
        return sum((c - expected) ** 2 / expected for c in counts)

    def imbalance_pct(self, counts: list[int]) -> float:
        """(max - min) / mean as a percentage."""
        mean = sum(counts) / len(counts) if counts else 0.0
        if mean == 0:
            return 0.0
        return (max(counts) - min(counts)) / mean * 100.0


def ddl_for_table(table: str) -> str:
    return f"""
        CREATE TABLE {table} (
            symbol SYMBOL,
            price DOUBLE,
            ts TIMESTAMP
        ) timestamp(ts) PARTITION BY DAY;
    """


def init_table(table: str) -> None:
    for port in SHARD_PG_PORTS:
        with pg_connect("127.0.0.1", port) as conn:
            conn.autocommit = True
            exec_sql(conn, f"DROP TABLE IF EXISTS {table}")
            exec_sql(conn, ddl_for_table(table))
    print(f"  ok  table {table} recreated on {len(SHARD_PG_PORTS)} shard(s)")


def make_ilp_line(table: str, symbol: str, price: float, ts_ns: int) -> str:
    return f"{table},{SHARD_KEY}={symbol} price={price} {ts_ns}\n"


def ingest(
    table: str,
    num_symbols: int,
    records_per_symbol: int,
    batch_size: int,
) -> int:
    """Write num_symbols × records_per_symbol lines via router ILP."""
    base_ts = time.time_ns()
    batch: list[str] = []
    sent = 0
    for sym_idx in range(num_symbols):
        sym = symbol_name(sym_idx)
        for rec in range(records_per_symbol):
            ts = base_ts + sent
            price = 100.0 + (sym_idx % 50) + rec * 0.01
            batch.append(make_ilp_line(table, sym, price, ts))
            sent += 1
            if len(batch) >= batch_size:
                send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "".join(batch))
                batch.clear()
    if batch:
        send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "".join(batch))
    return sent


def distinct_symbols_on_shard(port: int, table: str) -> set[str]:
    with pg_connect("127.0.0.1", port) as conn:
        conn.autocommit = True
        rows = exec_sql(conn, f"SELECT DISTINCT {SHARD_KEY} FROM {table}")
    return {str(row[0]) for row in rows}


def collect_report(table: str, num_symbols: int) -> SkewReport:
    shard_counts = [count_table_on_shard(port, table) for port in SHARD_PG_PORTS]
    symbol_counts: list[int] = []
    for port in SHARD_PG_PORTS:
        with pg_connect("127.0.0.1", port) as conn:
            conn.autocommit = True
            rows = exec_sql(
                conn,
                f"SELECT count(DISTINCT {SHARD_KEY}) FROM {table}",
            )
        symbol_counts.append(int(rows[0][0]))
    return SkewReport(
        shard_counts=shard_counts,
        symbol_counts=symbol_counts,
        total_records=sum(shard_counts),
        total_symbols=num_symbols,
    )


def verify_symbol_cohesion(table: str, num_symbols: int) -> int:
    """Each symbol must live on exactly one shard."""
    failures = 0
    symbol_shards: dict[str, list[int]] = {}
    for shard_idx, port in enumerate(SHARD_PG_PORTS):
        for sym in distinct_symbols_on_shard(port, table):
            symbol_shards.setdefault(sym, []).append(shard_idx)
    missing = num_symbols - len(symbol_shards)
    if missing > 0:
        print(f"  WARN  {missing} symbol(s) missing from shard scans")
    split = [sym for sym, shards in symbol_shards.items() if len(shards) > 1]
    if split:
        print(f"  FAIL  {len(split)} symbol(s) span multiple shards (e.g. {split[:3]})")
        failures += 1
    else:
        print(f"  ok  all {len(symbol_shards)} symbols co-located on a single shard")
    return failures


def print_report(report: SkewReport, expected_records: int) -> None:
    print("\n=== shard distribution ===")
    for i, (recs, syms) in enumerate(zip(report.shard_counts, report.symbol_counts)):
        pct = recs / report.total_records * 100 if report.total_records else 0
        print(f"  shard-{i}: {recs:>8} records ({pct:5.1f}%)  |  {syms:>5} distinct symbols")

    print(f"\n  total records : {report.total_records} (expected {expected_records})")
    print(f"  total symbols : {report.total_symbols}")
    print(f"  num shards    : {report.num_shards}")

    print("\n=== skew metrics (records) ===")
    rec_cv = report.coefficient_of_variation(report.shard_counts)
    rec_ratio = report.max_min_ratio(report.shard_counts)
    rec_chi = report.chi_square_uniform(report.shard_counts)
    rec_imb = report.imbalance_pct(report.shard_counts)
    print(f"  mean per shard : {report.mean_records():.1f}")
    print(f"  CV             : {rec_cv:.4f}  (0 = perfectly even)")
    print(f"  max/min ratio  : {rec_ratio:.3f}")
    print(f"  imbalance      : {rec_imb:.1f}%  ((max-min)/mean)")
    print(f"  chi-square     : {rec_chi:.2f}  (lower = closer to uniform)")

    print("\n=== skew metrics (symbols) ===")
    sym_cv = report.coefficient_of_variation(report.symbol_counts)
    sym_ratio = report.max_min_ratio(report.symbol_counts)
    sym_chi = report.chi_square_uniform(report.symbol_counts)
    sym_imb = report.imbalance_pct(report.symbol_counts)
    print(f"  mean per shard : {report.mean_symbols():.1f}")
    print(f"  CV             : {sym_cv:.4f}")
    print(f"  max/min ratio  : {sym_ratio:.3f}")
    print(f"  imbalance      : {sym_imb:.1f}%")
    print(f"  chi-square     : {sym_chi:.2f}")


def print_symbol_map(table: str, num_symbols: int) -> None:
    print("\n=== symbol → shard map (first 32) ===")
    placement: dict[str, int] = {}
    for shard_idx, port in enumerate(SHARD_PG_PORTS):
        for sym in sorted(distinct_symbols_on_shard(port, table)):
            placement[sym] = shard_idx
    for sym_idx in range(min(32, num_symbols)):
        sym = symbol_name(sym_idx)
        shard = placement.get(sym, "?")
        print(f"  {sym} -> shard-{shard}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="quest-router consistent-hash skew test")
    p.add_argument("--table", default=DEFAULT_TABLE)
    p.add_argument("--init", action="store_true", help="drop + recreate table on each shard")
    p.add_argument("--symbols", type=int, default=256, help="distinct shard keys (N)")
    p.add_argument("--records", type=int, default=100, help="ILP records per symbol (M)")
    p.add_argument("--batch-size", type=int, default=500, help="ILP lines per TCP send")
    p.add_argument("--settle-s", type=float, default=2.0, help="wait after ingest")
    p.add_argument("--skip-ingest", action="store_true", help="only measure existing data")
    p.add_argument("--skip-wait", action="store_true")
    p.add_argument("--show-symbols", action="store_true", help="print symbol→shard mapping")
    p.add_argument(
        "--max-cv",
        type=float,
        default=0.15,
        help="fail if record CV exceeds this (default 0.15 for ~256 symbols / 2 shards)",
    )
    return p.parse_args()


def main() -> int:
    require_psycopg2()
    args = parse_args()
    expected = args.symbols * args.records

    print("== quest-router hash skew test ==")
    print(f"table   : {args.table}")
    print(f"symbols : {args.symbols}  (N)")
    print(f"records : {args.records} per symbol  (M)")
    print(f"expect  : {expected} total rows")
    print(f"shards  : {len(SHARD_PG_PORTS)}  ({', '.join(str(p) for p in SHARD_PG_PORTS)})")

    if not args.skip_wait:
        print("\n1. Waiting for services...")
        wait_for_tcp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "router ILP")
        wait_for_tcp(ROUTER_PG_HOST, ROUTER_PG_PORT, "router PG")
        for port in SHARD_PG_PORTS:
            wait_for_tcp("127.0.0.1", port, f"shard PG :{port}")

    if args.init:
        print("\n2. Initializing table on shards...")
        init_table(args.table)

    failures = 0

    if not args.skip_ingest:
        print(f"\n3. Ingesting {expected} records via router ILP...")
        sent = ingest(args.table, args.symbols, args.records, args.batch_size)
        print(f"  ok  sent {sent} lines")
        print(f"\nWaiting {args.settle_s:.1f}s for QuestDB to settle...")
        time.sleep(args.settle_s)
    else:
        print("\n3. Skipping ingest (--skip-ingest)")

    print("\n4. Collecting shard counts...")
    report = collect_report(args.table, args.symbols)
    print_report(report, expected)

    if report.total_records < expected:
        print(f"\n  FAIL  only {report.total_records}/{expected} records visible on shards")
        failures += 1

    print("\n5. Verifying symbol co-location...")
    failures += verify_symbol_cohesion(args.table, args.symbols)

    rec_cv = report.coefficient_of_variation(report.shard_counts)
    if rec_cv > args.max_cv:
        print(f"\n  FAIL  record CV {rec_cv:.4f} exceeds --max-cv {args.max_cv}")
        failures += 1
    else:
        print(f"\n  ok  record CV {rec_cv:.4f} within --max-cv {args.max_cv}")

    if args.show_symbols:
        print_symbol_map(args.table, args.symbols)

    print()
    if failures:
        print(f"FAILED ({failures} check(s))")
        return 1
    print("PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
