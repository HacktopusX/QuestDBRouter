#!/usr/bin/env python3
"""
OHLCV ingest test for quest-router.

Creates a table with explicit open/high/low/close/volume columns on each shard,
then writes OHLCV bars via ILP through the router.

Modes:
  smoke  — schema check + one bar per symbol per interval (1s/1m/1h/1d)
  chart  — candlestick-friendly history (default: 288 x 5m bars, 24h window)
  stream — sustained ingest at 100 OHLCV records/sec (one tick per send, wall-clock timestamps)
  push   — burst ingest of 10_000 OHLCV records (configurable)

Chart mode writes evenly spaced candles ending at "now" with realistic bodies/wicks
so Grafana / QuestDB candlestick panels are not hairline-thin.

Usage:
  docker compose up -d --build
  pip install -r scripts/requirements.txt
  python scripts/test_ohlcv.py --init --mode chart
  python scripts/test_ohlcv.py --mode stream --duration 30
  python scripts/test_ohlcv.py --mode push

Symbols are fixed to btc-usdt and eth-usdt. Pass --init to create the table on shards.

Environment overrides:
  ROUTER_ILP_HOST, ROUTER_ILP_PORT   (default localhost:9009)
  ROUTER_PG_HOST, ROUTER_PG_PORT     (default localhost:8812)
  SHARD_PG_PORTS                     (default 18812,18822)
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (
    IlpWriter,
    ROUTER_ILP_HOST,
    ROUTER_ILP_PORT,
    ROUTER_PG_HOST,
    ROUTER_PG_PORT,
    SHARD_PG_PORTS,
    exec_sql,
    pg_connect,
    require_psycopg2,
    send_ilp,
    wait_for_tcp,
)

TABLE = "router_test_ohlcv"

COL_OPEN = '"open"'
COL_HIGH = '"high"'
COL_LOW = '"low"'
COL_CLOSE = '"close"'
COL_INTERVAL = '"interval"'
OHLCV_COLUMNS = ("open", "high", "low", "close", "volume")
TABLE_COLUMNS = ("symbol", "interval", *OHLCV_COLUMNS)

DEFAULT_STREAM_RATE = 100
DEFAULT_PUSH_COUNT = 10_000
DEFAULT_STREAM_DURATION_S = 10.0
DEFAULT_BATCH_SIZE = 500
DEFAULT_STREAM_BATCH_SIZE = 1
DEFAULT_STREAM_INTERVAL = "1s"

SYMBOLS = ("btc-usdt", "eth-usdt")
SYMBOL_BASE_PRICES: dict[str, float] = {
    "btc-usdt": 42_000.0,
    "eth-usdt": 2_500.0,
}

CHART_INTERVAL = "5m"
CHART_BARS = 288  # 24h of 5-minute candles

# Load-test rotation (1s / 1m / 1h / 1d). Chart mode uses 5m separately.
INTERVALS: tuple[tuple[str, int], ...] = (
    ("1s", 1_000_000_000),
    ("1m", 60 * 1_000_000_000),
    ("1h", 3_600 * 1_000_000_000),
    ("1d", 86_400 * 1_000_000_000),
)
CHART_INTERVALS: tuple[tuple[str, int], ...] = (
    ("1m", 60 * 1_000_000_000),
    ("5m", 5 * 60 * 1_000_000_000),
    ("1h", 3_600 * 1_000_000_000),
    ("1d", 86_400 * 1_000_000_000),
)
INTERVAL_STEP_NS: dict[str, int] = {
    name: ns for name, ns in (*INTERVALS, *CHART_INTERVALS)
}
INTERVAL_NAMES = tuple(name for name, _ in INTERVALS)


@dataclass(frozen=True)
class OhlcvBar:
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class IngestResult:
    name: str
    records: int = 0
    errors: int = 0
    duration_s: float = 0.0

    def throughput(self) -> float:
        if self.duration_s <= 0:
            return 0.0
        return self.records / self.duration_s


SMOKE_BARS = [
    OhlcvBar("btc-usdt", open=100.0, high=128.0, low=92.0, close=118.0, volume=1500.0),
    OhlcvBar("eth-usdt", open=2000.0, high=2085.0, low=1940.0, close=2060.0, volume=800.0),
]


@dataclass
class CandleGenerator:
    """Random-walk OHLC with visible bodies and wicks for candlestick charts."""

    base_price: float = SYMBOL_BASE_PRICES["btc-usdt"]
    body_pct: float = 0.018
    wick_pct: float = 0.012
    _last_close: float = field(init=False)

    def __post_init__(self) -> None:
        self._last_close = self.base_price

    def next_bar(self, seq: int, symbol_seed: int = 0) -> tuple[float, float, float, float, float]:
        phase = (seq + symbol_seed * 19) * 0.37
        trend = math.sin(phase) * self.body_pct * self._last_close
        noise = math.cos(phase * 1.63) * self.body_pct * 0.45 * self._last_close

        open_ = self._last_close
        close = open_ + trend + noise
        body_top = max(open_, close)
        body_bot = min(open_, close)

        upper_wick = (0.4 + abs(math.sin(phase * 2.1))) * self.wick_pct * open_
        lower_wick = (0.4 + abs(math.cos(phase * 1.7))) * self.wick_pct * open_
        high = body_top + upper_wick
        low = body_bot - lower_wick

        volume = 800.0 + (seq % 40) * 120.0 + abs(trend) * 2.0
        self._last_close = close
        return open_, high, low, close, volume


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def default_symbols() -> list[str]:
    return list(SYMBOLS)


def aligned_anchor_ns(interval_name: str) -> int:
    """Snap anchor to a clean boundary for the interval (UTC), at or before now."""
    now_s = int(time.time())
    if interval_name == "1s":
        base_s = now_s
    elif interval_name == "1m":
        base_s = now_s - (now_s % 60)
    elif interval_name == "5m":
        base_s = now_s - (now_s % 300)
    elif interval_name == "1h":
        base_s = now_s - (now_s % 3600)
    elif interval_name == "1d":
        base_s = now_s - (now_s % 86_400)
    else:
        base_s = now_s
    return base_s * 1_000_000_000


def ts_ns_backward(interval_name: str, bar_index: int, total_bars: int) -> int:
    """Evenly space candles backward from now — one bar width per step."""
    step_ns = INTERVAL_STEP_NS[interval_name]
    end_ns = aligned_anchor_ns(interval_name)
    return end_ns - (total_bars - 1 - bar_index) * step_ns


def interval_for_symbol_index(
    symbol_index: int,
    force_interval: str | None = None,
) -> tuple[str, int]:
    if force_interval is not None:
        return force_interval, INTERVAL_STEP_NS[force_interval]
    return INTERVALS[symbol_index % len(INTERVALS)]


def ts_ns_for_record(
    seq: int,
    num_symbols: int,
    bars_per_symbol: int,
    force_interval: str | None = None,
) -> tuple[int, int, str]:
    """Return (timestamp_ns, symbol_index, interval_name) for a record sequence."""
    symbol_index = seq % num_symbols
    bar_index = seq // num_symbols
    interval_name, _ = interval_for_symbol_index(symbol_index, force_interval)
    ts_ns = ts_ns_backward(interval_name, bar_index, bars_per_symbol)
    return ts_ns, symbol_index, interval_name


def ts_ns_for_live_stream(interval_name: str) -> int:
    """Wall-clock timestamp snapped to the candle interval (updates same bucket in-place)."""
    now_ns = time.time_ns()
    step_ns = INTERVAL_STEP_NS[interval_name]
    return (now_ns // step_ns) * step_ns


def format_ohlcv_line(
    symbol: str,
    interval_name: str,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    ts_ns: int,
) -> str:
    return (
        f"{TABLE},symbol={symbol},interval={interval_name} "
        f"open={open_},high={high},low={low},close={close},volume={volume} {ts_ns}\n"
    )


def make_generators(symbols: list[str]) -> dict[str, CandleGenerator]:
    return {
        sym: CandleGenerator(base_price=SYMBOL_BASE_PRICES.get(sym, 100.0))
        for sym in symbols
    }


def setup_table_on_shards() -> None:
    ddl = f"""
        CREATE TABLE {TABLE} (
            symbol SYMBOL,
            interval SYMBOL,
            {COL_OPEN} DOUBLE,
            {COL_HIGH} DOUBLE,
            {COL_LOW} DOUBLE,
            {COL_CLOSE} DOUBLE,
            volume DOUBLE,
            ts TIMESTAMP
        ) timestamp(ts) PARTITION BY DAY;
    """
    for port in SHARD_PG_PORTS:
        with pg_connect("127.0.0.1", port) as conn:
            conn.autocommit = True
            exec_sql(conn, f"DROP TABLE IF EXISTS {TABLE}")
            exec_sql(conn, ddl)
    print("  ok  OHLCV table recreated on all shards")


def flush_ilp_batch(batch: list[str], result: IngestResult) -> None:
    if not batch:
        return
    try:
        send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "".join(batch))
        result.records += len(batch)
    except OSError:
        result.errors += len(batch)


def run_stream(
    symbols: list[str],
    rate_per_sec: int,
    duration_s: float,
    batch_size: int,
    force_interval: str | None,
) -> IngestResult:
    """Sustained ingest at a fixed records/sec rate with wall-clock timestamps."""
    result = IngestResult(name="stream")
    num_symbols = len(symbols)
    generators = make_generators(symbols)
    target_total = int(rate_per_sec * duration_s)
    interval_name = force_interval or DEFAULT_STREAM_INTERVAL
    pacing_s = 1.0 / rate_per_sec
    batch: list[str] = []
    started = time.perf_counter()
    deadline = started + duration_s

    print(
        f"  streaming {target_total} records "
        f"({rate_per_sec}/s for {duration_s:.1f}s, {num_symbols} symbols)"
    )
    print(f"  candle interval: {interval_name} (live wall-clock, batch={batch_size})")

    seq = 0
    next_send = started
    try:
        with IlpWriter(ROUTER_ILP_HOST, ROUTER_ILP_PORT) as writer:
            while seq < target_total and time.perf_counter() < deadline:
                sleep_for = next_send - time.perf_counter()
                if sleep_for > 0:
                    time.sleep(sleep_for)

                symbol_index = seq % num_symbols
                sym = symbols[symbol_index]
                bar_index = seq // num_symbols
                ts_ns = ts_ns_for_live_stream(interval_name)
                o, h, l, c, v = generators[sym].next_bar(bar_index, symbol_index)
                line = format_ohlcv_line(sym, interval_name, o, h, l, c, v, ts_ns)
                seq += 1
                next_send += pacing_s

                if batch_size <= 1:
                    try:
                        writer.send_line(line)
                        result.records += 1
                    except OSError:
                        result.errors += 1
                else:
                    batch.append(line)
                    if len(batch) >= batch_size:
                        try:
                            writer.send_batch(batch)
                            result.records += len(batch)
                        except OSError:
                            result.errors += len(batch)
                        batch.clear()

            if batch:
                try:
                    writer.send_batch(batch)
                    result.records += len(batch)
                except OSError:
                    result.errors += len(batch)
    except OSError:
        result.errors += max(0, target_total - result.records)

    result.duration_s = time.perf_counter() - started
    return result


def run_push(
    total_records: int,
    symbols: list[str],
    batch_size: int,
    force_interval: str | None,
) -> IngestResult:
    """Burst ingest — send all records as fast as possible."""
    result = IngestResult(name="push")
    num_symbols = len(symbols)
    generators = make_generators(symbols)
    bars_per_symbol = max(1, (total_records + num_symbols - 1) // num_symbols)
    batch: list[str] = []

    print(f"  pushing {total_records} records ({num_symbols} symbols)")
    if force_interval:
        step_s = INTERVAL_STEP_NS[force_interval] // 1_000_000_000
        span_s = step_s * bars_per_symbol
        print(
            f"  candle interval: {force_interval}, "
            f"~{bars_per_symbol} bars/symbol, ~{span_s / 3600:.1f}h history each"
        )
    else:
        print("  candle intervals: 1s / 1m / 1h / 1d rotation (backward from now)")

    started = time.perf_counter()
    for seq in range(total_records):
        ts_ns, symbol_index, interval_name = ts_ns_for_record(
            seq, num_symbols, bars_per_symbol, force_interval
        )
        sym = symbols[symbol_index]
        bar_index = seq // num_symbols
        o, h, l, c, v = generators[sym].next_bar(bar_index, symbol_index)
        batch.append(format_ohlcv_line(sym, interval_name, o, h, l, c, v, ts_ns))
        if len(batch) >= batch_size:
            flush_ilp_batch(batch, result)
            batch.clear()
    flush_ilp_batch(batch, result)
    result.duration_s = time.perf_counter() - started
    return result


def run_chart(
    bars: int,
    symbols: list[str],
    interval_name: str,
    batch_size: int,
) -> IngestResult:
    """Seed candlestick-friendly history for dashboarding."""
    result = IngestResult(name="chart")
    generators = make_generators(symbols)
    batch: list[str] = []
    step_s = INTERVAL_STEP_NS[interval_name] // 1_000_000_000
    span_h = (bars * step_s) / 3600

    print(
        f"  chart seed: {bars} x {interval_name} candles, "
        f"{len(symbols)} symbol(s), ~{span_h:.1f}h window ending now"
    )

    started = time.perf_counter()
    for sym_idx, sym in enumerate(symbols):
        for bar_index in range(bars):
            ts_ns = ts_ns_backward(interval_name, bar_index, bars)
            o, h, l, c, v = generators[sym].next_bar(bar_index, sym_idx)
            batch.append(format_ohlcv_line(sym, interval_name, o, h, l, c, v, ts_ns))
            if len(batch) >= batch_size:
                flush_ilp_batch(batch, result)
                batch.clear()
    flush_ilp_batch(batch, result)
    result.duration_s = time.perf_counter() - started
    return result


def print_chart_query(symbol: str, interval_name: str) -> None:
    sym = sql_string(symbol)
    iv = sql_string(interval_name)
    print("\nQuestDB / Grafana candlestick query:")
    print(
        f"  SELECT ts, {COL_OPEN}, {COL_HIGH}, {COL_LOW}, {COL_CLOSE}, volume\n"
        f"  FROM {TABLE}\n"
        f"  WHERE symbol = {sym} AND {COL_INTERVAL} = {iv}\n"
        f"  ORDER BY ts"
    )


def write_smoke_bars() -> None:
    lines: list[str] = []
    for bar_idx, bar in enumerate(SMOKE_BARS):
        for interval_name, _ in INTERVALS:
            ts = ts_ns_backward(interval_name, 0, 1)
            lines.append(
                format_ohlcv_line(
                    bar.symbol,
                    interval_name,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    ts + bar_idx,
                )
            )
    send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "".join(lines))
    print(
        f"  ok  wrote {len(lines)} smoke bars via router "
        f"({len(SMOKE_BARS)} symbols x {len(INTERVALS)} intervals)"
    )


def read_bar_via_router(symbol: str, interval_name: str | None = None) -> OhlcvBar | None:
    sym = sql_string(symbol)
    interval_filter = ""
    if interval_name is not None:
        interval_filter = f" AND {COL_INTERVAL} = {sql_string(interval_name)}"
    with pg_connect(ROUTER_PG_HOST, ROUTER_PG_PORT) as conn:
        conn.autocommit = True
        rows = exec_sql(
            conn,
            f"""
            SELECT symbol, {COL_OPEN}, {COL_HIGH}, {COL_LOW}, {COL_CLOSE}, volume
            FROM {TABLE}
            WHERE symbol = {sym}{interval_filter}
            ORDER BY ts DESC
            LIMIT 1
            """,
        )
    if not rows:
        return None
    sym_val, open_, high, low, close, volume = rows[0]
    return OhlcvBar(
        symbol=str(sym_val),
        open=float(open_),
        high=float(high),
        low=float(low),
        close=float(close),
        volume=float(volume),
    )


def list_columns_on_shard(port: int) -> list[str]:
    with pg_connect("127.0.0.1", port) as conn:
        conn.autocommit = True
        rows = exec_sql(conn, f"SHOW COLUMNS FROM {TABLE}")
    return [str(row[0]).lower() for row in rows]


def count_total_via_router() -> int:
    with pg_connect(ROUTER_PG_HOST, ROUTER_PG_PORT) as conn:
        conn.autocommit = True
        rows = exec_sql(conn, f"SELECT count() FROM {TABLE}")
    return int(rows[0][0])


def count_by_interval_via_router() -> dict[str, int]:
    with pg_connect(ROUTER_PG_HOST, ROUTER_PG_PORT) as conn:
        conn.autocommit = True
        rows = exec_sql(
            conn,
            f"SELECT {COL_INTERVAL}, count() FROM {TABLE} "
            f"GROUP BY {COL_INTERVAL} ORDER BY {COL_INTERVAL}",
        )
    return {str(interval): int(count) for interval, count in rows}


def verify_interval_spread(required: tuple[str, ...] | None = None) -> int:
    """Confirm expected interval buckets received rows after load tests."""
    check = required or INTERVAL_NAMES
    failures = 0
    print(f"\nVerifying interval spread ({' / '.join(check)})...")
    try:
        counts = count_by_interval_via_router()
    except Exception as exc:
        print(f"  FAIL  interval count query: {exc}")
        return 1

    for name in check:
        got = counts.get(name, 0)
        print(f"  interval {name}: {got} rows")
        if got == 0:
            print(f"  FAIL  no rows for interval {name}")
            failures += 1

    if not failures:
        print("  ok  all interval buckets populated")
    return failures


def count_on_shard(port: int) -> int:
    with pg_connect("127.0.0.1", port) as conn:
        conn.autocommit = True
        rows = exec_sql(conn, f"SELECT count() FROM {TABLE}")
    return int(rows[0][0])


def bars_match(expected: OhlcvBar, got: OhlcvBar) -> bool:
    return (
        got.symbol == expected.symbol
        and abs(got.open - expected.open) <= 1e-6
        and abs(got.high - expected.high) <= 1e-6
        and abs(got.low - expected.low) <= 1e-6
        and abs(got.close - expected.close) <= 1e-6
        and abs(got.volume - expected.volume) <= 1e-6
    )


def print_ingest_result(result: IngestResult) -> None:
    print(f"\n=== {result.name} ===")
    print(f"records    : {result.records}")
    print(f"errors     : {result.errors}")
    print(f"duration   : {result.duration_s:.2f}s")
    print(f"throughput : {result.throughput():.1f} records/s")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="quest-router OHLCV ingest test")
    p.add_argument(
        "--init",
        action="store_true",
        help="create router_test_ohlcv on each shard (drop + recreate)",
    )
    p.add_argument(
        "--mode",
        choices=["smoke", "chart", "stream", "push"],
        default="chart",
        help="chart=candlestick seed (default), smoke=schema check, stream, push",
    )
    p.add_argument(
        "--chart-bars",
        type=int,
        default=CHART_BARS,
        help="candles per symbol in chart mode",
    )
    p.add_argument(
        "--interval",
        choices=list(INTERVAL_STEP_NS.keys()),
        default=None,
        help="force one candle width for stream/push (recommended: 5m)",
    )
    p.add_argument(
        "--stream-rate",
        type=int,
        default=DEFAULT_STREAM_RATE,
        help="records per second in stream mode",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=DEFAULT_STREAM_DURATION_S,
        help="stream mode run time in seconds",
    )
    p.add_argument(
        "--push-count",
        type=int,
        default=DEFAULT_PUSH_COUNT,
        help="total records in push mode",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help=f"ILP lines per TCP send (stream default {DEFAULT_STREAM_BATCH_SIZE}, push/chart default {DEFAULT_BATCH_SIZE})",
    )
    p.add_argument("--skip-wait", action="store_true", help="skip TCP readiness checks")
    p.add_argument(
        "--settle-s",
        type=float,
        default=2.0,
        help="seconds to wait after ingest before count verification",
    )
    return p.parse_args()


def run_smoke() -> int:
    failures = 0

    print("\n3. Verifying table columns exist on shard-0...")
    columns = list_columns_on_shard(SHARD_PG_PORTS[0])
    missing = [c for c in TABLE_COLUMNS if c not in columns]
    if missing:
        print(f"  FAIL  missing columns: {missing} (have {columns})")
        failures += 1
    else:
        print(f"  ok  columns present: {', '.join(TABLE_COLUMNS)}")

    print("\n4. Writing smoke bars via router (ILP)...")
    write_smoke_bars()
    time.sleep(2)

    print("\n5. Reading OHLCV per interval via router (PG wire)...")
    for expected in SMOKE_BARS:
        for interval_name, _ in INTERVALS:
            got = read_bar_via_router(expected.symbol, interval_name)
            if got is None:
                print(f"  FAIL  no row for {expected.symbol} interval={interval_name}")
                failures += 1
                continue
            if not bars_match(expected, got):
                print(
                    f"  FAIL  {expected.symbol}@{interval_name}: "
                    f"expected open={expected.open} high={expected.high} "
                    f"low={expected.low} close={expected.close} volume={expected.volume}; "
                    f"got open={got.open} high={got.high} "
                    f"low={got.low} close={got.close} volume={got.volume}"
                )
                failures += 1
            else:
                print(
                    f"  ok  {got.symbol}@{interval_name} "
                    f"open={got.open} high={got.high} low={got.low} "
                    f"close={got.close} volume={got.volume}"
                )

    failures += verify_interval_spread()

    expected_per_symbol = len(INTERVALS)
    print(
        f"\n6. Shard placement ({expected_per_symbol} bars per symbol, one per interval)..."
    )
    for expected in SMOKE_BARS:
        sym = sql_string(expected.symbol)
        counts = []
        for port in SHARD_PG_PORTS:
            with pg_connect("127.0.0.1", port) as conn:
                conn.autocommit = True
                rows = exec_sql(
                    conn,
                    f"SELECT count() FROM {TABLE} WHERE symbol = {sym}",
                )
            counts.append(int(rows[0][0]))
        total = sum(counts)
        if total != expected_per_symbol:
            print(
                f"  FAIL  {expected.symbol}: shard counts {counts} "
                f"(expected total {expected_per_symbol})"
            )
            failures += 1
        else:
            shard_idx = counts.index(expected_per_symbol)
            print(f"  ok  {expected.symbol} -> shard-{shard_idx} (counts: {counts})")

    return failures


def verify_counts(expected_total: int, settle_s: float) -> int:
    failures = 0
    print(f"\nWaiting {settle_s:.1f}s for QuestDB to settle...")
    time.sleep(settle_s)

    print("\nVerifying row counts...")
    try:
        router_total = count_total_via_router()
    except Exception as exc:
        print(f"  FAIL  router count query: {exc}")
        return 1

    shard_counts = [count_on_shard(port) for port in SHARD_PG_PORTS]
    shard_total = sum(shard_counts)

    print(f"  router total : {router_total}")
    for i, count in enumerate(shard_counts):
        print(f"  shard-{i} total: {count}")
    print(f"  sum shards   : {shard_total}")

    if router_total < expected_total:
        print(f"  FAIL  router count {router_total} < expected {expected_total}")
        failures += 1
    else:
        print(f"  ok  router ingested at least {expected_total} rows")

    if shard_total < expected_total:
        print(f"  FAIL  shard sum {shard_total} < expected {expected_total}")
        failures += 1
    else:
        print(f"  ok  shards hold at least {expected_total} rows combined")

    if shard_counts[0] == 0 or shard_counts[1] == 0:
        print(f"  WARN  uneven shard split {shard_counts}")
    else:
        print(f"  ok  both shards received data")

    return failures


def main() -> int:
    require_psycopg2()
    args = parse_args()

    symbols = default_symbols()
    print("== quest-router OHLCV test ==")
    print(f"mode    : {args.mode}")
    print(f"symbols : {', '.join(symbols)}")
    if args.mode == "chart":
        interval = args.interval or CHART_INTERVAL
        print(f"chart   : {args.chart_bars} x {interval} per symbol")
    elif args.interval:
        print(f"interval: {args.interval} (uniform)")
    print(f"router  : ILP {ROUTER_ILP_HOST}:{ROUTER_ILP_PORT}, PG {ROUTER_PG_HOST}:{ROUTER_PG_PORT}")

    if not args.skip_wait:
        print("\n1. Waiting for services...")
        wait_for_tcp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "router ILP")
        wait_for_tcp(ROUTER_PG_HOST, ROUTER_PG_PORT, "router PG")
        for port in SHARD_PG_PORTS:
            wait_for_tcp("127.0.0.1", port, f"shard PG :{port}")

    if args.init:
        print("\n2. Creating OHLCV table on each shard (direct PG)...")
        setup_table_on_shards()

    failures = 0

    if args.mode == "smoke":
        failures += run_smoke()
    elif args.mode == "chart":
        interval = args.interval or CHART_INTERVAL
        chart_batch = args.batch_size if args.batch_size is not None else DEFAULT_BATCH_SIZE
        expected = args.chart_bars * len(symbols)
        print(f"\n3. Chart seed ({interval}, {args.chart_bars} bars/symbol)...")
        result = run_chart(
            args.chart_bars,
            symbols,
            interval,
            chart_batch,
        )
        print_ingest_result(result)
        for sym in symbols:
            print_chart_query(sym, interval)
        if result.errors:
            failures += 1
        failures += verify_counts(expected, args.settle_s)
        failures += verify_interval_spread((interval,))
    elif args.mode == "stream":
        expected = int(args.stream_rate * args.duration)
        stream_interval = args.interval or DEFAULT_STREAM_INTERVAL
        stream_batch = (
            args.batch_size
            if args.batch_size is not None
            else DEFAULT_STREAM_BATCH_SIZE
        )
        print(f"\n3. Stream ingest ({args.stream_rate} records/s)...")
        result = run_stream(
            symbols,
            args.stream_rate,
            args.duration,
            stream_batch,
            stream_interval,
        )
        print_ingest_result(result)
        if result.errors:
            failures += 1
        failures += verify_counts(expected, args.settle_s)
        failures += verify_interval_spread((stream_interval,))
    elif args.mode == "push":
        push_batch = args.batch_size if args.batch_size is not None else DEFAULT_BATCH_SIZE
        print(f"\n3. Push ingest ({args.push_count} records)...")
        result = run_push(
            args.push_count,
            symbols,
            push_batch,
            args.interval,
        )
        print_ingest_result(result)
        if result.errors:
            failures += 1
        failures += verify_counts(args.push_count, args.settle_s)
        intervals = (args.interval,) if args.interval else INTERVAL_NAMES
        failures += verify_interval_spread(intervals)

    print()
    if failures:
        print(f"FAILED ({failures} check(s))")
        return 1
    print("PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
