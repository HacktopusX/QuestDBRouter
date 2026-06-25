"""Shared helpers for quest-router test scripts."""

from __future__ import annotations

import os
import socket
import time
from collections.abc import Callable

try:
    import psycopg2
except ImportError:
    psycopg2 = None  # type: ignore[assignment,misc]

ROUTER_ILP_HOST = os.environ.get("ROUTER_ILP_HOST", "127.0.0.1")
ROUTER_ILP_PORT = int(os.environ.get("ROUTER_ILP_PORT", "9009"))
ROUTER_PG_HOST = os.environ.get("ROUTER_PG_HOST", "127.0.0.1")
ROUTER_PG_PORT = int(os.environ.get("ROUTER_PG_PORT", "8812"))
METRICS_URL = os.environ.get(
    "METRICS_URL", f"http://{ROUTER_ILP_HOST}:9090/metrics"
)

SHARD_PG_PORTS = [
    int(p.strip())
    for p in os.environ.get("SHARD_PG_PORTS", "18812,18822").split(",")
    if p.strip()
]

PG_USER = os.environ.get("QUESTDB_USER", "admin")
PG_PASSWORD = os.environ.get("QUESTDB_PASSWORD", "quest")
PG_DATABASE = os.environ.get("QUESTDB_DATABASE", "qdb")
SHARD_KEY = os.environ.get("ROUTER_SHARD_KEY", "symbol")


def require_psycopg2() -> None:
    if psycopg2 is None:
        raise RuntimeError("Install dependencies: pip install -r scripts/requirements.txt")


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


def send_ilp(host: str, port: int, payload: str) -> None:
    with socket.create_connection((host, port), timeout=30) as sock:
        sock.sendall(payload.encode("utf-8"))


class IlpWriter:
    """Persistent ILP TCP connection for paced one-line sends."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._sock: socket.socket | None = None

    def __enter__(self) -> IlpWriter:
        self._sock = socket.create_connection((self.host, self.port), timeout=30)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def send_line(self, line: str) -> None:
        if not line.endswith("\n"):
            line = f"{line}\n"
        sock = self._sock
        if sock is None:
            raise OSError("IlpWriter is not connected")
        sock.sendall(line.encode("utf-8"))

    def send_batch(self, lines: list[str]) -> None:
        if not lines:
            return
        sock = self._sock
        if sock is None:
            raise OSError("IlpWriter is not connected")
        sock.sendall("".join(lines).encode("utf-8"))


def pg_connect(host: str, port: int):
    require_psycopg2()
    return psycopg2.connect(
        host=host,
        port=port,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE,
        connect_timeout=15,
    )


def exec_sql(conn, query: str, params: tuple | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        if cur.description:
            return cur.fetchall()
    return []


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def symbol_name(i: int) -> str:
    return f"SYM-{i:04d}"


def count_table_on_shard(port: int, table: str, where: str = "") -> int:
    clause = f" WHERE {where}" if where else ""
    with pg_connect("127.0.0.1", port) as conn:
        conn.autocommit = True
        rows = exec_sql(conn, f"SELECT count() FROM {table}{clause}")
    return int(rows[0][0])


def query_router(sql: str) -> list[tuple]:
    with pg_connect(ROUTER_PG_HOST, ROUTER_PG_PORT) as conn:
        conn.autocommit = True
        return exec_sql(conn, sql)


def query_shard(port: int, sql: str) -> list[tuple]:
    with pg_connect("127.0.0.1", port) as conn:
        conn.autocommit = True
        return exec_sql(conn, sql)


def query_all_shards(sql: str, shard_ports: list[int] | None = None) -> list[list[tuple]]:
    ports = shard_ports or SHARD_PG_PORTS
    return [query_shard(port, sql) for port in ports]


def merge_shard_rows(
    shard_results: list[list[tuple]],
    sort_key: Callable[[tuple], object] | None = None,
) -> list[tuple]:
    merged: list[tuple] = []
    for part in shard_results:
        merged.extend(part)
    if sort_key is not None:
        merged.sort(key=sort_key)
    return merged


def _normalize_cell(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, int):
        return value
    if hasattr(value, "timestamp"):
        return value.isoformat()
    return str(value)


def compare_row_sets(
    label: str,
    router_rows: list[tuple],
    merged_rows: list[tuple],
    *,
    show_samples: int = 3,
) -> int:
    """Return 0 if router and merged shard results are identical."""
    norm_router = [tuple(_normalize_cell(c) for c in row) for row in router_rows]
    norm_merged = [tuple(_normalize_cell(c) for c in row) for row in merged_rows]

    if len(norm_router) != len(norm_merged):
        print(
            f"  FAIL  {label}: row count router={len(norm_router)} "
            f"merged_shards={len(norm_merged)}"
        )
        return 1

    mismatches = 0
    for i, (router_row, merged_row) in enumerate(zip(norm_router, norm_merged)):
        if router_row != merged_row:
            mismatches += 1
            if mismatches <= show_samples:
                print(f"  FAIL  {label}[{i}]: router={router_row} merged={merged_row}")

    if mismatches:
        print(f"  FAIL  {label}: {mismatches} row(s) differ")
        return 1

    print(f"  ok  {label}: router == merged shards ({len(norm_router)} rows)")
    return 0


def compare_router_to_holding_shard(
    label: str,
    sql: str,
    table: str,
    symbol: str,
    *,
    shard_key: str | None = None,
) -> int:
    """
    For keyed single-shard queries: router result must match the one shard
    that holds `symbol`. Do not merge all shards — empty shards may still
    return a scalar aggregate row from QuestDB.
    """
    key = shard_key or SHARD_KEY
    sym = sql_string(symbol)
    print(f"\n--- {label} ---")
    shard_idx: int | None = None
    for idx, port in enumerate(SHARD_PG_PORTS):
        rows = query_shard(port, f"SELECT count() FROM {table} WHERE {key} = {sym}")
        if rows and int(rows[0][0]) > 0:
            shard_idx = idx
            break

    if shard_idx is None:
        print(f"  FAIL  symbol {symbol} not found on any shard")
        return 1

    port = SHARD_PG_PORTS[shard_idx]
    try:
        router_rows = query_router(sql)
        shard_rows = query_shard(port, sql)
    except Exception as exc:
        print(f"  FAIL  query: {exc}")
        return 1

    print(
        f"  router: {len(router_rows)} row(s), "
        f"shard-{shard_idx} (:{port}): {len(shard_rows)} row(s)"
    )
    return compare_row_sets(label, router_rows, shard_rows)


def compare_router_to_shards(
    label: str,
    sql: str,
    *,
    sort_key: Callable[[tuple], object] | None = None,
    shard_ports: list[int] | None = None,
) -> int:
    """
    Query router, query each shard with the same SQL, merge shard rows, compare.

    For federated queries (joins, scans, group-by) the merged per-shard results
    must match the router response exactly.
    """
    print(f"\n--- {label} ---")
    try:
        router_rows = query_router(sql)
    except Exception as exc:
        print(f"  FAIL  router query: {exc}")
        return 1

    ports = shard_ports or SHARD_PG_PORTS
    shard_parts: list[list[tuple]] = []
    for idx, port in enumerate(ports):
        try:
            part = query_shard(port, sql)
            print(f"  shard-{idx} (:{port}): {len(part)} row(s)")
            shard_parts.append(part)
        except Exception as exc:
            print(f"  FAIL  shard-{idx} query: {exc}")
            return 1

    merged_rows = merge_shard_rows(shard_parts, sort_key=sort_key)
    router_cmp = sorted(router_rows, key=sort_key) if sort_key else router_rows
    merged_cmp = sorted(merged_rows, key=sort_key) if sort_key else merged_rows
    print(f"  router: {len(router_rows)} row(s), merged shards: {len(merged_rows)} row(s)")
    return compare_row_sets(label, router_cmp, merged_cmp)


def setup_table_on_shards(table: str, shard_ports: list[int] | None = None) -> None:
    ports = shard_ports or SHARD_PG_PORTS
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            symbol SYMBOL,
            price DOUBLE,
            ts TIMESTAMP
        ) timestamp(ts) PARTITION BY DAY;
    """
    for port in ports:
        with pg_connect("127.0.0.1", port) as conn:
            conn.autocommit = True
            exec_sql(conn, ddl)
