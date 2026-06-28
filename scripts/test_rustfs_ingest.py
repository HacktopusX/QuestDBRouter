#!/usr/bin/env python3
"""
Smoke test: upload Nautilus Feather fixture to RustFS and verify quest-router ingest.

Prerequisites (from rust-something repo root)::

    docker compose -f docker-compose.objectstorage.yaml up -d --build
    pip install pyarrow boto3 psycopg2-binary

Usage::

    python scripts/gen_nautilus_feather_fixture.py scripts/fixtures/trade_tick.feather
    python scripts/test_rustfs_ingest.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from common import ROUTER_PG_HOST, ROUTER_PG_PORT, query_router, wait_for_tcp

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "trade_tick.feather"
OBJECT_KEY = "live/LIVE/test-instance/data/trades/BTCUSDT.BITUNIX/smoke.feather"
SYMBOL = "BTCUSDT"
TIMEOUT_SECS = float(os.environ.get("VERIFY_TIMEOUT_SECS", "120"))

RUSTFS_ENDPOINT = os.environ.get("RUSTFS_ENDPOINT", "http://127.0.0.1:9000")
RUSTFS_BUCKET = os.environ.get("RUSTFS_BUCKET", "market-data")
RUSTFS_ACCESS_KEY = os.environ.get("RUSTFS_ACCESS_KEY", "rustfsadmin")
RUSTFS_SECRET_KEY = os.environ.get("RUSTFS_SECRET_KEY", "rustfsadmin")
INGEST_HEALTH = os.environ.get("INGEST_HEALTH_URL", "http://127.0.0.1:9010/ingest/health")


def ensure_fixture() -> None:
    if FIXTURE.exists():
        return
    gen = Path(__file__).resolve().parent / "gen_nautilus_feather_fixture.py"
    subprocess.check_call([sys.executable, str(gen), str(FIXTURE)])


def upload_fixture() -> None:
    import boto3
    from botocore.client import Config

    client = boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    client.upload_file(str(FIXTURE), RUSTFS_BUCKET, OBJECT_KEY)
    print(f"  ok  uploaded s3://{RUSTFS_BUCKET}/{OBJECT_KEY}")


def wait_for_ingest_health() -> None:
    import urllib.request

    deadline = time.monotonic() + TIMEOUT_SECS
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(INGEST_HEALTH, timeout=2) as resp:
                if resp.status == 200:
                    print(f"  ok  ingest health {INGEST_HEALTH}")
                    return
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"ingest health not ready: {INGEST_HEALTH}")


def wait_for_rows(min_count: int = 1) -> int:
    deadline = time.monotonic() + TIMEOUT_SECS
    while time.monotonic() < deadline:
        try:
            rows = query_router(f"SELECT count() FROM trade_ticks WHERE symbol = '{SYMBOL}'")
            count = int(rows[0][0]) if rows else 0
            if count >= min_count:
                return count
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"No trade_ticks rows for {SYMBOL} within {TIMEOUT_SECS}s")


def main() -> int:
    print("RustFS ingest smoke test")
    wait_for_tcp(ROUTER_PG_HOST, ROUTER_PG_PORT, "quest-router PG")
    wait_for_ingest_health()
    print("\n0. Ensuring trade_ticks exists on all shards...")
    from init_ingest_tables import init_tables

    init_tables()
    ensure_fixture()
    print("\n1. Uploading Feather fixture to RustFS...")
    upload_fixture()
    print("\n2. Waiting for quest-router ingest -> QuestDB...")
    count = wait_for_rows()
    print(f"  ok  router reports {count} row(s) for {SYMBOL}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
