#!/usr/bin/env python3
"""Smoke test for quest-router ILP real-time stream (WebSocket + MessagePack)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time

import msgpack

sys.path.insert(0, os.path.dirname(__file__))
from common import ROUTER_ILP_HOST, ROUTER_ILP_PORT, send_ilp, wait_for_tcp

STREAM_HOST = os.environ.get("ROUTER_STREAM_HOST", ROUTER_ILP_HOST)
STREAM_PORT = int(os.environ.get("ROUTER_STREAM_PORT", "8080"))
STREAM_WS_URL = os.environ.get(
    "ROUTER_STREAM_WS", f"ws://{STREAM_HOST}:{STREAM_PORT}/ws"
)

TABLE = "router_test_trades"
SYMBOL = "BTC-OHLCV"
TOPIC = SYMBOL
TEST_PRICE = 4242.42


async def recv_msgpack(ws, timeout: float = 10.0) -> dict:
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    if isinstance(raw, str):
        raise AssertionError(f"expected binary MessagePack frame, got text: {raw[:200]}")
    return msgpack.unpackb(raw, raw=False)


async def test_live_tick() -> None:
    import websockets

    async with websockets.connect(STREAM_WS_URL) as ws:
        await ws.send(json.dumps({"op": "subscribe", "topics": [TOPIC]}))
        await asyncio.sleep(0.2)

        ts = int(time.time() * 1_000_000_000)
        line = f"{TABLE},symbol={SYMBOL} price={TEST_PRICE} {ts}\n"
        send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, line)

        row = await recv_msgpack(ws)
        assert row.get("measurement") == TABLE, row
        tags = dict(row.get("tags", []))
        assert tags.get("symbol") == SYMBOL, row
        fields = dict(row.get("fields", []))
        price = fields.get("price")
        assert price is not None and abs(float(price) - TEST_PRICE) < 1e-6, row
        print(f"  ok  live tick symbol={SYMBOL} price={price}")


async def test_replay() -> None:
    import websockets

    ts_base = int(time.time() * 1_000_000_000)
    for i in range(5):
        ts = ts_base + i
        line = f"{TABLE},symbol={SYMBOL} price={100.0 + i} {ts}\n"
        send_ilp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, line)
        await asyncio.sleep(0.05)

    await asyncio.sleep(0.3)

    async with websockets.connect(STREAM_WS_URL) as ws:
        await ws.send(
            json.dumps({"op": "replay", "topic": TOPIC, "last_n": 5})
        )
        payload = await recv_msgpack(ws)
        assert payload.get("op") == "replay", payload
        assert payload.get("topic") == TOPIC, payload
        rows = payload.get("rows", [])
        assert len(rows) >= 5, f"expected >=5 replay rows, got {len(rows)}"
        print(f"  ok  replay last_n=5 returned {len(rows)} rows")


async def main() -> None:
    print(f"stream smoke test ws={STREAM_WS_URL}")
    wait_for_tcp(STREAM_HOST, STREAM_PORT, "stream ws")
    wait_for_tcp(ROUTER_ILP_HOST, ROUTER_ILP_PORT, "router ilp")

    await test_live_tick()
    await test_replay()
    print("stream smoke test passed")


if __name__ == "__main__":
    asyncio.run(main())
