#!/usr/bin/env python3
"""Generate a minimal Nautilus-style trade Feather fixture for ingest tests."""

from __future__ import annotations

import struct
import sys
from pathlib import Path

try:
    import pyarrow as pa
    from pyarrow import ipc
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Install pyarrow: pip install pyarrow") from exc

PRECISION_BYTES = 8
KEY_INSTRUMENT_ID = b"instrument_id"
KEY_PRICE_PRECISION = b"price_precision"
KEY_SIZE_PRECISION = b"size_precision"


def fixed_binary_type(size: int = PRECISION_BYTES):
    """FixedSizeBinary dtype — works across pyarrow versions."""
    if hasattr(pa, "fixed_size_binary"):
        return pa.fixed_size_binary(size)
    # pa.binary(n) with n > 0 is FixedSizeBinary in older pyarrow.
    return pa.binary(size)


def fixed_price(value: float, precision: int) -> bytes:
    raw = int(round(value * (10**precision)))
    return struct.pack("<q", raw)


def main() -> int:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "scripts/fixtures/trade_tick.feather")
    out.parent.mkdir(parents=True, exist_ok=True)

    metadata = {
        KEY_INSTRUMENT_ID: b"BTCUSDT.BITUNIX",
        KEY_PRICE_PRECISION: b"2",
        KEY_SIZE_PRECISION: b"3",
    }
    fixed_bin = fixed_binary_type()
    schema = pa.schema(
        [
            pa.field("price", fixed_bin),
            pa.field("size", fixed_bin),
            pa.field("aggressor_side", pa.uint8()),
            pa.field("trade_id", pa.string()),
            pa.field("ts_event", pa.uint64()),
            pa.field("ts_init", pa.uint64()),
        ],
        metadata=metadata,
    )

    batch = pa.record_batch(
        [
            pa.array([fixed_price(50_000.25, 2)], type=fixed_bin),
            pa.array([fixed_price(1.0, 3)], type=fixed_bin),
            pa.array([1], type=pa.uint8()),
            pa.array(["t1"]),
            pa.array([1_700_000_000_000_000_000], type=pa.uint64()),
            pa.array([1_700_000_000_000_000_001], type=pa.uint64()),
        ],
        schema=schema,
    )

    with out.open("wb") as f:
        with ipc.new_stream(f, schema) as writer:
            writer.write_batch(batch)

    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
