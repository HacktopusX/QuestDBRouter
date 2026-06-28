# QuestDB Router

![QuestDB Router architecture](public/QuestDBRouter.png)

## Why this exists

QuestDB is built for speed on a single node. That works well until write volume or data size outgrow one machine. This project adds a thin routing layer in front of a QuestDB cluster so clients can keep using the same protocols they already know — ILP for ingestion and PostgreSQL wire for SQL — while writes and reads are spread across multiple QuestDB nodes.

The router is not a database. It is a smart front door: it decides which shard should handle each request and forwards traffic accordingly.

## Overview

Clients connect to the router as if it were a single QuestDB instance. Behind the scenes:

| Protocol | Default port | Role |
|----------|--------------|------|
| ILP (Influx Line Protocol) | 9009 | Write path — parse shard key, hash, forward line |
| PG wire (PostgreSQL protocol) | 8812 | Read/write SQL — route, fan out, or merge |
| WebSocket stream | 8080 | Optional live tick feed from ILP ingest |
| Prometheus metrics | 9090 | Shard health, routing, stream counters |
| Ingest HTTP | 9010 | Optional RustFS / Nautilus batch ingest |

### Routing behavior

- **Keyed writes (ILP)** — Extract a tag (for example `symbol`), consistent-hash to one shard, forward the raw line unchanged.
- **Keyed reads (SQL)** — `WHERE symbol = 'AAPL'` routes to a single shard via `ShardPgPool`.
- **Federated scans** — `SELECT * FROM trades` fans out to healthy shards and merges Arrow batches in DataFusion.
- **Aggregations & joins** — Global `COUNT`/`SUM`/`GROUP BY` and cross-shard joins use `FederatedExecutor`.
- **QuestDB dialect** — `SAMPLE BY` and `LATEST ON` with a shard-key predicate pass through verbatim to one shard.
- **Session SQL** — `BEGIN`, `COMMIT`, `ROLLBACK`, `SET`, and similar statements relay to a sticky backend connection (required for psycopg2 and other drivers).
- **Health-aware routing** — Unhealthy shards are excluded from ILP and PG routing when `health_check.exclude_unhealthy = true`.

Register sharded tables under `[[routing.tables]]` in config. Set `federated_enabled`, `max_federated_rows`, `scan_allow_order_by`, and `pg_pool_size` under `[routing]`.

## Quick start

```bash
# Local cluster + router (Docker)
docker compose up -d --build quest-router

# Smoke test (ILP write + PG read through the router)
pip install -r scripts/requirements.txt
python scripts/test_router.py
```

Optional live charts UI (WebSocket stream from router on `:8080`):

```bash
docker compose up -d charts
# Open http://localhost:5173
```

Configuration lives under `config/`. Start with `config/docker-quest-router.toml` for a two-shard Docker setup. For RustFS-backed batch ingest, use `docker-compose.objectstorage.yaml` and `config/docker-quest-router-objectstorage.toml`.

Run locally without Docker:

```bash
cargo run -- --config config/quest-router.toml
```

Requires Rust **1.89+** (`pgwire` 0.40).

## Testing

| Script | What it exercises |
|--------|-------------------|
| `scripts/test_router.py` | ILP ingest, keyed PG reads, basic routing |
| `scripts/test_health_routing.py` | Unhealthy shard exclusion |
| `scripts/test_stream.py` | WebSocket tick stream |
| `scripts/test_joins.py` | Federated joins |
| `scripts/load_test.py --mode scan` | Federated scan load |
| `scripts/test_ohlcv.py` | OHLCV table routing |
| `scripts/test_timeseries.py` | QuestDB time-series SQL |
| `scripts/test_rustfs_ingest.py` | RustFS batch ingest (object storage stack) |

Rust tests:

```bash
cargo test                                          # unit + phase1 + pgwire contract
cargo test --test pgwire_live -- --ignored          # live PG contract (Docker required)
```

DDL for sharded tables must be created on **each shard** directly — router DDL hits one shard only. See `scripts/common.py` for shared test helpers.

## Observability

- Prometheus metrics on `:9090` when `[metrics] enabled = true`
- Grafana dashboards in `config/grafana/dashboards/`
- `quest_router_shard_healthy` gauges reflect per-shard ILP/PG probe results

## Architecture

For module layout, request flows, and contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

```
src/
  app/           AppState, shard ring, stream hub
  config/        TOML config + env overrides (QUEST_ROUTER__*)
  server/        Listener orchestration, health probes
  protocol/      ILP server, PG gateway (datafusion-postgres), backend relay
  routing/       SQL classification, shard keys, consistent-hash ring, QuestDB dialect
  federated/     DataFusion scatter-gather execution
  metadata/      MetadataActor — shard health and topology
  pool/          Per-shard PG connection pools
  stream/        WebSocket broadcast hub for live ILP ticks
  ingest/        Optional RustFS / Nautilus batch ingest actor
frontend/        React charts terminal (Vite + lightweight-charts)
```

## Roadmap

- [x] Exclude unhealthy shards from routing
- [x] Federated scans, aggregates, and joins (DataFusion)
- [x] QuestDB dialect passthrough (`SAMPLE BY`, `LATEST ON`)
- [x] Live ILP stream over WebSocket
- [x] Config-driven table registry and per-table shard keys
- [x] RustFS / Nautilus batch ingest path
- [ ] Load balancing policies beyond consistent hashing
- [ ] Shard rebalancing and resharding
- [ ] Extended-protocol prepared statements on federated paths
- [ ] Multi-tenant isolation
- [ ] Production hardening: auth, TLS termination, deployment docs

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for architecture notes, local development setup, and pull request expectations.
