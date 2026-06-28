# Contributing to QuestDB Router

Thank you for your interest in this project. Contributions are welcome — bug reports, documentation, tests, and code changes all help.

This guide covers how the router is structured, how to run it locally, and what to expect when submitting changes.

## What you are working on

QuestDB Router sits between clients and multiple QuestDB nodes. It is a **routing proxy**, not a database. Clients use the same protocols they would with a single QuestDB instance:

| Protocol | Default port | Role |
|----------|--------------|------|
| ILP (Influx Line Protocol) | 9009 | Write path — ingest time-series lines |
| PG wire (PostgreSQL protocol) | 8812 | Read/write path — SQL over TCP |

The router picks a shard from a configurable key (for example `symbol`), forwards traffic, and (in future work) merges results for queries that span shards.

![QuestDB Router architecture](public/QuestDBRouter.png)

## Architecture

### Request flow

```
Client                    Router                         QuestDB shards
  |                         |                                  |
  |-- ILP line ------------>| parse key, hash, pick shard ---->| node N
  |-- SQL (PG wire) ------->| extract key or route query ----->| node N
  |<------------------------|<---------------------------------|
```

### Startup

1. `main.rs` parses CLI flags and starts the Tokio runtime.
2. `server::run` loads `config/quest-router.toml` (or path from `--config`).
3. `AppState` is built: shared config plus a `ShardRing` built from the shard list.
4. Two listeners start in parallel (`tokio::try_join!`):
   - ILP on `listen.ilp`
   - PG wire on `listen.pg`
5. A background task runs periodic TCP health probes against each shard.

### Crate layout

```
src/
  main.rs          Entry point, CLI, allocator
  lib.rs           Public module tree
  app/             AppState — config + shard ring, routing helpers
  config/          TOML config structs and file loading
  server/          Orchestration: listeners, health checks
  protocol/
    ilp.rs         ILP TCP server — line parsing, shard selection, upstream proxy
    pg.rs          PG wire server — SQL routing, backend relay via pgwire
  routing/
    keys.rs        Extract shard key from ILP lines and SQL
    ring.rs        Consistent-hash ring (virtual nodes per shard)
  metrics/         Prometheus metrics (optional)
config/            Example and Docker TOML configs
scripts/           Python smoke and load tests
```

### Write path (ILP)

For each incoming TCP connection:

1. Read newline-delimited ILP lines from the client.
2. Extract the configured shard key from the line (tag name from config, e.g. `symbol`).
3. Hash the key with the consistent-hash ring → `ShardConfig`.
4. Open (or reuse) a TCP connection to that shard's `ilp_address`.
5. Forward the raw line unchanged.

The hot path avoids unnecessary allocation where possible. Performance matters on this path.

### Read path (PG wire)

For each client connection:

1. `datafusion-postgres` accepts PostgreSQL wire protocol on `listen.pg`.
2. `RouterQueryHook` classifies SQL via `QueryRouter` / `classify_sql`:
   - **Single-shard** keyed reads → verbatim SQL to one shard via `ShardPgPool`
   - **Session SQL** (`BEGIN`, `COMMIT`, `SET`, etc.) → sticky `pgwire` client relay
   - **Federated** scans/aggregates → fan-out to healthy shards, merge in DataFusion
   - **QuestDB dialect** (`SAMPLE BY`, `LATEST ON`) with shard-key predicate → single-shard verbatim passthrough
3. Backend connections use `pgwire` as a **client** to each shard's `pg_address`.

Session-level SQL must continue to work because real drivers (e.g. psycopg2) depend on them.

### Shard selection

- **ILP**: shard key comes from a tag on each line.
- **SQL**: shard key is parsed from the query when possible (e.g. `WHERE symbol = 'AAPL'`).
- **Ring**: `hashring` + virtual nodes per shard; weights and `virtual_nodes` are configurable per shard.

Shard addresses use `host:port` strings (not `SocketAddr`) so Docker service names like `questdb-0:9009` work.

### Configuration

Config is TOML, with optional overrides via `QUEST_ROUTER__` environment variables (see `config::Config::from_file`).

Start from `config/docker-quest-router.toml` for a two-shard Docker setup.

## Local development

### Prerequisites

- Rust **1.89+** (required by `pgwire` 0.40)
- Docker and Docker Compose (for full stack testing)
- Python 3 + `pip` (for integration scripts)

### Build and run

```bash
# Unit tests
cargo test

# Release build
cargo build --release

# Run against a config file
cargo run -- --config config/quest-router.toml
```

### Docker stack

```bash
docker compose up -d --build quest-router
pip install -r scripts/requirements.txt
python scripts/test_router.py
```

The smoke test writes ILP through the router on port 9009, reads via PG on 8812, and can verify rows on individual shard ports.

Prometheus metrics are exposed on port 9090 when enabled in config.

### Logging

Set `RUST_LOG` for tracing output, for example:

```bash
RUST_LOG=quest_router=debug,info cargo run -- --config config/docker-quest-router.toml
```

## How to contribute

### Before you start

1. Open an issue for larger changes (new routing behavior, federated SQL, resharding) so approach can be discussed early.
2. For small fixes (typos, clear bugs, test gaps), a PR without a prior issue is fine.

### Pull request checklist

- [ ] `cargo test` passes
- [ ] `cargo clippy` is clean (or explain any new allows)
- [ ] Integration behavior verified when touching routing or protocol code (`python scripts/test_router.py` with Docker stack)
- [ ] Config changes documented in `config/` examples if new fields are added
- [ ] No unrelated refactors mixed into the same PR

### Code style

- Match existing module boundaries (`config/`, `protocol/`, `routing/`, `server/`, `app/`).
- Prefer focused changes over drive-by cleanup.
- Keep the router **transparent**: clients should not need custom SDKs or HTTP — ILP and PG wire only.
- On hot paths (ILP parsing, forwarding), avoid extra allocations and copying.
- Use `tracing` for operational logs; use `metrics` for counters and histograms where appropriate.

### Good first contributions

Areas that align with the [README todo](README.md#todo):

- Tests for SQL shard-key extraction and QuestDB dialect edge cases
- Documentation and config examples
- Load-balancing policy options beyond consistent hashing
- Unkeyed QuestDB dialect fan-out across shards (larger feature — discuss in an issue first)

### Phase 1 sign-off checklist

- `cargo test` (unit + `tests/phase1_routing.rs` + `tests/pgwire_contract.rs`)
- `python scripts/test_router.py` (Docker stack)
- `python scripts/test_health_routing.py` (optional)
- `cargo test --test pgwire_live -- --ignored` (live PG contract, Docker required)
- Prometheus `quest_router_shard_healthy` gauges on `:9090`

### Reporting bugs

Include:

- Config file (redact secrets)
- Client used (psycopg2, telegraf, custom ILP writer, etc.)
- Expected vs actual behavior
- Relevant logs with `RUST_LOG=debug`

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (MIT).
