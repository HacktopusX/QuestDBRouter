# QuestDB Router

![QuestDB Router architecture](public/QuestDBRouter.png)

## Why this exists

QuestDB is built for speed on a single node. That works well until write volume or data size outgrow one machine. This project adds a thin routing layer in front of a QuestDB cluster so clients can keep using the same protocols they already know — ILP for ingestion and PostgreSQL wire for SQL — while writes and reads are spread across multiple QuestDB nodes.

The router is not a database. It is a smart front door: it decides which shard should handle each request and forwards traffic accordingly.

## Overview

Clients connect to the router as if it were a single QuestDB instance. Behind the scenes:

- **Writes (ILP)** — Incoming lines are parsed, a shard key (for example a symbol tag) is hashed, and each line is sent to the right node.
- **Reads (PG wire)** — Simple keyed queries go to one shard. Broader scans and aggregations can fan out to every shard and merge results.

The goal is horizontal scaling without changing how applications talk to QuestDB.

## Quick start

```bash
# Local cluster + router (Docker)
docker compose up -d --build quest-router

# Smoke test
python scripts/test_router.py
```

Configuration lives under `config/`. See `config/docker-quest-router.toml` for a working local setup.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for architecture notes, local development setup, and how to open a pull request.

## Todo

- [ ] Exclude unhealthy shards from routing (health checks today are log-only)
- [ ] Load balancing policies beyond consistent hashing
- [ ] Shard rebalancing and resharding support
- [ ] Schema-aware routing
- [ ] Distributed SQL for joins and complex queries
- [ ] Multi-tenant isolation
- [ ] Production hardening: auth, TLS termination, deployment docs
