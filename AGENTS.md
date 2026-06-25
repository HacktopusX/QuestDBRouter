## Learned User Preferences

- Use codegraph MCP extensively instead of grep for finding and exploring the codebase.
- Use sem MCP for reviewing changes and comparisons.
- Performance is critical; favor zero-copy ILP parsing, release-profile tuning (fat LTO, mimalloc), and direct passthrough on hot paths.
- The router must be a transparent QuestDB front: no HTTP, clients use ILP (default 9009) and PGWire (default 8812) as if talking to a single node.
- Prefer standard Rust crate layout (`config/`, `protocol/`, `routing/`, `federated/`, `pool/`, `app/`, `server/`).
- Only create git commits when explicitly requested.

## Learned Workspace Facts

- Stack is tokio + pgwire for reads and a lean ILP line router for writes; Pingora/HTTP proxy was removed.
- Shard endpoints use hostname-aware `host:port` strings, not `SocketAddr` — Docker names like `questdb-0:9009` fail with `invalid socket address syntax`.
- `pgwire` 0.40+ needs rustc 1.89+; Docker/build images must match.
- Hybrid federated SQL: `SingleShard` keyed reads passthrough to one QuestDB node; `FullScan` / `AggregateScan` / `Join` / `GroupBy` fan out via `FederatedExecutor` + DataFusion on Arrow batches.
- Push SQL verbatim to each shard; DataFusion merges results only — do not let DataFusion replan QuestDB dialect (`count()`, `SAMPLE BY`, etc.).
- Register sharded tables in config (`[[routing.tables]]` with `sharded = true`); set `federated_enabled`, `max_federated_rows`, `scan_allow_order_by`, `pg_pool_size` under `[routing]`.
- PG session SQL (`BEGIN`, `COMMIT`, `ROLLBACK`, `SET`, etc.) must passthrough — psycopg2 opens implicit transactions and will fail if only `SELECT` is accepted.
- Federated scans/joins use simple queries for now; extended-protocol prepared statements on federated paths return a clear error (keyed `$1` reads still work on single-shard path).
- Full scans reject `ORDER BY` / `LIMIT` / `OFFSET` unless `scan_allow_order_by = true`; per-shard `LIMIT` is not a global top-K.
- Local/docker testing: `config/docker-quest-router.toml`, `docker compose up -d --build quest-router`, `scripts/test_router.py` (smoke), `scripts/load_test.py --mode scan` (federated).
- Test scripts create DDL on each shard directly (router DDL hits one shard only); truncate `router_test_trades` or recreate tables to avoid stale-data false failures.
- Shared test helpers live in `scripts/common.py` — import `SHARD_KEY` (env `ROUTER_SHARD_KEY`), not `ROUTER_SHARD_KEY` as a symbol.
- Health checks currently log only; unhealthy shards are not yet excluded from routing.
