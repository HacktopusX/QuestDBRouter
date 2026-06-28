//! Phase 1 routing integration tests (no live QuestDB required).

use quest_router::config::{Endpoint, ShardConfig};
use quest_router::metadata::{ClusterSnapshot, Protocol, ShardHealth};
use quest_router::routing::{RoutingError, ShardRing, TableRegistry};

fn test_shard(id: u32) -> ShardConfig {
    ShardConfig {
        id,
        ilp_address: Endpoint(format!("127.0.0.1:900{id}")),
        pg_address: Endpoint(format!("127.0.0.1:881{id}")),
        weight: 1,
        virtual_nodes: 64,
    }
}

#[test]
fn unhealthy_shard_skipped_for_ilp_routing() {
    let mut snap = ClusterSnapshot::new(
        vec![test_shard(0), test_shard(1)],
        TableRegistry::default(),
        true,
        1,
    );
    let primary = snap.ring.shard_by_key("BTC-USD").unwrap().id;
    snap.health.insert(
        primary,
        ShardHealth {
            ilp_ok: false,
            pg_ok: true,
        },
    );

    let routed = snap.shard_for_key("BTC-USD", Protocol::Ilp).unwrap();
    assert_ne!(routed.id, primary);
}

#[test]
fn all_shards_unhealthy_returns_error() {
    let mut snap = ClusterSnapshot::new(
        vec![test_shard(0), test_shard(1)],
        TableRegistry::default(),
        true,
        1,
    );
    snap.health.insert(0, ShardHealth { ilp_ok: false, pg_ok: false });
    snap.health.insert(1, ShardHealth { ilp_ok: false, pg_ok: false });

    let err = snap.shard_for_key("BTC-USD", Protocol::Ilp).unwrap_err();
    assert!(matches!(
        err,
        RoutingError::InsufficientHealthyShards { .. }
    ));
}

#[test]
fn healthy_pg_shards_list_excludes_bad_nodes() {
    let mut snap = ClusterSnapshot::new(
        vec![test_shard(0), test_shard(1)],
        TableRegistry::default(),
        true,
        1,
    );
    snap.health.insert(1, ShardHealth { ilp_ok: true, pg_ok: false });

    let healthy = snap.healthy_shards(Protocol::Pg);
    assert_eq!(healthy.len(), 1);
    assert_eq!(healthy[0].id, 0);
}

#[test]
fn ring_filtered_lookup_is_deterministic() {
    let ring = ShardRing::from_shards(vec![test_shard(0), test_shard(1)]);
    let mut excluded = std::collections::HashSet::new();
    excluded.insert(0);
    let shard = ring.shard_by_key_filtered("ETH-USD", &excluded).unwrap();
    assert_eq!(shard.id, 1);
}

#[test]
fn classify_sql_never_panics_on_arbitrary_input() {
    use quest_router::routing::{classify_sql, TableRegistry};

    let registry = TableRegistry::default();
    let samples = [
        "",
        ";;;",
        "SELECT",
        "SELECT * FROM",
        "NOT SQL AT ALL",
        "SELECT 1; SELECT 2",
        "INSERT INTO t VALUES (1)",
        "BEGIN",
        "SELECT * FROM trades WHERE symbol = 'X'",
        "SELECT ts, count() FROM trades WHERE symbol = 'BTC' SAMPLE BY 1h",
        "SELECT symbol, price FROM trades WHERE symbol = 'ETH' LATEST ON ts PARTITION BY symbol",
    ];
    for sql in samples {
        let _ = classify_sql(sql, "symbol", &registry, false);
    }
}
