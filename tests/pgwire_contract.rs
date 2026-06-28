//! PGWire contract tests (offline — no live server).

use quest_router::app::ResolvedRoute;
use quest_router::config::{Config, Endpoint, HealthCheckConfig, ListenConfig, RoutingConfig, ShardConfig};
use quest_router::metadata::MetadataActor;
use quest_router::routing::{classify_sql, DefaultQueryRouter, QueryRouter, RoutingError, SqlRouteError, TableRegistry};

#[test]
fn routing_errors_map_to_pg_sqlstates() {
    assert_eq!(
        RoutingError::Parse("bad".into()).pg_sqlstate().0,
        "42601"
    );
    assert_eq!(
        RoutingError::Unsupported("write".into()).pg_sqlstate().0,
        "42501"
    );
    assert_eq!(
        RoutingError::ShardUnhealthy { shard_id: 2 }.pg_sqlstate().0,
        "08006"
    );
    assert_eq!(
        RoutingError::InsufficientHealthyShards {
            required: 1,
            available: 0,
        }
        .pg_sqlstate()
        .0,
        "08006"
    );
}

#[test]
fn routing_error_converts_to_pgwire_user_error() {
    let err = RoutingError::ShardNotFound {
        key: "missing".into(),
    };
    let pg = err.to_pgwire();
    let msg = format!("{pg}");
    assert!(msg.contains("ERROR") || msg.contains("no shard"));
}

#[test]
fn questdb_dialect_unkeyed_maps_to_unsupported() {
    let err = classify_sql(
        "SELECT ts FROM trades SAMPLE BY 1h",
        "symbol",
        &TableRegistry::default(),
        false,
    )
    .unwrap_err();
    assert!(matches!(err, SqlRouteError::Unsupported(_)));
    let routed = RoutingError::from(err);
    assert_eq!(routed.pg_sqlstate().0, "42501");
}

#[tokio::test]
async fn questdb_dialect_keyed_resolves_single_shard() {
    let (metadata, join) = MetadataActor::spawn(test_config());
    let router = DefaultQueryRouter::new(metadata, false);
    let route = router
        .resolve_route(
            "SELECT ts, count() FROM trades WHERE symbol = 'BTC' SAMPLE BY 1h",
            None,
        )
        .unwrap();
    assert!(matches!(route, ResolvedRoute::Single(_)));
    join.abort();
}

fn test_config() -> Config {
    Config {
        listen: ListenConfig {
            ilp: "127.0.0.1:9009".parse().unwrap(),
            pg: "127.0.0.1:8812".parse().unwrap(),
            stream: None,
        },
        shards: vec![ShardConfig {
            id: 0,
            ilp_address: Endpoint("127.0.0.1:9000".into()),
            pg_address: Endpoint("127.0.0.1:8810".into()),
            weight: 1,
            virtual_nodes: 64,
        }],
        routing: RoutingConfig {
            shard_key: "symbol".into(),
            federated_enabled: true,
            max_federated_rows: 1000,
            scan_allow_order_by: false,
            pg_pool_size: 2,
            tables: vec![],
        },
        pg: Default::default(),
        health_check: HealthCheckConfig {
            interval_secs: 5,
            timeout_secs: 2,
            exclude_unhealthy: true,
            min_healthy_shards: 1,
        },
        metrics: Default::default(),
        stream: Default::default(),
        ingest: Default::default(),
    }
}
