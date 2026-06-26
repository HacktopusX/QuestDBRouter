//! PGWire contract tests (offline — no live server).

use quest_router::routing::RoutingError;

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
