//! Live PGWire contract tests against a running quest-router (Docker stack).
//!
//! Run with: `cargo test --test pgwire_live -- --ignored --nocapture`

use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::DefaultSimpleQueryHandler;
use pgwire::tokio::client::PgWireClient;
use std::sync::Arc;

fn router_pg_config() -> pgwire::api::client::Config {
    let host = std::env::var("ROUTER_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("ROUTER_PG_PORT").unwrap_or_else(|_| "8812".into());
    let user = std::env::var("QUESTDB_USER").unwrap_or_else(|_| "admin".into());
    let password = std::env::var("QUESTDB_PASSWORD").unwrap_or_else(|_| "quest".into());
    let database = std::env::var("QUESTDB_DATABASE").unwrap_or_else(|_| "qdb".into());
    let conn = format!(
        "host={host} port={port} user={user} password={password} dbname={database} sslmode=disable"
    );
    conn.parse().expect("valid pg config")
}

async fn connect_client() -> PgWireClient {
    let config = Arc::new(router_pg_config());
    let startup = DefaultStartupHandler::new();
    PgWireClient::connect(config, startup, None)
        .await
        .expect("connect to quest-router PG")
}

async fn simple_query_ok(client: &mut PgWireClient, sql: &str) {
    let handler = DefaultSimpleQueryHandler::new();
    let responses = client
        .simple_query(handler, sql)
        .await
        .unwrap_or_else(|e| panic!("query failed ({sql}): {e}"));
    assert!(
        !responses.is_empty(),
        "empty response for: {sql}"
    );
}

#[tokio::test]
#[ignore = "requires docker-compose quest-router stack"]
async fn begin_commit_passthrough() {
    let mut client = connect_client().await;
    simple_query_ok(&mut client, "BEGIN").await;
    simple_query_ok(&mut client, "COMMIT").await;
}

#[tokio::test]
#[ignore = "requires docker-compose quest-router stack"]
async fn keyed_select_routes() {
    let mut client = connect_client().await;
    simple_query_ok(
        &mut client,
        "SELECT 1 AS ok",
    )
    .await;
}

#[tokio::test]
#[ignore = "requires docker-compose quest-router stack"]
async fn keyed_sample_by_not_parse_error() {
    let mut client = connect_client().await;
    // Table may be empty; we only verify the router accepts dialect SQL (no 42601).
    let handler = DefaultSimpleQueryHandler::new();
    let sql = "SELECT ts, count() FROM router_test_trades WHERE symbol = 'BTC-USD' SAMPLE BY 1h";
    let result = client.simple_query(handler, sql).await;
    result.unwrap_or_else(|e| panic!("keyed SAMPLE BY failed: {e}"));
}
