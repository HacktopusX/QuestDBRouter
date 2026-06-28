use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use log::info;

pub fn init(config_enabled: bool, listen: Option<SocketAddr>) -> anyhow::Result<()> {
    describe_counter!(
        "quest_router_requests_total",
        "Total HTTP requests routed by operation and shard"
    );
    describe_histogram!(
        "quest_router_request_duration_seconds",
        "End-to-end request duration in seconds"
    );

    describe_histogram!(
        "quest_router_merge_latency_seconds",
        "Federated query merge duration in seconds"
    );
    describe_counter!(
        "quest_router_federated_queries_total",
        "Total federated queries by plan type"
    );
    describe_histogram!(
        "quest_router_federated_shards_queried",
        "Number of shards queried per federated request"
    );

    describe_counter!(
        "quest_router_stream_ticks_published_total",
        "Total ILP ticks published to stream topics"
    );
    describe_gauge!(
        "quest_router_stream_subscribers",
        "Active stream subscribers per topic"
    );
    describe_counter!(
        "quest_router_stream_client_lagged_total",
        "Stream client lag events (slow consumer)"
    );
    describe_counter!(
        "quest_router_stream_client_dropped_total",
        "Stream clients disconnected due to excessive lag"
    );
    describe_gauge!(
        "quest_router_shard_healthy",
        "Shard health probe status (1=healthy, 0=unhealthy)"
    );
    describe_counter!(
        "quest_router_ingest_events_received_total",
        "RustFS/S3 notification events accepted by ingest webhook"
    );
    describe_counter!(
        "quest_router_ingest_objects_processed_total",
        "Ingest objects processed by outcome"
    );
    describe_counter!(
        "quest_router_ingest_rows_forwarded_total",
        "ILP rows forwarded from object ingest by table"
    );
    describe_gauge!(
        "quest_router_ingest_mailbox_depth",
        "Current ingest actor mailbox depth"
    );
    describe_gauge!(
        "quest_router_ingest_reconcile_lag_objects",
        "Objects discovered by reconcile but not yet checkpointed"
    );

    if config_enabled {
        let addr = listen.unwrap_or_else(|| "127.0.0.1:9090".parse().expect("valid addr"));
        PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()?;
        info!("prometheus metrics exporter listening on {addr}");
    } else {
        PrometheusBuilder::new().install()?;
    }

    Ok(())
}

pub fn record_request(operation: &str, shard_id: Option<u32>) {
    let shard = shard_id.map(|id| id.to_string()).unwrap_or_else(|| "unknown".into());
    counter!(
        "quest_router_requests_total",
        "operation" => operation.to_string(),
        "shard" => shard
    )
        .increment(1);
}

pub fn record_duration(operation: &str, duration_secs: f64) {
    histogram!(
        "quest_router_request_duration_seconds",
        "operation" => operation.to_string()
    )
        .record(duration_secs);
}

pub fn record_merge_latency(duration_secs: f64) {
    histogram!("quest_router_merge_latency_seconds").record(duration_secs);
}

pub fn record_federated_query(plan: &str) {
    counter!(
        "quest_router_federated_queries_total",
        "plan" => plan.to_string()
    )
    .increment(1);
}

pub fn record_federated_shards(count: usize) {
    histogram!("quest_router_federated_shards_queried").record(count as f64);
}

pub fn record_stream_tick_published() {
    counter!("quest_router_stream_ticks_published_total").increment(1);
}

pub fn record_stream_subscriber(topic: &str, count: usize) {
    gauge!(
        "quest_router_stream_subscribers",
        "topic" => topic.to_string()
    )
    .set(count as f64);
}

pub fn record_stream_client_lagged() {
    counter!("quest_router_stream_client_lagged_total").increment(1);
}

pub fn record_stream_client_dropped() {
    counter!("quest_router_stream_client_dropped_total").increment(1);
}

pub fn record_shard_health(shard_id: u32, protocol: &str, healthy: bool) {
    gauge!(
        "quest_router_shard_healthy",
        "shard" => shard_id.to_string(),
        "protocol" => protocol.to_string(),
    )
    .set(if healthy { 1.0 } else { 0.0 });
}

pub fn record_ingest_events_received(source: crate::ingest::IngestSource, count: usize) {
    counter!(
        "quest_router_ingest_events_received_total",
        "source" => ingest_source_label(source),
    )
    .increment(count as u64);
}

pub fn record_ingest_object_processed(source: crate::ingest::IngestSource, status: &str) {
    counter!(
        "quest_router_ingest_objects_processed_total",
        "source" => ingest_source_label(source),
        "status" => status.to_string(),
    )
    .increment(1);
}

pub fn record_ingest_rows_forwarded(table: &str, rows: u64) {
    counter!(
        "quest_router_ingest_rows_forwarded_total",
        "table" => table.to_string(),
    )
    .increment(rows);
}

pub fn record_ingest_mailbox_depth(_bucket: &str, depth: usize) {
    gauge!("quest_router_ingest_mailbox_depth").set(depth as f64);
}

pub fn record_ingest_reconcile_lag(count: usize) {
    gauge!("quest_router_ingest_reconcile_lag_objects").set(count as f64);
}

fn ingest_source_label(source: crate::ingest::IngestSource) -> String {
    match source {
        crate::ingest::IngestSource::Webhook => "webhook".into(),
        crate::ingest::IngestSource::Reconcile => "reconcile".into(),
    }
}
