use std::net::SocketAddr;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use log::info;
use serde_json::Value;
use tracing::warn;

use crate::metrics;

use super::actor::{IngestHandle, IngestSource, ProcessObject};
use super::events::{matches_prefix, parse_notification};

#[derive(Clone)]
pub struct HttpState {
    pub handle: IngestHandle,
    pub default_bucket: String,
    pub prefix: String,
}

pub async fn serve(state: HttpState, listen: SocketAddr) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/ingest/events", post(ingest_events))
        .route("/ingest/health", get(ingest_health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!("ingest webhook listener ready on {listen}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ingest_health() -> StatusCode {
    StatusCode::OK
}

async fn ingest_events(
    State(state): State<HttpState>,
    body: Json<Value>,
) -> StatusCode {
    let body_str = match serde_json::to_string(&body.0) {
        Ok(s) => s,
        Err(e) => {
            warn!(err = %e, "ingest body serialization failed");
            return StatusCode::BAD_REQUEST;
        }
    };

    let refs = match parse_notification(&body_str, &state.default_bucket) {
        Ok(r) => r,
        Err(e) => {
            log::warn!("invalid ingest notification payload: {e:#}");
            return StatusCode::BAD_REQUEST;
        }
    };

    metrics::record_ingest_events_received(IngestSource::Webhook, refs.len());

    let mut enqueued = 0usize;
    for object_ref in refs {
        if !matches_prefix(&object_ref.key, &state.prefix) {
            continue;
        }
        let msg = ProcessObject {
            bucket: object_ref.bucket,
            key: object_ref.key,
            etag: object_ref.etag,
            source: IngestSource::Webhook,
        };
        match state.handle.enqueue(msg).await {
            Ok(()) => enqueued += 1,
            Err(_) => {
                let depth = state.handle.mailbox_depth();
                warn!(
                    bucket = %state.default_bucket,
                    depth,
                    "ingest mailbox full, returning 503"
                );
                metrics::record_ingest_mailbox_depth(&state.default_bucket, depth);
                return StatusCode::SERVICE_UNAVAILABLE;
            }
        }
    }

    if enqueued == 0 {
        StatusCode::OK
    } else {
        StatusCode::OK
    }
}
