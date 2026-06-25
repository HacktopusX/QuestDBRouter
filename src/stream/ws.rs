use crate::metrics;
use crate::stream::control::{ControlIn, ReplayOut};
use crate::stream::hub::{BroadcastHub, StreamTick};
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use log::{debug, warn};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;

const OUTBOUND_CAPACITY: usize = 256;
const WILDCARD_TOPIC: &str = "*";

static CONN_COUNTER: AtomicU64 = AtomicU64::new(0);

type SharedHub = Arc<BroadcastHub>;

#[derive(Clone)]
pub struct StreamState {
    pub hub: SharedHub,
}

pub fn router(state: StreamState) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

pub async fn serve(hub: SharedHub, listen: std::net::SocketAddr) -> anyhow::Result<()> {
    let state = StreamState { hub };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    log::info!("stream websocket listener ready on {listen}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<StreamState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: StreamState) {
    let conn_id = CONN_COUNTER.fetch_add(1, Ordering::Relaxed);
    debug!("stream ws connected conn_id={conn_id}");

    let (mut ws_tx, mut ws_rx) = socket.split();
    let outbound: Arc<tokio::sync::Mutex<VecDeque<Arc<[u8]>>>> =
        Arc::new(tokio::sync::Mutex::new(VecDeque::with_capacity(OUTBOUND_CAPACITY)));
    let outbound_notify = Arc::new(tokio::sync::Notify::new());

    let hub = state.hub.clone();
    let max_lag_drops = hub.config().max_client_lag_drops;
    let publish_notify = hub.publish_notify();

    let subscriptions: Arc<DashMap<String, broadcast::Receiver<Arc<StreamTick>>>> =
        Arc::new(DashMap::new());

    let out_for_reader = outbound.clone();
    let notify_for_reader = outbound_notify.clone();
    let subs_for_reader = subscriptions.clone();
    let hub_for_reader = hub.clone();
    let reader = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_rx.next().await {
            match msg {
                Message::Text(text) => {
                    match serde_json::from_str::<ControlIn>(&text) {
                        Ok(ControlIn::Subscribe { topics }) => {
                            for topic in topics {
                                if topic == WILDCARD_TOPIC {
                                    let rx = hub_for_reader.subscribe_wildcard();
                                    subs_for_reader.insert(WILDCARD_TOPIC.to_string(), rx);
                                } else {
                                    let rx = hub_for_reader.subscribe(&topic);
                                    subs_for_reader.insert(topic, rx);
                                }
                            }
                        }
                        Ok(ControlIn::Unsubscribe { topics }) => {
                            for topic in topics {
                                if topic == WILDCARD_TOPIC {
                                    hub_for_reader.unsubscribe_wildcard();
                                }
                                subs_for_reader.remove(&topic);
                                hub_for_reader.remove_subscriber(&topic);
                            }
                        }
                        Ok(ControlIn::Replay { topic, last_n }) => {
                            let rows = hub_for_reader.replay(&topic, last_n);
                            let payload = ReplayOut {
                                op: "replay",
                                topic: topic.clone(),
                                rows,
                            };
                            if let Ok(bytes) = rmp_serde::to_vec_named(&payload) {
                                enqueue_frame(
                                    &out_for_reader,
                                    &notify_for_reader,
                                    Arc::from(bytes.into_boxed_slice()),
                                )
                                .await;
                            }
                        }
                        Err(e) => {
                            warn!("stream ws invalid control conn_id={conn_id}: {e}");
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    let subs_for_fanout = subscriptions.clone();
    let out_for_fanout = outbound.clone();
    let notify_for_fanout = outbound_notify.clone();
    let fanout = tokio::spawn(async move {
        let mut lag_drops = 0u32;
        loop {
            if subs_for_fanout.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            loop {
                let mut seen = HashSet::new();
                let mut pending: VecDeque<Arc<[u8]>> = VecDeque::new();
                if !drain_subscriptions(
                    &subs_for_fanout,
                    &mut seen,
                    &mut pending,
                    &mut lag_drops,
                    max_lag_drops,
                ) {
                    break;
                }
                while let Some(wire) = pending.pop_front() {
                    enqueue_frame(&out_for_fanout, &notify_for_fanout, wire).await;
                }
            }

            if lag_drops >= max_lag_drops {
                metrics::record_stream_client_dropped();
                return;
            }

            let notified = publish_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let mut seen = HashSet::new();
            let mut pending: VecDeque<Arc<[u8]>> = VecDeque::new();
            if drain_subscriptions(
                &subs_for_fanout,
                &mut seen,
                &mut pending,
                &mut lag_drops,
                max_lag_drops,
            ) {
                while let Some(wire) = pending.pop_front() {
                    enqueue_frame(&out_for_fanout, &notify_for_fanout, wire).await;
                }
                continue;
            }
            if lag_drops >= max_lag_drops {
                metrics::record_stream_client_dropped();
                return;
            }
            notified.await;
        }
    });

    let outbound_for_writer = outbound.clone();
    let notify_for_writer = outbound_notify.clone();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_for_writer = shutdown.clone();
    let writer = tokio::spawn(async move {
        loop {
            let frame = {
                let mut q = outbound_for_writer.lock().await;
                q.pop_front()
            };
            if let Some(frame) = frame {
                if ws_tx
                    .send(Message::Binary(frame.to_vec().into()))
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }
            if shutdown_for_writer.load(Ordering::Relaxed) {
                break;
            }
            notify_for_writer.notified().await;
        }
    });

    tokio::select! {
        _ = reader => {},
        _ = fanout => {},
    }
    shutdown.store(true, Ordering::Relaxed);
    outbound_notify.notify_one();
    let _ = writer.await;

    for entry in subscriptions.iter() {
        if entry.key() == WILDCARD_TOPIC {
            hub.unsubscribe_wildcard();
        } else {
            hub.remove_subscriber(entry.key());
        }
    }

    debug!("stream ws disconnected conn_id={conn_id}");
}

/// Drain all subscription channels; returns true if any frames were collected.
fn drain_subscriptions(
    subs: &DashMap<String, broadcast::Receiver<Arc<StreamTick>>>,
    seen: &mut HashSet<usize>,
    pending: &mut VecDeque<Arc<[u8]>>,
    lag_drops: &mut u32,
    max_lag_drops: u32,
) -> bool {
    let mut got_any = false;
    let mut closed = Vec::new();

    for mut entry in subs.iter_mut() {
        let key = entry.key().clone();
        let rx = entry.value_mut();
        loop {
            match rx.try_recv() {
                Ok(tick) => {
                    let ptr = Arc::as_ptr(&tick) as usize;
                    if seen.insert(ptr) {
                        pending.push_back(tick.wire.clone());
                        got_any = true;
                    }
                }
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    metrics::record_stream_client_lagged();
                    *lag_drops = lag_drops.saturating_add(n as u32);
                    if *lag_drops >= max_lag_drops {
                        return got_any;
                    }
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    closed.push(key);
                    break;
                }
                Err(broadcast::error::TryRecvError::Empty) => break,
            }
        }
    }

    for key in closed {
        subs.remove(&key);
    }

    got_any
}

async fn enqueue_frame(
    queue: &Arc<tokio::sync::Mutex<VecDeque<Arc<[u8]>>>>,
    notify: &Arc<tokio::sync::Notify>,
    bytes: Arc<[u8]>,
) {
    let mut q = queue.lock().await;
    if q.len() >= OUTBOUND_CAPACITY {
        q.pop_front();
        metrics::record_stream_client_lagged();
    }
    q.push_back(bytes);
    notify.notify_one();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StreamConfig;
    use crate::routing::TableRegistry;

    #[test]
    fn replay_out_serializes_to_msgpack() {
        let payload = ReplayOut {
            op: "replay",
            topic: "BTC".into(),
            rows: vec![],
        };
        let bytes = rmp_serde::to_vec_named(&payload).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn hub_roundtrip_for_publish() {
        let registry = TableRegistry::from_config(
            &[crate::config::TableRoutingConfig {
                name: "router_test_trades".into(),
                sharded: true,
                shard_key: None,
                columns: vec![],
            }],
            "symbol",
        );
        let hub = BroadcastHub::new(StreamConfig::default(), registry);
        let mut rx = hub.subscribe("BTC-OHLCV");
        hub.publish(b"router_test_trades,symbol=BTC-OHLCV price=100 1\n");
        let tick = rx.try_recv().unwrap();
        assert_eq!(tick.row.measurement, "router_test_trades");
        assert!(!tick.wire.is_empty());
    }
}
