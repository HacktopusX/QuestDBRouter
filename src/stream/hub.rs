use crate::config::StreamConfig;
use crate::metrics;
use crate::routing::measurement_from_ilp_bytes;
use crate::routing::TableRegistry;
use crate::stream::row::{parse_ilp_row, IlpRow};
use crate::stream::topic::{derive_topic, is_broadcastable};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Notify};
use tracing::warn;

/// Pre-serialized tick shared across broadcast subscribers (one msgpack encode per publish).
pub struct StreamTick {
    pub row: Arc<IlpRow>,
    pub wire: Arc<[u8]>,
}

pub struct BroadcastHub {
    topics: DashMap<String, TopicChannel>,
    wildcard: broadcast::Sender<Arc<StreamTick>>,
    wildcard_subscribers: AtomicUsize,
    config: StreamConfig,
    registry: TableRegistry,
    publish_notify: Arc<Notify>,
}

struct TopicChannel {
    tx: broadcast::Sender<Arc<StreamTick>>,
    replay: RwLock<VecDeque<Arc<IlpRow>>>,
}

impl BroadcastHub {
    pub fn new(config: StreamConfig, registry: TableRegistry) -> Self {
        let (wildcard, _) = broadcast::channel(config.broadcast_capacity);
        Self {
            topics: DashMap::new(),
            wildcard,
            wildcard_subscribers: AtomicUsize::new(0),
            config,
            registry,
            publish_notify: Arc::new(Notify::new()),
        }
    }

    pub fn publish_notify(&self) -> Arc<Notify> {
        self.publish_notify.clone()
    }

    pub fn wake_waiters(&self) {
        self.publish_notify.notify_waiters();
    }

    /// True when any topic or wildcard subscriber is connected.
    pub fn has_subscribers(&self) -> bool {
        if self.wildcard_subscribers.load(Ordering::Relaxed) > 0 {
            return true;
        }
        self.topics
            .iter()
            .any(|e| e.value().tx.receiver_count() > 0)
    }

    /// Fire-and-forget publish on the ILP ingest path — never blocks on subscribers.
    pub fn publish(&self, line: &[u8]) {
        if !self.has_subscribers() {
            return;
        }

        let Some(measurement) = measurement_from_ilp_bytes(line) else {
            warn!("ILP parse failed in stream hub, dropping line");
            return;
        };
        if !is_broadcastable(measurement, &self.registry) {
            return;
        }

        let Some(row) = parse_ilp_row(line) else {
            warn!("ILP parse failed in stream hub, dropping line");
            return;
        };

        let topic = derive_topic(&row, &self.config);
        let wildcard = self.wildcard_subscribers.load(Ordering::Relaxed) > 0;
        let topic_subs = self.topic_receiver_count(&topic) > 0;
        if !wildcard && !topic_subs {
            return;
        }

        let Some(wire) = encode_tick(&row) else {
            return;
        };
        let tick = Arc::new(StreamTick {
            row: Arc::new(row),
            wire,
        });

        if topic_subs {
            self.send_to_topic(&topic, tick.clone());
        }
        if wildcard {
            let _ = self.wildcard.send(tick);
            self.publish_notify.notify_waiters();
        }
    }

    fn topic_receiver_count(&self, topic: &str) -> usize {
        self.topics
            .get(topic)
            .map(|ch| ch.tx.receiver_count())
            .unwrap_or(0)
    }

    fn send_to_topic(&self, topic: &str, tick: Arc<StreamTick>) {
        let channel = self.get_or_create_topic(topic);
        let receiver_count = channel.tx.receiver_count();
        let _ = channel.tx.send(tick.clone());

        if receiver_count > 0 {
            let mut replay = channel.replay.write();
            replay.push_back(tick.row.clone());
            while replay.len() > self.config.replay_window {
                replay.pop_front();
            }
        }

        metrics::record_stream_tick_published();
        self.publish_notify.notify_waiters();
    }

    fn get_or_create_topic(
        &self,
        topic: &str,
    ) -> dashmap::mapref::one::RefMut<'_, String, TopicChannel> {
        self.topics.entry(topic.to_string()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(self.config.broadcast_capacity);
            TopicChannel {
                tx,
                replay: RwLock::new(VecDeque::with_capacity(
                    self.config.replay_window.min(1024),
                )),
            }
        })
    }

    pub fn subscribe(&self, topic: &str) -> broadcast::Receiver<Arc<StreamTick>> {
        let channel = self.get_or_create_topic(topic);
        metrics::record_stream_subscriber(topic, channel.tx.receiver_count());
        let rx = channel.tx.subscribe();
        self.wake_waiters();
        rx
    }

    pub fn subscribe_wildcard(&self) -> broadcast::Receiver<Arc<StreamTick>> {
        self.wildcard_subscribers.fetch_add(1, Ordering::Relaxed);
        let rx = self.wildcard.subscribe();
        self.wake_waiters();
        rx
    }

    pub fn unsubscribe_wildcard(&self) {
        self.wildcard_subscribers.fetch_sub(1, Ordering::Relaxed);
        self.wake_waiters();
    }

    pub fn replay(&self, topic: &str, last_n: usize) -> Vec<IlpRow> {
        self.topics
            .get(topic)
            .map(|ch| {
                let replay = ch.replay.read();
                let start = replay.len().saturating_sub(last_n);
                replay
                    .iter()
                    .skip(start)
                    .map(|r| (**r).clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn config(&self) -> &StreamConfig {
        &self.config
    }

    pub fn remove_subscriber(&self, topic: &str) {
        if let Some(ch) = self.topics.get(topic) {
            let count = ch.tx.receiver_count();
            metrics::record_stream_subscriber(topic, count);
            if count == 0 {
                drop(ch);
                self.topics.remove(topic);
            }
        }
        self.wake_waiters();
    }
}

fn encode_tick(row: &IlpRow) -> Option<Arc<[u8]>> {
    match rmp_serde::to_vec_named(row) {
        Ok(v) => Some(Arc::from(v.into_boxed_slice())),
        Err(e) => {
            warn!(err = %e, "tick encode failed, dropping");
            None
        }
    }
}
