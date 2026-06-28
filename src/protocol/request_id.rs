use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(1);

/// Return a monotonically-increasing request ID for the current process.
///
/// IDs start at 1 and are unique within a single router process lifetime.
/// Used as a lightweight correlation anchor on `pg.route` and `ilp.connection` spans.
pub fn next_request_id() -> u64 {
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
