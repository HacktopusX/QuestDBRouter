use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Initialize unified logging: `log::` macros bridge into a single tracing subscriber.
///
/// Log level is read from `RUST_LOG` (default: `info`).
/// Set `QUEST_ROUTER__LOG__FORMAT=json` for JSON output.
/// Optional file path via `QUEST_ROUTER__LOG__FILE` (appends to file alongside stdout).
pub fn init() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let json = std::env::var("QUEST_ROUTER__LOG__FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    let file_path = std::env::var("QUEST_ROUTER__LOG__FILE")
        .ok()
        .filter(|p| !p.is_empty());

    match file_path {
        Some(path) => {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|e| anyhow::anyhow!("failed to open log file {path}: {e}"))?;
            let stdout_layer = if json {
                fmt::layer().json().with_writer(std::io::stdout).boxed()
            } else {
                fmt::layer()
                    .with_target(false)
                    .with_writer(std::io::stdout)
                    .boxed()
            };
            let file_layer = if json {
                fmt::layer().json().with_writer(file).boxed()
            } else {
                fmt::layer().with_target(false).with_writer(file).boxed()
            };
            tracing_subscriber::registry()
                .with(filter)
                .with(stdout_layer)
                .with(file_layer)
                .try_init()
                .map_err(|e| anyhow::anyhow!("failed to init tracing subscriber: {e}"))?;
        }
        None => {
            let layer = if json {
                fmt::layer().json().boxed()
            } else {
                fmt::layer().with_target(false).boxed()
            };
            tracing_subscriber::registry()
                .with(filter)
                .with(layer)
                .try_init()
                .map_err(|e| anyhow::anyhow!("failed to init tracing subscriber: {e}"))?;
        }
    }

    Ok(())
}
