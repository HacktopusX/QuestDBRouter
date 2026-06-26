use fast_log::config::Config;
use log::LevelFilter;

const DEFAULT_CHAN_LEN: usize = 100_000;

/// Initialize fast_log (`log::` macros) then tracing spans.
///
/// fast_log must install the global `log` logger first (it uses rbatis internally).
/// Tracing is layered on afterward via `try_init` so it never panics if already set.
///
/// Log level is read from `RUST_LOG` (default: `info`).
/// Optional file path via `QUEST_ROUTER__LOG__FILE`.
pub fn init() -> anyhow::Result<()> {
    let level = level_from_env();
    let mut config = Config::new()
        .console()
        .level(level)
        .chan_len(Some(DEFAULT_CHAN_LEN));

    if let Ok(path) = std::env::var("QUEST_ROUTER__LOG__FILE")
        && !path.is_empty()
    {
        config = config.file(&path);
    }

    fast_log::init(config)
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("failed to init logging: {e}"))?;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();

    Ok(())
}

fn level_from_env() -> LevelFilter {
    match std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "info".into())
        .to_ascii_lowercase()
        .as_str()
    {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        "off" => LevelFilter::Off,
        _ => LevelFilter::Info,
    }
}
