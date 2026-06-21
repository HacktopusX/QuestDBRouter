#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::Parser;
use quest_router::server::run;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "quest-router",
    about = "Transparent QuestDB sharding router (ILP writes, PG wire reads)"
)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "config/quest-router.toml")]
    config: String,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(&cli.config))
}
