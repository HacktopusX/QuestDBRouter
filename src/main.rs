#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::Parser;
use quest_router::logging;
use quest_router::server::run;

#[derive(Parser, Debug)]
#[command(
    name = "quest-router",
    about = "Transparent QuestDB sharding router (ILP writes, datafusion-postgres reads)"
)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "config/quest-router.toml")]
    config: String,
}

fn main() -> anyhow::Result<()> {
    logging::init()?;

    let cli = Cli::parse();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(&cli.config))
}
