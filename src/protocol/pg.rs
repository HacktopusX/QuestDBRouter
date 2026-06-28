use crate::app::AppState;
use crate::protocol::{pg_hook, pg_handlers, pg_session};
use log::info;
use std::net::SocketAddr;
use std::sync::Arc;

/// Analytical read-only PG endpoint backed by datafusion-postgres.
#[cfg(feature = "federated")]
pub async fn serve(state: AppState, listen: SocketAddr) -> anyhow::Result<()> {
    use datafusion_postgres::{serve_with_handlers, ServerOptions};

    let (session_context, catalog) = pg_session::build_session_context(&state).await?;
    let router_hook = Arc::new(pg_hook::RouterQueryHook::new(state.clone(), catalog.clone()));
    let hooks: Vec<Arc<dyn datafusion_postgres::QueryHook>> = vec![router_hook.clone()];

    let factory = Arc::new(pg_handlers::QuestDbHandlerFactory::new(
        session_context,
        hooks,
        router_hook,
    ));

    let host = match listen.ip() {
        std::net::IpAddr::V4(v4) => v4.to_string(),
        std::net::IpAddr::V6(v6) => format!("[{v6}]"),
    };
    let opts = ServerOptions::new()
        .with_host(host)
        .with_port(listen.port());

    info!("datafusion-postgres listener ready on {listen}");
    serve_with_handlers(factory, &opts).await?;
    Ok(())
}

#[cfg(not(feature = "federated"))]
pub async fn serve(_state: AppState, listen: SocketAddr) -> anyhow::Result<()> {
    anyhow::bail!(
        "PG analytical endpoint requires the federated feature; rebuild with --features federated (listen={listen})"
    )
}
