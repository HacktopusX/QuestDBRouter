use crate::metrics;
use crate::app::AppState;
use async_trait::async_trait;
use futures::{Sink, SinkExt, Stream, StreamExt};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::{DefaultSimpleQueryHandler, Response as ClientResponse};
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::messages::extendedquery::{
    Bind, Close, Describe, Execute, Flush, Parse, Sync as PgSync, TARGET_TYPE_BYTE_PORTAL,
    TARGET_TYPE_BYTE_STATEMENT,
};
use pgwire::api::results::{QueryResponse, Response};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireConnectionState, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::client::PgWireClient;
use pgwire::tokio::process_socket;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{debug, error};

pub async fn serve(state: AppState, listen: std::net::SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen).await?;
    let factory = Arc::new(HandlerFactory { state });
    tracing::info!(%listen, "pgwire listener ready");

    loop {
        let (socket, peer) = listener.accept().await?;
        let factory = factory.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, factory).await {
                error!(%peer, "pgwire connection error: {e:#}");
            }
        });
    }
}

struct HandlerFactory {
    state: AppState,
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(PgRouter {
            state: self.state.clone(),
        })
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(PgRouter {
            state: self.state.clone(),
        })
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(PgRouter {
            state: self.state.clone(),
        })
    }
}

#[derive(Clone)]
struct PgRouter {
    state: AppState,
}

struct PgConnState {
    backends: HashMap<u32, PgWireClient>,
    stmt_shards: HashMap<String, u32>,
    current_shard: Option<u32>,
}

impl PgConnState {
    fn new() -> Self {
        Self {
            backends: HashMap::new(),
            stmt_shards: HashMap::new(),
            current_shard: None,
        }
    }
}

fn conn_state<C: ClientInfo>(client: &C) -> Arc<Mutex<PgConnState>> {
    client
        .session_extensions()
        .get_or_insert_with(|| Mutex::new(PgConnState::new()))
}

fn wire_err(msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "XX000".into(),
        msg.into(),
    )))
}

fn stmt_name(name: &Option<String>) -> String {
    name.clone().unwrap_or_default()
}

async fn connect_backend<'a>(
    conn: &'a mut PgConnState,
    state: &AppState,
    shard_id: u32,
) -> PgWireResult<&'a mut PgWireClient> {
    if !conn.backends.contains_key(&shard_id) {
        let shard = state
            .shard_ring
            .shard_by_id(shard_id)
            .ok_or_else(|| wire_err("shard not found"))?;
        let config = state
            .backend_pg_config(&shard)
            .map_err(|e| wire_err(e.to_string()))?;
        let startup = DefaultStartupHandler::new();
        let client = PgWireClient::connect(Arc::new(config), startup, None)
            .await
            .map_err(|e| wire_err(e.to_string()))?;
        conn.backends.insert(shard_id, client);
    }
    Ok(conn.backends.get_mut(&shard_id).expect("backend exists"))
}

async fn relay_until_ready<B, C>(backend: &mut B, client: &mut C) -> PgWireResult<()>
where
    B: Stream<Item = Result<PgWireBackendMessage, PgWireError>> + Unpin,
    C: Sink<PgWireBackendMessage> + ClientInfo + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    while let Some(msg_result) = backend.next().await {
        let msg = msg_result?;
        let ready = matches!(msg, PgWireBackendMessage::ReadyForQuery(_));
        client.send(msg).await?;
        if ready {
            client.set_state(PgWireConnectionState::ReadyForQuery);
            break;
        }
    }
    Ok(())
}

fn client_to_server(responses: Vec<ClientResponse>) -> PgWireResult<Vec<Response>> {
    let mut out = Vec::with_capacity(responses.len());
    for resp in responses {
        out.push(match resp {
            ClientResponse::EmptyQuery => Response::EmptyQuery,
            ClientResponse::Query((_tag, fields, rows)) => {
                let schema = Arc::new(fields);
                let stream = futures::stream::iter(rows.into_iter().map(Ok));
                Response::Query(QueryResponse::new(schema, stream))
            }
            ClientResponse::Execution(tag) => Response::Execution(tag),
        });
    }
    Ok(out)
}

#[async_trait]
impl NoopStartupHandler for PgRouter {}

#[async_trait]
impl SimpleQueryHandler for PgRouter {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        let shard = self.state.route_sql(query);
        let shard_id = shard.id;
        metrics::record_request("read", Some(shard_id));
        debug!(shard_id, "pg simple query routed");

        let conn = conn_state(client);
        let mut guard = conn.lock().await;
        let backend = connect_backend(&mut guard, &self.state, shard_id).await?;
        let handler = DefaultSimpleQueryHandler::new();
        let responses = backend
            .simple_query(handler, query)
            .await
            .map_err(|e| wire_err(e.to_string()))?;
        client_to_server(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for PgRouter {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<NoopQueryParser> {
        Arc::new(NoopQueryParser)
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let shard = self.state.route_sql(&message.query);
        let shard_id = shard.id;
        let name = stmt_name(&message.name);
        let conn = conn_state(client);
        let mut guard = conn.lock().await;
        guard.stmt_shards.insert(name, shard_id);
        guard.current_shard = Some(shard_id);
        let backend = connect_backend(&mut guard, &self.state, shard_id).await?;
        backend.send(PgWireFrontendMessage::Parse(message)).await?;
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let stmt_key = stmt_name(&message.statement_name);
        let conn = conn_state(client);
        let mut guard = conn.lock().await;
        let shard_id = guard
            .stmt_shards
            .get(&stmt_key)
            .copied()
            .or(guard.current_shard)
            .ok_or_else(|| PgWireError::StatementNotFound(stmt_key))?;
        guard.current_shard = Some(shard_id);
        let backend = connect_backend(&mut guard, &self.state, shard_id).await?;
        backend.send(PgWireFrontendMessage::Bind(message)).await?;
        Ok(())
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn = conn_state(client);
        let mut guard = conn.lock().await;
        let shard_id = guard
            .current_shard
            .ok_or_else(|| PgWireError::PortalNotFound("".into()))?;
        metrics::record_request("read", Some(shard_id));
        let backend = connect_backend(&mut guard, &self.state, shard_id).await?;
        backend.send(PgWireFrontendMessage::Execute(message)).await?;
        Ok(())
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let shard_id = {
            let conn = conn_state(client);
            let guard = conn.lock().await;
            if message.target_type == TARGET_TYPE_BYTE_STATEMENT {
                let key = stmt_name(&message.name);
                guard
                    .stmt_shards
                    .get(&key)
                    .copied()
                    .or(guard.current_shard)
            } else if message.target_type == TARGET_TYPE_BYTE_PORTAL {
                guard.current_shard
            } else {
                guard.current_shard
            }
            .ok_or_else(|| PgWireError::StatementNotFound("describe target".into()))?
        };
        let conn = conn_state(client);
        let mut guard = conn.lock().await;
        guard.current_shard = Some(shard_id);
        let backend = connect_backend(&mut guard, &self.state, shard_id).await?;
        backend.send(PgWireFrontendMessage::Describe(message)).await?;
        Ok(())
    }

    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn = conn_state(client);
        let mut guard = conn.lock().await;
        let shard_id = guard
            .current_shard
            .ok_or_else(|| wire_err("no active shard"))?;
        let backend = connect_backend(&mut guard, &self.state, shard_id).await?;
        backend.send(PgWireFrontendMessage::Close(message)).await?;
        Ok(())
    }

    async fn on_flush<C>(&self, client: &mut C, message: Flush) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn = conn_state(client);
        let mut guard = conn.lock().await;
        let shard_id = guard
            .current_shard
            .ok_or_else(|| wire_err("no active shard"))?;
        let backend = connect_backend(&mut guard, &self.state, shard_id).await?;
        backend.send(PgWireFrontendMessage::Flush(message)).await?;
        Ok(())
    }

    async fn on_sync<C>(&self, client: &mut C, message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn = conn_state(client);
        let mut guard = conn.lock().await;
        let shard_id = guard
            .current_shard
            .ok_or_else(|| wire_err("no active shard"))?;
        let backend = connect_backend(&mut guard, &self.state, shard_id).await?;
        backend.send(PgWireFrontendMessage::Sync(message)).await?;
        relay_until_ready(backend, client).await?;
        Ok(())
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(wire_err("extended execute handled via sync relay"))
    }
}
