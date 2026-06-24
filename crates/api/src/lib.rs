use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

mod cluster;
mod engine;
mod error;
mod handlers;

use anyhow::Context;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};
use axum::{http::StatusCode, Router};
use clap::{Args, Parser, Subcommand};
use lumen_core::Catalog;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

pub use cluster::{serve_cluster, ClusterConfig};
pub use engine::{ClusterEngine, Engine, StandaloneEngine};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Parser)]
#[command(name = "lumen", about = "Lumen document database")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Run a single-node server.
    Standalone(StandaloneConfig),
    /// Run a node in a Raft cluster.
    Cluster(ClusterConfig),
}

#[derive(Debug, Clone, Args)]
pub struct StandaloneConfig {
    #[arg(long, env = "LUMEN_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,
    #[arg(long, env = "LUMEN_BIND", default_value = "127.0.0.1:7700")]
    pub bind: SocketAddr,
    #[arg(long, env = "LUMEN_CHECKPOINT_INTERVAL_SECS", default_value_t = 30)]
    pub checkpoint_interval_secs: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<dyn Engine>,
}

pub fn router(state: AppState) -> Router {
    with_layers(app_routes(state))
}

pub(crate) fn cluster_router(state: AppState, cluster: Arc<lumen_cluster::Cluster>) -> Router {
    with_layers(app_routes(state).merge(cluster::management_routes(cluster)))
}

fn app_routes(state: AppState) -> Router {
    Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/collections", get(handlers::list_collections))
        .route(
            "/collections/{name}",
            put(handlers::create_collection)
                .get(handlers::describe_collection)
                .delete(handlers::drop_collection),
        )
        .route(
            "/collections/{name}/documents",
            post(handlers::index_document),
        )
        .route(
            "/collections/{name}/documents/search",
            get(handlers::search_documents),
        )
        .route(
            "/collections/{name}/documents/{id}",
            put(handlers::put_document)
                .get(handlers::get_document)
                .delete(handlers::delete_document),
        )
        .with_state(state)
}

fn with_layers(router: Router) -> Router {
    router
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
}

pub async fn serve(config: StandaloneConfig) -> anyhow::Result<()> {
    let catalog = Arc::new(
        Catalog::open(&config.data_dir)
            .with_context(|| format!("open catalog at {}", config.data_dir.display()))?,
    );
    let engine: Arc<dyn Engine> = Arc::new(StandaloneEngine::new(Arc::clone(&catalog)));
    let state = AppState { engine };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let interval = Duration::from_secs(config.checkpoint_interval_secs);
    let checkpoint = tokio::spawn(checkpoint_loop(Arc::clone(&catalog), interval, shutdown_rx));

    let listener = TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("bind {}", config.bind))?;
    tracing::info!(addr = %config.bind, "lumen listening");

    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    let _ = shutdown_tx.send(true);
    checkpoint.await.context("join checkpoint task")?;
    Ok(())
}

async fn checkpoint_loop(
    catalog: Arc<Catalog>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick.tick().await;
    loop {
        tokio::select! {
            _ = tick.tick() => run_checkpoint(&catalog).await,
            _ = shutdown.changed() => {
                run_checkpoint(&catalog).await;
                return;
            }
        }
    }
}

async fn run_checkpoint(catalog: &Arc<Catalog>) {
    let catalog = Arc::clone(catalog);
    match tokio::task::spawn_blocking(move || catalog.checkpoint()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::error!(error = %e, "checkpoint failed"),
        Err(e) => tracing::error!(error = %e, "checkpoint task panicked"),
    }
}

pub(crate) async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                term.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
