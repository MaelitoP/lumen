use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

mod error;
mod handlers;

use anyhow::Context;
use axum::routing::{get, post, put};
use axum::{http::StatusCode, Router};
use clap::Parser;
use lumen_core::Catalog;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Parser)]
#[command(name = "lumen", about = "Lumen single-node document database")]
pub struct Config {
    #[arg(long, env = "LUMEN_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,
    #[arg(long, env = "LUMEN_BIND", default_value = "127.0.0.1:7700")]
    pub bind: SocketAddr,
    #[arg(long, env = "LUMEN_CHECKPOINT_INTERVAL_SECS", default_value_t = 30)]
    pub checkpoint_interval_secs: u64,
}

#[derive(Clone, Debug)]
pub struct AppState {
    pub catalog: Arc<Catalog>,
}

pub fn router(state: AppState) -> Router {
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
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .with_state(state)
}

pub async fn serve(config: Config) -> anyhow::Result<()> {
    let catalog = Arc::new(
        Catalog::open(&config.data_dir)
            .with_context(|| format!("open catalog at {}", config.data_dir.display()))?,
    );
    let state = AppState {
        catalog: Arc::clone(&catalog),
    };

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

async fn shutdown_signal() {
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
