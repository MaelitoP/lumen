use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Args;
use lumen_cluster::{Cluster, ClusterOptions, Node};
use serde::Deserialize;
use tokio::net::TcpListener;

use crate::engine::{ClusterEngine, Engine};
use crate::error::ApiError;
use crate::AppState;

#[derive(Debug, Clone, Args)]
pub struct ClusterConfig {
    #[arg(long)]
    pub id: u64,
    #[arg(long, env = "LUMEN_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,
    #[arg(long, env = "LUMEN_BIND", default_value = "127.0.0.1:7700")]
    pub bind: SocketAddr,
    #[arg(long)]
    pub raft_addr: String,
    #[arg(long = "peer", value_parser = parse_peer)]
    pub peers: Vec<(u64, String)>,
    #[arg(long)]
    pub peers_file: Option<PathBuf>,
    #[arg(long, default_value = "lumen")]
    pub cluster_name: String,
    #[arg(long, default_value_t = 30)]
    pub checkpoint_interval_secs: u64,
}

pub async fn serve_cluster(config: ClusterConfig) -> anyhow::Result<()> {
    let mut seed = BTreeMap::new();
    for (id, addr) in config.peers {
        seed.insert(id, node(id, addr));
    }
    if let Some(path) = &config.peers_file {
        for (id, addr) in read_peers_file(path)? {
            seed.insert(id, node(id, addr));
        }
    }

    let cluster = Cluster::start(ClusterOptions {
        id: config.id,
        data_dir: config.data_dir,
        raft_addr: config.raft_addr.clone(),
        seed_peers: seed,
        cluster_name: config.cluster_name,
        checkpoint_interval: Duration::from_secs(config.checkpoint_interval_secs),
    })
    .await?;

    let engine: Arc<dyn Engine> = Arc::new(ClusterEngine::new(Arc::clone(&cluster)));
    let app = crate::cluster_router(AppState { engine }, Arc::clone(&cluster));

    let listener = TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("bind {}", config.bind))?;
    tracing::info!(node = config.id, app = %config.bind, raft = %config.raft_addr, "lumen cluster node listening");

    let result = axum::serve(listener, app)
        .with_graceful_shutdown(crate::shutdown_signal())
        .await
        .context("server error");

    cluster.shutdown().await?;
    result
}

pub(crate) fn management_routes(cluster: Arc<Cluster>) -> Router {
    Router::new()
        .route("/cluster/init", post(init))
        .route("/cluster/learners", post(add_learner))
        .route("/cluster/membership", post(change_membership))
        .route("/cluster/metrics", get(metrics))
        .with_state(cluster)
}

async fn init(
    State(cluster): State<Arc<Cluster>>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let members = parse_init_members(&body)?;
    cluster.init(members).await?;
    Ok(StatusCode::OK)
}

async fn add_learner(
    State(cluster): State<Arc<Cluster>>,
    Json(body): Json<LearnerBody>,
) -> Result<impl IntoResponse, ApiError> {
    cluster.add_learner(body.node_id, body.rpc_addr).await?;
    Ok(StatusCode::OK)
}

async fn change_membership(
    State(cluster): State<Arc<Cluster>>,
    Json(body): Json<MembershipBody>,
) -> Result<impl IntoResponse, ApiError> {
    let members: BTreeSet<u64> = body.members.into_iter().collect();
    cluster.change_membership(members, body.retain).await?;
    Ok(StatusCode::OK)
}

async fn metrics(State(cluster): State<Arc<Cluster>>) -> impl IntoResponse {
    Json(cluster.metrics())
}

fn parse_init_members(body: &[u8]) -> Result<Option<BTreeMap<u64, Node>>, ApiError> {
    if body.is_empty() {
        return Ok(None);
    }
    let parsed: InitBody =
        serde_json::from_slice(body).map_err(|e| ApiError::Mapping(e.to_string()))?;
    if parsed.members.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        parsed
            .members
            .into_iter()
            .map(|m| (m.node_id, node(m.node_id, m.rpc_addr)))
            .collect(),
    ))
}

fn read_peers_file(path: &Path) -> anyhow::Result<Vec<(u64, String)>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read peers file {}", path.display()))?;
    let mut peers = Vec::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (id, addr) = parse_peer(line).map_err(anyhow::Error::msg)?;
        peers.push((id, addr));
    }
    Ok(peers)
}

fn parse_peer(value: &str) -> Result<(u64, String), String> {
    let (id, addr) = value
        .split_once('=')
        .ok_or_else(|| "expected <id>=<addr>".to_string())?;
    let id = id.parse().map_err(|_| format!("invalid peer id: {id}"))?;
    Ok((id, addr.to_string()))
}

fn node(id: u64, rpc_addr: String) -> Node {
    Node {
        node_id: id,
        rpc_addr,
    }
}

#[derive(Debug, Deserialize)]
struct InitBody {
    #[serde(default)]
    members: Vec<MemberSpec>,
}

#[derive(Debug, Deserialize)]
struct MemberSpec {
    node_id: u64,
    rpc_addr: String,
}

#[derive(Debug, Deserialize)]
struct LearnerBody {
    node_id: u64,
    rpc_addr: String,
}

#[derive(Debug, Deserialize)]
struct MembershipBody {
    members: Vec<u64>,
    #[serde(default)]
    retain: bool,
}
