use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lumen_core::{Catalog, LogMark, Mapping, SearchResults};
use lumen_proto::v1 as proto;
use openraft::error::{CheckIsLeaderError, ClientWriteError, InitializeError, RaftError};
use openraft::{ChangeMembers, Config, LogId, RaftMetrics, ServerState};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::network::{NetworkFactory, RaftServer};
use crate::state_machine::StateMachine;
use crate::type_config::{raft_config, LumenRaft, Node, NodeId, Response};
use crate::LogStore;

const STATE_DIR: &str = "state";
const RAFT_DIR: &str = "raft";

/// Errors surfaced to the HTTP layer.
///
/// `ForwardToLeader(Some(node))` becomes a 307 to the leader; `None` (leader
/// unknown) and `Unavailable` (no quorum) become 503.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    Core(#[from] lumen_core::Error),
    #[error("not leader")]
    ForwardToLeader(Option<Node>),
    #[error("cluster already initialized")]
    AlreadyInitialized,
    #[error("cluster unavailable")]
    Unavailable,
    #[error("cluster fatal error: {0}")]
    Fatal(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Clone)]
pub struct ClusterOptions {
    pub id: NodeId,
    pub data_dir: PathBuf,
    pub raft_addr: String,
    pub seed_peers: BTreeMap<NodeId, Node>,
    pub cluster_name: String,
    pub checkpoint_interval: Duration,
}

#[derive(Debug, Clone)]
pub struct CreateOutcome {
    pub mapping: Mapping,
    pub created: bool,
}

#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub id: String,
    pub created: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterMetrics {
    pub id: NodeId,
    pub current_term: u64,
    pub state: String,
    pub current_leader: Option<NodeId>,
    pub last_log_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub voters: Vec<NodeId>,
    pub members: Vec<NodeId>,
}

/// A running cluster node: the local `Raft`, its state machine, the peer-facing
/// `RaftService` server, and a checkpoint loop that advances the committed mark.
pub struct Cluster {
    id: NodeId,
    self_node: Node,
    seed_peers: BTreeMap<NodeId, Node>,
    raft: LumenRaft,
    sm: StateMachine,
    raft_server: JoinHandle<()>,
    checkpoint: JoinHandle<()>,
    /// Serializes collection DDL admission with its commit, so the conflict check
    /// in `build_*_command` is atomic with the log append and apply. Without it two
    /// concurrent conflicting creates could both pass admission and commit.
    ddl: Mutex<()>,
}

impl Cluster {
    pub async fn start(opts: ClusterOptions) -> anyhow::Result<Arc<Self>> {
        let catalog = Catalog::open(opts.data_dir.join(STATE_DIR))?;
        let raft_dir = opts.data_dir.join(RAFT_DIR);
        let sm = StateMachine::open(catalog, &raft_dir)?;
        let log_store = LogStore::open(&raft_dir)?;

        let config = Arc::new(
            Config {
                cluster_name: opts.cluster_name,
                ..raft_config()
            }
            .validate()?,
        );

        let raft = LumenRaft::new(opts.id, config, NetworkFactory, log_store, sm.clone()).await?;

        let listener = TcpListener::bind(&opts.raft_addr).await?;
        let rpc_addr = listener.local_addr()?.to_string();
        let service = RaftServer::new(raft.clone()).into_service();
        let raft_server = tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
            {
                tracing::error!(error = %e, "raft service server stopped");
            }
        });

        let checkpoint = tokio::spawn(checkpoint_loop(
            raft.clone(),
            sm.clone(),
            opts.checkpoint_interval,
        ));

        let self_node = Node {
            node_id: opts.id,
            rpc_addr,
        };
        Ok(Arc::new(Self {
            id: opts.id,
            self_node,
            seed_peers: opts.seed_peers,
            raft,
            sm,
            raft_server,
            checkpoint,
            ddl: Mutex::new(()),
        }))
    }

    pub async fn init(&self, members: Option<BTreeMap<NodeId, Node>>) -> Result<(), ClientError> {
        let members = members.unwrap_or_else(|| self.default_members());
        match self.raft.initialize(members).await {
            Ok(()) => Ok(()),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                Err(ClientError::AlreadyInitialized)
            }
            Err(RaftError::APIError(InitializeError::NotInMembers(e))) => Err(ClientError::Core(
                lumen_core::Error::Validation(e.to_string()),
            )),
            Err(RaftError::Fatal(f)) => Err(ClientError::Fatal(Box::new(f))),
        }
    }

    pub async fn add_learner(&self, id: NodeId, rpc_addr: String) -> Result<(), ClientError> {
        let node = Node {
            node_id: id,
            rpc_addr,
        };
        self.raft
            .add_learner(id, node, true)
            .await
            .map_err(classify_write)?;
        Ok(())
    }

    pub async fn change_membership(
        &self,
        members: BTreeSet<NodeId>,
        retain: bool,
    ) -> Result<(), ClientError> {
        self.raft
            .change_membership(ChangeMembers::ReplaceAllVoters(members), retain)
            .await
            .map_err(classify_write)?;
        Ok(())
    }

    pub fn metrics(&self) -> ClusterMetrics {
        let metrics = self.raft.metrics();
        let borrowed = metrics.borrow();
        to_cluster_metrics(&borrowed)
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn raft_addr(&self) -> &str {
        &self.self_node.rpc_addr
    }

    pub async fn wait_for_leader(&self, timeout: Duration) -> anyhow::Result<NodeId> {
        let metrics = self
            .wait_until(timeout, |m| m.current_leader.is_some())
            .await?;
        Ok(metrics.current_leader.expect("leader present after wait"))
    }

    /// Awaits a metrics condition or times out; the predicate sees the same
    /// [`ClusterMetrics`] that [`Self::metrics`] returns.
    pub async fn wait_until(
        &self,
        timeout: Duration,
        cond: impl Fn(&ClusterMetrics) -> bool + Send,
    ) -> anyhow::Result<ClusterMetrics> {
        let metrics = self
            .raft
            .wait(Some(timeout))
            .metrics(
                move |m| cond(&to_cluster_metrics(m)),
                "wait_until condition",
            )
            .await?;
        Ok(to_cluster_metrics(&metrics))
    }

    pub async fn create_collection(
        &self,
        name: &str,
        mapping: Mapping,
    ) -> Result<CreateOutcome, ClientError> {
        let _ddl = self.ddl.lock().await;
        self.ensure_leader()?;
        match self
            .sm
            .catalog()
            .build_create_command(name, mapping.clone())?
        {
            Some(cmd) => {
                self.client_write(cmd).await?;
                Ok(CreateOutcome {
                    mapping,
                    created: true,
                })
            }
            None => Ok(CreateOutcome {
                mapping,
                created: false,
            }),
        }
    }

    pub async fn index(
        &self,
        collection: &str,
        id: Option<&str>,
        source: &[u8],
    ) -> Result<WriteOutcome, ClientError> {
        self.ensure_leader()?;
        let cmd = self
            .sm
            .catalog()
            .build_index_command(collection, id, source)?;
        let resp = self.client_write(cmd).await?;
        Ok(WriteOutcome {
            id: resp.id,
            created: resp.created,
        })
    }

    pub async fn delete(&self, collection: &str, id: &str) -> Result<(), ClientError> {
        self.ensure_leader()?;
        let cmd = self.sm.catalog().build_delete_command(collection, id)?;
        self.client_write(cmd).await?;
        Ok(())
    }

    pub async fn drop_collection(&self, name: &str) -> Result<(), ClientError> {
        let _ddl = self.ddl.lock().await;
        self.ensure_leader()?;
        let cmd = self.sm.catalog().build_drop_command(name)?;
        self.client_write(cmd).await?;
        Ok(())
    }

    /// Linearizable get-by-id: confirms leadership via the read-index, then reads
    /// the local state machine. Reflects every write committed before the call.
    pub async fn linearizable_get(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Vec<u8>, ClientError> {
        self.raft
            .ensure_linearizable()
            .await
            .map_err(classify_read)?;
        let mark = self
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(to_mark)
            .unwrap_or_default();
        let sm = self.sm.clone();
        let collection = collection.to_owned();
        let id = id.to_owned();
        spawn_read(move || {
            sm.catalog().commit_collection(&collection, mark)?;
            sm.catalog().get_document(&collection, &id)
        })
        .await
    }

    /// Leader-local search. **Eventually consistent**: it serves the leader's last
    /// committed (searchable) view, which may trail the most recently applied
    /// writes until the next checkpoint commit. A multi-doc scan is not made
    /// linearizable; use [`Self::linearizable_get`] for read-your-write by id.
    pub async fn search(
        &self,
        collection: &str,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResults, ClientError> {
        self.ensure_leader()?;
        let sm = self.sm.clone();
        let collection = collection.to_owned();
        let query = query.to_owned();
        spawn_read(move || sm.catalog().get(&collection)?.search(&query, limit, offset)).await
    }

    pub fn list(&self) -> Vec<String> {
        self.sm.catalog().list()
    }

    pub fn describe(&self, name: &str) -> Result<Mapping, ClientError> {
        Ok(self.sm.catalog().describe(name)?)
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.raft.shutdown().await?;
        self.checkpoint.abort();
        self.raft_server.abort();
        Ok(())
    }

    async fn client_write(&self, cmd: proto::Command) -> Result<Response, ClientError> {
        let resp = self.raft.client_write(cmd).await.map_err(classify_write)?;
        Ok(resp.data)
    }

    fn default_members(&self) -> BTreeMap<NodeId, Node> {
        let mut members = self.seed_peers.clone();
        members.insert(self.id, self.self_node.clone());
        members
    }

    fn ensure_leader(&self) -> Result<(), ClientError> {
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader == Some(self.id) {
            Ok(())
        } else {
            Err(self.forward_or_unavailable(&metrics))
        }
    }

    fn forward_or_unavailable(&self, metrics: &RaftMetrics<NodeId, Node>) -> ClientError {
        let Some(leader) = metrics.current_leader else {
            return ClientError::Unavailable;
        };
        match metrics.membership_config.membership().get_node(&leader) {
            Some(node) => ClientError::ForwardToLeader(Some(node.clone())),
            None => ClientError::Unavailable,
        }
    }
}

async fn spawn_read<T, F>(f: F) -> Result<T, ClientError>
where
    F: FnOnce() -> lumen_core::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result.map_err(ClientError::Core),
        Err(e) => Err(ClientError::Fatal(Box::new(e))),
    }
}

async fn checkpoint_loop(raft: LumenRaft, sm: StateMachine, interval: Duration) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tick.tick().await;
    loop {
        tick.tick().await;
        let Some(applied) = raft.metrics().borrow().last_applied else {
            continue;
        };
        let mark = to_mark(applied);
        let sm = sm.clone();
        match tokio::task::spawn_blocking(move || sm.catalog().checkpoint_applied(mark)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "cluster checkpoint failed"),
            Err(e) => tracing::error!(error = %e, "cluster checkpoint task panicked"),
        }
    }
}

fn to_mark(log_id: LogId<u64>) -> LogMark {
    LogMark {
        term: log_id.leader_id.term,
        node: log_id.leader_id.node_id,
        index: log_id.index,
    }
}

fn classify_write(err: RaftError<NodeId, ClientWriteError<NodeId, Node>>) -> ClientError {
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => {
            ClientError::ForwardToLeader(f.leader_node)
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(e)) => {
            ClientError::Core(lumen_core::Error::Validation(e.to_string()))
        }
        RaftError::Fatal(f) => ClientError::Fatal(Box::new(f)),
    }
}

fn classify_read(err: RaftError<NodeId, CheckIsLeaderError<NodeId, Node>>) -> ClientError {
    match err {
        RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f)) => {
            ClientError::ForwardToLeader(f.leader_node)
        }
        RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_)) => ClientError::Unavailable,
        RaftError::Fatal(f) => ClientError::Fatal(Box::new(f)),
    }
}

fn server_state(state: ServerState) -> &'static str {
    match state {
        ServerState::Learner => "learner",
        ServerState::Follower => "follower",
        ServerState::Candidate => "candidate",
        ServerState::Leader => "leader",
        ServerState::Shutdown => "shutdown",
    }
}

fn to_cluster_metrics(m: &RaftMetrics<NodeId, Node>) -> ClusterMetrics {
    let membership = m.membership_config.membership();
    ClusterMetrics {
        id: m.id,
        current_term: m.current_term,
        state: server_state(m.state).to_string(),
        current_leader: m.current_leader,
        last_log_index: m.last_log_index,
        last_applied_index: m.last_applied.map(|l| l.index),
        voters: membership.voter_ids().collect(),
        members: membership.nodes().map(|(id, _)| *id).collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use openraft::error::{Fatal, ForwardToLeader, QuorumNotEnough};

    use super::*;

    fn node(id: NodeId) -> Node {
        Node {
            node_id: id,
            rpc_addr: format!("127.0.0.1:{id}"),
        }
    }

    #[test]
    fn write_forward_to_known_leader_maps_to_forward() {
        let err = RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader::new(
            7,
            node(7),
        )));
        match classify_write(err) {
            ClientError::ForwardToLeader(Some(leader)) => assert_eq!(leader.node_id, 7),
            other => panic!("expected ForwardToLeader(Some), got {other:?}"),
        }
    }

    #[test]
    fn write_forward_to_unknown_leader_keeps_none() {
        let err = RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader::empty()));
        assert!(matches!(
            classify_write(err),
            ClientError::ForwardToLeader(None)
        ));
    }

    #[test]
    fn write_fatal_maps_to_fatal() {
        let err: RaftError<NodeId, ClientWriteError<NodeId, Node>> =
            RaftError::Fatal(Fatal::Panicked);
        assert!(matches!(classify_write(err), ClientError::Fatal(_)));
    }

    #[test]
    fn read_quorum_not_enough_maps_to_unavailable() {
        let err = RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(QuorumNotEnough {
            cluster: "c".to_string(),
            got: BTreeSet::new(),
        }));
        assert!(matches!(classify_read(err), ClientError::Unavailable));
    }

    #[test]
    fn read_forward_to_leader_maps_to_forward() {
        let err = RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader::new(
            3,
            node(3),
        )));
        match classify_read(err) {
            ClientError::ForwardToLeader(Some(leader)) => assert_eq!(leader.node_id, 3),
            other => panic!("expected ForwardToLeader(Some), got {other:?}"),
        }
    }

    #[test]
    fn server_state_names_are_stable() {
        assert_eq!(server_state(ServerState::Leader), "leader");
        assert_eq!(server_state(ServerState::Follower), "follower");
        assert_eq!(server_state(ServerState::Learner), "learner");
    }
}
