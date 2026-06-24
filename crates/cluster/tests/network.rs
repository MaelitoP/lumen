use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use lumen_cluster::{
    raft_config, LogStore, LumenRaft, NetworkFactory, Node, NodeId, RaftServer, StateMachine,
};
use lumen_core::{Catalog, LogMark};
use lumen_proto::v1 as proto;
use openraft::{Config, LogId, SnapshotPolicy};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const BOOKS_UUID: &str = "11111111-1111-1111-1111-111111111111";

struct TestNode {
    raft: LumenRaft,
    sm: StateMachine,
    addr: String,
    server: JoinHandle<()>,
    _dir: TempDir,
}

async fn start_node(id: NodeId) -> TestNode {
    start_node_with(id, raft_config()).await
}

async fn start_node_with(id: NodeId, config: Config) -> TestNode {
    let dir = TempDir::new().unwrap();
    let catalog = Catalog::open(dir.path().join("state")).unwrap();
    let sm = StateMachine::new(catalog);
    let log_store = LogStore::open(&dir.path().join("log")).unwrap();

    let config = Arc::new(
        Config {
            cluster_name: "lumen-network-test".to_string(),
            ..config
        }
        .validate()
        .unwrap(),
    );

    let raft = LumenRaft::new(id, config, NetworkFactory, log_store, sm.clone())
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let service = RaftServer::new(raft.clone()).into_service();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    TestNode {
        raft,
        sm,
        addr,
        server,
        _dir: dir,
    }
}

fn node(id: NodeId, addr: &str) -> Node {
    Node {
        node_id: id,
        rpc_addr: addr.to_string(),
    }
}

fn mark(log_id: LogId<NodeId>) -> LogMark {
    LogMark {
        term: log_id.leader_id.term,
        node: log_id.leader_id.node_id,
        index: log_id.index,
    }
}

fn create_books() -> proto::Command {
    proto::Command {
        op: Some(proto::command::Op::CreateCollection(
            proto::CreateCollection {
                collection: "books".to_string(),
                uuid: BOOKS_UUID.to_string(),
                mapping: Some(proto::Mapping {
                    fields: vec![proto::Field {
                        name: "title".to_string(),
                        r#type: proto::FieldType::Text as i32,
                        indexed: true,
                        fast: false,
                    }],
                }),
            },
        )),
    }
}

fn index(id: &str, title: &str) -> proto::Command {
    proto::Command {
        op: Some(proto::command::Op::IndexDocument(proto::IndexDocument {
            collection: "books".to_string(),
            id: id.to_string(),
            source: format!(r#"{{"title":"{title}"}}"#).into_bytes(),
        })),
    }
}

#[tokio::test]
async fn two_nodes_elect_replicate_and_apply() {
    let n1 = start_node(1).await;
    let n2 = start_node(2).await;

    let mut members = BTreeMap::new();
    members.insert(1, node(1, &n1.addr));
    members.insert(2, node(2, &n2.addr));

    n1.raft.initialize(members).await.unwrap();

    n1.raft
        .wait(Some(Duration::from_secs(10)))
        .current_leader(1, "node 1 wins the election after vote exchange")
        .await
        .unwrap();

    n1.raft.client_write(create_books()).await.unwrap();
    let write = n1.raft.client_write(index("b1", "alpha")).await.unwrap();

    n2.raft
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(
            Some(write.log_id.index),
            "node 2 applies the replicated write",
        )
        .await
        .unwrap();

    n2.sm
        .catalog()
        .checkpoint_applied(mark(write.log_id))
        .unwrap();
    let books = n2.sm.catalog().get("books").unwrap();
    assert_eq!(books.search("alpha", 10, 0).unwrap().total, 1);

    n1.raft.shutdown().await.unwrap();
    n2.raft.shutdown().await.unwrap();
    n1.server.abort();
    n2.server.abort();
}

#[tokio::test]
async fn lagging_learner_catches_up_via_snapshot_over_the_wire() {
    let n1 = start_node_with(
        1,
        Config {
            snapshot_policy: SnapshotPolicy::LogsSinceLast(1),
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        },
    )
    .await;

    let mut members = BTreeMap::new();
    members.insert(1, node(1, &n1.addr));
    n1.raft.initialize(members).await.unwrap();
    n1.raft
        .wait(Some(Duration::from_secs(10)))
        .current_leader(1, "node 1 leads the single-node cluster")
        .await
        .unwrap();

    n1.raft.client_write(create_books()).await.unwrap();
    n1.raft.client_write(index("b1", "alpha")).await.unwrap();
    let write = n1.raft.client_write(index("b2", "beta")).await.unwrap();

    n1.sm
        .catalog()
        .checkpoint_applied(mark(write.log_id))
        .unwrap();
    n1.raft.trigger().snapshot().await.unwrap();
    n1.raft
        .wait(Some(Duration::from_secs(10)))
        .snapshot(write.log_id, "snapshot built over the committed writes")
        .await
        .unwrap();
    n1.raft
        .trigger()
        .purge_log(write.log_id.index)
        .await
        .unwrap();
    n1.raft
        .wait(Some(Duration::from_secs(10)))
        .purged(Some(write.log_id), "log purged past the writes")
        .await
        .unwrap();

    let n2 = start_node(2).await;
    n1.raft
        .add_learner(2, node(2, &n2.addr), true)
        .await
        .unwrap();

    n2.raft
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(
            Some(write.log_id.index),
            "node 2 catches up via snapshot install over the wire",
        )
        .await
        .unwrap();

    let books = n2.sm.catalog().get("books").unwrap();
    assert_eq!(books.search("alpha", 10, 0).unwrap().total, 1);
    assert_eq!(books.search("beta", 10, 0).unwrap().total, 1);

    n1.raft.shutdown().await.unwrap();
    n2.raft.shutdown().await.unwrap();
    n1.server.abort();
    n2.server.abort();
}
