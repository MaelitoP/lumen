use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lumen_cluster::{ClientError, Cluster, ClusterOptions};
use lumen_core::{Error, Mapping};
use tempfile::TempDir;

const TIMEOUT: Duration = Duration::from_secs(10);
const MAPPING: &str = r#"{"fields":{"title":{"type":"text","indexed":true}}}"#;
const DOC: &[u8] = br#"{"title":"the hobbit"}"#;

struct TestNode {
    cluster: Arc<Cluster>,
    _dir: TempDir,
}

async fn start(id: u64) -> TestNode {
    let dir = TempDir::new().unwrap();
    let cluster = start_on(id, dir.path().to_path_buf(), "127.0.0.1:0".to_string()).await;
    TestNode { cluster, _dir: dir }
}

async fn start_on(id: u64, data_dir: PathBuf, raft_addr: String) -> Arc<Cluster> {
    Cluster::start(ClusterOptions {
        id,
        data_dir,
        raft_addr,
        seed_peers: BTreeMap::new(),
        cluster_name: "lifecycle-test".to_string(),
        checkpoint_interval: Duration::from_secs(3600),
    })
    .await
    .unwrap()
}

async fn shutdown(nodes: impl IntoIterator<Item = TestNode>) {
    for node in nodes {
        node.cluster.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn three_node_cluster_forms_and_reports_leader_and_voters() {
    let n1 = start(1).await;
    let n2 = start(2).await;
    let n3 = start(3).await;

    n1.cluster.init(None).await.unwrap();
    n1.cluster.wait_for_leader(TIMEOUT).await.unwrap();

    n1.cluster
        .add_learner(2, n2.cluster.raft_addr().to_string())
        .await
        .unwrap();
    n1.cluster
        .add_learner(3, n3.cluster.raft_addr().to_string())
        .await
        .unwrap();
    n1.cluster
        .change_membership([1, 2, 3].into_iter().collect(), false)
        .await
        .unwrap();

    let metrics = n1.cluster.metrics();
    assert_eq!(metrics.current_leader, Some(1));
    let mut voters = metrics.voters;
    voters.sort_unstable();
    assert_eq!(voters, vec![1, 2, 3]);

    shutdown([n1, n2, n3]).await;
}

#[tokio::test]
async fn second_init_is_rejected() {
    let n1 = start(1).await;
    n1.cluster.init(None).await.unwrap();
    n1.cluster.wait_for_leader(TIMEOUT).await.unwrap();

    let err = n1.cluster.init(None).await.unwrap_err();
    assert!(
        matches!(err, ClientError::AlreadyInitialized),
        "expected AlreadyInitialized, got {err:?}"
    );

    shutdown([n1]).await;
}

#[tokio::test]
async fn concurrent_conflicting_creates_do_not_both_commit() {
    let n1 = start(1).await;
    n1.cluster.init(None).await.unwrap();
    n1.cluster.wait_for_leader(TIMEOUT).await.unwrap();

    let a: Mapping =
        serde_json::from_str(r#"{"fields":{"title":{"type":"text","indexed":true}}}"#).unwrap();
    let b: Mapping =
        serde_json::from_str(r#"{"fields":{"title":{"type":"keyword","indexed":true}}}"#).unwrap();

    let (r1, r2) = tokio::join!(
        n1.cluster.create_collection("books", a),
        n1.cluster.create_collection("books", b),
    );

    let results = [r1, r2];
    let created = results
        .iter()
        .filter(|r| matches!(r, Ok(outcome) if outcome.created))
        .count();
    let conflicts = results
        .iter()
        .filter(|r| matches!(r, Err(ClientError::Core(Error::SchemaConflict { .. }))))
        .count();
    assert_eq!(created, 1, "exactly one conflicting create may commit");
    assert_eq!(conflicts, 1, "the loser must be rejected at admission");

    shutdown([n1]).await;
}

#[tokio::test]
async fn follower_write_redirects_and_linearizable_get_reflects_leader_write() {
    let n1 = start(1).await;
    let n2 = start(2).await;

    n1.cluster.init(None).await.unwrap();
    n1.cluster.wait_for_leader(TIMEOUT).await.unwrap();
    n1.cluster
        .add_learner(2, n2.cluster.raft_addr().to_string())
        .await
        .unwrap();
    n1.cluster
        .change_membership([1, 2].into_iter().collect(), false)
        .await
        .unwrap();
    n2.cluster.wait_for_leader(TIMEOUT).await.unwrap();

    let mapping: Mapping = serde_json::from_str(MAPPING).unwrap();
    n1.cluster
        .create_collection("books", mapping)
        .await
        .unwrap();

    let err = n2
        .cluster
        .index("books", Some("b1"), DOC)
        .await
        .unwrap_err();
    match err {
        ClientError::ForwardToLeader(Some(node)) => assert_eq!(node.node_id, 1),
        other => panic!("expected ForwardToLeader(node 1), got {other:?}"),
    }

    let outcome = n1.cluster.index("books", Some("b1"), DOC).await.unwrap();
    assert_eq!(outcome.id, "b1");
    assert!(outcome.created);

    let source = n1.cluster.linearizable_get("books", "b1").await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&source).unwrap();
    assert_eq!(value["title"], "the hobbit");

    shutdown([n1, n2]).await;
}

#[tokio::test]
async fn killing_the_leader_reelects_redirects_and_the_killed_node_rejoins() {
    let dir1 = TempDir::new().unwrap();
    let n1 = start_on(1, dir1.path().to_path_buf(), "127.0.0.1:0".to_string()).await;
    let addr1 = n1.raft_addr().to_string();
    let n2 = start(2).await;
    let n3 = start(3).await;

    n1.init(None).await.unwrap();
    n1.wait_for_leader(TIMEOUT).await.unwrap();
    n1.add_learner(2, n2.cluster.raft_addr().to_string())
        .await
        .unwrap();
    n1.add_learner(3, n3.cluster.raft_addr().to_string())
        .await
        .unwrap();
    n1.change_membership([1, 2, 3].into_iter().collect(), false)
        .await
        .unwrap();

    let mapping: Mapping = serde_json::from_str(MAPPING).unwrap();
    n1.create_collection("books", mapping).await.unwrap();
    let written = n1.index("books", Some("b1"), DOC).await.unwrap();
    assert_eq!(written.id, "b1");
    assert!(written.created);

    let source = n1.linearizable_get("books", "b1").await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&source).unwrap();
    assert_eq!(value["title"], "the hobbit");

    n1.shutdown().await.unwrap();
    drop(n1);

    let elected = n2
        .cluster
        .wait_until(TIMEOUT, |m| matches!(m.current_leader, Some(l) if l != 1))
        .await
        .unwrap();
    let leader_id = elected.current_leader.unwrap();
    let (leader, follower) = if leader_id == 2 {
        (&n2, &n3)
    } else {
        (&n3, &n2)
    };
    follower
        .cluster
        .wait_until(TIMEOUT, move |m| m.current_leader == Some(leader_id))
        .await
        .unwrap();

    let written = leader
        .cluster
        .index("books", Some("b2"), DOC)
        .await
        .unwrap();
    assert_eq!(written.id, "b2");

    let err = follower
        .cluster
        .index("books", Some("b3"), DOC)
        .await
        .unwrap_err();
    match err {
        ClientError::ForwardToLeader(Some(node)) => assert_eq!(node.node_id, leader_id),
        other => panic!("expected ForwardToLeader(node {leader_id}), got {other:?}"),
    }

    let target = leader.cluster.metrics().last_applied_index.unwrap();
    let rejoined = start_on(1, dir1.path().to_path_buf(), addr1).await;
    rejoined
        .wait_until(TIMEOUT, move |m| m.last_applied_index >= Some(target))
        .await
        .unwrap();

    rejoined.shutdown().await.unwrap();
    shutdown([n2, n3]).await;
}

#[tokio::test]
async fn write_without_a_leader_is_unavailable() {
    let n1 = start(1).await;

    let err = n1
        .cluster
        .index("books", Some("b1"), DOC)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ClientError::Unavailable),
        "expected Unavailable on an uninitialized node, got {err:?}"
    );

    shutdown([n1]).await;
}
