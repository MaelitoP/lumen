use std::io::Cursor; // used by declare_raft_types! below

pub use lumen_proto::raft::Node;
use lumen_proto::v1::Command;

pub type NodeId = u64;

#[derive(Debug, Clone, Default)]
pub struct Response {
    pub id: String,
    pub created: bool,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = Response,
        NodeId = u64,
        Node = Node,
);

pub type LumenRaft = openraft::Raft<TypeConfig>;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::io::Cursor;
    use std::ops::RangeBounds;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::storage::{
        LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
        Snapshot,
    };
    use openraft::{
        Config, Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError,
        StoredMembership, Vote,
    };

    use super::*;

    #[derive(Default)]
    struct LogInner {
        log: BTreeMap<u64, Entry<TypeConfig>>,
        last_purged: Option<LogId<u64>>,
        vote: Option<Vote<u64>>,
        committed: Option<LogId<u64>>,
    }

    #[derive(Default)]
    struct MemLogStore {
        inner: Mutex<LogInner>,
    }

    impl RaftLogReader<TypeConfig> for Arc<MemLogStore> {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
            &mut self,
            range: RB,
        ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
            let inner = self.inner.lock().unwrap();
            Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
        }
    }

    impl RaftLogStorage<TypeConfig> for Arc<MemLogStore> {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
            let inner = self.inner.lock().unwrap();
            let last = inner.log.values().next_back().map(|e| e.log_id);
            Ok(LogState {
                last_purged_log_id: inner.last_purged,
                last_log_id: last.or(inner.last_purged),
            })
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }

        async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
            self.inner.lock().unwrap().vote = Some(*vote);
            Ok(())
        }

        async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
            Ok(self.inner.lock().unwrap().vote)
        }

        async fn save_committed(
            &mut self,
            committed: Option<LogId<u64>>,
        ) -> Result<(), StorageError<u64>> {
            self.inner.lock().unwrap().committed = committed;
            Ok(())
        }

        async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
            Ok(self.inner.lock().unwrap().committed)
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: LogFlushed<TypeConfig>,
        ) -> Result<(), StorageError<u64>>
        where
            I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
            I::IntoIter: OptionalSend,
        {
            {
                let mut inner = self.inner.lock().unwrap();
                for entry in entries {
                    inner.log.insert(entry.log_id.index, entry);
                }
            }
            callback.log_io_completed(Ok(()));
            Ok(())
        }

        async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
            let mut inner = self.inner.lock().unwrap();
            let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
            for k in keys {
                inner.log.remove(&k);
            }
            Ok(())
        }

        async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
            let mut inner = self.inner.lock().unwrap();
            inner.last_purged = Some(log_id);
            let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
            for k in keys {
                inner.log.remove(&k);
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct SmInner {
        last_applied: Option<LogId<u64>>,
        last_membership: StoredMembership<u64, Node>,
    }

    #[derive(Default)]
    struct MemStateMachine {
        inner: Mutex<SmInner>,
    }

    impl RaftStateMachine<TypeConfig> for Arc<MemStateMachine> {
        type SnapshotBuilder = Self;

        async fn applied_state(
            &mut self,
        ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, Node>), StorageError<u64>> {
            let inner = self.inner.lock().unwrap();
            Ok((inner.last_applied, inner.last_membership.clone()))
        }

        async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<u64>>
        where
            I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
            I::IntoIter: OptionalSend,
        {
            let mut inner = self.inner.lock().unwrap();
            let mut responses = Vec::new();
            for entry in entries {
                inner.last_applied = Some(entry.log_id);
                if let EntryPayload::Membership(membership) = &entry.payload {
                    inner.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                }
                responses.push(Response {
                    id: String::new(),
                    created: false,
                });
            }
            Ok(responses)
        }

        async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
            self.clone()
        }

        async fn begin_receiving_snapshot(
            &mut self,
        ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
            Ok(Box::new(Cursor::new(Vec::new())))
        }

        async fn install_snapshot(
            &mut self,
            meta: &SnapshotMeta<u64, Node>,
            _snapshot: Box<Cursor<Vec<u8>>>,
        ) -> Result<(), StorageError<u64>> {
            let mut inner = self.inner.lock().unwrap();
            inner.last_applied = meta.last_log_id;
            inner.last_membership = meta.last_membership.clone();
            Ok(())
        }

        async fn get_current_snapshot(
            &mut self,
        ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
            Ok(None)
        }
    }

    impl RaftSnapshotBuilder<TypeConfig> for Arc<MemStateMachine> {
        async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
            let (last_log_id, last_membership) = {
                let inner = self.inner.lock().unwrap();
                (inner.last_applied, inner.last_membership.clone())
            };
            Ok(Snapshot {
                meta: SnapshotMeta {
                    last_log_id,
                    last_membership,
                    snapshot_id: "mem".to_string(),
                },
                snapshot: Box::new(Cursor::new(Vec::new())),
            })
        }
    }

    fn no_peers<E: std::error::Error + 'static>() -> RPCError<u64, Node, E> {
        let err = std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "single-node fake: no peers",
        );
        RPCError::Unreachable(Unreachable::new(&err))
    }

    struct MemNetwork;

    impl RaftNetworkFactory<TypeConfig> for MemNetwork {
        type Network = MemClient;

        async fn new_client(&mut self, _target: u64, _node: &Node) -> Self::Network {
            MemClient
        }
    }

    struct MemClient;

    impl RaftNetwork<TypeConfig> for MemClient {
        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, Node, RaftError<u64>>> {
            Err(no_peers())
        }

        async fn install_snapshot(
            &mut self,
            _rpc: InstallSnapshotRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<
            InstallSnapshotResponse<u64>,
            RPCError<u64, Node, RaftError<u64, InstallSnapshotError>>,
        > {
            Err(no_peers())
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<u64>,
            _option: RPCOption,
        ) -> Result<VoteResponse<u64>, RPCError<u64, Node, RaftError<u64>>> {
            Err(no_peers())
        }
    }

    #[tokio::test]
    async fn single_node_constructs_and_initializes() {
        let config = Arc::new(
            Config {
                cluster_name: "lumen-test".to_string(),
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let raft = LumenRaft::new(
            1,
            config,
            MemNetwork,
            Arc::new(MemLogStore::default()),
            Arc::new(MemStateMachine::default()),
        )
        .await
        .unwrap();

        let mut members = BTreeMap::new();
        members.insert(
            1,
            Node {
                node_id: 1,
                rpc_addr: "127.0.0.1:1".to_string(),
            },
        );
        raft.initialize(members).await.unwrap();

        let metrics = raft
            .wait(Some(Duration::from_secs(10)))
            .current_leader(1, "single node elects itself")
            .await
            .unwrap();

        assert_eq!(metrics.current_leader, Some(1));
        assert_eq!(
            metrics
                .membership_config
                .membership()
                .voter_ids()
                .collect::<Vec<_>>(),
            vec![1]
        );

        raft.shutdown().await.unwrap();
    }
}
