use std::io::{Seek, SeekFrom};
use std::sync::{Arc, Mutex};

use lumen_core::{Catalog, LogMark};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    AnyError, CommittedLeaderId, Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};

use crate::type_config::{Node, Response, TypeConfig};

/// Applies committed Raft commands to the local catalog.
///
/// Snapshots contain the catalog metadata and all Tantivy files for each
/// collection. Installing a snapshot replaces the local state, so a follower
/// that is too far behind can catch up without replaying old log entries.
#[derive(Clone, Debug)]
pub struct StateMachine {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    catalog: Catalog,
    state: Mutex<SmState>,
}

#[derive(Default, Debug)]
struct SmState {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, Node>,
    current_snapshot: Option<SnapshotMeta<u64, Node>>,
}

impl StateMachine {
    pub fn new(catalog: Catalog) -> Self {
        Self {
            inner: Arc::new(Inner {
                catalog,
                state: Mutex::new(SmState::default()),
            }),
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.inner.catalog
    }
}

/// Returns the last log id that is safe to report as applied.
///
/// When collections have committed data, the catalog decides the applied
/// point. When only blank or membership entries were applied, there is no
/// collection state yet, so we fall back to the in-memory value.
///
/// The caller must hold state.
fn applied_log_id(
    catalog: &Catalog,
    state: &SmState,
) -> Result<Option<LogId<u64>>, StorageError<u64>> {
    Ok(match catalog.min_committed_mark().map_err(read_sm)? {
        Some(mark) => Some(to_log_id(mark)),
        None => state.last_applied,
    })
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, Node>), StorageError<u64>> {
        let state = self.inner.state.lock().expect("sm poisoned");
        Ok((
            applied_log_id(&self.inner.catalog, &state)?,
            state.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut state = inner.state.lock().expect("sm poisoned");
            let mut responses = Vec::with_capacity(entries.len());
            for entry in entries {
                let log_id = entry.log_id;
                state.last_applied = Some(log_id);
                let response = match entry.payload {
                    EntryPayload::Blank => Response::default(),
                    EntryPayload::Membership(membership) => {
                        state.last_membership = StoredMembership::new(Some(log_id), membership);
                        Response::default()
                    }
                    EntryPayload::Normal(command) => {
                        let mark = LogMark {
                            term: log_id.leader_id.term,
                            node: log_id.leader_id.node_id,
                            index: log_id.index,
                        };
                        let outcome = inner
                            .catalog
                            .apply_command(mark, &command)
                            .map_err(|e| apply_err(log_id, e))?;
                        Response {
                            id: outcome.id,
                            created: outcome.created,
                        }
                    }
                };
                responses.push(response);
            }
            Ok(responses)
        })
        .await
        .map_err(join_err)?
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<u64>> {
        let file = tempfile::tempfile().map_err(io_sm)?;
        Ok(Box::new(tokio::fs::File::from_std(file)))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, Node>,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError<u64>> {
        let mut received = snapshot.into_std().await;
        let inner = Arc::clone(&self.inner);
        let meta = meta.clone();
        tokio::task::spawn_blocking(move || {
            received.seek(SeekFrom::Start(0)).map_err(io_sm)?;
            let mut staged = tempfile::NamedTempFile::new().map_err(io_sm)?;
            std::io::copy(&mut received, staged.as_file_mut()).map_err(io_sm)?;
            staged.as_file().sync_all().map_err(io_sm)?;

            inner
                .catalog
                .install_snapshot(staged.path())
                .map_err(write_sm)?;
            let mut state = inner.state.lock().expect("sm poisoned");
            state.last_applied = meta.last_log_id;
            state.last_membership = meta.last_membership.clone();
            state.current_snapshot = Some(meta);
            Ok(())
        })
        .await
        .map_err(join_err)?
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let meta = self
            .inner
            .state
            .lock()
            .expect("sm poisoned")
            .current_snapshot
            .clone();
        let (Some(meta), Some(path)) = (meta, self.inner.catalog.current_snapshot()) else {
            return Ok(None);
        };
        let file = tokio::fs::File::open(&path).await.map_err(io_sm)?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(file),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let inner = Arc::clone(&self.inner);
        // Build the archive and read the applied point while holding the same lock,
        // synchronously inside the blocking task. This keeps `last_log_id` in sync
        // with the data in the archive. Without the lock, a concurrent `apply` could
        // move one forward without the other.
        let (path, meta) = tokio::task::spawn_blocking(move || {
            let mut state = inner.state.lock().expect("sm poisoned");
            let path = inner.catalog.build_snapshot().map_err(write_sm)?;
            let last_log_id = applied_log_id(&inner.catalog, &state)?;
            let meta = SnapshotMeta {
                last_log_id,
                last_membership: state.last_membership.clone(),
                snapshot_id: snapshot_id(last_log_id),
            };
            state.current_snapshot = Some(meta.clone());
            Ok::<_, StorageError<u64>>((path, meta))
        })
        .await
        .map_err(join_err)??;
        let file = tokio::fs::File::open(&path).await.map_err(io_sm)?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(file),
        })
    }
}

fn to_log_id(mark: LogMark) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(mark.term, mark.node), mark.index)
}

fn snapshot_id(last_log_id: Option<LogId<u64>>) -> String {
    match last_log_id {
        Some(id) => {
            let term = id.leader_id.term;
            let node = id.leader_id.node_id;
            let index = id.index;
            format!("{term}-{node}-{index}")
        }
        None => "init".to_string(),
    }
}

fn apply_err(log_id: LogId<u64>, e: lumen_core::Error) -> StorageError<u64> {
    StorageIOError::apply(log_id, AnyError::new(&e)).into()
}

fn read_sm(e: lumen_core::Error) -> StorageError<u64> {
    StorageIOError::read_state_machine(AnyError::new(&e)).into()
}

fn write_sm(e: lumen_core::Error) -> StorageError<u64> {
    StorageIOError::write_state_machine(AnyError::new(&e)).into()
}

fn io_sm(e: std::io::Error) -> StorageError<u64> {
    StorageIOError::write_state_machine(AnyError::new(&e)).into()
}

fn join_err(e: tokio::task::JoinError) -> StorageError<u64> {
    StorageIOError::write_state_machine(AnyError::new(&e)).into()
}

#[cfg(test)]
mod tests {
    use lumen_proto::v1 as proto;
    use openraft::EntryPayload;
    use tempfile::TempDir;

    use super::*;

    const BOOKS_UUID: &str = "11111111-1111-1111-1111-111111111111";

    fn normal(term: u64, index: u64, op: proto::command::Op) -> Entry<TypeConfig> {
        Entry {
            log_id: to_log_id(LogMark {
                term,
                node: 0,
                index,
            }),
            payload: EntryPayload::Normal(proto::Command { op: Some(op) }),
        }
    }

    fn create_books() -> proto::command::Op {
        proto::command::Op::CreateCollection(proto::CreateCollection {
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
        })
    }

    fn create_books_conflicting() -> proto::command::Op {
        proto::command::Op::CreateCollection(proto::CreateCollection {
            collection: "books".to_string(),
            uuid: "22222222-2222-2222-2222-222222222222".to_string(),
            mapping: Some(proto::Mapping {
                fields: vec![proto::Field {
                    name: "title".to_string(),
                    r#type: proto::FieldType::Keyword as i32,
                    indexed: true,
                    fast: false,
                }],
            }),
        })
    }

    fn index(id: &str, title: &str) -> proto::command::Op {
        proto::command::Op::IndexDocument(proto::IndexDocument {
            collection: "books".to_string(),
            id: id.to_string(),
            source: format!(r#"{{"title":"{title}"}}"#).into_bytes(),
        })
    }

    #[tokio::test]
    async fn conflicting_committed_create_applies_as_total_noop() {
        use std::collections::BTreeMap;

        use lumen_core::{FieldSpec, FieldType, Mapping};

        let dir = TempDir::new().unwrap();
        let mut sm = StateMachine::new(Catalog::open(dir.path().join("state")).unwrap());

        sm.apply([
            normal(1, 1, create_books()),
            normal(1, 2, create_books_conflicting()),
        ])
        .await
        .expect("apply must stay total over a committed conflicting create");

        let mut fields = BTreeMap::new();
        fields.insert(
            "title".to_string(),
            FieldSpec {
                ty: FieldType::Text,
                indexed: true,
                fast: false,
            },
        );
        let original = Mapping::new(fields).unwrap();
        assert_eq!(sm.catalog().describe("books").unwrap(), original);
    }

    #[tokio::test]
    async fn command_stream_survives_restart_without_loss_or_double_effect() {
        let dir = TempDir::new().unwrap();
        let state = dir.path().join("state");

        {
            let mut sm = StateMachine::new(Catalog::open(&state).unwrap());
            sm.apply([
                normal(1, 1, create_books()),
                normal(1, 2, index("b1", "alpha")),
                normal(1, 3, index("b2", "beta")),
            ])
            .await
            .unwrap();

            // Entries have been applied but not checkpointed yet. After a restart, openraft
            // may send them again.
            assert_eq!(
                sm.applied_state().await.unwrap().0,
                Some(to_log_id(LogMark::default()))
            );

            sm.catalog()
                .checkpoint_applied(LogMark {
                    term: 1,
                    node: 0,
                    index: 3,
                })
                .unwrap();
            assert_eq!(
                sm.applied_state().await.unwrap().0,
                Some(to_log_id(LogMark {
                    term: 1,
                    node: 0,
                    index: 3
                }))
            );
        }

        let mut sm = StateMachine::new(Catalog::open(&state).unwrap());
        let books = sm.catalog().get("books").unwrap();
        assert_eq!(books.search("alpha", 10, 0).unwrap().total, 1);
        assert_eq!(books.search("beta", 10, 0).unwrap().total, 1);
        assert_eq!(
            sm.applied_state().await.unwrap().0,
            Some(to_log_id(LogMark {
                term: 1,
                node: 0,
                index: 3
            }))
        );
    }

    #[tokio::test]
    async fn re_applying_committed_entries_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let mut sm = StateMachine::new(Catalog::open(dir.path().join("state")).unwrap());
        sm.apply([
            normal(1, 1, create_books()),
            normal(1, 2, index("b1", "alpha")),
        ])
        .await
        .unwrap();
        sm.catalog()
            .checkpoint_applied(LogMark {
                term: 1,
                node: 0,
                index: 2,
            })
            .unwrap();

        // openraft may send already-applied entries again during catch-up. Re-applying
        // the same document must still leave only one copy.
        sm.apply([normal(1, 2, index("b1", "alpha"))])
            .await
            .unwrap();
        sm.catalog()
            .checkpoint_applied(LogMark {
                term: 1,
                node: 0,
                index: 2,
            })
            .unwrap();

        assert_eq!(
            sm.catalog()
                .get("books")
                .unwrap()
                .search("alpha", 10, 0)
                .unwrap()
                .total,
            1
        );
    }

    #[tokio::test]
    async fn follower_catches_up_via_snapshot_install() {
        let leader_dir = TempDir::new().unwrap();
        let mut leader = StateMachine::new(Catalog::open(leader_dir.path().join("state")).unwrap());
        leader
            .apply([
                normal(1, 1, create_books()),
                normal(1, 2, index("b1", "alpha")),
                normal(1, 3, index("b2", "beta")),
            ])
            .await
            .unwrap();
        leader
            .catalog()
            .checkpoint_applied(LogMark {
                term: 1,
                node: 0,
                index: 3,
            })
            .unwrap();

        let snapshot = leader
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();

        let follower_dir = TempDir::new().unwrap();
        let mut follower =
            StateMachine::new(Catalog::open(follower_dir.path().join("state")).unwrap());
        let mut received = follower.begin_receiving_snapshot().await.unwrap();
        *received = *snapshot.snapshot;
        follower
            .install_snapshot(&snapshot.meta, received)
            .await
            .unwrap();

        let books = follower.catalog().get("books").unwrap();
        assert_eq!(books.search("alpha", 10, 0).unwrap().total, 1);
        assert_eq!(books.search("beta", 10, 0).unwrap().total, 1);
        assert_eq!(
            follower.applied_state().await.unwrap().0,
            Some(to_log_id(LogMark {
                term: 1,
                node: 0,
                index: 3
            })),
        );
    }
}
