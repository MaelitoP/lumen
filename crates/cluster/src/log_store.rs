use std::fmt::Debug;
use std::fs::{self, File};
use std::io::{self, ErrorKind, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use lumen_core::SegmentedLog;
use lumen_proto::raft;
use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{AnyError, Entry, LogId, OptionalSend, StorageError, StorageIOError, Vote};
use prost::Message;

use crate::type_config::TypeConfig;

const LOG_DIR: &str = "log";
const META_FILE: &str = "raft-meta.pb";
const META_TMP: &str = ".raft-meta.pb.tmp";

#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Inner>,
}

struct Inner {
    dir: PathBuf,
    log: Mutex<SegmentedLog>,
    meta: Mutex<Meta>,
}

#[derive(Default)]
struct Meta {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged: Option<LogId<u64>>,
}

impl LogStore {
    pub fn open(dir: &Path) -> Result<Self, StorageError<u64>> {
        let mut log = SegmentedLog::open(&dir.join(LOG_DIR)).map_err(read_logs)?;
        let meta = load_meta(dir)?;
        if let Some(purged) = meta.last_purged {
            log.purge_through(purged.index).map_err(write_logs)?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                dir: dir.to_path_buf(),
                log: Mutex::new(log),
                meta: Mutex::new(meta),
            }),
        })
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let log = self.inner.log.lock().expect("log poisoned");
        let (from, to) = concrete_range(&range, log.last_index());
        log.read_range(from, to)
            .map_err(read_logs)?
            .into_iter()
            .map(|(_, bytes)| decode_entry(&bytes))
            .collect()
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let last_purged = self.inner.meta.lock().expect("meta poisoned").last_purged;
        let log = self.inner.log.lock().expect("log poisoned");
        let last_log_id = match log.last_index() {
            Some(index) => {
                let entry = log
                    .read_range(index, index.saturating_add(1))
                    .map_err(read_logs)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| read_logs(missing_record(index)))?;
                Some(decode_entry(&entry.1)?.log_id)
            }
            None => last_purged,
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut meta = self.inner.meta.lock().expect("meta poisoned");
        meta.vote = Some(*vote);
        persist_meta(&self.inner.dir, &meta).map_err(write_vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.meta.lock().expect("meta poisoned").vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let mut meta = self.inner.meta.lock().expect("meta poisoned");
        meta.committed = committed;
        persist_meta(&self.inner.dir, &meta).map_err(write_logs)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.meta.lock().expect("meta poisoned").committed)
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
            let mut log = self.inner.log.lock().expect("log poisoned");
            for entry in entries {
                let index = entry.log_id.index;
                let proto: raft::Entry = entry.into();
                log.append(index, &proto.encode_to_vec())
                    .map_err(write_logs)?;
            }
            log.sync().map_err(write_logs)?;
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.inner
            .log
            .lock()
            .expect("log poisoned")
            .truncate_from(log_id.index)
            .map_err(write_logs)
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        {
            let mut meta = self.inner.meta.lock().expect("meta poisoned");
            meta.last_purged = Some(log_id);
            persist_meta(&self.inner.dir, &meta).map_err(write_logs)?;
        }
        self.inner
            .log
            .lock()
            .expect("log poisoned")
            .purge_through(log_id.index)
            .map_err(write_logs)
    }
}

fn concrete_range(range: &impl RangeBounds<u64>, last: Option<u64>) -> (u64, u64) {
    let from = match range.start_bound() {
        Bound::Included(&s) => s,
        Bound::Excluded(&s) => s.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let to = match range.end_bound() {
        Bound::Included(&e) => e.saturating_add(1),
        Bound::Excluded(&e) => e,
        Bound::Unbounded => last.map_or(0, |l| l.saturating_add(1)),
    };
    (from, to)
}

fn decode_entry(bytes: &[u8]) -> Result<Entry<TypeConfig>, StorageError<u64>> {
    let proto = raft::Entry::decode(bytes).map_err(read_logs)?;
    Entry::<TypeConfig>::try_from(proto).map_err(read_logs)
}

fn load_meta(dir: &Path) -> Result<Meta, StorageError<u64>> {
    let bytes = match fs::read(dir.join(META_FILE)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Meta::default()),
        Err(e) => return Err(read_vote(e)),
    };
    let proto = raft::RaftMeta::decode(bytes.as_slice()).map_err(read_vote)?;
    Ok(Meta {
        vote: proto
            .vote
            .map(TryInto::try_into)
            .transpose()
            .map_err(read_vote)?,
        committed: proto
            .committed
            .map(TryInto::try_into)
            .transpose()
            .map_err(read_logs)?,
        last_purged: proto
            .last_purged
            .map(TryInto::try_into)
            .transpose()
            .map_err(read_logs)?,
    })
}

fn persist_meta(dir: &Path, meta: &Meta) -> io::Result<()> {
    let proto = raft::RaftMeta {
        vote: meta.vote.map(Into::into),
        committed: meta.committed.map(Into::into),
        last_purged: meta.last_purged.map(Into::into),
    };
    let tmp = dir.join(META_TMP);
    {
        let mut file = File::create(&tmp)?;
        file.write_all(&proto.encode_to_vec())?;
        file.sync_all()?;
    }
    fs::rename(&tmp, dir.join(META_FILE))?;
    File::open(dir)?.sync_all()
}

fn missing_record(index: u64) -> io::Error {
    io::Error::new(
        ErrorKind::UnexpectedEof,
        format!("log record {index} present in index but unreadable"),
    )
}

fn read_logs(e: impl std::error::Error + 'static) -> StorageError<u64> {
    StorageIOError::read_logs(AnyError::new(&e)).into()
}

fn write_logs(e: impl std::error::Error + 'static) -> StorageError<u64> {
    StorageIOError::write_logs(AnyError::new(&e)).into()
}

fn read_vote(e: impl std::error::Error + 'static) -> StorageError<u64> {
    StorageIOError::read_vote(AnyError::new(&e)).into()
}

fn write_vote(e: impl std::error::Error + 'static) -> StorageError<u64> {
    StorageIOError::write_vote(AnyError::new(&e)).into()
}

#[cfg(test)]
mod tests {
    use openraft::CommittedLeaderId;
    use tempfile::TempDir;

    use super::*;

    fn log_id(term: u64, node: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, node), index)
    }

    #[tokio::test]
    async fn meta_round_trips_across_reopen() {
        let dir = TempDir::new().unwrap();
        let vote = Vote::new(3, 7);
        let committed = log_id(2, 5, 9);
        let purged = log_id(1, 4, 4);
        {
            let mut store = LogStore::open(dir.path()).unwrap();
            store.save_vote(&vote).await.unwrap();
            store.save_committed(Some(committed)).await.unwrap();
            store.purge(purged).await.unwrap();
        }
        let mut store = LogStore::open(dir.path()).unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        assert_eq!(store.read_committed().await.unwrap(), Some(committed));
        assert_eq!(
            store.get_log_state().await.unwrap().last_purged_log_id,
            Some(purged)
        );
    }
}
