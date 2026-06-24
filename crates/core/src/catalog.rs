use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use lumen_proto::v1 as proto;
use prost::Message;
use serde_json::Value;
use uuid::Uuid;

use crate::collection::{Collection, LogMark, Upserted};
use crate::error::{Error, Result};
use crate::mapping::Mapping;
use crate::sync::fsync;
use crate::wal::Wal;

const SNAPSHOT_FILE: &str = "catalog.pb";
const SNAPSHOT_TMP: &str = ".catalog.pb.tmp";
const QUARANTINE_DIR: &str = "_quarantine";
const SNAPSHOTS_DIR: &str = "_snapshots";
const CURRENT_SNAPSHOT: &str = "current.tar";
const MAX_NAME_LEN: usize = 255;

#[derive(Debug)]
struct Entry {
    uuid: Uuid,
    created_seq: u64,
    collection: Arc<Collection>,
}

#[derive(Debug)]
pub struct Catalog {
    root: PathBuf,
    seq: AtomicU64,
    wal: Mutex<Wal>,
    collections: Mutex<HashMap<String, Entry>>,
}

#[derive(Debug)]
pub struct Created {
    pub collection: Arc<Collection>,
    pub created: bool,
}

/// Result of applying one `Command`.
///
/// `id` is set for document writes and deletes. `created` is best-effort, same
/// as [`Upserted::created`].
#[derive(Debug, Default, Clone)]
pub struct ApplyOutcome {
    pub id: String,
    pub created: bool,
}

impl Catalog {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;

        let snapshot = load_snapshot(&root)?;
        let applied_seq = snapshot.applied_seq;

        let (wal, entries) = Wal::open(&root)?;

        // If a collection from the snapshot was dropped in the WAL tail, do not
        // reopen it.
        //
        // The drop is newer than the snapshot and its directory has already been
        // removed.
        let dropped_in_tail: HashSet<&str> = entries
            .iter()
            .filter(|e| e.seq > applied_seq)
            .filter_map(|e| match e.command.as_ref()?.op.as_ref()? {
                proto::command::Op::DropCollection(drop) => Some(drop.collection.as_str()),
                _ => None,
            })
            .collect();

        let mut collections = HashMap::with_capacity(snapshot.entries.len());
        let mut max_seq = applied_seq;
        for entry in &entries {
            max_seq = max_seq.max(entry.seq);
        }
        for entry in snapshot.entries {
            if dropped_in_tail.contains(entry.name.as_str()) {
                continue;
            }
            let uuid = Uuid::parse_str(&entry.uuid).map_err(|_| {
                Error::Recovery(format!("invalid uuid {:?} in snapshot", entry.uuid))
            })?;
            let proto_mapping = entry
                .mapping
                .ok_or_else(|| Error::Recovery(format!("catalog entry {uuid} has no mapping")))?;
            let mapping = Mapping::try_from(proto_mapping)?;
            if !root.join(uuid.to_string()).exists() {
                return Err(Error::Recovery(format!(
                    "catalog entry {uuid} has no data dir"
                )));
            }
            max_seq = max_seq.max(entry.created_seq);
            collections.insert(
                entry.name,
                Entry {
                    uuid,
                    created_seq: entry.created_seq,
                    collection: Arc::new(Collection::open(&root, uuid, mapping)?),
                },
            );
        }

        let catalog = Self {
            root,
            seq: AtomicU64::new(max_seq),
            wal: Mutex::new(wal),
            collections: Mutex::new(collections),
        };

        let mut replayed = false;
        for entry in &entries {
            if entry.seq <= applied_seq {
                continue;
            }
            if let Some(command) = &entry.command {
                catalog.apply_command(
                    LogMark {
                        term: 0,
                        node: 0,
                        index: entry.seq,
                    },
                    command,
                )?;
                replayed = true;
            }
        }

        catalog.sweep_orphans(catalog.seq.load(Ordering::Relaxed))?;

        if replayed {
            catalog.checkpoint()?;
        }
        Ok(catalog)
    }

    pub fn create(&self, name: &str, mapping: Mapping) -> Result<Created> {
        validate_name(name)?;
        let mut wal = self.wal.lock().expect("write lock poisoned");

        match self.get(name) {
            Ok(existing) if existing.mapping() == &mapping => {
                return Ok(Created {
                    collection: existing,
                    created: false,
                })
            }
            Ok(_) => {
                return Err(Error::SchemaConflict {
                    name: name.to_owned(),
                })
            }
            Err(Error::CollectionNotFound(_)) => {}
            Err(e) => return Err(e),
        }

        let seq = self.next_seq();
        let uuid = Uuid::new_v4();
        wal.append(&wal_entry(
            seq,
            proto::command::Op::CreateCollection(proto::CreateCollection {
                collection: name.to_owned(),
                uuid: uuid.to_string(),
                mapping: Some(mapping.clone().into()),
            }),
        ))?;
        let collection = self.install_collection(name, uuid, mapping, seq)?;
        Ok(Created {
            collection,
            created: true,
        })
    }

    pub fn drop_collection(&self, name: &str) -> Result<()> {
        let mut wal = self.wal.lock().expect("write lock poisoned");
        let uuid = self
            .collections()
            .get(name)
            .map(|entry| entry.uuid)
            .ok_or_else(|| Error::CollectionNotFound(name.to_owned()))?;

        let seq = self.next_seq();
        wal.append(&wal_entry(
            seq,
            proto::command::Op::DropCollection(proto::DropCollection {
                collection: name.to_owned(),
            }),
        ))?;
        self.collections().remove(name);
        self.drop_dir(uuid)
    }

    pub fn upsert_document(
        &self,
        collection: &str,
        id: Option<&str>,
        source: &[u8],
    ) -> Result<Upserted> {
        let target = self.get(collection)?;
        let parsed: Value = serde_json::from_slice(source)
            .map_err(|e| Error::Validation(format!("invalid JSON: {e}")))?;
        target.mapping().validate_document(&parsed)?;
        let id = or_new_id(id);

        let mut wal = self.wal.lock().expect("write lock poisoned");
        let seq = self.next_seq();
        wal.append(&wal_entry(
            seq,
            proto::command::Op::IndexDocument(proto::IndexDocument {
                collection: collection.to_owned(),
                id: id.clone(),
                source: source.to_vec(),
            }),
        ))?;
        let created = target.apply_upsert(&id, source, &parsed)?;
        Ok(Upserted { id, created })
    }

    pub fn get_document(&self, collection: &str, id: &str) -> Result<Vec<u8>> {
        self.get(collection)?
            .source(id)?
            .ok_or_else(|| Error::DocumentNotFound(id.to_owned()))
    }

    pub fn delete_document(&self, collection: &str, id: &str) -> Result<bool> {
        let target = self.get(collection)?;
        let mut wal = self.wal.lock().expect("write lock poisoned");
        let seq = self.next_seq();
        wal.append(&wal_entry(
            seq,
            proto::command::Op::DeleteDocument(proto::DeleteDocument {
                collection: collection.to_owned(),
                id: id.to_owned(),
            }),
        ))?;
        target.apply_delete(id)
    }

    /// Validates and builds a `CreateCollection` command; does not write the WAL
    /// or apply it.
    ///
    /// `None` means the collection already exists with the same mapping (a
    /// committed no-op). The caller replicates and applies the command.
    pub fn build_create_command(
        &self,
        name: &str,
        mapping: Mapping,
    ) -> Result<Option<proto::Command>> {
        validate_name(name)?;
        match self.get(name) {
            Ok(existing) if existing.mapping() == &mapping => return Ok(None),
            Ok(_) => {
                return Err(Error::SchemaConflict {
                    name: name.to_owned(),
                })
            }
            Err(Error::CollectionNotFound(_)) => {}
            Err(e) => return Err(e),
        }
        Ok(Some(command(proto::command::Op::CreateCollection(
            proto::CreateCollection {
                collection: name.to_owned(),
                uuid: Uuid::new_v4().to_string(),
                mapping: Some(mapping.into()),
            },
        ))))
    }

    /// Validates and builds an `IndexDocument` command, minting `_id` when absent;
    /// does not write the WAL or apply it.
    ///
    /// The minted `_id` is baked into the command so every replica applies the
    /// same id. The caller replicates and applies the command.
    pub fn build_index_command(
        &self,
        collection: &str,
        id: Option<&str>,
        source: &[u8],
    ) -> Result<proto::Command> {
        let target = self.get(collection)?;
        let parsed: Value = serde_json::from_slice(source)
            .map_err(|e| Error::Validation(format!("invalid JSON: {e}")))?;
        target.mapping().validate_document(&parsed)?;
        Ok(command(proto::command::Op::IndexDocument(
            proto::IndexDocument {
                collection: collection.to_owned(),
                id: or_new_id(id),
                source: source.to_vec(),
            },
        )))
    }

    /// Builds a `DeleteDocument` command, erroring if the collection is absent;
    /// does not write the WAL or apply it.
    pub fn build_delete_command(&self, collection: &str, id: &str) -> Result<proto::Command> {
        self.get(collection)?;
        Ok(command(proto::command::Op::DeleteDocument(
            proto::DeleteDocument {
                collection: collection.to_owned(),
                id: id.to_owned(),
            },
        )))
    }

    /// Builds a `DropCollection` command, erroring if the collection is absent;
    /// does not write the WAL or apply it.
    pub fn build_drop_command(&self, name: &str) -> Result<proto::Command> {
        self.get(name)?;
        Ok(command(proto::command::Op::DropCollection(
            proto::DropCollection {
                collection: name.to_owned(),
            },
        )))
    }

    /// Applies a committed `Command` without writing it to the WAL.
    ///
    /// The caller owns log ordering and durability. Already-applied document
    /// commands are skipped, so replay is safe. Business validation happens
    /// before the leader accepts the command.
    pub fn apply_command(&self, mark: LogMark, command: &proto::Command) -> Result<ApplyOutcome> {
        match command.op.as_ref() {
            Some(proto::command::Op::CreateCollection(create)) => {
                self.apply_create(mark, create)?;
                Ok(ApplyOutcome::default())
            }
            Some(proto::command::Op::DropCollection(drop)) => {
                self.apply_drop(mark, &drop.collection)?;
                Ok(ApplyOutcome::default())
            }
            Some(proto::command::Op::IndexDocument(index)) => self.apply_index(mark, index),
            Some(proto::command::Op::DeleteDocument(delete)) => self.apply_delete_doc(mark, delete),
            None => Ok(ApplyOutcome::default()),
        }
    }

    /// Returns the lowest committed mark across all collections.
    ///
    /// This is the log point that is safe to report as applied. `None` means the
    /// catalog has no collections.
    pub fn min_committed_mark(&self) -> Result<Option<LogMark>> {
        let map = self.collections();
        let mut min: Option<LogMark> = None;
        for entry in map.values() {
            let mark = entry.collection.committed_mark()?.unwrap_or_default();
            min = Some(match min {
                Some(current) if current.index <= mark.index => current,
                _ => mark,
            });
        }
        Ok(min)
    }

    /// Commits every collection at `mark` and writes a new catalog snapshot.
    ///
    /// Unlike [`Self::checkpoint`], this does not trim the WAL because the caller
    /// owns the log.
    pub fn checkpoint_applied(&self, mark: LogMark) -> Result<()> {
        self.commit_all(mark)
    }

    /// Commits one collection through `mark` if it is behind, making applied but
    /// uncommitted writes searchable.
    ///
    /// The linearizable read path uses this so a get reflects writes that were
    /// applied (buffered) but not yet committed by the periodic checkpoint.
    pub fn commit_collection(&self, name: &str, mark: LogMark) -> Result<()> {
        let target = self.get(name)?;
        if target.committed_mark()?.unwrap_or_default().index < mark.index {
            target.commit(mark)?;
        }
        Ok(())
    }

    /// Builds a snapshot archive and returns its path.
    ///
    /// The archive contains `catalog.pb` and the Tantivy files for every collection.
    /// Each collection is pinned while it is copied; see [`Collection::archive_into`].
    ///
    /// The caller is responsible for the snapshot metadata. In particular,
    /// `meta.last_log_id` must not be ahead of the committed mark returned by
    /// [`Self::min_committed_mark`].
    pub fn build_snapshot(&self) -> Result<PathBuf> {
        let snapshots = self.snapshots_dir()?;
        let staging = snapshots.join("build");
        reset_dir(&staging)?;

        let applied_seq = self.seq.load(Ordering::Relaxed);
        fs::write(
            staging.join(SNAPSHOT_FILE),
            self.snapshot_proto(applied_seq).encode_to_vec(),
        )?;
        {
            let map = self.collections();
            for entry in map.values() {
                entry
                    .collection
                    .archive_into(&staging.join(entry.uuid.to_string()))?;
            }
        }

        let tmp = snapshots.join(".current.tar.tmp");
        {
            let mut builder = tar::Builder::new(File::create(&tmp)?);
            builder.append_dir_all(".", &staging)?;
            fsync(&builder.into_inner()?)?;
        }
        let current = snapshots.join(CURRENT_SNAPSHOT);
        fs::rename(&tmp, &current)?;
        fsync_dir(&snapshots)?;
        fs::remove_dir_all(&staging)?;
        Ok(current)
    }

    /// Installs a snapshot archive and replaces the local catalog.
    ///
    /// The install is ordered so `catalog.pb` is renamed last. A crash before that
    /// rename keeps the old catalog active; any already-copied collection dirs are
    /// treated as orphans and quarantined on the next open. A crash after the rename
    /// leaves the new catalog with its collection dirs already in place.
    pub fn install_snapshot(&self, archive: &Path) -> Result<()> {
        let snapshots = self.snapshots_dir()?;
        let staging = snapshots.join("install");
        reset_dir(&staging)?;
        tar::Archive::new(File::open(archive)?).unpack(&staging)?;
        fsync_dir(&staging)?;

        let snapshot =
            proto::CatalogSnapshot::decode(fs::read(staging.join(SNAPSHOT_FILE))?.as_slice())
                .map_err(|e| {
                    Error::Recovery(format!("corrupt catalog snapshot in archive: {e}"))
                })?;

        let aside = snapshots.join("aside");
        reset_dir(&aside)?;
        for entry in &snapshot.entries {
            let uuid = Uuid::parse_str(&entry.uuid).map_err(|_| {
                Error::Recovery(format!("invalid uuid {:?} in archive", entry.uuid))
            })?;
            let dst = self.root.join(uuid.to_string());
            if dst.exists() {
                fs::rename(&dst, aside.join(uuid.to_string()))?;
            }
            fs::rename(staging.join(uuid.to_string()), &dst)?;
        }
        fsync_dir(&self.root)?;

        fs::rename(staging.join(SNAPSHOT_FILE), self.root.join(SNAPSHOT_FILE))?;
        fsync_dir(&self.root)?;

        self.rebuild(&snapshot)?;
        self.sweep_orphans(snapshot.applied_seq)?;
        self.publish_snapshot(archive, &snapshots)?;

        let _ = fs::remove_dir_all(&aside);
        let _ = fs::remove_dir_all(&staging);
        Ok(())
    }

    pub fn current_snapshot(&self) -> Option<PathBuf> {
        let path = self.root.join(SNAPSHOTS_DIR).join(CURRENT_SNAPSHOT);
        path.exists().then_some(path)
    }

    pub fn checkpoint(&self) -> Result<()> {
        let mut wal = self.wal.lock().expect("write lock poisoned");
        let high_water = self.seq.load(Ordering::Relaxed);
        self.commit_all(LogMark {
            term: 0,
            node: 0,
            index: high_water,
        })?;
        // Safe because `commit_all` just advanced every collection to `high_water`.
        wal.trim(high_water)?;
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Arc<Collection>> {
        self.collections()
            .get(name)
            .map(|entry| Arc::clone(&entry.collection))
            .ok_or_else(|| Error::CollectionNotFound(name.to_owned()))
    }

    pub fn describe(&self, name: &str) -> Result<Mapping> {
        Ok(self.get(name)?.mapping().clone())
    }

    pub fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.collections().keys().cloned().collect();
        names.sort();
        names
    }

    fn apply_create(&self, mark: LogMark, create: &proto::CreateCollection) -> Result<()> {
        let mapping = Mapping::try_from(
            create
                .mapping
                .clone()
                .ok_or_else(|| Error::Recovery("create command has no mapping".into()))?,
        )?;
        {
            let map = self.collections();
            match map.get(&create.collection) {
                Some(existing) if existing.collection.mapping() == &mapping => return Ok(()),
                Some(_) => {
                    tracing::error!(
                        collection = %create.collection,
                        "committed create conflicts with existing mapping; keeping existing",
                    );
                    return Ok(());
                }
                None => {}
            }
        }
        let uuid = Uuid::parse_str(&create.uuid).map_err(|_| {
            Error::Recovery(format!("create command has invalid uuid {:?}", create.uuid))
        })?;
        self.install_collection(&create.collection, uuid, mapping, mark.index)?;
        self.persist(mark.index)
    }

    fn apply_drop(&self, mark: LogMark, name: &str) -> Result<()> {
        let uuid = self.collections().remove(name).map(|entry| entry.uuid);
        if let Some(uuid) = uuid {
            self.drop_dir(uuid)?;
            self.persist(mark.index)?;
        }
        Ok(())
    }

    fn apply_index(&self, mark: LogMark, index: &proto::IndexDocument) -> Result<ApplyOutcome> {
        let Some(target) = self.lookup(&index.collection) else {
            return Ok(ApplyOutcome::default());
        };
        if mark.index <= target.committed_mark()?.unwrap_or_default().index {
            return Ok(ApplyOutcome {
                id: index.id.clone(),
                created: false,
            });
        }
        let parsed: Value = serde_json::from_slice(&index.source)
            .map_err(|e| Error::Recovery(format!("index command has invalid json: {e}")))?;
        let created = target.apply_upsert(&index.id, &index.source, &parsed)?;
        Ok(ApplyOutcome {
            id: index.id.clone(),
            created,
        })
    }

    fn apply_delete_doc(
        &self,
        mark: LogMark,
        delete: &proto::DeleteDocument,
    ) -> Result<ApplyOutcome> {
        let Some(target) = self.lookup(&delete.collection) else {
            return Ok(ApplyOutcome::default());
        };
        if mark.index <= target.committed_mark()?.unwrap_or_default().index {
            return Ok(ApplyOutcome::default());
        }
        target.apply_delete(&delete.id)?;
        Ok(ApplyOutcome {
            id: delete.id.clone(),
            created: false,
        })
    }

    fn install_collection(
        &self,
        name: &str,
        uuid: Uuid,
        mapping: Mapping,
        created_seq: u64,
    ) -> Result<Arc<Collection>> {
        let collection = if self.root.join(uuid.to_string()).exists() {
            Collection::open(&self.root, uuid, mapping)?
        } else {
            Collection::create(&self.root, uuid, mapping)?
        };
        let collection = Arc::new(collection);
        self.collections().insert(
            name.to_owned(),
            Entry {
                uuid,
                created_seq,
                collection: Arc::clone(&collection),
            },
        );
        Ok(collection)
    }

    fn drop_dir(&self, uuid: Uuid) -> Result<()> {
        let dir = self.root.join(uuid.to_string());
        if dir.exists() {
            fs::remove_dir_all(dir)?;
            fsync_dir(&self.root)?;
        }
        Ok(())
    }

    fn rebuild(&self, snapshot: &proto::CatalogSnapshot) -> Result<()> {
        let mut rebuilt = HashMap::with_capacity(snapshot.entries.len());
        let mut max_seq = snapshot.applied_seq;
        for entry in &snapshot.entries {
            let uuid = Uuid::parse_str(&entry.uuid).map_err(|_| {
                Error::Recovery(format!("invalid uuid {:?} in archive", entry.uuid))
            })?;
            let mapping =
                Mapping::try_from(entry.mapping.clone().ok_or_else(|| {
                    Error::Recovery(format!("archive entry {uuid} has no mapping"))
                })?)?;
            max_seq = max_seq.max(entry.created_seq);
            rebuilt.insert(
                entry.name.clone(),
                Entry {
                    uuid,
                    created_seq: entry.created_seq,
                    collection: Arc::new(Collection::open(&self.root, uuid, mapping)?),
                },
            );
        }
        *self.collections() = rebuilt;
        self.seq.store(max_seq, Ordering::Relaxed);
        Ok(())
    }

    fn sweep_orphans(&self, high: u64) -> Result<()> {
        let live: HashSet<Uuid> = self.collections().values().map(|e| e.uuid).collect();
        for uuid in collection_dirs(&self.root)? {
            if !live.contains(&uuid) {
                quarantine(&self.root, uuid, high)?;
            }
        }
        Ok(())
    }

    fn snapshots_dir(&self) -> Result<PathBuf> {
        let dir = self.root.join(SNAPSHOTS_DIR);
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn publish_snapshot(&self, archive: &Path, snapshots: &Path) -> Result<()> {
        let tmp = snapshots.join(".current.tar.tmp");
        fs::copy(archive, &tmp)?;
        fs::rename(&tmp, snapshots.join(CURRENT_SNAPSHOT))?;
        fsync_dir(snapshots)
    }

    fn commit_all(&self, mark: LogMark) -> Result<()> {
        {
            let map = self.collections();
            for entry in map.values() {
                entry.collection.commit(mark)?;
            }
        }
        self.persist(mark.index)
    }

    fn lookup(&self, name: &str) -> Option<Arc<Collection>> {
        self.collections()
            .get(name)
            .map(|entry| Arc::clone(&entry.collection))
    }

    fn collections(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.collections.lock().expect("catalog poisoned")
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn persist(&self, applied_seq: u64) -> Result<()> {
        write_snapshot(&self.root, &self.snapshot_proto(applied_seq))
    }

    fn snapshot_proto(&self, applied_seq: u64) -> proto::CatalogSnapshot {
        let map = self.collections();
        proto::CatalogSnapshot {
            applied_seq,
            entries: map
                .iter()
                .map(|(name, entry)| proto::CatalogEntry {
                    name: name.clone(),
                    uuid: entry.uuid.to_string(),
                    mapping: Some(entry.collection.mapping().clone().into()),
                    created_seq: entry.created_seq,
                })
                .collect(),
        }
    }
}

fn wal_entry(seq: u64, op: proto::command::Op) -> proto::WalEntry {
    proto::WalEntry {
        seq,
        command: Some(command(op)),
    }
}

fn command(op: proto::command::Op) -> proto::Command {
    proto::Command { op: Some(op) }
}

fn or_new_id(id: Option<&str>) -> String {
    id.map_or_else(|| Uuid::new_v4().to_string(), str::to_owned)
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(Error::Validation(format!(
            "collection name must be 1..={MAX_NAME_LEN} bytes"
        )));
    }
    if name.starts_with('_') {
        return Err(Error::Validation(format!(
            "collection name {name:?} uses the reserved '_' prefix"
        )));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        return Err(Error::Validation(format!(
            "collection name {name:?} must match [a-z0-9_-]"
        )));
    }
    Ok(())
}

fn load_snapshot(root: &Path) -> Result<proto::CatalogSnapshot> {
    match fs::read(root.join(SNAPSHOT_FILE)) {
        Ok(bytes) => proto::CatalogSnapshot::decode(bytes.as_slice())
            .map_err(|e| Error::Recovery(format!("corrupt catalog snapshot: {e}"))),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(proto::CatalogSnapshot::default()),
        Err(e) => Err(Error::Io(e)),
    }
}

fn write_snapshot(root: &Path, snapshot: &proto::CatalogSnapshot) -> Result<()> {
    let tmp = root.join(SNAPSHOT_TMP);
    {
        let mut file = File::create(&tmp)?;
        file.write_all(&snapshot.encode_to_vec())?;
        fsync(&file)?;
    }
    fs::rename(&tmp, root.join(SNAPSHOT_FILE))?;
    fsync_dir(root)?;
    Ok(())
}

fn fsync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn reset_dir(dir: &Path) -> Result<()> {
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    fs::create_dir_all(dir)?;
    Ok(())
}

fn collection_dirs(root: &Path) -> Result<HashSet<Uuid>> {
    let mut dirs = HashSet::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == QUARANTINE_DIR {
            continue;
        }
        if let Ok(uuid) = Uuid::parse_str(&name) {
            dirs.insert(uuid);
        }
    }
    Ok(dirs)
}

fn quarantine(root: &Path, uuid: Uuid, seq: u64) -> Result<()> {
    let dest_dir = root.join(QUARANTINE_DIR);
    fs::create_dir_all(&dest_dir)?;
    let dest = dest_dir.join(format!("{uuid}.{seq}"));
    tracing::warn!(%uuid, dest = %dest.display(), "orphan collection dir; quarantining");
    fs::rename(root.join(uuid.to_string()), &dest)?;
    fsync_dir(root)?;
    Ok(())
}
