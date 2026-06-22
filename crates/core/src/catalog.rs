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

use crate::collection::{Collection, Upserted};
use crate::error::{Error, Result};
use crate::mapping::Mapping;
use crate::sync::fsync;
use crate::wal::Wal;

const SNAPSHOT_FILE: &str = "catalog.pb";
const SNAPSHOT_TMP: &str = ".catalog.pb.tmp";
const QUARANTINE_DIR: &str = "_quarantine";
const MAX_NAME_LEN: usize = 255;

#[derive(Debug)]
struct Entry {
    uuid: Uuid,
    created_seq: u64,
    collection: Arc<Collection>,
}

struct CollectionMeta {
    uuid: Uuid,
    mapping: Mapping,
    created_seq: u64,
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

impl Catalog {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;

        let snapshot = load_snapshot(&root)?;
        let applied_seq = snapshot.applied_seq;

        let mut metas: HashMap<String, CollectionMeta> = HashMap::new();
        for entry in snapshot.entries {
            let uuid = Uuid::parse_str(&entry.uuid).map_err(|_| {
                Error::Recovery(format!("invalid uuid {:?} in snapshot", entry.uuid))
            })?;
            let proto_mapping = entry
                .mapping
                .ok_or_else(|| Error::Recovery(format!("catalog entry {uuid} has no mapping")))?;
            metas.insert(
                entry.name,
                CollectionMeta {
                    uuid,
                    mapping: Mapping::try_from(proto_mapping)?,
                    created_seq: entry.created_seq,
                },
            );
        }

        let (wal, entries) = Wal::open(&root)?;
        let mut max_seq = applied_seq;
        let mut replayed = false;

        for entry in &entries {
            max_seq = max_seq.max(entry.seq);
            if entry.seq <= applied_seq {
                continue;
            }
            match command_op(entry) {
                Some(proto::command::Op::CreateCollection(create)) => {
                    let proto_mapping = create
                        .mapping
                        .clone()
                        .ok_or_else(|| Error::Recovery("wal create has no mapping".into()))?;
                    let mapping = Mapping::try_from(proto_mapping)?;
                    match metas.get(&create.collection) {
                        Some(existing) if existing.mapping == mapping => {}
                        Some(_) => {
                            return Err(Error::SchemaConflict {
                                name: create.collection.clone(),
                            })
                        }
                        None => {
                            let uuid = Uuid::parse_str(&create.uuid).map_err(|_| {
                                Error::Recovery(format!(
                                    "wal create has invalid uuid {:?}",
                                    create.uuid
                                ))
                            })?;
                            metas.insert(
                                create.collection.clone(),
                                CollectionMeta {
                                    uuid,
                                    mapping,
                                    created_seq: entry.seq,
                                },
                            );
                            replayed = true;
                        }
                    }
                }
                Some(proto::command::Op::DropCollection(drop)) => {
                    if let Some(meta) = metas.remove(&drop.collection) {
                        let dir = root.join(meta.uuid.to_string());
                        if dir.exists() {
                            fs::remove_dir_all(dir)?;
                        }
                        replayed = true;
                    }
                }
                _ => {}
            }
        }

        let mut collections = HashMap::with_capacity(metas.len());
        let mut skips: HashMap<String, u64> = HashMap::new();
        for (name, meta) in metas {
            let dir = root.join(meta.uuid.to_string());
            let collection = if dir.exists() {
                Collection::open(&root, meta.uuid, meta.mapping)?
            } else if meta.created_seq > applied_seq {
                Collection::create(&root, meta.uuid, meta.mapping)?
            } else {
                return Err(Error::Recovery(format!(
                    "catalog entry {} has no data dir",
                    meta.uuid
                )));
            };
            let skip = collection.committed_high_water()?.max(meta.created_seq);
            max_seq = max_seq.max(meta.created_seq);
            skips.insert(name.clone(), skip);
            collections.insert(
                name,
                Entry {
                    uuid: meta.uuid,
                    created_seq: meta.created_seq,
                    collection: Arc::new(collection),
                },
            );
        }

        for entry in &entries {
            let seq = entry.seq;
            match command_op(entry) {
                Some(proto::command::Op::IndexDocument(index))
                    if skips.get(&index.collection).is_some_and(|skip| seq > *skip) =>
                {
                    let parsed: Value = serde_json::from_slice(&index.source)
                        .map_err(|e| Error::Recovery(format!("wal index has invalid json: {e}")))?;
                    collections[&index.collection].collection.apply_upsert(
                        &index.id,
                        &index.source,
                        &parsed,
                    )?;
                    replayed = true;
                }
                Some(proto::command::Op::DeleteDocument(delete))
                    if skips
                        .get(&delete.collection)
                        .is_some_and(|skip| seq > *skip) =>
                {
                    collections[&delete.collection]
                        .collection
                        .apply_delete(&delete.id)?;
                    replayed = true;
                }
                _ => {}
            }
        }

        let live: HashSet<Uuid> = collections.values().map(|entry| entry.uuid).collect();
        for uuid in collection_dirs(&root)? {
            if !live.contains(&uuid) {
                quarantine(&root, uuid, max_seq)?;
            }
        }

        let catalog = Self {
            root,
            seq: AtomicU64::new(max_seq),
            wal: Mutex::new(wal),
            collections: Mutex::new(collections),
        };
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
        let collection = Arc::new(Collection::create(&self.root, uuid, mapping)?);
        self.collections().insert(
            name.to_owned(),
            Entry {
                uuid,
                created_seq: seq,
                collection: Arc::clone(&collection),
            },
        );
        Ok(Created {
            collection,
            created: true,
        })
    }

    pub fn drop(&self, name: &str) -> Result<()> {
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
        fs::remove_dir_all(self.root.join(uuid.to_string()))?;
        fsync_dir(&self.root)?;
        Ok(())
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
        let id = id.map_or_else(|| Uuid::new_v4().to_string(), str::to_owned);

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

    pub fn checkpoint(&self) -> Result<()> {
        let wal = self.wal.lock().expect("write lock poisoned");
        let high_water = self.seq.load(Ordering::Relaxed);
        {
            let map = self.collections();
            for entry in map.values() {
                entry.collection.commit(high_water)?;
            }
        }
        self.persist(high_water)?;
        // safe at the head: checkpoint just advanced every consumer to `high_water`.
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

    fn collections(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.collections.lock().expect("catalog poisoned")
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn persist(&self, applied_seq: u64) -> Result<()> {
        let snapshot = {
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
        };
        write_snapshot(&self.root, &snapshot)
    }
}

fn wal_entry(seq: u64, op: proto::command::Op) -> proto::WalEntry {
    proto::WalEntry {
        seq,
        command: Some(proto::Command { op: Some(op) }),
    }
}

fn command_op(entry: &proto::WalEntry) -> Option<&proto::command::Op> {
    entry.command.as_ref().and_then(|c| c.op.as_ref())
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
