use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use lumen_proto::v1 as proto;
use prost::Message;
use uuid::Uuid;

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::mapping::Mapping;

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

#[derive(Debug)]
pub struct Catalog {
    root: PathBuf,
    seq: AtomicU64,
    ddl: Mutex<()>,
    collections: Mutex<HashMap<String, Entry>>,
}

impl Catalog {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;

        let snapshot = load_snapshot(&root)?;
        let applied_seq = snapshot.applied_seq;

        let mut claimed: HashMap<Uuid, proto::CatalogEntry> = HashMap::new();
        for entry in snapshot.entries {
            let uuid = Uuid::parse_str(&entry.uuid).map_err(|_| {
                Error::Recovery(format!("invalid uuid {:?} in snapshot", entry.uuid))
            })?;
            claimed.insert(uuid, entry);
        }

        let on_disk = collection_dirs(&root)?;

        for uuid in claimed.keys() {
            if !on_disk.contains(uuid) {
                return Err(Error::Recovery(format!(
                    "catalog entry {uuid} has no data dir"
                )));
            }
        }

        for uuid in &on_disk {
            if !claimed.contains_key(uuid) {
                quarantine(&root, *uuid, applied_seq)?;
            }
        }

        let mut max_seq = applied_seq;
        let mut collections = HashMap::with_capacity(claimed.len());
        for (uuid, entry) in claimed {
            let proto_mapping = entry
                .mapping
                .ok_or_else(|| Error::Recovery(format!("catalog entry {uuid} has no mapping")))?;
            let mapping = Mapping::try_from(proto_mapping)?;
            let collection = Collection::open(&root, uuid, mapping)?;
            max_seq = max_seq.max(entry.created_seq);
            collections.insert(
                entry.name,
                Entry {
                    uuid,
                    created_seq: entry.created_seq,
                    collection: Arc::new(collection),
                },
            );
        }

        Ok(Self {
            root,
            seq: AtomicU64::new(max_seq),
            ddl: Mutex::new(()),
            collections: Mutex::new(collections),
        })
    }

    pub fn create(&self, name: &str, mapping: Mapping) -> Result<Arc<Collection>> {
        validate_name(name)?;
        let _ddl = self.ddl.lock().expect("ddl poisoned");

        match self.get(name) {
            Ok(existing) if existing.mapping() == &mapping => return Ok(existing),
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
        let collection = Arc::new(Collection::create(&self.root, uuid, mapping)?);

        self.collections().insert(
            name.to_owned(),
            Entry {
                uuid,
                created_seq: seq,
                collection: Arc::clone(&collection),
            },
        );

        if let Err(e) = self.persist(seq) {
            self.collections().remove(name);
            return Err(e);
        }
        Ok(collection)
    }

    pub fn drop(&self, name: &str) -> Result<()> {
        let _ddl = self.ddl.lock().expect("ddl poisoned");

        let (uuid, created_seq, collection) = {
            let map = self.collections();
            let entry = map
                .get(name)
                .ok_or_else(|| Error::CollectionNotFound(name.to_owned()))?;
            (entry.uuid, entry.created_seq, Arc::clone(&entry.collection))
        };
        let seq = self.next_seq();

        self.collections().remove(name);

        if let Err(e) = self.persist(seq) {
            self.collections().insert(
                name.to_owned(),
                Entry {
                    uuid,
                    created_seq,
                    collection,
                },
            );
            return Err(e);
        }

        fs::remove_dir_all(self.root.join(uuid.to_string()))?;
        fsync_dir(&self.root)?;
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
        file.sync_all()?;
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
