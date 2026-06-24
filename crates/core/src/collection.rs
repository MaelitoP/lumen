use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tantivy::collector::Count;
use tantivy::query::TermQuery;
use tantivy::schema::{IndexRecordOption, Term};
use tantivy::{Index, IndexReader, IndexWriter};
use uuid::Uuid;

const META_FILE: &str = "meta.json";
const MANAGED_FILE: &str = ".managed.json";

use crate::document::build_doc;
use crate::error::{Error, Result};
use crate::mapping::{Mapping, ID_FIELD};
use crate::search::{self, SearchResults};

const WRITER_HEAP_BYTES: usize = 15_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upserted {
    pub id: String,
    /// Best-effort.
    ///
    /// `created` is checked against the last committed reader. If the same `_id`
    /// is upserted again before the next commit, this can return `true` again.
    pub created: bool,
}

/// Log position a collection has committed.
///
/// The value is stored in the Tantivy commit payload. It keeps the full Raft
/// `LogId` fields: `term`, `node`, and `index`. `term` and `node` are needed
/// because openraft compares committed entries by leader id, not only by index.
///
/// The single-node write path is not driven by Raft, so it stores `0` for
/// `term` and `node`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LogMark {
    pub term: u64,
    pub node: u64,
    pub index: u64,
}

pub struct Collection {
    uuid: Uuid,
    dir: PathBuf,
    mapping: Mapping,
    index: Index,
    writer: Mutex<IndexWriter>,
    reader: IndexReader,
}

impl Collection {
    pub(crate) fn create(root: &Path, uuid: Uuid, mapping: Mapping) -> Result<Self> {
        let dir = collection_dir(root, uuid);
        fs::create_dir(&dir)?;
        let index = Index::create_in_dir(&dir, mapping.to_schema())?;
        Self::from_index(uuid, dir, mapping, index)
    }

    pub(crate) fn open(root: &Path, uuid: Uuid, mapping: Mapping) -> Result<Self> {
        let dir = collection_dir(root, uuid);
        let index = Index::open_in_dir(&dir)?;
        Self::from_index(uuid, dir, mapping, index)
    }

    fn from_index(uuid: Uuid, dir: PathBuf, mapping: Mapping, index: Index) -> Result<Self> {
        let writer = index.writer(WRITER_HEAP_BYTES)?;
        let reader = index.reader()?;
        Ok(Self {
            uuid,
            dir,
            mapping,
            index,
            writer: Mutex::new(writer),
            reader,
        })
    }

    /// Copies the files from the latest Tantivy commit into `dest`.
    ///
    /// Holding a searcher pins the committed segment files while they are copied, so
    /// a background merge cannot remove them during the archive.
    pub(crate) fn archive_into(&self, dest: &Path) -> Result<()> {
        self.reader.reload()?;
        let _pin = self.reader.searcher();
        let meta = self.index.load_metas()?;

        let mut files: HashSet<PathBuf> = HashSet::new();
        files.insert(PathBuf::from(META_FILE));
        files.insert(PathBuf::from(MANAGED_FILE));
        for segment in &meta.segments {
            files.extend(segment.list_files());
        }

        fs::create_dir_all(dest)?;
        for rel in files {
            let from = self.dir.join(&rel);
            if from.exists() {
                fs::copy(&from, dest.join(&rel))?;
            }
        }
        Ok(())
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn mapping(&self) -> &Mapping {
        &self.mapping
    }

    pub(crate) fn apply_upsert(&self, id: &str, source: &[u8], parsed: &Value) -> Result<bool> {
        let schema = self.index.schema();
        let doc = build_doc(&schema, &self.mapping, id, source, parsed)?;
        let created = !self.id_exists(id)?;
        let writer = self.writer.lock().expect("writer poisoned");
        writer.delete_term(self.id_term(id));
        writer.add_document(doc)?;
        Ok(created)
    }

    pub(crate) fn apply_delete(&self, id: &str) -> Result<bool> {
        let existed = self.id_exists(id)?;
        let writer = self.writer.lock().expect("writer poisoned");
        writer.delete_term(self.id_term(id));
        Ok(existed)
    }

    pub(crate) fn commit(&self, mark: LogMark) -> Result<()> {
        let mut writer = self.writer.lock().expect("writer poisoned");
        let mut prepared = writer.prepare_commit()?;
        let payload = serde_json::to_string(&mark).expect("log mark serializes");
        prepared.set_payload(&payload);
        prepared.commit()?;
        drop(writer);
        self.reader.reload()?;
        Ok(())
    }

    pub(crate) fn committed_mark(&self) -> Result<Option<LogMark>> {
        match self.index.load_metas()?.payload {
            Some(payload) => Ok(Some(
                serde_json::from_str::<LogMark>(&payload)
                    .map_err(|e| Error::Recovery(format!("invalid commit payload: {e}")))?,
            )),
            None => Ok(None),
        }
    }

    pub fn search(&self, query: &str, limit: usize, offset: usize) -> Result<SearchResults> {
        search::execute(&self.index, &self.reader, query, limit, offset)
    }

    pub fn source(&self, id: &str) -> Result<Option<Vec<u8>>> {
        search::source_by_id(&self.index, &self.reader, id)
    }

    fn id_term(&self, id: &str) -> Term {
        let id_field = self
            .index
            .schema()
            .get_field(ID_FIELD)
            .expect("schema always has _id");
        Term::from_field_text(id_field, id)
    }

    fn id_exists(&self, id: &str) -> Result<bool> {
        let searcher = self.reader.searcher();
        let query = TermQuery::new(self.id_term(id), IndexRecordOption::Basic);
        Ok(searcher.search(&query, &Count)? > 0)
    }
}

impl fmt::Debug for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collection")
            .field("uuid", &self.uuid)
            .field("mapping", &self.mapping)
            .finish_non_exhaustive()
    }
}

fn collection_dir(root: &Path, uuid: Uuid) -> PathBuf {
    root.join(uuid.to_string())
}
