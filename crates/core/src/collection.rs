use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tantivy::{Index, IndexReader, IndexWriter};
use uuid::Uuid;

use crate::error::Result;
use crate::mapping::Mapping;

const WRITER_HEAP_BYTES: usize = 15_000_000;

pub struct Collection {
    uuid: Uuid,
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
        Self::from_index(uuid, mapping, index)
    }

    pub(crate) fn open(root: &Path, uuid: Uuid, mapping: Mapping) -> Result<Self> {
        let index = Index::open_in_dir(collection_dir(root, uuid))?;
        Self::from_index(uuid, mapping, index)
    }

    fn from_index(uuid: Uuid, mapping: Mapping, index: Index) -> Result<Self> {
        let writer = index.writer(WRITER_HEAP_BYTES)?;
        let reader = index.reader()?;
        Ok(Self {
            uuid,
            mapping,
            index,
            writer: Mutex::new(writer),
            reader,
        })
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn mapping(&self) -> &Mapping {
        &self.mapping
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn reader(&self) -> &IndexReader {
        &self.reader
    }

    pub fn writer(&self) -> &Mutex<IndexWriter> {
        &self.writer
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
