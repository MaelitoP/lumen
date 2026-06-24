use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Bytes;
use lumen_cluster::Cluster;
use lumen_core::{Catalog, Mapping, SearchResults};

use crate::error::ApiError;

pub struct CreateOutcome {
    pub mapping: Mapping,
    pub created: bool,
}

pub struct WriteOutcome {
    pub id: String,
    pub created: bool,
}

/// The operations the HTTP handlers need, served either by the local catalog
/// (standalone) or routed through Raft (cluster).
#[async_trait]
pub trait Engine: Send + Sync + 'static {
    async fn create_collection(
        &self,
        name: String,
        mapping: Mapping,
    ) -> Result<CreateOutcome, ApiError>;
    async fn drop_collection(&self, name: String) -> Result<(), ApiError>;
    async fn index(
        &self,
        name: String,
        id: Option<String>,
        source: Bytes,
    ) -> Result<WriteOutcome, ApiError>;
    async fn delete(&self, name: String, id: String) -> Result<(), ApiError>;
    async fn get_document(&self, name: String, id: String) -> Result<Vec<u8>, ApiError>;
    async fn search(
        &self,
        name: String,
        query: String,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResults, ApiError>;
    async fn list(&self) -> Result<Vec<String>, ApiError>;
    async fn describe(&self, name: String) -> Result<Mapping, ApiError>;
}

pub struct StandaloneEngine {
    catalog: Arc<Catalog>,
}

impl StandaloneEngine {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl Engine for StandaloneEngine {
    async fn create_collection(
        &self,
        name: String,
        mapping: Mapping,
    ) -> Result<CreateOutcome, ApiError> {
        let created = run(self.catalog.clone(), move |c| c.create(&name, mapping)).await?;
        Ok(CreateOutcome {
            mapping: created.collection.mapping().clone(),
            created: created.created,
        })
    }

    async fn drop_collection(&self, name: String) -> Result<(), ApiError> {
        run(self.catalog.clone(), move |c| c.drop_collection(&name)).await
    }

    async fn index(
        &self,
        name: String,
        id: Option<String>,
        source: Bytes,
    ) -> Result<WriteOutcome, ApiError> {
        let upserted = run(self.catalog.clone(), move |c| {
            c.upsert_document(&name, id.as_deref(), &source)
        })
        .await?;
        Ok(WriteOutcome {
            id: upserted.id,
            created: upserted.created,
        })
    }

    async fn delete(&self, name: String, id: String) -> Result<(), ApiError> {
        run(self.catalog.clone(), move |c| {
            c.delete_document(&name, &id).map(|_| ())
        })
        .await
    }

    async fn get_document(&self, name: String, id: String) -> Result<Vec<u8>, ApiError> {
        run(self.catalog.clone(), move |c| c.get_document(&name, &id)).await
    }

    async fn search(
        &self,
        name: String,
        query: String,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResults, ApiError> {
        run(self.catalog.clone(), move |c| {
            c.get(&name)?.search(&query, limit, offset)
        })
        .await
    }

    async fn list(&self) -> Result<Vec<String>, ApiError> {
        run(self.catalog.clone(), |c| Ok(c.list())).await
    }

    async fn describe(&self, name: String) -> Result<Mapping, ApiError> {
        run(self.catalog.clone(), move |c| c.describe(&name)).await
    }
}

async fn run<T, F>(catalog: Arc<Catalog>, f: F) -> Result<T, ApiError>
where
    F: FnOnce(&Catalog) -> lumen_core::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(move || f(&catalog)).await {
        Ok(result) => result.map_err(ApiError::from),
        Err(error) => {
            tracing::error!(%error, "catalog task panicked");
            Err(ApiError::Internal)
        }
    }
}

pub struct ClusterEngine {
    cluster: Arc<Cluster>,
}

impl ClusterEngine {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self { cluster }
    }
}

#[async_trait]
impl Engine for ClusterEngine {
    async fn create_collection(
        &self,
        name: String,
        mapping: Mapping,
    ) -> Result<CreateOutcome, ApiError> {
        let outcome = self.cluster.create_collection(&name, mapping).await?;
        Ok(CreateOutcome {
            mapping: outcome.mapping,
            created: outcome.created,
        })
    }

    async fn drop_collection(&self, name: String) -> Result<(), ApiError> {
        self.cluster.drop_collection(&name).await?;
        Ok(())
    }

    async fn index(
        &self,
        name: String,
        id: Option<String>,
        source: Bytes,
    ) -> Result<WriteOutcome, ApiError> {
        let outcome = self.cluster.index(&name, id.as_deref(), &source).await?;
        Ok(WriteOutcome {
            id: outcome.id,
            created: outcome.created,
        })
    }

    async fn delete(&self, name: String, id: String) -> Result<(), ApiError> {
        self.cluster.delete(&name, &id).await?;
        Ok(())
    }

    async fn get_document(&self, name: String, id: String) -> Result<Vec<u8>, ApiError> {
        Ok(self.cluster.linearizable_get(&name, &id).await?)
    }

    async fn search(
        &self,
        name: String,
        query: String,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResults, ApiError> {
        Ok(self.cluster.search(&name, &query, limit, offset).await?)
    }

    async fn list(&self) -> Result<Vec<String>, ApiError> {
        Ok(self.cluster.list())
    }

    async fn describe(&self, name: String) -> Result<Mapping, ApiError> {
        Ok(self.cluster.describe(&name)?)
    }
}
