use lumen_core::Catalog;
use openraft::testing::{StoreBuilder, Suite};
use openraft::{AnyError, StorageError, StorageIOError};
use tempfile::TempDir;

use crate::type_config::TypeConfig;
use crate::{LogStore, StateMachine};

struct Builder;

impl StoreBuilder<TypeConfig, LogStore, StateMachine, TempDir> for Builder {
    async fn build(&self) -> Result<(TempDir, LogStore, StateMachine), StorageError<u64>> {
        let dir = TempDir::new().map_err(setup_err)?;
        let log_store = LogStore::open(dir.path())?;
        let catalog = Catalog::open(dir.path().join("state")).map_err(setup_err)?;
        Ok((dir, log_store, StateMachine::new(catalog)))
    }
}

fn setup_err(e: impl std::error::Error + 'static) -> StorageError<u64> {
    StorageIOError::read(AnyError::new(&e)).into()
}

#[test]
fn passes_openraft_conformance_suite() {
    Suite::<TypeConfig, LogStore, StateMachine, Builder, TempDir>::test_all(Builder).unwrap();
}
