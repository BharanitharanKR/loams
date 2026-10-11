//! [`MemCatalog`]: the file catalog with its pointers in memory, for tests.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard};

use loams_store::Store;

use super::files::{FileCatalog, Pointers};
use super::{CatalogError, LakeRoot};

#[derive(Debug, Default)]
struct State {
    namespaces: BTreeMap<Vec<String>, HashMap<String, String>>,
    tables: BTreeMap<(Vec<String>, String), String>,
}

#[derive(Debug, Default)]
struct MemPointers(Mutex<State>);

impl MemPointers {
    fn state(&self) -> MutexGuard<'_, State> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Pointers for MemPointers {
    fn namespaces(&self) -> Result<Vec<Vec<String>>, CatalogError> {
        Ok(self.state().namespaces.keys().cloned().collect())
    }

    fn namespace(
        &self,
        namespace: &[String],
    ) -> Result<Option<HashMap<String, String>>, CatalogError> {
        Ok(self.state().namespaces.get(namespace).cloned())
    }

    fn add_namespace(
        &self,
        namespace: &[String],
        properties: &HashMap<String, String>,
    ) -> Result<bool, CatalogError> {
        let mut state = self.state();
        if state.namespaces.contains_key(namespace) {
            return Ok(false);
        }
        state
            .namespaces
            .insert(namespace.to_vec(), properties.clone());
        Ok(true)
    }

    fn remove_namespace(&self, namespace: &[String]) -> Result<bool, CatalogError> {
        Ok(self.state().namespaces.remove(namespace).is_some())
    }

    fn tables(&self, namespace: &[String]) -> Result<Vec<String>, CatalogError> {
        Ok(self
            .state()
            .tables
            .keys()
            .filter(|(ns, _)| ns == namespace)
            .map(|(_, name)| name.clone())
            .collect())
    }

    fn table(&self, namespace: &[String], name: &str) -> Result<Option<String>, CatalogError> {
        Ok(self
            .state()
            .tables
            .get(&(namespace.to_vec(), name.to_string()))
            .cloned())
    }

    fn add_table(
        &self,
        namespace: &[String],
        name: &str,
        location: &str,
    ) -> Result<bool, CatalogError> {
        let mut state = self.state();
        let key = (namespace.to_vec(), name.to_string());
        if state.tables.contains_key(&key) {
            return Ok(false);
        }
        state.tables.insert(key, location.to_string());
        Ok(true)
    }

    fn swap_table(
        &self,
        namespace: &[String],
        name: &str,
        old: &str,
        new: &str,
    ) -> Result<bool, CatalogError> {
        let mut state = self.state();
        match state
            .tables
            .get_mut(&(namespace.to_vec(), name.to_string()))
        {
            Some(current) if current == old => {
                *current = new.to_string();
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn remove_table(&self, namespace: &[String], name: &str) -> Result<bool, CatalogError> {
        Ok(self
            .state()
            .tables
            .remove(&(namespace.to_vec(), name.to_string()))
            .is_some())
    }

    fn move_table(
        &self,
        from: (&[String], &str),
        to: (&[String], &str),
    ) -> Result<(), CatalogError> {
        let mut state = self.state();
        let to_key = (to.0.to_vec(), to.1.to_string());
        let from_key = (from.0.to_vec(), from.1.to_string());
        if !state.tables.contains_key(&from_key) {
            return Err(CatalogError::NoSuchTable(format!(
                "{}.{}",
                from.0.join("."),
                from.1
            )));
        }
        if state.tables.contains_key(&to_key) {
            return Err(CatalogError::TableExists(format!(
                "{}.{}",
                to.0.join("."),
                to.1
            )));
        }
        if let Some(location) = state.tables.remove(&from_key) {
            state.tables.insert(to_key, location);
        }
        Ok(())
    }
}

/// The House's file catalog in memory: pointers in a map, metadata files in a
/// store (an in-memory one in most tests).
#[derive(Debug)]
pub struct MemCatalog(FileCatalog<MemPointers>);

impl MemCatalog {
    /// A catalog writing its metadata files to `store`, whose objects workers
    /// reach as `root` (`s3://<bucket>/<prefix>/…`).
    pub fn new(store: Store, root: LakeRoot) -> Self {
        Self(FileCatalog::new(MemPointers::default(), store, root))
    }

    /// A catalog over a fresh in-memory store, rooted at `s3://local/` (what
    /// `house-cache` names a `memory://` store).
    pub fn in_memory() -> Self {
        Self::new(Store::in_memory(), LakeRoot::local())
    }
}

super::files::delegate_house_catalog!(MemCatalog);
