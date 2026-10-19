//! A catalog whose metadata files the House writes itself: [`super::MemCatalog`]
//! and [`super::LocalCatalog`] differ only in where they keep their pointers.
//!
//! A table is a pointer to its current metadata file. A commit reads that file,
//! checks the requirements and applies the updates (`loams_house_lake::metadata`),
//! writes the next file beside it (never overwriting one), and then swaps the
//! pointer from the file it read to the new one with compare-and-set; when the
//! swap fails someone else committed first and the commit is a
//! [`CatalogError::Conflict`]. The written file is then an orphan, which HS1 Task
//! 15's orphan collection removes.

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;
use loams_house_lake::metadata::{self, MetadataFile};
use loams_house_lake::{NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate};
use loams_store::{Store, StoreError};

use super::{CatalogError, HouseCatalog, LakeRoot, LakeTable, namespace_text, table_text};

/// Where a file catalog keeps namespaces and table pointers. Every method is one
/// atomic step.
pub(super) trait Pointers: Send + Sync + fmt::Debug {
    /// Every namespace, with its properties.
    fn namespaces(&self) -> Result<Vec<Vec<String>>, CatalogError>;
    /// One namespace's properties, if it exists.
    fn namespace(
        &self,
        namespace: &[String],
    ) -> Result<Option<HashMap<String, String>>, CatalogError>;
    /// Adds a namespace; `false` when it exists.
    fn add_namespace(
        &self,
        namespace: &[String],
        properties: &HashMap<String, String>,
    ) -> Result<bool, CatalogError>;
    /// Removes a namespace; `false` when it does not exist.
    fn remove_namespace(&self, namespace: &[String]) -> Result<bool, CatalogError>;
    /// The names of a namespace's tables.
    fn tables(&self, namespace: &[String]) -> Result<Vec<String>, CatalogError>;
    /// A table's metadata location, if it exists.
    fn table(&self, namespace: &[String], name: &str) -> Result<Option<String>, CatalogError>;
    /// Adds a table pointer; `false` when the name is taken.
    fn add_table(
        &self,
        namespace: &[String],
        name: &str,
        location: &str,
    ) -> Result<bool, CatalogError>;
    /// Swaps a pointer from `old` to `new`; `false` when it no longer points at
    /// `old` (or the table is gone).
    fn swap_table(
        &self,
        namespace: &[String],
        name: &str,
        old: &str,
        new: &str,
    ) -> Result<bool, CatalogError>;
    /// Removes a table pointer; `false` when it does not exist.
    fn remove_table(&self, namespace: &[String], name: &str) -> Result<bool, CatalogError>;
    /// Moves a pointer. `Err(NoSuchTable)` / `Err(TableExists)` as the REST
    /// catalog answers.
    fn move_table(
        &self,
        from: (&[String], &str),
        to: (&[String], &str),
    ) -> Result<(), CatalogError>;
}

/// A catalog over [`Pointers`] and a store for the metadata files.
pub(super) struct FileCatalog<P> {
    pointers: P,
    store: Store,
    root: LakeRoot,
}

impl<P: fmt::Debug> fmt::Debug for FileCatalog<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileCatalog")
            .field("pointers", &self.pointers)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

fn unavailable(err: StoreError) -> CatalogError {
    CatalogError::Unavailable(format!("the store failed: {err}"))
}

impl<P: Pointers> FileCatalog<P> {
    pub(super) fn new(pointers: P, store: Store, root: LakeRoot) -> Self {
        Self {
            pointers,
            store,
            root,
        }
    }

    pub(super) fn pointers(&self) -> &P {
        &self.pointers
    }

    /// The store path of a location under the root.
    fn path_of(&self, location: &str) -> Result<String, CatalogError> {
        self.root
            .relative(location)
            .map(str::to_string)
            .ok_or_else(|| {
                CatalogError::Invalid(format!(
                    "{location} is outside the House's store ({})",
                    self.root
                ))
            })
    }

    async fn read(
        &self,
        location: &str,
    ) -> Result<loams_house_lake::spec::TableMetadata, CatalogError> {
        let path = self.path_of(location)?;
        let (bytes, _) = self.store.get(&path).await.map_err(|err| match err {
            StoreError::NotFound { .. } => {
                CatalogError::Invalid(format!("metadata file {location} does not exist"))
            }
            other => unavailable(other),
        })?;
        metadata::from_json(location, &bytes).map_err(|err| CatalogError::Invalid(err.to_string()))
    }

    async fn write(
        &self,
        file: &MetadataFile,
        metadata: &loams_house_lake::spec::TableMetadata,
    ) -> Result<String, CatalogError> {
        let location = file.to_string();
        let bytes =
            metadata::to_json(metadata).map_err(|err| CatalogError::Invalid(err.to_string()))?;
        self.store
            .put_if_absent(&self.path_of(&location)?, Bytes::from(bytes))
            .await
            .map_err(unavailable)?;
        Ok(location)
    }

    fn require_namespace(&self, namespace: &NamespaceIdent) -> Result<(), CatalogError> {
        match self.pointers.namespace(namespace.as_ref())? {
            Some(_) => Ok(()),
            None => Err(CatalogError::NoSuchNamespace(namespace_text(namespace))),
        }
    }

    fn pointer(&self, table: &TableIdent) -> Result<String, CatalogError> {
        self.pointers
            .table(table.namespace().as_ref(), table.name())?
            .ok_or_else(|| CatalogError::NoSuchTable(table_text(table)))
    }
}

#[async_trait]
impl<P: Pointers> HouseCatalog for FileCatalog<P> {
    async fn list_root_databases(&self) -> Result<Vec<NamespaceIdent>, CatalogError> {
        let mut roots: Vec<NamespaceIdent> = self
            .pointers
            .namespaces()?
            .into_iter()
            .filter(|ns| ns.len() == 1)
            .filter_map(|ns| NamespaceIdent::from_vec(ns).ok())
            .collect();
        roots.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        Ok(roots)
    }

    async fn list_databases(
        &self,
        parent: &NamespaceIdent,
    ) -> Result<Vec<NamespaceIdent>, CatalogError> {
        self.require_namespace(parent)?;
        let depth = parent.as_ref().len() + 1;
        let mut children: Vec<NamespaceIdent> = self
            .pointers
            .namespaces()?
            .into_iter()
            .filter(|ns| ns.len() == depth && ns.starts_with(parent.as_ref()))
            .filter_map(|ns| NamespaceIdent::from_vec(ns).ok())
            .collect();
        children.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        Ok(children)
    }

    async fn create_database(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<(), CatalogError> {
        if let Some(parent) = namespace.parent() {
            self.require_namespace(&parent)?;
        }
        if self
            .pointers
            .add_namespace(namespace.as_ref(), &properties)?
        {
            Ok(())
        } else {
            Err(CatalogError::NamespaceExists(namespace_text(namespace)))
        }
    }

    async fn database_properties(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<HashMap<String, String>, CatalogError> {
        self.pointers
            .namespace(namespace.as_ref())?
            .ok_or_else(|| CatalogError::NoSuchNamespace(namespace_text(namespace)))
    }

    async fn drop_database(&self, namespace: &NamespaceIdent) -> Result<(), CatalogError> {
        self.require_namespace(namespace)?;
        let has_children =
            self.pointers.namespaces()?.iter().any(|ns| {
                ns.len() > namespace.as_ref().len() && ns.starts_with(namespace.as_ref())
            });
        if has_children || !self.pointers.tables(namespace.as_ref())?.is_empty() {
            return Err(CatalogError::NotEmpty(namespace_text(namespace)));
        }
        if self.pointers.remove_namespace(namespace.as_ref())? {
            Ok(())
        } else {
            Err(CatalogError::NoSuchNamespace(namespace_text(namespace)))
        }
    }

    async fn list_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>, CatalogError> {
        self.require_namespace(namespace)?;
        let mut names = self.pointers.tables(namespace.as_ref())?;
        names.sort();
        Ok(names
            .into_iter()
            .map(|name| TableIdent::new(namespace.clone(), name))
            .collect())
    }

    async fn load_table(&self, table: &TableIdent) -> Result<LakeTable, CatalogError> {
        let location = self.pointer(table)?;
        let metadata = self.read(&location).await?;
        Ok(LakeTable {
            ident: table.clone(),
            metadata_location: location,
            metadata,
        })
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<LakeTable, CatalogError> {
        self.require_namespace(namespace)?;
        let ident = TableIdent::new(namespace.clone(), creation.name.clone());
        if self
            .pointers
            .table(namespace.as_ref(), &creation.name)?
            .is_some()
        {
            return Err(CatalogError::TableExists(table_text(&ident)));
        }
        let table_location = creation
            .location
            .clone()
            .ok_or_else(|| CatalogError::Invalid("a table needs a location".to_string()))?;
        self.path_of(&table_location)?;
        let metadata =
            metadata::create(creation).map_err(|err| CatalogError::Invalid(err.to_string()))?;
        let location = self
            .write(&MetadataFile::first(&table_location), &metadata)
            .await?;
        if !self
            .pointers
            .add_table(namespace.as_ref(), ident.name(), &location)?
        {
            return Err(CatalogError::TableExists(table_text(&ident)));
        }
        Ok(LakeTable {
            ident,
            metadata_location: location,
            metadata,
        })
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<(), CatalogError> {
        if self
            .pointers
            .remove_table(table.namespace().as_ref(), table.name())?
        {
            Ok(())
        } else {
            Err(CatalogError::NoSuchTable(table_text(table)))
        }
    }

    async fn rename_table(&self, from: &TableIdent, to: &TableIdent) -> Result<(), CatalogError> {
        self.require_namespace(to.namespace())?;
        self.pointers.move_table(
            (from.namespace().as_ref(), from.name()),
            (to.namespace().as_ref(), to.name()),
        )
    }

    async fn commit(
        &self,
        table: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<LakeTable, CatalogError> {
        let current = self.load_table(table).await?;
        let file = MetadataFile::parse(&current.metadata_location)
            .map_err(|err| CatalogError::Invalid(err.to_string()))?;
        let conflict = |message: String| CatalogError::Conflict {
            table: table_text(table),
            message,
        };
        let next_metadata = metadata::commit(&current.metadata, &file, &requirements, updates)
            .map_err(|err| match err {
                loams_house_lake::LakeError::Conflict(message) => conflict(message),
                other => CatalogError::Invalid(other.to_string()),
            })?;
        let location = self.write(&file.next(), &next_metadata).await?;
        if self.pointers.swap_table(
            table.namespace().as_ref(),
            table.name(),
            &current.metadata_location,
            &location,
        )? {
            Ok(LakeTable {
                ident: table.clone(),
                metadata_location: location,
                metadata: next_metadata,
            })
        } else {
            Err(conflict(
                "the table's metadata changed while this commit was written".to_string(),
            ))
        }
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: &str,
    ) -> Result<LakeTable, CatalogError> {
        self.require_namespace(table.namespace())?;
        let metadata = self.read(metadata_location).await?;
        if !self
            .pointers
            .add_table(table.namespace().as_ref(), table.name(), metadata_location)?
        {
            return Err(CatalogError::TableExists(table_text(table)));
        }
        Ok(LakeTable {
            ident: table.clone(),
            metadata_location: metadata_location.to_string(),
            metadata,
        })
    }
}

/// `impl HouseCatalog for $name` by delegating to its `FileCatalog` (field `.0`).
macro_rules! delegate_house_catalog {
    ($name:ident) => {
        #[async_trait::async_trait]
        impl $crate::catalog::HouseCatalog for $name {
            async fn list_root_databases(
                &self,
            ) -> Result<Vec<loams_house_lake::NamespaceIdent>, $crate::catalog::CatalogError> {
                self.0.list_root_databases().await
            }
            async fn list_databases(
                &self,
                parent: &loams_house_lake::NamespaceIdent,
            ) -> Result<Vec<loams_house_lake::NamespaceIdent>, $crate::catalog::CatalogError> {
                self.0.list_databases(parent).await
            }
            async fn create_database(
                &self,
                namespace: &loams_house_lake::NamespaceIdent,
                properties: std::collections::HashMap<String, String>,
            ) -> Result<(), $crate::catalog::CatalogError> {
                self.0.create_database(namespace, properties).await
            }
            async fn database_properties(
                &self,
                namespace: &loams_house_lake::NamespaceIdent,
            ) -> Result<std::collections::HashMap<String, String>, $crate::catalog::CatalogError>
            {
                self.0.database_properties(namespace).await
            }
            async fn drop_database(
                &self,
                namespace: &loams_house_lake::NamespaceIdent,
            ) -> Result<(), $crate::catalog::CatalogError> {
                self.0.drop_database(namespace).await
            }
            async fn list_tables(
                &self,
                namespace: &loams_house_lake::NamespaceIdent,
            ) -> Result<Vec<loams_house_lake::TableIdent>, $crate::catalog::CatalogError> {
                self.0.list_tables(namespace).await
            }
            async fn load_table(
                &self,
                table: &loams_house_lake::TableIdent,
            ) -> Result<$crate::catalog::LakeTable, $crate::catalog::CatalogError> {
                self.0.load_table(table).await
            }
            async fn create_table(
                &self,
                namespace: &loams_house_lake::NamespaceIdent,
                creation: loams_house_lake::TableCreation,
            ) -> Result<$crate::catalog::LakeTable, $crate::catalog::CatalogError> {
                self.0.create_table(namespace, creation).await
            }
            async fn drop_table(
                &self,
                table: &loams_house_lake::TableIdent,
            ) -> Result<(), $crate::catalog::CatalogError> {
                self.0.drop_table(table).await
            }
            async fn rename_table(
                &self,
                from: &loams_house_lake::TableIdent,
                to: &loams_house_lake::TableIdent,
            ) -> Result<(), $crate::catalog::CatalogError> {
                self.0.rename_table(from, to).await
            }
            async fn commit(
                &self,
                table: &loams_house_lake::TableIdent,
                requirements: Vec<loams_house_lake::TableRequirement>,
                updates: Vec<loams_house_lake::TableUpdate>,
            ) -> Result<$crate::catalog::LakeTable, $crate::catalog::CatalogError> {
                self.0.commit(table, requirements, updates).await
            }
            async fn register_table(
                &self,
                table: &loams_house_lake::TableIdent,
                metadata_location: &str,
            ) -> Result<$crate::catalog::LakeTable, $crate::catalog::CatalogError> {
                self.0.register_table(table, metadata_location).await
            }
        }
    };
}
pub(super) use delegate_house_catalog;
