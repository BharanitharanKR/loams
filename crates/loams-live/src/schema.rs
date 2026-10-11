//! The schema a deployment declares (design §45 §3.3, §4; LV1 plan Task 6):
//! its tables and their indexes, the deployed schema record, and how a
//! deploy applies it.
//!
//! In Task 6 a schema applies **additively**: a declared table that does
//! not exist is created with its indexes, and nothing else changes. A table
//! the schema leaves out keeps its data, and an existing table whose
//! declared indexes differ from its own refuses the deploy
//! ([`LiveError::FailedPrecondition`]) until online index changes (Task 9).
//! Validators arrive with Task 8. While the active deployment declares at
//! least one table, the schema record exists and an insert into an
//! undeclared table fails ([`catalog::table_for_insert`]); an empty schema
//! removes the record, so tables are created on their first insert again.

use std::collections::BTreeMap;

use buffa::Message;
use loams_kv::Txn;

use crate::catalog::{self, IndexSpec};
use crate::docs::Reads;
use crate::keys::AppKeys;
use crate::{Limits, LiveError, pb};

/// A deployed schema: tables and their user indexes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    pub tables: Vec<TableSchema>,
}

/// A table as a schema declares it. `by_id` and `by_creation_time` are
/// implicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub name: String,
    pub indexes: Vec<IndexSpec>,
}

/// What applying a schema changes (or, in a dry run, would change).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaChanges {
    /// The tables it creates, in declaration order.
    pub create: Vec<String>,
}

impl Schema {
    /// The schema of its wire form (not checked; see [`Schema::check`]).
    pub fn from_proto(p: &pb::Schema) -> Self {
        Schema {
            tables: p
                .tables
                .iter()
                .map(|t| TableSchema {
                    name: t.name.clone(),
                    indexes: t
                        .indexes
                        .iter()
                        .map(|i| IndexSpec {
                            name: i.name.clone(),
                            fields: i.fields.clone(),
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// The wire form, which the deployment record and the schema record
    /// store.
    pub fn to_proto(&self) -> pb::Schema {
        pb::Schema {
            tables: self
                .tables
                .iter()
                .map(|t| pb::TableSchema {
                    name: t.name.clone(),
                    indexes: t
                        .indexes
                        .iter()
                        .map(|i| pb::IndexSchema {
                            name: i.name.clone(),
                            fields: i.fields.clone(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Checks what needs no catalog: table names, a table declared twice,
    /// and each table's indexes (as [`catalog::define_table`] does).
    pub fn check(&self, limits: &Limits) -> Result<(), LiveError> {
        for (n, table) in self.tables.iter().enumerate() {
            catalog::check_name("table", &table.name)?;
            if self.tables[..n].iter().any(|t| t.name == table.name) {
                return Err(LiveError::invalid(format!(
                    "table '{}' is declared twice in the schema",
                    table.name
                )));
            }
            catalog::check_indexes(&table.indexes, limits)?;
        }
        Ok(())
    }

    fn decode(bytes: &[u8]) -> Result<Self, LiveError> {
        let p = pb::Schema::decode_from_slice(bytes)
            .map_err(|e| LiveError::Corrupt(format!("schema record: {e}")))?;
        Ok(Schema::from_proto(&p))
    }
}

/// The deployed schema record, if the active deployment declares tables.
pub async fn load(r: &mut impl Reads, app: &AppKeys) -> Result<Option<Schema>, LiveError> {
    match r.get(&app.schema()).await? {
        Some(bytes) => Schema::decode(&bytes).map(Some),
        None => Ok(None),
    }
}

/// What applying `schema` would change, read from the catalog: the declared
/// tables that do not exist. An existing table whose user indexes differ
/// from the declared ones (by name and fields, in any order) is
/// [`LiveError::FailedPrecondition`]: index changes wait for Task 9.
pub async fn plan(
    r: &mut impl Reads,
    app: &AppKeys,
    schema: &Schema,
) -> Result<SchemaChanges, LiveError> {
    let mut changes = SchemaChanges::default();
    for table in &schema.tables {
        let Some(existing) = catalog::load_table(r, app, &table.name).await? else {
            changes.create.push(table.name.clone());
            continue;
        };
        let have: BTreeMap<&str, &[String]> = existing
            .indexes
            .iter()
            .map(|i| (i.name.as_str(), i.fields.as_slice()))
            .collect();
        let want: BTreeMap<&str, &[String]> = table
            .indexes
            .iter()
            .map(|i| (i.name.as_str(), i.fields.as_slice()))
            .collect();
        if have != want {
            return Err(LiveError::FailedPrecondition(format!(
                "table '{}' exists with other indexes than the schema declares; changing the \
                 indexes of an existing table needs online index changes (LV1 Task 9)",
                table.name
            )));
        }
    }
    Ok(changes)
}

/// Applies `schema` in `txn`: creates the tables [`plan`] finds missing,
/// with their indexes, and writes the schema record (or removes it, for a
/// schema with no tables).
pub async fn apply(
    txn: &mut Txn,
    app: &AppKeys,
    schema: &Schema,
    limits: &Limits,
) -> Result<SchemaChanges, LiveError> {
    let changes = plan(txn, app, schema).await?;
    for name in &changes.create {
        let declared = schema
            .tables
            .iter()
            .find(|t| &t.name == name)
            .map(|t| t.indexes.as_slice())
            .unwrap_or_default();
        catalog::define_table(txn, app, name, declared, limits).await?;
    }
    if schema.tables.is_empty() {
        txn.delete(&app.schema()).await?;
    } else {
        txn.put(&app.schema(), schema.to_proto().encode_to_vec())
            .await?;
    }
    Ok(changes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str, indexes: &[(&str, &[&str])]) -> TableSchema {
        TableSchema {
            name: name.into(),
            indexes: indexes
                .iter()
                .map(|(n, f)| IndexSpec {
                    name: (*n).into(),
                    fields: f.iter().map(|f| (*f).to_string()).collect(),
                })
                .collect(),
        }
    }

    #[test]
    fn schema_round_trips_through_its_wire_form() {
        let schema = Schema {
            tables: vec![
                table("notes", &[("by_body", &["body"]), ("by_ab", &["a", "b"])]),
                table("tags", &[]),
            ],
        };
        assert_eq!(Schema::from_proto(&schema.to_proto()), schema);
        let bytes = schema.to_proto().encode_to_vec();
        assert_eq!(Schema::decode(&bytes).expect("decodes"), schema);
        assert!(Schema::decode(b"\xff\xff").is_err());
    }

    #[test]
    fn check_refuses_bad_names_twice_declared_tables_and_bad_indexes() {
        let limits = Limits::default();
        assert!(Schema::default().check(&limits).is_ok());
        let ok = Schema {
            tables: vec![table("notes", &[("by_body", &["body"])])],
        };
        assert!(ok.check(&limits).is_ok());
        for bad in [
            Schema {
                tables: vec![table("_notes", &[])],
            },
            Schema {
                tables: vec![table("notes", &[]), table("notes", &[])],
            },
            Schema {
                tables: vec![table("notes", &[("by_id", &["x"])])],
            },
            Schema {
                tables: vec![table("notes", &[("empty", &[])])],
            },
        ] {
            let err = bad.check(&limits).expect_err("refused");
            assert!(matches!(err, LiveError::InvalidArgument(_)), "{err}");
        }
    }
}
