//! The schema a deployment declares (design §45 §3.3, §4; LV1 plan Tasks 6
//! and 8): its tables, their indexes and their validators, the deployed
//! schema record, and how a deploy applies it.
//!
//! **Tables** apply additively (Task 6): a declared table that does not
//! exist is created with its indexes and validator, and nothing else
//! changes. A table the schema leaves out keeps its data and its validator,
//! and an existing table whose declared indexes differ from its own refuses
//! the deploy ([`LiveError::FailedPrecondition`]) until online index
//! changes (Task 9). While the active deployment declares at least one
//! table, the schema record exists and an insert into an undeclared table
//! fails ([`catalog::table_for_insert`]); an empty schema removes the
//! record, so tables are created on their first insert again.
//!
//! **Validators** (Task 8, design §45 §4.1) are a table's
//! [`TableValidation`](catalog::TableValidation): every write must match the validator
//! ([`check_document`]), `_system:*` writes included. A deploy that changes
//! an existing table's validator ([`SchemaChanges::validators`]) applies a
//! **looser** one ([`validate::accepts_all`]) at once, and so a stricter one
//! of a table declared with `schema_validation: false`. A **stricter** one
//! is first checked against the table's documents
//! ([`SchemaChanges::validate`]): the deploy [enforces](enforce) it on new
//! writes alongside the old one, in a transaction that fences every
//! mutation in flight, then scans the table in a [`backfill::TableJob`]
//! with the [`Conformance`] visitor. The swap ([`apply`]) makes it the
//! table's validator only if that check is the deployment's own; a failed
//! or abandoned check is [released](release), and the old validator holds
//! alone again.

use std::collections::{BTreeMap, BTreeSet};

use buffa::Message;
use futures::future::BoxFuture;
use loams_kv::Txn;
use ulid::Ulid;

use crate::backfill::{self, Finding, JobKind, Visitor};
use crate::catalog::{self, Enforcing, IndexSpec, TableDef};
use crate::docs::{Doc, Reads};
use crate::keys::AppKeys;
use crate::validate::{self, Validator};
use crate::{Limits, LiveError, LiveValue, TableId, pb};

/// A deployed schema: tables, their user indexes and their validators.
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
    /// The validator every write must match ([`Validator::check_table`]'s
    /// shape); `None`: any document.
    pub validator: Option<Validator>,
    /// Whether a deploy that makes the validator stricter checks the
    /// table's documents first (design §45 §4.1); `false` applies it to new
    /// writes only. `true` by default.
    pub schema_validation: bool,
}

impl Default for TableSchema {
    fn default() -> Self {
        TableSchema {
            name: String::new(),
            indexes: Vec::new(),
            validator: None,
            schema_validation: true,
        }
    }
}

/// What applying a schema changes (or, in a dry run, would change).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaChanges {
    /// The tables it creates, in declaration order.
    pub create: Vec<String>,
    /// The existing tables whose validator it changes, in declaration
    /// order.
    pub validators: Vec<String>,
    /// Of those, the ones whose documents it checks first (a stricter
    /// validator, with `schema_validation`).
    pub validate: Vec<String>,
}

/// A stricter validator a deploy checks a table's documents against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Validation {
    /// The table whose documents are checked.
    pub table: TableId,
    /// The table's name, for findings and messages.
    pub name: String,
    /// The stricter validator the documents must match.
    pub validator: Validator,
}

impl Schema {
    /// The schema of its wire form. A malformed validator (one without a
    /// kind, too deep) is [`LiveError::InvalidArgument`]; its shape is
    /// checked by [`Schema::check`].
    pub fn from_proto(p: &pb::Schema) -> Result<Self, LiveError> {
        Ok(Schema {
            tables: p
                .tables
                .iter()
                .map(|t| {
                    Ok(TableSchema {
                        name: t.name.clone(),
                        indexes: t
                            .indexes
                            .iter()
                            .map(|i| IndexSpec {
                                name: i.name.clone(),
                                fields: i.fields.clone(),
                            })
                            .collect(),
                        validator: t
                            .validator
                            .as_option()
                            .map(Validator::from_proto)
                            .transpose()
                            .map_err(|e| {
                                LiveError::invalid(format!(
                                    "table '{}': its validator: {e}",
                                    t.name
                                ))
                            })?,
                        schema_validation: t.schema_validation.unwrap_or(true),
                    })
                })
                .collect::<Result<_, LiveError>>()?,
        })
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
                    validator: t
                        .validator
                        .as_ref()
                        .map(|v| buffa::MessageField::some(v.to_proto()))
                        .unwrap_or_default(),
                    schema_validation: (!t.schema_validation).then_some(false),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Checks what needs no catalog: table names, a table declared twice,
    /// each table's indexes (as [`catalog::define_table`] does) and its
    /// validator's shape ([`Validator::check_table`]).
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
            if let Some(validator) = &table.validator {
                validator.check_table().map_err(|e| {
                    LiveError::invalid(format!("table '{}': its validator: {e}", table.name))
                })?;
            }
        }
        Ok(())
    }

    /// The declared table `name`.
    fn table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.iter().find(|t| t.name == name)
    }

    fn decode(bytes: &[u8]) -> Result<Self, LiveError> {
        let p = pb::Schema::decode_from_slice(bytes)
            .map_err(|e| LiveError::Corrupt(format!("schema record: {e}")))?;
        Schema::from_proto(&p).map_err(|e| LiveError::Corrupt(format!("schema record: {e}")))
    }
}

/// The deployed schema record, if the active deployment declares tables.
pub async fn load(r: &mut impl Reads, app: &AppKeys) -> Result<Option<Schema>, LiveError> {
    match r.get(&app.schema()).await? {
        Some(bytes) => Schema::decode(&bytes).map(Some),
        None => Ok(None),
    }
}

/// Whether replacing validator `old` with `new` needs no check of existing
/// documents (`None` is no validator: any document).
pub fn loosens(new: Option<&Validator>, old: Option<&Validator>) -> bool {
    match (new, old) {
        (None, _) => true,
        (Some(new), None) => *new == Validator::Any,
        (Some(new), Some(old)) => validate::accepts_all(new, old),
    }
}

/// What applying `schema` would change, read from the catalog: the declared
/// tables that do not exist, the existing ones whose validator changes and,
/// of those, the ones whose documents a stricter validator must be checked
/// against. An existing table whose user indexes differ from the declared
/// ones (by name and fields, in any order) is
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
        let old = existing.validation.validator.as_ref();
        let new = table.validator.as_ref();
        if old != new {
            changes.validators.push(table.name.clone());
            if table.schema_validation && !loosens(new, old) {
                changes.validate.push(table.name.clone());
            }
        }
    }
    Ok(changes)
}

/// Applies `schema` for `deployment` in `txn` (the swap): creates the
/// tables [`plan`] finds missing, with their indexes and validators; sets
/// each changed validator; and writes the schema record (or removes it, for
/// a schema with no tables). A stricter validator that must be checked
/// first is set only if the table enforces it for `deployment` (its check
/// passed, [`enforce`]); the check's state and job are cleared with it.
/// Otherwise the swap is [`LiveError::FailedPrecondition`].
pub async fn apply(
    txn: &mut Txn,
    app: &AppKeys,
    schema: &Schema,
    limits: &Limits,
    deployment: Ulid,
) -> Result<SchemaChanges, LiveError> {
    let changes = plan(txn, app, schema).await?;
    for declared in &schema.tables {
        let name = &declared.name;
        let mut table = if changes.create.contains(name) {
            let table = catalog::define_table(txn, app, name, &declared.indexes, limits).await?;
            if declared.validator.is_none() {
                continue;
            }
            table
        } else if changes.validators.contains(name) {
            existing(txn, app, name).await?
        } else {
            continue;
        };
        if changes.validate.contains(name) {
            let checked = table.validation.enforcing.as_ref().is_some_and(|e| {
                e.deployment == deployment && Some(&e.validator) == declared.validator.as_ref()
            });
            if !checked {
                return Err(LiveError::FailedPrecondition(format!(
                    "table '{name}': its stricter validator was not checked against its \
                     documents for deployment {deployment} (the check was released or \
                     replaced); deploy again"
                )));
            }
            table.validation.enforcing = None;
            backfill::delete_job(txn, app, JobKind::Validate, table.id).await?;
        }
        table.validation.validator = declared.validator.clone();
        put_table(txn, app, &table).await?;
    }
    if schema.tables.is_empty() {
        txn.delete(&app.schema()).await?;
    } else {
        txn.put(&app.schema(), schema.to_proto().encode_to_vec())
            .await?;
    }
    Ok(changes)
}

/// Starts `deployment`'s checks of `schema`'s stricter validators in `txn`
/// (design §45 §4.1): each table of [`plan`]'s `validate` enforces its new
/// validator on new writes ([`TableValidation::enforcing`](catalog::TableValidation::enforcing)), and gets a
/// validation job whose lease `owner` holds for `lease`. A table another
/// deployment is checking, with a live job, is
/// [`LiveError::FailedPrecondition`]; an abandoned check (its jobs' leases
/// all expired) is replaced. The caller fences the journal in the same
/// transaction, so no mutation that started before it commits after it.
pub async fn enforce(
    txn: &mut Txn,
    app: &AppKeys,
    schema: &Schema,
    deployment: Ulid,
    owner: &str,
    lease: std::time::Duration,
) -> Result<Vec<Validation>, LiveError> {
    let now_ms = txn.start_ts().physical_ms();
    let changes = plan(txn, app, schema).await?;
    if changes.validate.is_empty() {
        return Ok(Vec::new());
    }
    let live = live_checks(txn, app, now_ms).await?;
    let mut out = Vec::with_capacity(changes.validate.len());
    for name in &changes.validate {
        let validator = schema
            .table(name)
            .and_then(|t| t.validator.clone())
            .ok_or_else(|| LiveError::Internal(format!("table '{name}' has no new validator")))?;
        let mut table = existing(txn, app, name).await?;
        if let Some(held) = &table.validation.enforcing
            && held.deployment != deployment
            && live.contains(&held.deployment)
        {
            return Err(LiveError::FailedPrecondition(format!(
                "table '{name}' is being validated by deployment {}; deploy again once that \
                 deploy finishes",
                held.deployment
            )));
        }
        table.validation.enforcing = Some(Enforcing {
            validator: validator.clone(),
            deployment,
        });
        put_table(txn, app, &table).await?;
        backfill::create_job(
            txn,
            app,
            JobKind::Validate,
            table.id,
            deployment,
            owner,
            lease,
        )
        .await?;
        out.push(Validation {
            table: table.id,
            name: name.clone(),
            validator,
        });
    }
    Ok(out)
}

/// Ends `deployment`'s checks in `txn`: every table that enforces a
/// validator for it stops, and its validation jobs are deleted. Returns
/// whether there was any.
pub async fn release(txn: &mut Txn, app: &AppKeys, deployment: Ulid) -> Result<bool, LiveError> {
    let mut any = false;
    for mut table in catalog::list_tables(txn, app).await? {
        if table
            .validation
            .enforcing
            .as_ref()
            .is_some_and(|e| e.deployment == deployment)
        {
            table.validation.enforcing = None;
            put_table(txn, app, &table).await?;
            any = true;
        }
        if backfill::load_job(txn, app, JobKind::Validate, table.id)
            .await?
            .is_some_and(|j| j.deployment == deployment)
        {
            backfill::delete_job(txn, app, JobKind::Validate, table.id).await?;
            any = true;
        }
    }
    Ok(any)
}

/// What [`release_abandoned`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Abandoned {
    /// The deployments whose checks were released, in table order.
    pub released: Vec<Ulid>,
    /// The deployments whose checks are still running.
    pub live: BTreeSet<Ulid>,
}

/// The janitor's part (LV1 plan Task 8): releases, in `txn`, every check
/// whose deployment has no job with a live lease at `now_ms` (its deploy
/// died, or its node), and deletes jobs left without a check.
pub async fn release_abandoned(
    txn: &mut Txn,
    app: &AppKeys,
    now_ms: u64,
) -> Result<Abandoned, LiveError> {
    let live = live_checks(txn, app, now_ms).await?;
    let mut released = Vec::new();
    for mut table in catalog::list_tables(txn, app).await? {
        let job = backfill::load_job(txn, app, JobKind::Validate, table.id).await?;
        let checking = table.validation.enforcing.as_ref().map(|e| e.deployment);
        if let Some(deployment) = checking
            && !live.contains(&deployment)
        {
            table.validation.enforcing = None;
            put_table(txn, app, &table).await?;
            if !released.contains(&deployment) {
                released.push(deployment);
            }
        }
        if let Some(job) = job
            && !live.contains(&job.deployment)
        {
            backfill::delete_job(txn, app, JobKind::Validate, table.id).await?;
        }
    }
    Ok(Abandoned { released, live })
}

/// The deployments with a validation job whose lease is live at `now_ms`.
/// A deploy checks its tables one after another, so a job that is done
/// keeps its deployment's other checks alive only while one of them runs.
async fn live_checks(
    txn: &mut Txn,
    app: &AppKeys,
    now_ms: u64,
) -> Result<BTreeSet<Ulid>, LiveError> {
    let mut live = BTreeSet::new();
    for table in catalog::list_tables(txn, app).await? {
        if let Some(job) = backfill::load_job(txn, app, JobKind::Validate, table.id).await?
            && job.lease_live(now_ms)
        {
            live.insert(job.deployment);
        }
    }
    Ok(live)
}

async fn existing(txn: &mut Txn, app: &AppKeys, name: &str) -> Result<TableDef, LiveError> {
    catalog::load_table(txn, app, name)
        .await?
        .ok_or_else(|| LiveError::Internal(format!("table '{name}' vanished during the deploy")))
}

async fn put_table(txn: &mut Txn, app: &AppKeys, table: &TableDef) -> Result<(), LiveError> {
    txn.put(&app.table(table.id), table.to_proto().encode_to_vec())
        .await?;
    Ok(())
}

/// The ids of the tables `validator`'s `id(table)`s name, read in `r`; a
/// table that does not exist is left out (no id is of it).
pub(crate) async fn id_tables(
    r: &mut impl Reads,
    app: &AppKeys,
    validator: &Validator,
) -> Result<BTreeMap<String, TableId>, LiveError> {
    let mut tables = BTreeMap::new();
    for name in validator.id_tables() {
        if let Some(table) = catalog::load_table(r, app, &name).await? {
            tables.insert(name, table.id);
        }
    }
    Ok(tables)
}

/// Checks a document's `fields`, about to be written to `table` in `txn`,
/// against the table's validators: the active one, and a stricter one a
/// deploy is checking. A mismatch is [`LiveError::SchemaViolation`] naming
/// the table, the field paths and, for the stricter one, the deployment.
pub async fn check_document(
    txn: &mut Txn,
    app: &AppKeys,
    table: &TableDef,
    fields: &BTreeMap<String, LiveValue>,
) -> Result<(), LiveError> {
    for (validator, checking) in table.validation.validators() {
        let tables = id_tables(txn, app, validator).await?;
        if let Err(violations) = validate::check_fields(validator, fields, &tables) {
            let whose = match checking {
                None => "its validator".to_string(),
                Some(d) => format!("the stricter validator deployment {d} is checking"),
            };
            return Err(LiveError::SchemaViolation {
                message: format!(
                    "table '{}': the document does not match {whose}: {}",
                    table.name,
                    validate::describe(&violations)
                ),
                violations,
            });
        }
    }
    Ok(())
}

/// The validation scan's [`Visitor`]: a document that does not match the
/// validator is a [`Finding`] whose detail names the field paths.
#[derive(Debug)]
pub struct Conformance {
    /// The validator each scanned document must match.
    pub validator: Validator,
}

impl Visitor for Conformance {
    fn visit<'a>(
        &'a self,
        txn: &'a mut Txn,
        app: &'a AppKeys,
        docs: &'a [Doc],
    ) -> BoxFuture<'a, Result<Vec<Finding>, LiveError>> {
        Box::pin(async move {
            let tables = id_tables(txn, app, &self.validator).await?;
            Ok(docs
                .iter()
                .filter_map(|doc| {
                    validate::check_fields(&self.validator, &doc.fields, &tables)
                        .err()
                        .map(|violations| Finding {
                            id: doc.id,
                            detail: validate::describe(&violations),
                        })
                })
                .collect())
        })
    }
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
            ..TableSchema::default()
        }
    }

    #[test]
    fn schema_round_trips_through_its_wire_form() {
        let validator = Validator::Object(BTreeMap::from([
            ("body".to_string(), Validator::String),
            (
                "n".to_string(),
                Validator::Optional(Box::new(Validator::Literal(LiveValue::F64(1.5)))),
            ),
        ]));
        let schema = Schema {
            tables: vec![
                TableSchema {
                    validator: Some(validator),
                    schema_validation: false,
                    ..table("notes", &[("by_body", &["body"]), ("by_ab", &["a", "b"])])
                },
                table("tags", &[]),
            ],
        };
        assert_eq!(
            Schema::from_proto(&schema.to_proto()).expect("valid"),
            schema
        );
        let wire = schema.to_proto();
        assert_eq!(wire.tables[0].schema_validation, Some(false));
        assert_eq!(
            wire.tables[1].schema_validation, None,
            "true is the default"
        );
        let bytes = schema.to_proto().encode_to_vec();
        assert_eq!(Schema::decode(&bytes).expect("decodes"), schema);
        assert!(Schema::decode(b"\xff\xff").is_err());
    }

    #[test]
    fn a_malformed_validator_is_refused_on_the_wire() {
        let mut wire = Schema {
            tables: vec![table("notes", &[])],
        }
        .to_proto();
        wire.tables[0].validator = buffa::MessageField::some(pb::Validator::default());
        let err = Schema::from_proto(&wire).expect_err("no kind");
        assert!(matches!(err, LiveError::InvalidArgument(_)), "{err}");
        assert!(err.to_string().contains("notes"), "{err}");
    }

    #[test]
    fn loosening_needs_no_scan_and_tightening_does() {
        let loose = Validator::Object(BTreeMap::from([(
            "n".to_string(),
            Validator::Optional(Box::new(Validator::Int64)),
        )]));
        let strict = Validator::Object(BTreeMap::from([("n".to_string(), Validator::Int64)]));
        assert!(loosens(None, Some(&strict)));
        assert!(loosens(Some(&loose), Some(&strict)));
        assert!(!loosens(Some(&strict), Some(&loose)));
        assert!(!loosens(Some(&strict), None));
        assert!(loosens(Some(&Validator::Any), None));
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
            Schema {
                tables: vec![TableSchema {
                    validator: Some(Validator::String),
                    ..table("notes", &[])
                }],
            },
            Schema {
                tables: vec![TableSchema {
                    validator: Some(Validator::Object(BTreeMap::from([(
                        "_id".to_string(),
                        Validator::String,
                    )]))),
                    ..table("notes", &[])
                }],
            },
        ] {
            let err = bad.check(&limits).expect_err("refused");
            assert!(matches!(err, LiveError::InvalidArgument(_)), "{err}");
        }
    }
}
