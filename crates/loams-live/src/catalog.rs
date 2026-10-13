//! Tables and their indexes (design §20 §4.1–§4.2): [`TableDef`],
//! [`IndexDef`], their catalog records and the catalog operations.
//!
//! A table is created on its first insert unless a schema is deployed
//! ([`table_for_insert`]); [`define_table`] creates a table or changes its
//! indexes, and refuses an index change on a non-empty table (R1 plan
//! Ruling 5; a deploy changes the indexes of a non-empty table online, LV1
//! plan Task 9, through [`IndexState`]). Table ids come from the catalog
//! counter, from 1; index ids from the table's `next_index_id`, from 2, and
//! are never reused. A table's
//! [`TableValidation`] (LV1 plan Task 8) rides in its record, so every write
//! that loads the table has its validators.
//!
//! Every catalog change also appends a `catalog` journal entry in its own
//! transaction (LV1 plan Task 11, [`record_changes`]): a table created, a
//! table's indexes or validators changed ([`put_table`] compares the record
//! with the stored one), a deployment activated. Every node's subscription
//! manager sees it at the tick that contains it.

use buffa::{Message, MessageField};
use loams_kv::Txn;
use ulid::Ulid;

use crate::cursor::CursorKey;
use crate::docs::Reads;
use crate::ids::{IndexId, TableId};
use crate::journal::{Journal, MAX_SHARDS};
use crate::keys::AppKeys;
use crate::validate::Validator;
use crate::{Limits, LiveError, pb};

/// The longest table or index name, in bytes.
pub const MAX_NAME_BYTES: usize = 64;

/// The name of the built-in index on the document key.
pub const BY_ID: &str = "by_id";
/// The name of the built-in index on `_creationTime`, then `_id`.
pub const BY_CREATION_TIME: &str = "by_creation_time";
/// The system field of a document's id.
pub const ID_FIELD: &str = "_id";
/// The system field of a document's creation time.
pub const CREATION_TIME_FIELD: &str = "_creationTime";

/// A table and its user indexes. `by_id` and `by_creation_time` are implicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    pub id: TableId,
    pub name: String,
    pub indexes: Vec<IndexDef>,
    /// The id the next user index gets.
    pub next_index_id: u32,
    /// The validators its writes must match (LV1 plan Task 8).
    pub validation: TableValidation,
}

/// A table's validators (design §45 §4.1; LV1 plan Task 8): the one every
/// write must match, as the active deployment's schema declares it, and a
/// stricter one a deploy is checking the table's documents against, which
/// writes must match too until that deploy activates or fails.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableValidation {
    pub validator: Option<Validator>,
    pub enforcing: Option<Enforcing>,
}

/// A validator enforced ahead of its deploy, which is checking it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enforcing {
    pub validator: Validator,
    pub deployment: Ulid,
}

impl TableValidation {
    /// The validators a write must match, with the deployment checking the
    /// second one.
    pub fn validators(&self) -> impl Iterator<Item = (&Validator, Option<Ulid>)> {
        self.validator.iter().map(|v| (v, None)).chain(
            self.enforcing
                .iter()
                .map(|e| (&e.validator, Some(e.deployment))),
        )
    }
}

/// A user index: up to 16 top-level fields, then `_creationTime` and `_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    pub id: IndexId,
    pub name: String,
    pub fields: Vec<String>,
    pub state: IndexState,
}

/// Where a user index is in its life (LV1 plan Task 9, design §45 §4.2;
/// row T9-2). A table may hold a ready index and a backfilling one of the
/// same name (a deploy that changes the index's fields); a name resolves
/// to the ready one ([`TableDef::index_named`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    /// Every write maintains it, and `deployment`'s backfill job writes the
    /// entries of the documents already there; a query on it is
    /// [`LiveError::IndexNotReady`]. The swap of `deployment` makes it
    /// ready.
    Backfilling { deployment: Ulid },
    /// Queries read it; every write maintains it.
    Ready,
    /// No write maintains it and no query finds it; its entries are being
    /// deleted, then the definition goes
    /// ([`drop_indexes`](crate::backfill::drop_indexes)).
    Dropping,
}

impl IndexState {
    /// The name in messages and statuses.
    pub fn name(self) -> &'static str {
        match self {
            IndexState::Backfilling { .. } => "backfilling",
            IndexState::Ready => "ready",
            IndexState::Dropping => "dropping",
        }
    }
}

impl IndexDef {
    /// Whether writes maintain its entries (it is not being dropped).
    pub fn maintained(&self) -> bool {
        self.state != IndexState::Dropping
    }
}

/// A user index as a schema declares it: a name and its fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub name: String,
    pub fields: Vec<String>,
}

impl TableDef {
    /// The fields of the index of this id: `None` for `by_id` (it has no
    /// entries), for an unknown id and for an index being dropped; the
    /// built-in `by_creation_time` has no fields. A query reads the index
    /// only once it is ready ([`TableDef::readable`]).
    pub fn index_fields(&self, id: IndexId) -> Option<&[String]> {
        if id == IndexId::BY_CREATION_TIME {
            return Some(&[]);
        }
        self.indexes
            .iter()
            .find(|i| i.id == id && i.maintained())
            .map(|i| i.fields.as_slice())
    }

    /// Refuses a read of index `id` unless it is ready (or built in): a
    /// backfilling index is [`LiveError::IndexNotReady`], one being dropped
    /// or unknown [`LiveError::NotFound`].
    pub fn readable(&self, id: IndexId) -> Result<(), LiveError> {
        if id == IndexId::BY_ID || id == IndexId::BY_CREATION_TIME {
            return Ok(());
        }
        match self.indexes.iter().find(|i| i.id == id) {
            Some(i) if i.state == IndexState::Ready => Ok(()),
            Some(i) if i.maintained() => Err(LiveError::IndexNotReady(format!(
                "index '{}' of table '{}' is still backfilling; query it once the deploy that \
                 adds it is active",
                i.name, self.name
            ))),
            _ => Err(LiveError::NotFound(format!(
                "index {} of table '{}'",
                id.0, self.name
            ))),
        }
    }

    /// The user index a query naming `name` reads: the ready one of that
    /// name, else a backfilling one (whose read is then refused); never one
    /// being dropped.
    pub fn index_named(&self, name: &str) -> Option<&IndexDef> {
        let named = || self.indexes.iter().filter(|i| i.name == name);
        named()
            .find(|i| i.state == IndexState::Ready)
            .or_else(|| named().find(|i| i.maintained()))
    }

    /// The ready user indexes, in definition order (what the active
    /// deployment's schema declares).
    pub fn ready_indexes(&self) -> impl Iterator<Item = &IndexDef> {
        self.indexes.iter().filter(|i| i.state == IndexState::Ready)
    }

    /// The id of the index named `name`, built-ins included
    /// ([`index_named`](Self::index_named)).
    pub fn index_id(&self, name: &str) -> Option<IndexId> {
        match name {
            BY_ID => Some(IndexId::BY_ID),
            BY_CREATION_TIME => Some(IndexId::BY_CREATION_TIME),
            _ => self.index_named(name).map(|i| i.id),
        }
    }

    /// The catalog record.
    pub fn to_proto(&self) -> pb::TableDef {
        pb::TableDef {
            format: 1,
            id: self.id.0,
            name: self.name.clone(),
            indexes: self
                .indexes
                .iter()
                .map(|i| {
                    let (state, deployment) = match i.state {
                        IndexState::Backfilling { deployment } => (
                            pb::IndexState::INDEX_STATE_BACKFILLING,
                            deployment.to_bytes().to_vec(),
                        ),
                        IndexState::Ready => (pb::IndexState::INDEX_STATE_READY, Vec::new()),
                        IndexState::Dropping => (pb::IndexState::INDEX_STATE_DROPPING, Vec::new()),
                    };
                    pb::IndexDef {
                        id: i.id.0,
                        name: i.name.clone(),
                        fields: i.fields.clone(),
                        state: state.into(),
                        deployment,
                        ..Default::default()
                    }
                })
                .collect(),
            next_index_id: self.next_index_id,
            validator: self
                .validation
                .validator
                .as_ref()
                .map(|v| MessageField::some(v.to_proto()))
                .unwrap_or_default(),
            enforcing: self
                .validation
                .enforcing
                .as_ref()
                .map(|e| {
                    MessageField::some(pb::EnforcingValidator {
                        validator: MessageField::some(e.validator.to_proto()),
                        deployment: e.deployment.to_bytes().to_vec(),
                        ..Default::default()
                    })
                })
                .unwrap_or_default(),
            ..Default::default()
        }
    }

    /// The table of a catalog record.
    pub fn from_proto(p: pb::TableDef) -> Result<Self, LiveError> {
        if p.format != 1 {
            return Err(LiveError::Corrupt(format!(
                "table record format {} (expected 1)",
                p.format
            )));
        }
        Ok(TableDef {
            id: TableId(p.id),
            name: p.name,
            indexes: p
                .indexes
                .into_iter()
                .map(|i| {
                    let state = match i.state.as_known() {
                        Some(
                            pb::IndexState::INDEX_STATE_UNSPECIFIED
                            | pb::IndexState::INDEX_STATE_READY,
                        ) => IndexState::Ready,
                        Some(pb::IndexState::INDEX_STATE_DROPPING) => IndexState::Dropping,
                        Some(pb::IndexState::INDEX_STATE_BACKFILLING) => {
                            let deployment: [u8; 16] =
                                i.deployment.as_slice().try_into().map_err(|_| {
                                    LiveError::Corrupt(format!(
                                        "table record: index '{}' backfills for a {}-byte \
                                         deployment",
                                        i.name,
                                        i.deployment.len()
                                    ))
                                })?;
                            IndexState::Backfilling {
                                deployment: Ulid::from_bytes(deployment),
                            }
                        }
                        None => {
                            return Err(LiveError::Corrupt(format!(
                                "table record: index '{}' in an unknown state {:?}",
                                i.name, i.state
                            )));
                        }
                    };
                    Ok(IndexDef {
                        id: IndexId(i.id),
                        name: i.name,
                        fields: i.fields,
                        state,
                    })
                })
                .collect::<Result<_, LiveError>>()?,
            next_index_id: p.next_index_id,
            validation: TableValidation {
                validator: p.validator.as_option().map(stored_validator).transpose()?,
                enforcing: p
                    .enforcing
                    .as_option()
                    .map(|e| {
                        let validator = e.validator.as_option().ok_or_else(|| {
                            LiveError::Corrupt(
                                "table record: an enforcing entry with no validator".into(),
                            )
                        })?;
                        let deployment: [u8; 16] =
                            e.deployment.as_slice().try_into().map_err(|_| {
                                LiveError::Corrupt(format!(
                                    "table record: a {}-byte enforcing deployment",
                                    e.deployment.len()
                                ))
                            })?;
                        Ok::<_, LiveError>(Enforcing {
                            validator: stored_validator(validator)?,
                            deployment: Ulid::from_bytes(deployment),
                        })
                    })
                    .transpose()?,
            },
        })
    }

    fn decode(bytes: &[u8]) -> Result<Self, LiveError> {
        let p = pb::TableDef::decode_from_slice(bytes)
            .map_err(|e| LiveError::Corrupt(format!("table record: {e}")))?;
        TableDef::from_proto(p)
    }
}

/// A validator read from a stored record.
fn stored_validator(p: &pb::Validator) -> Result<Validator, LiveError> {
    Validator::from_proto(p).map_err(|e| LiveError::Corrupt(format!("table record: {e}")))
}

/// Checks a table or index name: 1 to 64 bytes of ASCII letters, digits and
/// `_`, starting with a letter (names starting with `_` are reserved).
pub fn check_name(what: &str, name: &str) -> Result<(), LiveError> {
    let mut chars = name.chars();
    let ok = name.len() <= MAX_NAME_BYTES
        && chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(LiveError::invalid(format!(
            "{what} name '{name}': use 1 to {MAX_NAME_BYTES} ASCII letters, digits and '_', \
             starting with a letter"
        )))
    }
}

/// The table named `name`.
pub async fn load_table(
    r: &mut impl Reads,
    app: &AppKeys,
    name: &str,
) -> Result<Option<TableDef>, LiveError> {
    let Some(id) = r.get(&app.table_name(name)).await? else {
        return Ok(None);
    };
    let id = decode_u32(&id, "table name record")?;
    let table = load_table_by_id(r, app, TableId(id)).await?;
    match table {
        Some(t) if t.name == name => Ok(Some(t)),
        _ => Err(LiveError::Corrupt(format!(
            "table name '{name}' points at table {id}, which does not name it back"
        ))),
    }
}

/// The table with this id.
pub async fn load_table_by_id(
    r: &mut impl Reads,
    app: &AppKeys,
    id: TableId,
) -> Result<Option<TableDef>, LiveError> {
    match r.get(&app.table(id)).await? {
        None => Ok(None),
        Some(bytes) => TableDef::decode(&bytes).map(Some),
    }
}

/// Every table, by id.
pub async fn list_tables(r: &mut impl Reads, app: &AppKeys) -> Result<Vec<TableDef>, LiveError> {
    r.scan(&app.tables(), usize::MAX, false)
        .await?
        .into_iter()
        .map(|(_, v)| TableDef::decode(&v))
        .collect()
}

/// The table an insert into `name` writes to: the existing table, or a new
/// one without user indexes when no schema is deployed (§20 §4.1). With a
/// deployed schema, a missing table is [`LiveError::NotFound`]. A new
/// table's `TABLE_CREATED` change is returned, not recorded: the mutation
/// records it in its own journal entry (LV1 plan Task 11).
pub async fn table_for_insert(
    txn: &mut Txn,
    app: &AppKeys,
    name: &str,
    limits: &Limits,
) -> Result<(TableDef, Option<pb::CatalogChange>), LiveError> {
    check_name("table", name)?;
    if let Some(table) = load_table(txn, app, name).await? {
        return Ok((table, None));
    }
    if txn.get(&app.schema()).await?.is_some() {
        return Err(LiveError::NotFound(format!(
            "table '{name}' is not in the deployed schema"
        )));
    }
    let table = create_table(txn, app, name, &[], limits).await?;
    let created = change(
        pb::CatalogChangeKind::CATALOG_CHANGE_KIND_TABLE_CREATED,
        Some(table.id),
        None,
    );
    Ok((table, Some(created)))
}

/// Creates the table `name` with `indexes`, or changes an existing table's
/// indexes to `indexes` (matched by name; an index whose fields change is
/// replaced by a new id). An index change on a table that holds a document
/// is [`LiveError::FailedPrecondition`] (R1 plan Ruling 5); a table whose
/// indexes already match is returned unchanged.
pub async fn define_table(
    txn: &mut Txn,
    app: &AppKeys,
    name: &str,
    indexes: &[IndexSpec],
    limits: &Limits,
) -> Result<TableDef, LiveError> {
    check_name("table", name)?;
    check_indexes(indexes, limits)?;
    let Some(mut table) = load_table(txn, app, name).await? else {
        let table = create_table(txn, app, name, indexes, limits).await?;
        let created = change(
            pb::CatalogChangeKind::CATALOG_CHANGE_KIND_TABLE_CREATED,
            Some(table.id),
            None,
        );
        record_changes(txn, app, TABLE_CHANGE, vec![created]).await?;
        return Ok(table);
    };
    if table.indexes.iter().any(|i| i.state != IndexState::Ready) {
        return Err(LiveError::FailedPrecondition(format!(
            "table '{name}' has an index change in progress (a deploy's backfill or drop)"
        )));
    }
    let same = table.indexes.len() == indexes.len()
        && table
            .indexes
            .iter()
            .zip(indexes)
            .all(|(have, want)| have.name == want.name && have.fields == want.fields);
    if same {
        return Ok(table);
    }
    let (lo, hi) = {
        let docs = app.documents(table.id);
        (docs.lo, docs.hi)
    };
    let any = txn.scan(&lo, Some(&hi), 1).await?;
    if !any.is_empty() {
        return Err(LiveError::FailedPrecondition(format!(
            "index changes need an empty table in R1: table '{name}' has documents"
        )));
    }
    let mut next = table.next_index_id.max(IndexId::FIRST_USER);
    let mut defs = Vec::with_capacity(indexes.len());
    for spec in indexes {
        let kept = table
            .indexes
            .iter()
            .find(|i| i.name == spec.name && i.fields == spec.fields);
        let id = match kept {
            Some(i) => i.id,
            None => {
                next += 1;
                IndexId(next - 1)
            }
        };
        defs.push(IndexDef {
            id,
            name: spec.name.clone(),
            fields: spec.fields.clone(),
            state: IndexState::Ready,
        });
    }
    table.indexes = defs;
    table.next_index_id = next;
    put_table(txn, app, &table).await?;
    Ok(table)
}

/// Creates the table `name`; its caller records the change.
async fn create_table(
    txn: &mut Txn,
    app: &AppKeys,
    name: &str,
    indexes: &[IndexSpec],
    limits: &Limits,
) -> Result<TableDef, LiveError> {
    check_indexes(indexes, limits)?;
    let counter = app.table_counter();
    let id = match txn.get(&counter).await? {
        None => 1,
        Some(bytes) => decode_u32(&bytes, "table counter")?,
    };
    let next = id
        .checked_add(1)
        .ok_or_else(|| LiveError::limit("tables", "the app has 2^32 − 1 tables"))?;
    let mut index_id = IndexId::FIRST_USER;
    let table = TableDef {
        id: TableId(id),
        name: name.to_string(),
        indexes: indexes
            .iter()
            .map(|spec| {
                index_id += 1;
                IndexDef {
                    id: IndexId(index_id - 1),
                    name: spec.name.clone(),
                    fields: spec.fields.clone(),
                    state: IndexState::Ready,
                }
            })
            .collect(),
        next_index_id: index_id,
        validation: TableValidation::default(),
    };
    txn.put(&counter, next.to_be_bytes().to_vec()).await?;
    txn.put(&app.table_name(name), id.to_be_bytes().to_vec())
        .await?;
    txn.put(&app.table(table.id), table.to_proto().encode_to_vec())
        .await?;
    Ok(table)
}

/// Checks a table's index specs: their count, names (unique, not
/// built-in) and fields.
pub(crate) fn check_indexes(indexes: &[IndexSpec], limits: &Limits) -> Result<(), LiveError> {
    if indexes.len() > limits.max_indexes {
        return Err(LiveError::limit(
            "max_indexes",
            format!(
                "{} indexes, more than {}",
                indexes.len(),
                limits.max_indexes
            ),
        ));
    }
    for (n, spec) in indexes.iter().enumerate() {
        check_name("index", &spec.name)?;
        if spec.name == BY_ID || spec.name == BY_CREATION_TIME {
            return Err(LiveError::invalid(format!(
                "index name '{}' is a built-in index",
                spec.name
            )));
        }
        if indexes[..n].iter().any(|other| other.name == spec.name) {
            return Err(LiveError::invalid(format!(
                "index '{}' is defined twice",
                spec.name
            )));
        }
        if spec.fields.is_empty() {
            return Err(LiveError::invalid(format!(
                "index '{}' has no fields",
                spec.name
            )));
        }
        if spec.fields.len() > limits.max_index_fields {
            return Err(LiveError::limit(
                "max_index_fields",
                format!(
                    "index '{}' has {} fields, more than {}",
                    spec.name,
                    spec.fields.len(),
                    limits.max_index_fields
                ),
            ));
        }
        for field in &spec.fields {
            if field.is_empty() || field.starts_with('_') {
                return Err(LiveError::invalid(format!(
                    "index '{}': field '{field}' cannot be indexed (system fields are appended \
                     to every index)",
                    spec.name
                )));
            }
            // Documents cannot hold a longer name (Limits::check_fields), so
            // such an index would hold only nulls.
            if field.len() > limits.max_field_name_bytes {
                return Err(LiveError::limit(
                    "max_field_name_bytes",
                    format!(
                        "index '{}': a field name has {} bytes, more than {}",
                        spec.name,
                        field.len(),
                        limits.max_field_name_bytes
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// The app's journal shard count, from its settings record; `None` before
/// the app's runner first opened (R1 plan row T10-1).
pub async fn load_journal_shards(
    r: &mut impl Reads,
    app: &AppKeys,
) -> Result<Option<u16>, LiveError> {
    let Some(bytes) = r.get(&app.app_def()).await? else {
        return Ok(None);
    };
    let def = pb::AppDef::decode_from_slice(&bytes)
        .map_err(|e| LiveError::Corrupt(format!("app record: {e}")))?;
    if def.format != 1 {
        return Err(LiveError::Corrupt(format!(
            "app record format {} (expected 1)",
            def.format
        )));
    }
    let shards = u16::try_from(def.journal_shards)
        .ok()
        .filter(|&n| (1..=MAX_SHARDS).contains(&n))
        .ok_or_else(|| {
            LiveError::Corrupt(format!(
                "app record: {} journal shards (1 to {MAX_SHARDS})",
                def.journal_shards
            ))
        })?;
    Ok(Some(shards))
}

/// The app's journal shard count: the stored one, or `shards` written as
/// the app's setting when it has none yet.
pub async fn ensure_journal_shards(
    txn: &mut Txn,
    app: &AppKeys,
    shards: u16,
) -> Result<u16, LiveError> {
    if let Some(stored) = load_journal_shards(txn, app).await? {
        return Ok(stored);
    }
    check_shards(shards)?;
    txn.put(&app.app_def(), app_def(shards)).await?;
    Ok(shards)
}

/// The app's pagination cursor key (LV1 plan Task 4), from its catalog
/// record; `None` before the app's runner first opened.
pub async fn load_cursor_key(
    r: &mut impl Reads,
    app: &AppKeys,
) -> Result<Option<CursorKey>, LiveError> {
    match r.get(&app.cursor_key()).await? {
        Some(bytes) => Ok(Some(CursorKey::from_record(&bytes)?)),
        None => Ok(None),
    }
}

/// The app's pagination cursor key: the stored one, or a new random key
/// written as the app's record when it has none yet. It never changes, so
/// cursors stay valid across nodes and restarts.
pub async fn ensure_cursor_key(txn: &mut Txn, app: &AppKeys) -> Result<CursorKey, LiveError> {
    if let Some(key) = load_cursor_key(txn, app).await? {
        return Ok(key);
    }
    let key = CursorKey::random()?;
    txn.put(&app.cursor_key(), key.as_bytes().to_vec()).await?;
    Ok(key)
}

/// Changes the app's journal shard count to `shards`. Refused with
/// [`LiveError::FailedPrecondition`] unless the journal is empty (no head,
/// entry or checkpoint was ever written; R2 migrates a non-empty one). The
/// transaction also locks every head of the current count, so a mutation
/// that read the old count and commits concurrently conflicts with it.
pub async fn set_journal_shards(
    txn: &mut Txn,
    app: &AppKeys,
    shards: u16,
) -> Result<(), LiveError> {
    check_shards(shards)?;
    let current = load_journal_shards(txn, app).await?;
    if current == Some(shards) {
        return Ok(());
    }
    let journal = app.journal();
    let (lo, hi) = journal.bounds();
    if !txn.scan(lo, hi, 1).await?.is_empty() {
        return Err(LiveError::FailedPrecondition(format!(
            "the journal shard count can change only while the journal is empty in R1: \
             it has {} shards and entries",
            current.unwrap_or(0)
        )));
    }
    if let Some(current) = current {
        txn.lock_keys((0..current).map(|s| app.journal_head(s)))
            .await?;
    }
    txn.put(&app.app_def(), app_def(shards)).await?;
    Ok(())
}

fn check_shards(shards: u16) -> Result<(), LiveError> {
    if shards == 0 || shards > MAX_SHARDS {
        return Err(LiveError::invalid(format!(
            "a journal has 1 to {MAX_SHARDS} shards, not {shards}"
        )));
    }
    Ok(())
}

fn app_def(shards: u16) -> Vec<u8> {
    pb::AppDef {
        format: 1,
        journal_shards: u32::from(shards),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Writes `table`'s record in `txn`, and records what changed from the
/// stored record (LV1 plan Task 11): `TABLE_CREATED` for a new table,
/// `INDEXES` and `VALIDATOR` for an existing one. Rewriting a record
/// unchanged records nothing.
pub(crate) async fn put_table(
    txn: &mut Txn,
    app: &AppKeys,
    table: &TableDef,
) -> Result<(), LiveError> {
    let before = load_table_by_id(txn, app, table.id).await?;
    txn.put(&app.table(table.id), table.to_proto().encode_to_vec())
        .await?;
    record_changes(
        txn,
        app,
        TABLE_CHANGE,
        table_changes(before.as_ref(), table),
    )
    .await
}

/// The catalog changes from `before` (`None`: no table) to `after`.
fn table_changes(before: Option<&TableDef>, after: &TableDef) -> Vec<pb::CatalogChange> {
    let kinds = match before {
        None => vec![pb::CatalogChangeKind::CATALOG_CHANGE_KIND_TABLE_CREATED],
        Some(before) => {
            let mut kinds = Vec::new();
            if before.indexes != after.indexes || before.next_index_id != after.next_index_id {
                kinds.push(pb::CatalogChangeKind::CATALOG_CHANGE_KIND_INDEXES);
            }
            if before.validation != after.validation {
                kinds.push(pb::CatalogChangeKind::CATALOG_CHANGE_KIND_VALIDATOR);
            }
            kinds
        }
    };
    kinds
        .into_iter()
        .map(|kind| change(kind, Some(after.id), None))
        .collect()
}

/// The `function` of a journal entry that records table changes.
pub const TABLE_CHANGE: &str = "live.catalog.table";

/// The journal shard catalog entries go to. One shard, so a transaction
/// that changes several records writes one more journal head, not several.
pub const CATALOG_SHARD: u16 = 0;

/// A catalog change of `kind`, about `table` or `deployment`.
pub fn change(
    kind: pb::CatalogChangeKind,
    table: Option<TableId>,
    deployment: Option<Ulid>,
) -> pb::CatalogChange {
    pb::CatalogChange {
        kind: kind.into(),
        table_id: table.map(|t| t.0),
        deployment: deployment.map(|d| d.to_string()).unwrap_or_default(),
        ..Default::default()
    }
}

/// Appends `changes` to the app's journal in `txn`, as one entry whose
/// function is `function` (LV1 plan Task 11), on [`CATALOG_SHARD`]. It
/// commits with the change, so every tailer sees the change at the tick
/// whose snapshot first contains it. Nothing is recorded before the app's
/// runner first opened (no journal yet, so no tailer either).
pub async fn record_changes(
    txn: &mut Txn,
    app: &AppKeys,
    function: &str,
    changes: Vec<pb::CatalogChange>,
) -> Result<(), LiveError> {
    if changes.is_empty() {
        return Ok(());
    }
    let Some(shards) = load_journal_shards(txn, app).await? else {
        return Ok(());
    };
    let entry = pb::JournalEntry {
        function: function.to_string(),
        catalog: changes,
        ..Default::default()
    };
    Journal::new(app.clone(), shards)?
        .append_to(txn, CATALOG_SHARD, entry)
        .await?;
    Ok(())
}

fn decode_u32(bytes: &[u8], what: &str) -> Result<u32, LiveError> {
    let raw: [u8; 4] = bytes
        .try_into()
        .map_err(|_| LiveError::Corrupt(format!("{what}: {} bytes, expected 4", bytes.len())))?;
    Ok(u32::from_be_bytes(raw))
}
