//! Schema mapping and the evolution policy (CN2 Task 5, D356).
//!
//! Arrow is the canonical schema. Every family supplies a [`TypeMapper`] from
//! its native types (PG OIDs, MySQL column types, BSON, CQL, ClickHouse, ES
//! mappings, Delta, …) to Arrow fields and back; [`arrow::SqlTypeMapper`] is
//! the shared mapper for SQL-shaped type names that families extend.
//!
//! A source tracks each table's schema with a [`SchemaTracker`] under the
//! instance's [`Evolution`]: an accepted change is emitted as a
//! `schema-change` event ([`SchemaChange::event`]); anything else is a
//! [`SchemaError::Incompatible`], which converts to a
//! [`ErrorClass::SchemaIncompatible`] error and pauses the instance with the
//! before and after schemas in `last_error`.
//!
//! A mapping that loses information (unsigned 64-bit to signed, a timestamp
//! without a timezone, a decimal over 38 digits) is refused with
//! [`SchemaError::Lossy`] unless the instance lists the column's path in
//! `lossy` ([`Lossy`]).

use std::collections::BTreeSet;
use std::fmt::Write as _;

pub use arrow_schema::SchemaRef;
use arrow_schema::{DataType, Field, Schema};

use crate::envelope::{CloudEvent, EnvelopeBuilder, ids};
use crate::error::{ConnectorError, ErrorClass};
use crate::position::Position;

pub mod arrow;
pub mod evolution;
pub mod registry;

pub use evolution::{FieldChange, SchemaTracker, compare};

/// Field metadata: the maximum length of a `varchar(n)`/`char(n)` column,
/// which Arrow's `Utf8` does not carry itself. Evolution reads it to tell a
/// widening (`varchar(10)` → `varchar(40)`) from a narrowing.
pub const MAX_LENGTH_KEY: &str = "loams.max_length";
/// Field metadata: the column's full path (`<schema>.<table>.<column>`), the
/// key `lossy` lists. Set by mappers; `from_arrow` falls back to the name.
pub const PATH_KEY: &str = "loams.path";
/// Field metadata: present when a mapping was accepted as lossy, with what it
/// loses.
pub const LOSSY_KEY: &str = "loams.lossy";

/// A family's native column type, as its mapper reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeType {
    /// The column's path, `<schema>.<table>.<column>` or as deep as the
    /// family's names go; its last segment is the Arrow field's name.
    pub path: String,
    /// The type as the system names it (`bigint unsigned`, `numeric(12,2)`,
    /// `timestamp with time zone`, an OID's name, a BSON type).
    pub type_name: String,
    pub nullable: bool,
}

impl NativeType {
    pub fn new(path: impl Into<String>, type_name: impl Into<String>, nullable: bool) -> Self {
        Self {
            path: path.into(),
            type_name: type_name.into(),
            nullable,
        }
    }

    /// The column's own name: the path's last `.`-separated segment.
    pub fn column(&self) -> &str {
        self.path.rsplit('.').next().unwrap_or(&self.path)
    }
}

/// Why a schema could not be mapped or evolved.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("{path}: the type {native} has no Arrow mapping")]
    Unsupported { path: String, native: String },
    #[error(
        "{path}: mapping {from} to {to} is lossy ({reason}); list `{path}` in the instance's `lossy` to accept it"
    )]
    Lossy {
        path: String,
        from: String,
        to: String,
        reason: String,
    },
    #[error("{table}: incompatible schema change: {reason}; before: {before}; after: {after}")]
    Incompatible {
        table: String,
        reason: String,
        before: String,
        after: String,
    },
    #[error("schema: {0}")]
    Invalid(String),
}

/// Schema drift, a refused lossy mapping and an unmappable type pause the
/// instance (`SchemaIncompatible`); an invalid configuration is fatal.
impl From<SchemaError> for ConnectorError {
    fn from(e: SchemaError) -> Self {
        let class = match e {
            SchemaError::Invalid(_) => ErrorClass::Fatal,
            _ => ErrorClass::SchemaIncompatible,
        };
        ConnectorError::new(class, e.to_string())
    }
}

/// One per family (CN2 Shared contracts). The method names are the
/// contract's; `from_arrow` takes `&self` because a mapper carries the
/// instance's `lossy` paths and the target's dialect.
#[allow(clippy::wrong_self_convention)]
pub trait TypeMapper: Send + Sync {
    fn to_arrow(&self, native: &NativeType) -> Result<Field, SchemaError>;
    fn from_arrow(&self, field: &Field) -> Result<NativeType, SchemaError>;
}

/// How far a source lets a table's schema evolve (CN2 Task 5).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Evolution {
    /// New nullable columns (also inside structs) and new tables.
    #[default]
    Additive,
    /// `Additive`, plus lossless widenings: int32 → int64, varchar(n) →
    /// varchar(m > n), decimal precision up, NOT NULL → nullable.
    Widen,
    /// No change at all: any change pauses the instance.
    Pause,
}

impl Evolution {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "additive" => Some(Evolution::Additive),
            "widen" => Some(Evolution::Widen),
            "pause" => Some(Evolution::Pause),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Evolution::Additive => "additive",
            Evolution::Widen => "widen",
            Evolution::Pause => "pause",
        }
    }
}

/// The column paths an instance accepts a lossy mapping for (`lossy:
/// [<path>]`). Exact paths only: accepting a loss is a decision per column.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Lossy {
    paths: BTreeSet<String>,
}

impl Lossy {
    /// No lossy mapping is accepted (the default).
    pub fn none() -> Self {
        Self::default()
    }

    pub fn paths<I, S>(paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            paths: paths.into_iter().map(Into::into).collect(),
        }
    }

    pub fn allows(&self, path: &str) -> bool {
        self.paths.contains(path)
    }

    /// `Ok(field)` marked with what it loses when `path` is listed, else
    /// [`SchemaError::Lossy`].
    pub fn accept(
        &self,
        path: &str,
        from: &str,
        field: Field,
        reason: &str,
    ) -> Result<Field, SchemaError> {
        if self.allows(path) {
            let mut md = field.metadata().clone();
            md.insert(LOSSY_KEY.to_owned(), reason.to_owned());
            Ok(field.with_metadata(md))
        } else {
            Err(SchemaError::Lossy {
                path: path.to_owned(),
                from: from.to_owned(),
                to: render_type(field.data_type()),
                reason: reason.to_owned(),
            })
        }
    }
}

/// An instance's schema settings, read from its config: `evolution`
/// (`additive` by default, `widen` or `pause`) and `lossy` (column paths).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaPolicy {
    pub evolution: Evolution,
    pub lossy: Lossy,
}

impl SchemaPolicy {
    pub fn from_config(config: &serde_json::Value) -> Result<Self, ConnectorError> {
        let evolution = match config.get("evolution") {
            None | Some(serde_json::Value::Null) => Evolution::default(),
            Some(serde_json::Value::String(s)) => Evolution::parse(s).ok_or_else(|| {
                ConnectorError::fatal(format!(
                    "config: evolution must be additive, widen or pause, not {s:?}"
                ))
            })?,
            Some(_) => return Err(ConnectorError::fatal("config: evolution must be a string")),
        };
        let lossy = match config.get("lossy") {
            None | Some(serde_json::Value::Null) => Lossy::none(),
            Some(serde_json::Value::Array(items)) => {
                let mut paths = Vec::with_capacity(items.len());
                for item in items {
                    let serde_json::Value::String(p) = item else {
                        return Err(ConnectorError::fatal(
                            "config: lossy must be a list of column paths",
                        ));
                    };
                    paths.push(p.clone());
                }
                Lossy::paths(paths)
            }
            Some(_) => {
                return Err(ConnectorError::fatal(
                    "config: lossy must be a list of column paths",
                ));
            }
        };
        Ok(Self { evolution, lossy })
    }
}

/// A change to a table's schema, emitted as
/// `io.loams.dev.flow.<connector>.schema-change.v1` (§33 §7).
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaChange {
    pub table: String,
    /// The empty schema for a new table.
    pub before: SchemaRef,
    pub after: SchemaRef,
    /// The source position the new schema applies from.
    pub at: Position,
}

impl SchemaChange {
    /// The topic routes deliver schema changes to: `<db>.schema_changes`
    /// (§33 §7). Routing is the runtime's; the SDK only names it.
    pub fn topic(db: &str) -> String {
        format!("{db}.schema_changes")
    }

    /// The `schema-change` event: `subject` is the table, the id derives
    /// from the position (a re-read emits the same id), and `data` is JSON
    /// `{table, at, before: {fields}, after: {fields}}`.
    pub fn event(&self, envelope: &EnvelopeBuilder) -> CloudEvent {
        let data = serde_json::json!({
            "table": self.table,
            "at": self.at.display,
            "before": schema_json(&self.before),
            "after": schema_json(&self.after),
        });
        envelope
            .event(
                "schema-change",
                &self.table,
                ids::change(&self.at.display, &self.table, "schema-change"),
            )
            .with_subject(self.table.clone())
            .with_data("application/json", data.to_string())
    }
}

/// A schema as JSON: `{"fields": [{name, type, nullable, metadata?, fields?}]}`.
/// `type` is Arrow's own rendering of the type (`Int64`, `Utf8`, …).
pub fn schema_json(schema: &Schema) -> serde_json::Value {
    serde_json::json!({
        "fields": schema.fields().iter().map(|f| field_json(f)).collect::<Vec<_>>()
    })
}

fn field_json(f: &Field) -> serde_json::Value {
    let mut o = serde_json::Map::new();
    o.insert("name".into(), f.name().clone().into());
    o.insert("type".into(), render_type(f.data_type()).into());
    o.insert("nullable".into(), f.is_nullable().into());
    if !f.metadata().is_empty() {
        let md: serde_json::Map<String, serde_json::Value> = f
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().into()))
            .collect();
        o.insert("metadata".into(), md.into());
    }
    if let DataType::Struct(children) = f.data_type() {
        o.insert(
            "fields".into(),
            children.iter().map(|c| field_json(c)).collect(),
        );
    }
    serde_json::Value::Object(o)
}

/// A type for messages: Arrow's rendering.
pub fn render_type(t: &DataType) -> String {
    t.to_string()
}

/// A schema for messages and `last_error`: `{id: Int32, name: Utf8?}`, with
/// `?` marking a nullable field and `(n)` a maximum length.
pub fn render_schema(schema: &Schema) -> String {
    let mut s = String::from("{");
    for (i, f) in schema.fields().iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        let _ = write!(s, "{}: {}", f.name(), render_type(f.data_type()));
        if let Some(n) = f.metadata().get(MAX_LENGTH_KEY) {
            let _ = write!(s, "({n})");
        }
        if f.is_nullable() {
            s.push('?');
        }
    }
    s.push('}');
    s
}
