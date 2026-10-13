//! Validators (LV1 plan Tasks 4 and 8; design §45 §4.1): the vocabulary
//! function arguments and table schemas share, and [`check`].
//!
//! A [`Validator`] is `v.null | int64 | float64 | boolean | string | bytes |
//! array | object | literal | union | optional | any | id(table)`, as
//! `loams:server`'s `v` builds it. Values match strictly: an `int64` is a
//! [`LiveValue::I64`] (a JavaScript `bigint`), a `float64` a
//! [`LiveValue::F64`] (a number); an object has exactly its fields, except
//! that an `optional` field may be missing; an `id(table)` is a document id
//! in its text form (checksum included) of that table.
//!
//! A failed check returns every [`FieldViolation`] (up to
//! [`MAX_VIOLATIONS`]), each naming the field path. Messages name types and
//! the validator's own literals, never the checked value (no user data in
//! errors).
//!
//! A table's validator (Task 8) is checked against a document's own fields
//! by [`check_fields`]; it is stored as the `loams.live.v1.Validator`
//! message ([`Validator::from_proto`], [`Validator::to_proto`]) and its
//! shape is checked when a schema is deployed ([`Validator::check_table`]).
//! [`accepts_all`] tells a looser validator, which applies at once, from a
//! stricter one, whose deploy first checks the table's documents.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::pb::__buffa::oneof::validator::Kind;
use crate::{DocId, LiveError, LiveValue, TableId, catalog, pb};

/// The most violations one check reports.
pub const MAX_VIOLATIONS: usize = 16;

/// The deepest validator a stored form or a schema may hold.
pub const MAX_VALIDATOR_DEPTH: usize = 64;

/// The most nodes (validators at any depth) a table's validator may hold:
/// it is read with the table on every write.
pub const MAX_VALIDATOR_NODES: usize = 4096;

/// A validator of [`LiveValue`]s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Validator {
    Null,
    Int64,
    Float64,
    Boolean,
    String,
    Bytes,
    /// An array whose elements all match.
    Array(Box<Validator>),
    /// An object with exactly these fields; a field whose validator is
    /// [`Validator::Optional`] may be missing.
    Object(BTreeMap<String, Validator>),
    /// Exactly this scalar (null, boolean, int64, float64 or string).
    Literal(LiveValue),
    /// Any of the members.
    Union(Vec<Validator>),
    /// An object field that may be missing; anywhere else it is its inner
    /// validator (there is no "missing" outside an object).
    Optional(Box<Validator>),
    Any,
    /// A document id of the named table.
    Id(String),
}

impl Validator {
    /// The validator's name in messages (`v.<name>()`).
    pub fn name(&self) -> &'static str {
        match self {
            Validator::Null => "null",
            Validator::Int64 => "int64",
            Validator::Float64 => "float64",
            Validator::Boolean => "boolean",
            Validator::String => "string",
            Validator::Bytes => "bytes",
            Validator::Array(_) => "array",
            Validator::Object(_) => "object",
            Validator::Literal(_) => "literal",
            Validator::Union(_) => "union",
            Validator::Optional(_) => "optional",
            Validator::Any => "any",
            Validator::Id(_) => "id",
        }
    }

    /// The tables its `id(table)` validators name, so a caller can resolve
    /// them for [`check_ids`].
    pub fn id_tables(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        self.collect_tables(&mut out);
        out
    }

    fn collect_tables(&self, out: &mut BTreeSet<String>) {
        match self {
            Validator::Id(table) => {
                out.insert(table.clone());
            }
            Validator::Array(v) | Validator::Optional(v) => v.collect_tables(out),
            Validator::Object(fields) => fields.values().for_each(|v| v.collect_tables(out)),
            Validator::Union(members) => members.iter().for_each(|v| v.collect_tables(out)),
            _ => {}
        }
    }
}

impl Validator {
    /// The validator of its wire form: its structure only (every kind set,
    /// a literal a valid value), at most [`MAX_VALIDATOR_DEPTH`] deep;
    /// [`check_table`](Self::check_table) checks a table validator's shape.
    pub fn from_proto(p: &pb::Validator) -> Result<Self, LiveError> {
        Self::from_proto_at(p, 0)
    }

    fn from_proto_at(p: &pb::Validator, depth: usize) -> Result<Self, LiveError> {
        if depth > MAX_VALIDATOR_DEPTH {
            return Err(LiveError::invalid(format!(
                "a validator nested deeper than {MAX_VALIDATOR_DEPTH}"
            )));
        }
        let inner = |v: &pb::Validator| Self::from_proto_at(v, depth + 1).map(Box::new);
        Ok(match &p.kind {
            None => return Err(LiveError::invalid("a Validator has no kind set")),
            Some(Kind::Primitive(kind)) => match kind.as_known() {
                Some(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_NULL) => Validator::Null,
                Some(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_INT64) => Validator::Int64,
                Some(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_FLOAT64) => Validator::Float64,
                Some(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_BOOLEAN) => Validator::Boolean,
                Some(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_STRING) => Validator::String,
                Some(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_BYTES) => Validator::Bytes,
                Some(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_ANY) => Validator::Any,
                _ => {
                    return Err(LiveError::invalid(format!(
                        "a Validator with an unknown primitive {kind:?}"
                    )));
                }
            },
            Some(Kind::Array(element)) => Validator::Array(inner(element)?),
            Some(Kind::Optional(element)) => Validator::Optional(inner(element)?),
            Some(Kind::Object(o)) => Validator::Object(
                o.fields
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), Self::from_proto_at(v, depth + 1)?)))
                    .collect::<Result<_, LiveError>>()?,
            ),
            Some(Kind::Union(u)) => Validator::Union(
                u.members
                    .iter()
                    .map(|v| Self::from_proto_at(v, depth + 1))
                    .collect::<Result<_, _>>()?,
            ),
            Some(Kind::Literal(value)) => {
                Validator::Literal(LiveValue::from_proto((**value).clone())?)
            }
            Some(Kind::Id(table)) => Validator::Id(table.clone()),
        })
    }

    /// The wire form, which deploys send and the catalog stores.
    pub fn to_proto(&self) -> pb::Validator {
        let primitive = |p: pb::ValidatorPrimitive| Kind::Primitive(p.into());
        let kind = match self {
            Validator::Null => primitive(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_NULL),
            Validator::Int64 => primitive(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_INT64),
            Validator::Float64 => primitive(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_FLOAT64),
            Validator::Boolean => primitive(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_BOOLEAN),
            Validator::String => primitive(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_STRING),
            Validator::Bytes => primitive(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_BYTES),
            Validator::Any => primitive(pb::ValidatorPrimitive::VALIDATOR_PRIMITIVE_ANY),
            Validator::Array(element) => Kind::Array(Box::new(element.to_proto())),
            Validator::Optional(element) => Kind::Optional(Box::new(element.to_proto())),
            Validator::Object(fields) => Kind::Object(Box::new(pb::ValidatorObject {
                fields: fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_proto()))
                    .collect(),
                ..Default::default()
            })),
            Validator::Union(members) => Kind::Union(Box::new(pb::ValidatorUnion {
                members: members.iter().map(Validator::to_proto).collect(),
                ..Default::default()
            })),
            Validator::Literal(value) => Kind::Literal(Box::new(value.to_proto())),
            Validator::Id(table) => Kind::Id(table.clone()),
        };
        pb::Validator {
            kind: Some(kind),
            ..Default::default()
        }
    }

    /// Checks the shape of a table's validator (design §45 §4.1): a
    /// `v.object(…)` or a union of them, over the document's own fields (a
    /// top-level field name is not empty and does not start with `_`);
    /// `optional` only as an object field; unions not empty; literals
    /// scalars (a double finite); `id` naming a valid table name; at most
    /// [`MAX_VALIDATOR_DEPTH`] deep and [`MAX_VALIDATOR_NODES`] nodes. The
    /// error says what is wrong, for the schema's own message.
    pub fn check_table(&self) -> Result<(), String> {
        let tops: Vec<&BTreeMap<String, Validator>> = match self {
            Validator::Object(fields) => vec![fields],
            Validator::Union(members) if !members.is_empty() => members
                .iter()
                .map(|m| match m {
                    Validator::Object(fields) => Ok(fields),
                    other => Err(format!(
                        "a union of a table validator has only v.object() members, not v.{}()",
                        other.name()
                    )),
                })
                .collect::<Result<_, _>>()?,
            other => {
                return Err(format!(
                    "a table validator is a v.object() or a v.union() of them, not v.{}()",
                    other.name()
                ));
            }
        };
        for name in tops.iter().flat_map(|fields| fields.keys()) {
            if name.is_empty() || name.starts_with('_') {
                return Err(format!(
                    "field '{name}': a table validator declares the document's own fields \
                     (not empty, not starting with '_')"
                ));
            }
        }
        let mut nodes = 0;
        self.check_node(false, 0, &mut nodes)
    }

    fn check_node(&self, field: bool, depth: usize, nodes: &mut usize) -> Result<(), String> {
        *nodes += 1;
        if *nodes > MAX_VALIDATOR_NODES {
            return Err(format!(
                "a validator of more than {MAX_VALIDATOR_NODES} nodes"
            ));
        }
        if depth > MAX_VALIDATOR_DEPTH {
            return Err(format!(
                "a validator nested deeper than {MAX_VALIDATOR_DEPTH}"
            ));
        }
        match self {
            Validator::Array(element) => element.check_node(false, depth + 1, nodes),
            Validator::Optional(_) if !field => Err(
                "v.optional() marks an object field that may be missing; use it only as a \
                 field of v.object()"
                    .into(),
            ),
            Validator::Optional(inner) => inner.check_node(false, depth + 1, nodes),
            Validator::Object(fields) => fields
                .values()
                .try_for_each(|v| v.check_node(true, depth + 1, nodes)),
            Validator::Union(members) if members.is_empty() => {
                Err("v.union() needs at least one member".into())
            }
            Validator::Union(members) => members
                .iter()
                .try_for_each(|v| v.check_node(false, depth + 1, nodes)),
            Validator::Literal(LiveValue::F64(f)) if !f.is_finite() => {
                Err("v.literal() takes a finite number".into())
            }
            Validator::Literal(
                LiveValue::Null
                | LiveValue::Bool(_)
                | LiveValue::I64(_)
                | LiveValue::F64(_)
                | LiveValue::Str(_),
            ) => Ok(()),
            Validator::Literal(other) => Err(format!(
                "v.literal() takes a scalar, not {}",
                other.type_name()
            )),
            Validator::Id(table) => catalog::check_name("table", table)
                .map_err(|_| format!("v.id({table:?}) does not name a valid table")),
            Validator::Null
            | Validator::Int64
            | Validator::Float64
            | Validator::Boolean
            | Validator::String
            | Validator::Bytes
            | Validator::Any => Ok(()),
        }
    }
}

/// Whether `new` accepts every value `old` accepts, so replacing `old` with
/// `new` needs no check of existing data (design §45 §4.1: loosening
/// applies at once). Conservative: `false` when it cannot tell, which only
/// costs a scan. An `id(t)` is accepted by `string` and `id(t)` only, and a
/// literal by what [`check`] accepts it with (never by an `id`, whose table
/// [`check`] does not see).
pub fn accepts_all(new: &Validator, old: &Validator) -> bool {
    use Validator as V;
    match (new, old) {
        (V::Any, _) => true,
        (_, V::Any) => false,
        (V::Optional(new), old) => accepts_all(new, old),
        (new, V::Optional(old)) => accepts_all(new, old),
        (new, V::Union(olds)) => olds.iter().all(|old| accepts_all(new, old)),
        (V::Union(news), old) => news.iter().any(|new| accepts_all(new, old)),
        (V::Id(_), V::Literal(_)) => false,
        (new, V::Literal(literal)) => check(new, literal).is_ok(),
        (V::Null, V::Null)
        | (V::Int64, V::Int64)
        | (V::Float64, V::Float64)
        | (V::Boolean, V::Boolean)
        | (V::String, V::String | V::Id(_))
        | (V::Bytes, V::Bytes) => true,
        (V::Id(new), V::Id(old)) => new == old,
        (V::Array(new), V::Array(old)) => accepts_all(new, old),
        (V::Object(new), V::Object(old)) => {
            // A field the old validator allows but the new one does not
            // know would be refused as unknown.
            old.keys().all(|name| new.contains_key(name))
                && new.iter().all(|(name, n)| {
                    let (n, n_optional) = unwrap_optional(n);
                    match old.get(name) {
                        // Old documents never have the field.
                        None => n_optional,
                        Some(o) => {
                            let (o, o_optional) = unwrap_optional(o);
                            (n_optional || !o_optional) && accepts_all(n, o)
                        }
                    }
                })
        }
        _ => false,
    }
}

fn unwrap_optional(v: &Validator) -> (&Validator, bool) {
    match v {
        Validator::Optional(inner) => (inner, true),
        v => (v, false),
    }
}

/// One step of a field path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathElem {
    Field(String),
    Index(usize),
}

/// A value that does not match its validator at `path`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldViolation {
    pub path: Vec<PathElem>,
    pub message: String,
}

impl fmt::Display for FieldViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", render_path(&self.path), self.message)
    }
}

/// A path as JavaScript would write it from the value `$`: `$.a[3]["b c"]`.
pub fn render_path(path: &[PathElem]) -> String {
    let mut out = String::from("$");
    for elem in path {
        match elem {
            PathElem::Index(i) => out.push_str(&format!("[{i}]")),
            PathElem::Field(name) if is_identifier(name) => {
                out.push('.');
                out.push_str(name);
            }
            PathElem::Field(name) => out.push_str(&format!("[{name:?}]")),
        }
    }
    out
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// The violations as one message: the first few, then how many more.
pub fn describe(violations: &[FieldViolation]) -> String {
    const SHOWN: usize = 4;
    let mut parts: Vec<String> = violations
        .iter()
        .take(SHOWN)
        .map(|v| v.to_string())
        .collect();
    if violations.len() > SHOWN {
        parts.push(format!("and {} more", violations.len() - SHOWN));
    }
    parts.join("; ")
}

/// Checks `value` against `validator`. An `id(table)` is checked as a
/// well-formed document id only; [`check_ids`] also checks its table.
pub fn check(validator: &Validator, value: &LiveValue) -> Result<(), Vec<FieldViolation>> {
    Checker::new(None, MAX_VIOLATIONS).finish(validator, value)
}

/// Like [`check`], with each `id(table)` also checked against `tables`, the
/// ids of the tables [`Validator::id_tables`] names. A table missing from
/// `tables` does not exist, so no id is of it.
pub fn check_ids(
    validator: &Validator,
    value: &LiveValue,
    tables: &BTreeMap<String, TableId>,
) -> Result<(), Vec<FieldViolation>> {
    Checker::new(Some(tables), MAX_VIOLATIONS).finish(validator, value)
}

/// Checks a document's own fields against its table's validator (a
/// `v.object(…)` or a union of them, [`Validator::check_table`]), each
/// `id(table)` against `tables` as [`check_ids`] does. Paths start at the
/// document: `$.field`.
pub fn check_fields(
    validator: &Validator,
    fields: &BTreeMap<String, LiveValue>,
    tables: &BTreeMap<String, TableId>,
) -> Result<(), Vec<FieldViolation>> {
    let mut checker = Checker::new(Some(tables), MAX_VIOLATIONS);
    checker.document(validator, fields);
    if checker.out.is_empty() {
        Ok(())
    } else {
        Err(checker.out)
    }
}

struct Checker<'t> {
    tables: Option<&'t BTreeMap<String, TableId>>,
    /// The most violations kept; checking stops once it is reached.
    limit: usize,
    path: Vec<PathElem>,
    out: Vec<FieldViolation>,
}

impl<'t> Checker<'t> {
    fn new(tables: Option<&'t BTreeMap<String, TableId>>, limit: usize) -> Self {
        Checker {
            tables,
            limit,
            path: Vec::new(),
            out: Vec::new(),
        }
    }

    fn finish(
        mut self,
        validator: &Validator,
        value: &LiveValue,
    ) -> Result<(), Vec<FieldViolation>> {
        self.run(validator, value);
        if self.out.is_empty() {
            Ok(())
        } else {
            Err(self.out)
        }
    }

    fn violation(&mut self, message: String) {
        if self.out.len() < self.limit {
            self.out.push(FieldViolation {
                path: self.path.clone(),
                message,
            });
        }
    }

    fn mismatch(&mut self, validator: &Validator, value: &LiveValue) {
        self.violation(format!(
            "expected {}, got {}",
            validator.name(),
            value.type_name()
        ));
    }

    fn at(&mut self, elem: PathElem, validator: &Validator, value: &LiveValue) {
        self.path.push(elem);
        self.run(validator, value);
        self.path.pop();
    }

    fn run(&mut self, validator: &Validator, value: &LiveValue) {
        if self.out.len() >= self.limit {
            return;
        }
        let matches = match (validator, value) {
            (Validator::Any, _)
            | (Validator::Null, LiveValue::Null)
            | (Validator::Int64, LiveValue::I64(_))
            | (Validator::Float64, LiveValue::F64(_))
            | (Validator::Boolean, LiveValue::Bool(_))
            | (Validator::String, LiveValue::Str(_))
            | (Validator::Bytes, LiveValue::Bytes(_)) => true,
            (Validator::Optional(inner), value) => {
                self.run(inner, value);
                true
            }
            (Validator::Literal(literal), value) => {
                if literal != value {
                    self.violation(format!("expected the literal {}", literal_text(literal)));
                }
                true
            }
            (Validator::Id(table), LiveValue::Str(text)) => {
                self.id(table, text);
                true
            }
            (Validator::Array(element), LiveValue::Array(items)) => {
                for (i, item) in items.iter().enumerate() {
                    self.at(PathElem::Index(i), element, item);
                }
                true
            }
            (Validator::Object(fields), LiveValue::Object(present)) => {
                self.object(fields, present);
                true
            }
            (Validator::Union(members), value) => {
                // A member fails at its first violation: only whether it
                // matches counts.
                let tables = self.tables;
                if !members
                    .iter()
                    .any(|m| Checker::new(tables, 1).finish(m, value).is_ok())
                {
                    self.violation(format!(
                        "matches none of the union's {} members",
                        members.len()
                    ));
                }
                true
            }
            _ => false,
        };
        if !matches {
            self.mismatch(validator, value);
        }
    }

    /// A document's fields, as an object, without copying them.
    fn document(&mut self, validator: &Validator, fields: &BTreeMap<String, LiveValue>) {
        match validator {
            Validator::Any => {}
            Validator::Optional(inner) => self.document(inner, fields),
            Validator::Object(declared) => self.object(declared, fields),
            Validator::Union(members) => {
                let tables = self.tables;
                if !members.iter().any(|m| {
                    let mut one = Checker::new(tables, 1);
                    one.document(m, fields);
                    one.out.is_empty()
                }) {
                    self.violation(format!(
                        "matches none of the union's {} members",
                        members.len()
                    ));
                }
            }
            other => self.violation(format!("expected {}, got object", other.name())),
        }
    }

    fn id(&mut self, table: &str, text: &str) {
        let Ok(id) = text.parse::<DocId>() else {
            self.violation(format!(
                "expected an id of table '{table}', got a string that is not a document id"
            ));
            return;
        };
        let Some(tables) = self.tables else {
            return;
        };
        match tables.get(table) {
            Some(t) if *t == id.table => {}
            Some(_) => self.violation(format!(
                "expected an id of table '{table}', got an id of another table"
            )),
            None => self.violation(format!(
                "expected an id of table '{table}', which does not exist"
            )),
        }
    }

    fn object(
        &mut self,
        fields: &BTreeMap<String, Validator>,
        present: &BTreeMap<String, LiveValue>,
    ) {
        for name in present.keys().filter(|k| !fields.contains_key(*k)) {
            self.path.push(PathElem::Field(name.clone()));
            self.violation("unknown field".into());
            self.path.pop();
        }
        for (name, validator) in fields {
            let (inner, optional) = match validator {
                Validator::Optional(inner) => (&**inner, true),
                v => (v, false),
            };
            match present.get(name) {
                Some(item) => self.at(PathElem::Field(name.clone()), inner, item),
                None if optional => {}
                None => {
                    self.path.push(PathElem::Field(name.clone()));
                    self.violation("missing required field".into());
                    self.path.pop();
                }
            }
        }
    }
}

/// A literal as JavaScript writes it.
fn literal_text(v: &LiveValue) -> String {
    match v {
        LiveValue::Null => "null".into(),
        LiveValue::Bool(b) => b.to_string(),
        LiveValue::I64(i) => format!("{i}n"),
        LiveValue::F64(f) => f.to_string(),
        LiveValue::Str(s) => format!("{s:?}"),
        other => other.type_name().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(pairs: &[(&str, LiveValue)]) -> LiveValue {
        LiveValue::Object(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    fn paths(r: Result<(), Vec<FieldViolation>>) -> Vec<String> {
        r.err()
            .unwrap_or_default()
            .iter()
            .map(|v| render_path(&v.path))
            .collect()
    }

    #[test]
    fn objects_have_exactly_their_fields() {
        let v = Validator::Object(BTreeMap::from([
            ("a".to_string(), Validator::Int64),
            (
                "b".to_string(),
                Validator::Optional(Box::new(Validator::String)),
            ),
        ]));
        assert!(check(&v, &obj(&[("a", LiveValue::I64(1))])).is_ok());
        assert_eq!(
            paths(check(
                &v,
                &obj(&[("b", LiveValue::Null), ("c", LiveValue::Null)])
            )),
            vec!["$.c", "$.a", "$.b"]
        );
    }

    #[test]
    fn ids_are_checked_for_form_and_table() {
        let id = DocId::random(TableId(3)).expect("an id").to_string();
        let v = Validator::Id("users".into());
        assert!(check(&v, &LiveValue::Str(id.clone())).is_ok());
        assert!(check(&v, &LiveValue::Str("x".into())).is_err());
        let users = BTreeMap::from([("users".to_string(), TableId(3))]);
        let posts = BTreeMap::from([("users".to_string(), TableId(4))]);
        assert!(check_ids(&v, &LiveValue::Str(id.clone()), &users).is_ok());
        assert!(check_ids(&v, &LiveValue::Str(id.clone()), &posts).is_err());
        assert!(check_ids(&v, &LiveValue::Str(id), &BTreeMap::new()).is_err());
        let nested = Validator::Union(vec![
            Validator::Array(Box::new(Validator::Id("a".into()))),
            Validator::Id("b".into()),
        ]);
        assert_eq!(
            nested.id_tables(),
            BTreeSet::from(["a".to_string(), "b".to_string()])
        );
    }

    fn fields(pairs: &[(&str, Validator)]) -> Validator {
        Validator::Object(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    fn opt(v: Validator) -> Validator {
        Validator::Optional(Box::new(v))
    }

    #[test]
    fn validators_round_trip_through_their_wire_form() {
        let v = Validator::Union(vec![
            fields(&[
                ("a", Validator::Array(Box::new(Validator::Int64))),
                ("b", opt(Validator::Id("users".into()))),
                ("c", Validator::Literal(LiveValue::Str("on".into()))),
                ("d", Validator::Literal(LiveValue::F64(1.5))),
            ]),
            fields(&[
                ("e", Validator::Null),
                ("f", Validator::Float64),
                ("g", Validator::Boolean),
                ("h", Validator::String),
                ("i", Validator::Bytes),
                ("j", Validator::Any),
            ]),
        ]);
        assert_eq!(Validator::from_proto(&v.to_proto()).expect("valid"), v);
        assert!(Validator::from_proto(&pb::Validator::default()).is_err());
        let mut deep = Validator::Int64;
        for _ in 0..=MAX_VALIDATOR_DEPTH {
            deep = Validator::Array(Box::new(deep));
        }
        assert!(Validator::from_proto(&deep.to_proto()).is_err());
    }

    #[test]
    fn table_validators_are_objects_with_optional_only_as_fields() {
        let ok = fields(&[("a", opt(Validator::Int64)), ("b", Validator::String)]);
        assert_eq!(ok.check_table(), Ok(()));
        assert_eq!(
            Validator::Union(vec![ok.clone(), fields(&[])]).check_table(),
            Ok(())
        );
        for bad in [
            Validator::String,
            Validator::Any,
            Validator::Union(vec![]),
            Validator::Union(vec![ok.clone(), Validator::Null]),
            fields(&[("_id", Validator::String)]),
            fields(&[("", Validator::String)]),
            fields(&[("a", Validator::Array(Box::new(opt(Validator::Int64))))]),
            fields(&[("a", Validator::Union(vec![]))]),
            fields(&[("a", Validator::Literal(LiveValue::F64(f64::NAN)))]),
            fields(&[("a", Validator::Literal(LiveValue::Array(vec![])))]),
            fields(&[("a", Validator::Id("1bad".into()))]),
        ] {
            assert!(bad.check_table().is_err(), "{bad:?}");
        }
        let wide = Validator::Object(
            (0..MAX_VALIDATOR_NODES)
                .map(|i| (format!("f{i}"), Validator::Int64))
                .collect(),
        );
        assert!(wide.check_table().is_err(), "too many nodes");
    }

    #[test]
    fn looser_validators_accept_all_the_stricter_ones_accept() {
        let strict = fields(&[("a", Validator::Int64), ("b", Validator::String)]);
        let optional_b = fields(&[("a", Validator::Int64), ("b", opt(Validator::String))]);
        let new_field = fields(&[
            ("a", Validator::Int64),
            ("b", Validator::String),
            ("c", opt(Validator::Bytes)),
        ]);
        let widened = fields(&[
            (
                "a",
                Validator::Union(vec![Validator::Int64, Validator::Float64]),
            ),
            ("b", Validator::String),
        ]);
        for looser in [&optional_b, &new_field, &widened, &Validator::Any] {
            assert!(accepts_all(looser, &strict), "{looser:?}");
        }
        for (new, old) in [
            (&strict, &optional_b),
            // A new required field: old documents lack it.
            (
                &fields(&[
                    ("a", Validator::Int64),
                    ("b", Validator::String),
                    ("c", Validator::Bytes),
                ]),
                &strict,
            ),
            // A field dropped: old documents have it.
            (&fields(&[("a", Validator::Int64)]), &strict),
            (&strict, &widened),
        ] {
            assert!(!accepts_all(new, old), "{new:?} ⊉ {old:?}");
        }
        let id = Validator::Id("users".into());
        assert!(accepts_all(&Validator::String, &id));
        assert!(!accepts_all(&id, &Validator::String));
        assert!(!accepts_all(&Validator::Id("posts".into()), &id));
        let on = Validator::Literal(LiveValue::Str("on".into()));
        assert!(accepts_all(&Validator::String, &on));
        assert!(!accepts_all(&id, &on), "an id's table is not checked here");
        let either = Validator::Union(vec![on.clone(), Validator::Null]);
        assert!(accepts_all(
            &Validator::Union(vec![Validator::String, Validator::Null]),
            &either
        ));
        assert!(!accepts_all(&Validator::String, &either));
        assert!(accepts_all(
            &Validator::Array(Box::new(Validator::Any)),
            &Validator::Array(Box::new(Validator::Int64))
        ));
    }

    #[test]
    fn documents_are_checked_as_objects_or_unions_of_them() {
        let note = fields(&[("body", Validator::String), ("n", opt(Validator::Int64))]);
        let tables = BTreeMap::new();
        let doc = |pairs: &[(&str, LiveValue)]| -> BTreeMap<String, LiveValue> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect()
        };
        assert!(
            check_fields(
                &note,
                &doc(&[("body", LiveValue::Str("x".into()))]),
                &tables
            )
            .is_ok()
        );
        let err = check_fields(&note, &doc(&[("n", LiveValue::Str("x".into()))]), &tables)
            .expect_err("two violations");
        let paths: Vec<String> = err.iter().map(|v| render_path(&v.path)).collect();
        assert_eq!(paths, ["$.body", "$.n"]);
        let either = Validator::Union(vec![
            note.clone(),
            fields(&[("kind", Validator::Literal(LiveValue::Str("tag".into())))]),
        ]);
        assert!(
            check_fields(
                &either,
                &doc(&[("kind", LiveValue::Str("tag".into()))]),
                &tables
            )
            .is_ok()
        );
        let err = check_fields(&either, &doc(&[("kind", LiveValue::I64(1))]), &tables)
            .expect_err("no member");
        assert_eq!(render_path(&err[0].path), "$");
        assert!(check_fields(&Validator::Any, &doc(&[("x", LiveValue::Null)]), &tables).is_ok());
        assert!(check_fields(&Validator::String, &doc(&[]), &tables).is_err());
    }

    #[test]
    fn violations_are_capped_and_never_show_the_value() {
        let v = Validator::Array(Box::new(Validator::Int64));
        let value = LiveValue::Array(vec![LiveValue::Str("secret".into()); 100]);
        let violations = check(&v, &value).expect_err("all strings");
        assert_eq!(violations.len(), MAX_VIOLATIONS);
        let text = describe(&violations);
        assert!(!text.contains("secret"), "{text}");
        assert!(text.ends_with("and 12 more"), "{text}");
        let lit = Validator::Literal(LiveValue::Str("on".into()));
        let text = describe(&check(&lit, &LiveValue::Str("secret".into())).expect_err("no"));
        assert_eq!(text, "$: expected the literal \"on\"");
    }
}
