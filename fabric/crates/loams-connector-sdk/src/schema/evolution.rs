//! The evolution policy (CN2 Task 5): which schema changes a source accepts.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

use super::{Evolution, MAX_LENGTH_KEY, SchemaChange, SchemaError, render_schema, render_type};
use crate::position::Position;

/// One accepted difference between two schemas. Paths are `.`-joined field
/// names (`address.zip`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldChange {
    /// A new nullable field (`Additive`).
    Added { path: String },
    /// A lossless type widening (`Widen`).
    Widened {
        path: String,
        from: String,
        to: String,
    },
    /// NOT NULL → nullable (`Widen`).
    Relaxed { path: String },
}

/// Compare `before` with `after` under `evolution`. `Ok` lists the accepted
/// changes (empty: the same shape); `Err` is the reason the change is
/// incompatible. Field order is ignored; metadata other than
/// [`MAX_LENGTH_KEY`] is ignored.
pub fn compare(
    before: &Schema,
    after: &Schema,
    evolution: Evolution,
) -> Result<Vec<FieldChange>, String> {
    let mut changes = Vec::new();
    compare_fields(before.fields(), after.fields(), "", evolution, &mut changes)?;
    if evolution == Evolution::Pause && !changes.is_empty() {
        return Err(format!(
            "the instance's evolution is `pause`, and the schema changed ({})",
            describe(&changes)
        ));
    }
    Ok(changes)
}

fn describe(changes: &[FieldChange]) -> String {
    changes
        .iter()
        .map(|c| match c {
            FieldChange::Added { path } => format!("{path} added"),
            FieldChange::Widened { path, from, to } => format!("{path} widened {from} → {to}"),
            FieldChange::Relaxed { path } => format!("{path} made nullable"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_owned()
    } else {
        format!("{prefix}.{name}")
    }
}

fn compare_fields(
    before: &Fields,
    after: &Fields,
    prefix: &str,
    evolution: Evolution,
    changes: &mut Vec<FieldChange>,
) -> Result<(), String> {
    for b in before {
        let path = join(prefix, b.name());
        match after.find(b.name()) {
            None => return Err(format!("column {path} was dropped")),
            Some((_, a)) => compare_field(b, a, &path, evolution, changes)?,
        }
    }
    for a in after {
        if before.find(a.name()).is_none() {
            let path = join(prefix, a.name());
            if !a.is_nullable() {
                return Err(format!(
                    "new column {path} is NOT NULL, so rows written before it have no value"
                ));
            }
            changes.push(FieldChange::Added { path });
        }
    }
    Ok(())
}

fn compare_field(
    b: &Field,
    a: &Field,
    path: &str,
    evolution: Evolution,
    changes: &mut Vec<FieldChange>,
) -> Result<(), String> {
    let widen = |changes: &mut Vec<FieldChange>, change: FieldChange| {
        if evolution == Evolution::Additive {
            Err(format!(
                "{} needs the instance's evolution to be `widen`",
                describe(std::slice::from_ref(&change))
            ))
        } else {
            changes.push(change);
            Ok(())
        }
    };

    match (b.is_nullable(), a.is_nullable()) {
        (true, false) => return Err(format!("column {path} became NOT NULL")),
        (false, true) => widen(
            changes,
            FieldChange::Relaxed {
                path: path.to_owned(),
            },
        )?,
        _ => {}
    }

    match (b.data_type(), a.data_type()) {
        (DataType::Struct(bf), DataType::Struct(af)) => {
            compare_fields(bf, af, path, evolution, changes)
        }
        (DataType::List(be), DataType::List(ae))
        | (DataType::LargeList(be), DataType::LargeList(ae)) => {
            compare_field(be, ae, &join(path, be.name()), evolution, changes)
        }
        (bt, at) if bt == at => match (max_length(b), max_length(a)) {
            (Some(m), Some(n)) if n > m => widen(
                changes,
                FieldChange::Widened {
                    path: path.to_owned(),
                    from: format!("{}({m})", render_type(bt)),
                    to: format!("{}({n})", render_type(at)),
                },
            ),
            (Some(m), None) => widen(
                changes,
                FieldChange::Widened {
                    path: path.to_owned(),
                    from: format!("{}({m})", render_type(bt)),
                    to: render_type(at),
                },
            ),
            (Some(m), Some(n)) if n < m => {
                Err(format!("column {path} narrowed from length {m} to {n}"))
            }
            (None, Some(n)) => Err(format!(
                "column {path} narrowed from unbounded to length {n}"
            )),
            _ => Ok(()),
        },
        (bt, at) if is_lossless_widening(bt, at) => widen(
            changes,
            FieldChange::Widened {
                path: path.to_owned(),
                from: render_type(bt),
                to: render_type(at),
            },
        ),
        (bt, at) => Err(format!(
            "column {path} changed type from {} to {}, which is not a lossless widening",
            render_type(bt),
            render_type(at)
        )),
    }
}

fn max_length(f: &Field) -> Option<u64> {
    f.metadata().get(MAX_LENGTH_KEY)?.parse().ok()
}

/// Whether every value of `from` is exactly representable in `to`.
pub fn is_lossless_widening(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    match (from, to) {
        (Int8, Int16 | Int32 | Int64)
        | (Int16, Int32 | Int64)
        | (Int32, Int64)
        | (UInt8, UInt16 | UInt32 | UInt64 | Int16 | Int32 | Int64)
        | (UInt16, UInt32 | UInt64 | Int32 | Int64)
        | (UInt32, UInt64 | Int64)
        | (Float16, Float32 | Float64)
        | (Float32, Float64)
        | (Utf8, LargeUtf8)
        | (Binary, LargeBinary) => true,
        (Decimal128(p1, s1), Decimal128(p2, s2) | Decimal256(p2, s2))
        | (Decimal256(p1, s1), Decimal256(p2, s2)) => decimal_fits(*p1, *s1, *p2, *s2),
        _ => false,
    }
}

/// `decimal(p1, s1)` fits `decimal(p2, s2)` when neither the fraction nor the
/// integer part loses a digit.
fn decimal_fits(p1: u8, s1: i8, p2: u8, s2: i8) -> bool {
    let int1 = i16::from(p1) - i16::from(s1);
    let int2 = i16::from(p2) - i16::from(s2);
    s2 >= s1 && int2 >= int1
}

/// Each table's current schema, evolved under one [`Evolution`].
#[derive(Debug, Clone)]
pub struct SchemaTracker {
    evolution: Evolution,
    tables: BTreeMap<String, SchemaRef>,
}

impl SchemaTracker {
    pub fn new(evolution: Evolution) -> Self {
        Self {
            evolution,
            tables: BTreeMap::new(),
        }
    }

    pub fn evolution(&self) -> Evolution {
        self.evolution
    }

    /// Record a table's schema without a change (from a snapshot, a
    /// checkpoint or the target). Under `pause`, only seeded tables may be
    /// read.
    pub fn seed(&mut self, table: impl Into<String>, schema: SchemaRef) {
        self.tables.insert(table.into(), schema);
    }

    pub fn current(&self, table: &str) -> Option<SchemaRef> {
        self.tables.get(table).cloned()
    }

    /// The schema of `table` as of `at`. `Ok(None)`: no change. `Ok(Some)`:
    /// an accepted change, now current, to emit as a `schema-change` event.
    /// `Err`: an incompatible change, with both schemas; the tracked schema
    /// stays as it was.
    pub fn observe(
        &mut self,
        table: &str,
        after: SchemaRef,
        at: &Position,
    ) -> Result<Option<SchemaChange>, SchemaError> {
        let before = match self.tables.get(table) {
            Some(b) if b.fields() == after.fields() => return Ok(None),
            Some(b) => b.clone(),
            None => Arc::new(Schema::empty()),
        };
        let new_table = !self.tables.contains_key(table);
        let verdict = if new_table {
            // A new table is additive whatever its columns' nullability:
            // it has no earlier rows.
            match self.evolution {
                Evolution::Pause => {
                    Err("the instance's evolution is `pause`, and a new table appeared".to_owned())
                }
                _ => Ok(()),
            }
        } else {
            compare(&before, &after, self.evolution).map(|_| ())
        };
        match verdict {
            Ok(()) => {
                self.tables.insert(table.to_owned(), after.clone());
                Ok(Some(SchemaChange {
                    table: table.to_owned(),
                    before,
                    after,
                    at: at.clone(),
                }))
            }
            Err(reason) => Err(SchemaError::Incompatible {
                table: table.to_owned(),
                reason,
                before: render_schema(&before),
                after: render_schema(&after),
            }),
        }
    }
}
