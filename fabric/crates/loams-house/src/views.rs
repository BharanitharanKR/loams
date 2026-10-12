//! The read views of §49 §7 (HS1 Task 11): the DDL a worker's control
//! connection runs so that a lake table's name reads its pinned snapshot.
//!
//! A table `d.t` becomes a view in a `Memory` database `d` of the worker:
//!
//! ```text
//! CREATE VIEW `d`.`t` AS
//! SELECT CAST(`a`, 'UInt64') AS `a`, …
//! FROM icebergS3('http://127.0.0.1:9180/<bucket>/<table location>/', 'dummy', 'dummy',
//!                SETTINGS iceberg_metadata_file_path = 'metadata/<pinned>.metadata.json')
//! SETTINGS iceberg_snapshot_id = <pinned>
//! ```
//!
//! * The bucket is read through `house-cache` only: the worker's endpoint is its
//!   forwarder (`Ready::s3_endpoint`), and the keys are dummies that never reach
//!   the store (§49 §13.1, D766).
//! * The metadata file **and** the snapshot are pinned, so a commit that lands
//!   while the query runs is never seen and nothing is listed (R1.2). Iceberg
//!   time travel is the same view with an older snapshot: `iceberg_snapshot_id`
//!   is not a setting of the table function in chDB 26.9, but a view's own
//!   `SETTINGS` apply to its read alone (measured: two views of one table at two
//!   snapshots in one query each read their own).
//! * ClickHouse types are restored with `CAST`s (Iceberg has no `UInt8`, `Enum`,
//!   `LowCardinality` …, R10.4), and a column added by `ALTER` (an optional
//!   Iceberg field) reads its `DEFAULT` for rows written before it.
//! * `FINAL` (or `final = 1`) on a `ReplacingMergeTree` or `SummingMergeTree`
//!   table reads a **merge view** (§49 §6.2): Replacing keeps, per sorting key,
//!   the row with the greatest `(ver, file sequence number, row number)` and
//!   drops it when its `is_deleted` is 1; Summing groups by the key and sums.
//!   chDB ignores `FINAL` on a view (measured), so the user's text runs as it is.
//! * An external table (another writer's) reads as chDB reads Iceberg: no casts.

use loams_house_lake::files::LiveFile;

use crate::catalog::LakeRoot;
use crate::ddl::{self, LakeEngine, TableClass, quote_name, quote_string};
use crate::errors::{ChError, HouseError};
use crate::expr;
use crate::read::ReadPin;
use crate::types::ChType;

/// `CREATE DATABASE IF NOT EXISTS d ENGINE = Memory`: where a database's views
/// live in the worker.
pub fn database_ddl(database: &str) -> String {
    format!(
        "CREATE DATABASE IF NOT EXISTS {} ENGINE = Memory",
        quote_name(database)
    )
}

/// `DROP TABLE IF EXISTS d.t`: a name the catalog does not have must not read
/// a view an earlier statement left in a worker that shares its engine.
pub fn drop_ddl(database: &str, name: &str) -> String {
    format!(
        "DROP TABLE IF EXISTS {}.{}",
        quote_name(database),
        quote_name(name)
    )
}

/// The table function that reads a pin through the worker's endpoint.
pub fn source(pin: &ReadPin, endpoint: &str, root: &LakeRoot) -> Result<String, HouseError> {
    let unreadable = |why: &str| {
        HouseError::from(ChError::access_denied(format!(
            "Table {}.{} cannot be read: {why}",
            pin.table.database, pin.table.name
        )))
    };
    let key = root
        .relative(&pin.location)
        .ok_or_else(|| unreadable("its location is outside the House's store"))?;
    let location = pin.location.trim_end_matches('/');
    let metadata = pin
        .metadata_file
        .strip_prefix(location)
        .and_then(|rest| rest.strip_prefix('/'))
        .ok_or_else(|| unreadable("its metadata file is outside its location"))?;
    let mut url = endpoint.trim_end_matches('/').to_string();
    url.push('/');
    url.push_str(root.bucket());
    let key = key.trim_matches('/');
    if !key.is_empty() {
        url.push('/');
        url.push_str(key);
    }
    url.push('/');
    Ok(format!(
        "icebergS3({}, 'dummy', 'dummy', SETTINGS iceberg_metadata_file_path = {})",
        quote_string(&url),
        quote_string(metadata)
    ))
}

/// How a view reads its table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode<'a> {
    /// The rows as stored.
    Plain,
    /// `FINAL`: the merge view of a Replacing or Summing table, with the live
    /// files of the pinned snapshot (for their sequence numbers).
    Final(&'a [LiveFile]),
}

/// The view DDL for a pin.
pub fn view_ddl(pin: &ReadPin, source: &str, mode: Mode<'_>) -> Result<String, HouseError> {
    let name = format!(
        "{}.{}",
        quote_name(&pin.table.database),
        quote_name(&pin.table.name)
    );
    let settings = pin
        .snapshot_id
        .filter(|_| pin.travel)
        .map(|id| format!("\nSETTINGS iceberg_snapshot_id = {id}"))
        .unwrap_or_default();
    let body = if pin.snapshot_id.is_none() {
        // No snapshot (a new table, or a time before its first): no rows, with
        // the table's columns.
        empty(pin)?
    } else if pin.table.class == TableClass::External {
        format!("SELECT * FROM {source}")
    } else {
        let engine = LakeEngine::parse(&pin.table.engine).ok();
        match (mode, engine) {
            (Mode::Final(files), Some(engine @ LakeEngine::Replacing { .. })) => {
                replacing(pin, source, &engine, files)?
            }
            (Mode::Final(_), Some(engine @ LakeEngine::Summing { .. })) => {
                summing(pin, source, &engine)?
            }
            _ => format!("SELECT {}\nFROM {source}", casts(pin)?.join(", ")),
        }
    };
    Ok(format!(
        "CREATE OR REPLACE VIEW {name} AS\n{body}{settings}"
    ))
}

/// Whether a view of `pin` in `mode` carries expression text from the
/// catalog's properties — a column's `DEFAULT` (for rows written before an
/// `ALTER … ADD COLUMN`) or the sorting key (`FINAL`) — rather than only names,
/// types and literals the House quotes itself. Such a view is checked by the
/// deny list before a worker runs it (HS1 Task 11): another writer of a shared
/// REST catalog could set those properties.
pub fn carries_expressions(pin: &ReadPin, mode: Mode<'_>) -> bool {
    if pin.table.class == TableClass::External || pin.snapshot_id.is_none() {
        return false;
    }
    let merge = matches!(mode, Mode::Final(_))
        && matches!(
            LakeEngine::parse(&pin.table.engine),
            Ok(LakeEngine::Replacing { .. } | LakeEngine::Summing { .. })
        );
    merge
        || columns(pin).iter().any(|c| {
            !c.default_expression.is_empty()
                && pin.fields.iter().any(|f| f.name == c.name && !f.required)
        })
}

/// The `SELECT` of a view DDL (what follows `AS`), for the deny list.
pub fn view_body(ddl: &str) -> &str {
    ddl.split_once(" AS\n").map_or(ddl, |(_, body)| body)
}

/// A table function the deny list allows and that a syntax-only analysis
/// never opens, standing in for a view's `icebergS3` source while its body is
/// checked.
pub const CHECKED_SOURCE: &str = "null('loams_checked UInt8')";

/// The columns a pin reads: the table's stored columns that the pinned
/// snapshot's schema has (a time-travelled read sees the schema of its time).
fn columns(pin: &ReadPin) -> Vec<&ddl::ChColumn> {
    pin.table
        .columns
        .iter()
        .filter(|c| c.stored() && pin.fields.iter().any(|f| f.name == c.name))
        .collect()
}

/// `CAST(…) AS c` for every column, restoring its ClickHouse type.
fn casts(pin: &ReadPin) -> Result<Vec<String>, HouseError> {
    columns(pin)
        .into_iter()
        .map(|column| {
            let ty = ChType::parse(&column.ch_type)?;
            let required = pin
                .fields
                .iter()
                .find(|f| f.name == column.name)
                .is_some_and(|f| f.required);
            let name = quote_name(&column.name);
            // A conversion to `DateTime` keeps its source's time zone, and
            // Iceberg's `timestamptz` reads as UTC: a column declared without a
            // zone drops it first, so it reads as declared.
            let read = match ty.base() {
                ChType::DateTime64(precision, None) if *precision > 6 => {
                    format!("fromUnixTimestamp64Nano(toUnixTimestamp64Nano({name}))")
                }
                ChType::DateTime(None) | ChType::DateTime64(_, None) => {
                    format!("fromUnixTimestamp64Micro(toUnixTimestamp64Micro({name}))")
                }
                _ => name.clone(),
            };
            let value = if required || ty.is_nullable() {
                read
            } else {
                // An optional field of a non-Nullable column: one `ALTER … ADD
                // COLUMN` made (R10.6). Rows written before it read its default.
                // The default is catalog text: printed again from its tree,
                // never spliced (R11.11).
                let default =
                    if column.default_kind == "DEFAULT" || column.default_kind == "MATERIALIZED" {
                        let expression = expr::reprint(&column.default_expression)
                            .map_err(|why| catalog_refusal(pin, &why))?;
                        format!("CAST({expression}, {})", quote_string(&column.ch_type))
                    } else {
                        format!("defaultValueOfTypeName({})", quote_string(&column.ch_type))
                    };
                format!("ifNull({read}, {default})")
            };
            Ok(format!(
                "CAST({value}, {}) AS {name}",
                quote_string(&column.ch_type)
            ))
        })
        .collect()
}

/// A view with the table's columns and no rows.
fn empty(pin: &ReadPin) -> Result<String, HouseError> {
    let structure: Vec<String> = pin
        .table
        .columns
        .iter()
        .filter(|c| c.stored())
        .map(|c| format!("{} {}", quote_name(&c.name), c.ch_type))
        .collect();
    if structure.is_empty() {
        return Err(HouseError::from(ChError::bad_arguments(format!(
            "Table {}.{} has no columns",
            pin.table.database, pin.table.name
        ))));
    }
    Ok(format!(
        "SELECT * FROM null({})",
        quote_string(&structure.join(", "))
    ))
}

/// The sorting key's elements, each printed again from its tree (R11.11);
/// `tuple()` has none.
fn key(pin: &ReadPin) -> Result<Vec<(String, Option<String>)>, HouseError> {
    expr::key_elements(&pin.table.order_by).map_err(|why| catalog_refusal(pin, &why))
}

/// `497`: a table's catalog properties hold text the House does not run as an
/// expression (R11.4, R11.11).
pub fn catalog_refusal(pin: &ReadPin, why: &str) -> HouseError {
    HouseError::from(ChError::access_denied(format!(
        "Table {}.{}: its catalog properties hold an expression the House does not run ({why})",
        pin.table.database, pin.table.name
    )))
}

const SEQ: &str = "__loams_seq";
const ROW: &str = "__loams_row";

/// Replacing `FINAL`: per sorting key, the row with the greatest `(ver, file
/// sequence number, row number)` (the last inserted when there is no `ver`), and
/// none when that row's `is_deleted` is 1.
fn replacing(
    pin: &ReadPin,
    source: &str,
    engine: &LakeEngine,
    files: &[LiveFile],
) -> Result<String, HouseError> {
    let LakeEngine::Replacing { ver, is_deleted } = engine else {
        return Err(HouseError::from(ChError::bad_arguments(
            "not a ReplacingMergeTree table",
        )));
    };
    let mut inner = casts(pin)?;
    let names: Vec<String> = files.iter().map(|f| quote_string(file_name(f))).collect();
    let seqs: Vec<String> = files
        .iter()
        .map(|f| f.sequence_number.to_string())
        .collect();
    inner.push(format!(
        "transform(_file, [{}]::Array(String), [{}]::Array(Int64), toInt64(0)) AS {SEQ}",
        names.join(", "),
        seqs.join(", ")
    ));
    inner.push(format!("ifNull(_row_number, 0) AS {ROW}"));
    let mut order = Vec::new();
    if let Some(ver) = ver {
        order.push(format!("{} DESC", quote_name(ver)));
    }
    order.push(format!("{SEQ} DESC"));
    order.push(format!("{ROW} DESC"));
    let key: Vec<String> = key(pin)?.into_iter().map(|(text, _)| text).collect();
    let limit_by = if key.is_empty() {
        "tuple()".to_string()
    } else {
        key.join(", ")
    };
    let outer: Vec<String> = columns(pin).iter().map(|c| quote_name(&c.name)).collect();
    let deleted = is_deleted
        .as_ref()
        .map(|c| format!("\nWHERE {} = 0", quote_name(c)))
        .unwrap_or_default();
    Ok(format!(
        "SELECT {}\nFROM (\nSELECT {}\nFROM {source}\nORDER BY {}\nLIMIT 1 BY {limit_by}\n){deleted}",
        outer.join(", "),
        inner.join(", "),
        order.join(", ")
    ))
}

/// Whether a ClickHouse type is one a `SummingMergeTree` sums by default.
fn is_numeric(ty: &str) -> bool {
    let Ok(parsed) = ChType::parse(ty) else {
        return false;
    };
    if parsed.is_nullable() {
        return false;
    }
    match parsed.base() {
        ChType::Simple(name) => {
            name.starts_with("Int")
                || name.starts_with("UInt")
                || name.starts_with("Float")
                || name.starts_with("Decimal")
        }
        ChType::Decimal(..) => true,
        _ => false,
    }
}

/// Summing `FINAL`: rows grouped by the sorting key, the summed columns added
/// (in their own type, wrapping as ClickHouse's merge does), any value of the
/// others, and a group whose sums are all zero dropped.
fn summing(pin: &ReadPin, source: &str, engine: &LakeEngine) -> Result<String, HouseError> {
    let LakeEngine::Summing { columns: summed } = engine else {
        return Err(HouseError::from(ChError::bad_arguments(
            "not a SummingMergeTree table",
        )));
    };
    let key = key(pin)?;
    let key_columns: Vec<&str> = key.iter().filter_map(|(_, c)| c.as_deref()).collect();
    let columns = columns(pin);
    let is_summed = |c: &ddl::ChColumn| match summed {
        Some(list) => list.iter().any(|s| s == &c.name),
        None => !key_columns.contains(&c.name.as_str()) && is_numeric(&c.ch_type),
    };
    let mut select = Vec::new();
    let mut sums = Vec::new();
    for column in &columns {
        let name = quote_name(&column.name);
        if key_columns.contains(&column.name.as_str()) {
            select.push(name);
        } else if is_summed(column) {
            select.push(format!("sumWithOverflow({name}) AS {name}"));
            sums.push(format!("{name} = 0"));
        } else {
            select.push(format!("any({name}) AS {name}"));
        }
    }
    let group_by = if key.is_empty() {
        "tuple()".to_string()
    } else {
        key.iter()
            .map(|(text, _)| text.clone())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let having = if sums.is_empty() {
        String::new()
    } else {
        format!("\nHAVING NOT ({})", sums.join(" AND "))
    };
    Ok(format!(
        "SELECT {}\nFROM (\nSELECT {}\nFROM {source}\n)\nGROUP BY {group_by}{having}",
        select.join(", "),
        casts(pin)?.join(", ")
    ))
}

/// A file's name, as chDB's `_file` gives it.
fn file_name(file: &LiveFile) -> &str {
    file.location
        .rsplit_once('/')
        .map_or(file.location.as_str(), |(_, name)| name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::{ChColumn, ChTable};
    use crate::read::PinnedField;

    fn column(name: &str, ty: &str) -> ChColumn {
        ChColumn {
            name: name.to_string(),
            ch_type: ty.to_string(),
            ..ChColumn::default()
        }
    }

    fn pin(engine: &str, order_by: &str, columns: Vec<ChColumn>) -> ReadPin {
        let fields = columns
            .iter()
            .map(|c| PinnedField {
                name: c.name.clone(),
                required: !c.ch_type.starts_with("Nullable"),
            })
            .collect();
        ReadPin {
            table: ChTable {
                database: "db".to_string(),
                name: "t".to_string(),
                class: TableClass::Lake,
                engine: engine.to_string(),
                columns,
                order_by: order_by.to_string(),
                primary_key: String::new(),
                partition_by: String::new(),
                settings: Vec::new(),
                comment: String::new(),
                ddl: String::new(),
            },
            location: "s3://local/ns/1/house/db/t-1".to_string(),
            metadata_file: "s3://local/ns/1/house/db/t-1/metadata/00002-x.metadata.json"
                .to_string(),
            snapshot_id: Some(42),
            fields,
            travel: true,
        }
    }

    #[test]
    fn source_names_the_endpoint_bucket_and_pinned_file() {
        let p = pin("MergeTree", "k", vec![column("k", "UInt64")]);
        let root = LakeRoot::from_store_url("s3://bkt/pre").expect("root");
        assert!(
            source(&p, "http://127.0.0.1:9180/", &root).is_err(),
            "outside the root"
        );
        let local = source(&p, "http://127.0.0.1:9180/", &LakeRoot::local()).expect("source");
        assert_eq!(
            local,
            "icebergS3('http://127.0.0.1:9180/local/ns/1/house/db/t-1/', 'dummy', 'dummy', \
             SETTINGS iceberg_metadata_file_path = 'metadata/00002-x.metadata.json')"
        );
    }

    #[test]
    fn plain_view_casts_and_pins_the_snapshot() {
        let mut added = column("n", "UInt32");
        added.default_kind = "DEFAULT".to_string();
        added.default_expression = "k + 1".to_string();
        let mut p = pin(
            "MergeTree",
            "k",
            vec![
                column("k", "UInt64"),
                column("s", "Nullable(String)"),
                added,
            ],
        );
        p.fields[2].required = false;
        let ddl = view_ddl(&p, "SRC", Mode::Plain).expect("view");
        assert_eq!(
            ddl,
            "CREATE OR REPLACE VIEW db.t AS\nSELECT CAST(k, 'UInt64') AS k, CAST(s, \
             'Nullable(String)') AS s, CAST(ifNull(n, CAST(plus(`k`, 1), 'UInt32')), 'UInt32') AS n\n\
             FROM SRC\nSETTINGS iceberg_snapshot_id = 42"
        );
        // FINAL on a MergeTree table is the plain view.
        assert_eq!(view_ddl(&p, "SRC", Mode::Final(&[])).expect("view"), ddl);
    }

    #[test]
    fn no_snapshot_reads_no_rows() {
        let mut p = pin("MergeTree", "k", vec![column("k", "UInt64")]);
        p.snapshot_id = None;
        assert_eq!(
            view_ddl(&p, "SRC", Mode::Plain).expect("view"),
            "CREATE OR REPLACE VIEW db.t AS\nSELECT * FROM null('k UInt64')"
        );
    }

    #[test]
    fn merge_views() {
        let p = pin(
            "ReplacingMergeTree(v, del)",
            "(k, toDate(d))",
            vec![
                column("k", "UInt64"),
                column("d", "DateTime"),
                column("v", "UInt32"),
                column("del", "UInt8"),
            ],
        );
        let files = [LiveFile {
            location: "s3://local/x/data/a.parquet".to_string(),
            record_count: 1,
            size_bytes: 1,
            partition: "tuple()".to_string(),
            sequence_number: 3,
            data: true,
        }];
        let ddl = view_ddl(&p, "SRC", Mode::Final(&files)).expect("view");
        assert!(ddl.contains("transform(_file, ['a.parquet']::Array(String), [3]::Array(Int64)"));
        assert!(ddl.contains("ORDER BY v DESC, __loams_seq DESC, __loams_row DESC"));
        assert!(ddl.contains("LIMIT 1 BY `k`, toDate(`d`)"));
        assert!(ddl.contains("WHERE del = 0"));

        let s = pin(
            "SummingMergeTree",
            "k",
            vec![
                column("k", "UInt64"),
                column("n", "Int64"),
                column("label", "String"),
            ],
        );
        let ddl = view_ddl(&s, "SRC", Mode::Final(&[])).expect("view");
        assert!(ddl.starts_with(
            "CREATE OR REPLACE VIEW db.t AS\nSELECT k, sumWithOverflow(n) AS n, any(label) AS label"
        ));
        assert!(ddl.contains("GROUP BY `k`\nHAVING NOT (n = 0)"));
    }

    #[test]
    fn catalog_text_is_reprinted_or_refused() {
        // R11.11: a DEFAULT or a sorting key from the catalog never reaches the
        // control connection as text.
        for default in [
            "1); DROP TABLE x; --",
            "(SELECT count() FROM system.one)",
            "numbers(1)",
            "1, 2",
        ] {
            let mut added = column("n", "UInt32");
            added.default_kind = "DEFAULT".to_string();
            added.default_expression = default.to_string();
            let mut p = pin("MergeTree", "k", vec![column("k", "UInt64"), added]);
            p.fields[1].required = false;
            let err = view_ddl(&p, "SRC", Mode::Plain).expect_err(default);
            assert_eq!(err.code(), 497, "{default}: {err}");
        }
        for order_by in [
            "k, (SELECT 1)",
            "(k, url('http://127.0.0.1:1/x', 'CSV', 'a String'))",
            "`k) FROM numbers(1) --`(k)",
            "k); DROP DATABASE default; --",
        ] {
            let p = pin(
                "ReplacingMergeTree",
                order_by,
                vec![column("k", "UInt64"), column("v", "UInt8")],
            );
            let err = view_ddl(&p, "SRC", Mode::Final(&[])).expect_err(order_by);
            assert_eq!(err.code(), 497, "{order_by}: {err}");
            let s = pin("SummingMergeTree", order_by, vec![column("k", "UInt64")]);
            let err = view_ddl(&s, "SRC", Mode::Final(&[])).expect_err(order_by);
            assert_eq!(err.code(), 497, "{order_by}: {err}");
        }
    }
}
