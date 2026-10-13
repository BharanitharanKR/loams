//! Tables: their columns and keys from `information_schema`, their Arrow
//! schema, and the chunked `SELECT … WHERE (key) > (last) ORDER BY key
//! LIMIT n` that batch reads and the CDC snapshot share.

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use base64::Engine as _;
use loams_connector_sdk::ConnectorError;
use loams_connector_sdk::schema::{Lossy, NativeType, TypeMapper};
use serde_json::Value;

use crate::config::TableName;
use crate::conn::MyConn;
use crate::history::TableDef;
use crate::types::{ColumnDef, Kind, MyTypeMapper, hex_literal, quote_ident, quote_str};

/// A table as a reader sees it.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: TableName,
    pub def: TableDef,
    /// The key the reader pages by (the primary key, or the configured one).
    pub key: Vec<String>,
    pub schema: SchemaRef,
}

/// The Arrow schema of `def` (paths `database.table.column` for `lossy`).
pub fn arrow_schema(
    t: &TableName,
    def: &TableDef,
    lossy: &Lossy,
) -> Result<SchemaRef, ConnectorError> {
    let mapper = MyTypeMapper::new(lossy.clone());
    let fields = def
        .columns
        .iter()
        .map(|c| {
            mapper
                .to_arrow(&NativeType::new(
                    format!("{t}.{}", c.name),
                    c.column_type.clone(),
                    c.nullable,
                ))
                .map_err(ConnectorError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// `t`'s columns and primary key now, or `None` if it does not exist.
pub async fn describe_def(
    conn: &mut MyConn,
    t: &TableName,
) -> Result<Option<TableDef>, ConnectorError> {
    let sql = format!(
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION; \
         SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
         WHERE TABLE_SCHEMA = {0} AND TABLE_NAME = {1} AND CONSTRAINT_NAME = 'PRIMARY' \
         ORDER BY ORDINAL_POSITION",
        quote_str(&t.database),
        quote_str(&t.name)
    );
    let mut rs = conn
        .query_all(&sql)
        .await
        .map_err(|e| e.to_connector(&format!("describing {t}")))?;
    if rs.len() != 2 {
        return Err(ConnectorError::fatal(format!(
            "describing {t}: unexpected answer"
        )));
    }
    let keys = rs.pop().unwrap_or_default();
    let cols = rs.pop().unwrap_or_default();
    if cols.rows.is_empty() {
        return Ok(None);
    }
    let text = |v: &Option<Vec<u8>>| {
        v.as_ref()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default()
    };
    Ok(Some(TableDef {
        columns: cols
            .rows
            .iter()
            .map(|r| ColumnDef::new(text(&r[0]), text(&r[1]), text(&r[2]) == "YES"))
            .collect(),
        key: keys.rows.iter().map(|r| text(&r[0])).collect(),
    }))
}

/// The base tables of `database`.
pub async fn list_tables(
    conn: &mut MyConn,
    database: &str,
) -> Result<Vec<TableName>, ConnectorError> {
    let rs = conn
        .query(&format!(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = {} AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
            quote_str(database)
        ))
        .await
        .map_err(|e| e.to_connector(&format!("listing the tables of {database}")))?;
    Ok(rs
        .rows
        .iter()
        .filter_map(|r| r[0].as_ref())
        .map(|b| TableName::new(database, String::from_utf8_lossy(b)))
        .collect())
}

/// `t` for a reader: columns, the paging key (`key`, or the primary key)
/// and the Arrow schema. A table without either key is refused.
pub async fn describe(
    conn: &mut MyConn,
    t: &TableName,
    key: &[String],
    lossy: &Lossy,
) -> Result<TableInfo, ConnectorError> {
    let def = describe_def(conn, t)
        .await?
        .ok_or_else(|| ConnectorError::fatal(format!("table {t} does not exist")))?;
    info(t, def, key, lossy)
}

/// A [`TableInfo`] from a known definition.
pub fn info(
    t: &TableName,
    def: TableDef,
    key: &[String],
    lossy: &Lossy,
) -> Result<TableInfo, ConnectorError> {
    let key = if key.is_empty() {
        def.key.clone()
    } else {
        key.to_vec()
    };
    if key.is_empty() {
        return Err(ConnectorError::fatal(format!(
            "{t} has no primary key: set `key`"
        )));
    }
    for k in &key {
        if def.column(k).is_none() {
            return Err(ConnectorError::fatal(format!("{t} has no key column {k}")));
        }
    }
    let schema = arrow_schema(t, &def, lossy)?;
    Ok(TableInfo {
        name: t.clone(),
        def,
        key,
        schema,
    })
}

/// A canonical JSON value of `col` as a SQL literal.
pub fn literal(v: &Value, col: &ColumnDef) -> Result<String, String> {
    Ok(match (v, col.kind()) {
        (Value::Null, _) => "NULL".to_owned(),
        (Value::Number(n), _) => n.to_string(),
        (Value::Bool(b), _) => if *b { "1" } else { "0" }.to_owned(),
        (Value::String(s), Kind::Binary) => hex_literal(
            &base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(|_| format!("column {}: not base64", col.name))?,
        ),
        (Value::String(s), Kind::Timestamp) => {
            // `…Z` to the session's UTC.
            quote_str(s.trim_end_matches('Z').replacen('T', " ", 1).as_str())
        }
        (Value::String(s), Kind::Bit) => s
            .parse::<u64>()
            .map(|n| n.to_string())
            .map_err(|_| format!("column {}: not a bit value", col.name))?,
        (Value::String(s), _) => quote_str(s),
        (other, _) => quote_str(&other.to_string()),
    })
}

/// The chunked read after `after` (the last key read, in key order).
pub fn chunk_sql(t: &TableInfo, after: Option<&[Value]>, limit: usize) -> Result<String, String> {
    let cols: Vec<String> = t.def.columns.iter().map(|c| quote_ident(&c.name)).collect();
    let keys: Vec<String> = t.key.iter().map(|k| quote_ident(k)).collect();
    let filter = match after {
        None => String::new(),
        Some(vals) => {
            let lits = t
                .key
                .iter()
                .zip(vals)
                .map(|(k, v)| {
                    let col = t
                        .def
                        .column(k)
                        .ok_or_else(|| format!("no key column {k}"))?;
                    literal(v, col)
                })
                .collect::<Result<Vec<_>, _>>()?;
            format!(" WHERE ({}) > ({})", keys.join(", "), lits.join(", "))
        }
    };
    Ok(format!(
        "SELECT {} FROM {}{filter} ORDER BY {} LIMIT {limit}",
        cols.join(", "),
        t.name.quoted(),
        keys.join(", ")
    ))
}

/// The key values of a canonical row (in `t.def` order).
pub fn key_of(t: &TableInfo, row: &[Value]) -> Vec<Value> {
    t.key
        .iter()
        .map(|k| {
            t.def
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(k))
                .and_then(|i| row.get(i).cloned())
                .unwrap_or(Value::Null)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table() -> TableInfo {
        let def = TableDef {
            columns: vec![
                ColumnDef::new("id", "bigint", false),
                ColumnDef::new("sub", "varchar(10)", false),
                ColumnDef::new("v", "varbinary(4)", true),
            ],
            key: vec!["id".into(), "sub".into()],
        };
        info(&TableName::new("d", "t"), def, &[], &Lossy::none()).expect("info")
    }

    #[test]
    fn chunks_page_by_the_key() {
        let t = table();
        assert_eq!(
            chunk_sql(&t, None, 10).expect("sql"),
            "SELECT `id`, `sub`, `v` FROM `d`.`t` ORDER BY `id`, `sub` LIMIT 10"
        );
        assert_eq!(
            chunk_sql(&t, Some(&[json!(5), json!("x")]), 10).expect("sql"),
            "SELECT `id`, `sub`, `v` FROM `d`.`t` WHERE (`id`, `sub`) > (5, CONVERT(X'78' USING utf8mb4)) ORDER BY `id`, `sub` LIMIT 10"
        );
        assert_eq!(
            key_of(&t, &[json!(1), json!("a"), json!(null)]),
            [json!(1), json!("a")]
        );
    }

    #[test]
    fn literals_by_kind() {
        let bin = ColumnDef::new("b", "blob", true);
        assert_eq!(literal(&json!("AP8="), &bin), Ok("X'00FF'".into()));
        let ts = ColumnDef::new("t", "timestamp(6)", true);
        assert_eq!(
            literal(&json!("2024-01-02T03:04:05.000006Z"), &ts),
            Ok(quote_str("2024-01-02 03:04:05.000006"))
        );
        assert_eq!(literal(&json!(null), &ts), Ok("NULL".into()));
        assert!(literal(&json!("!"), &bin).is_err());
    }

    #[test]
    fn a_table_without_a_key_is_refused() {
        let def = TableDef {
            columns: vec![ColumnDef::new("a", "int", true)],
            key: vec![],
        };
        assert!(info(&TableName::new("d", "t"), def.clone(), &[], &Lossy::none()).is_err());
        assert!(
            info(
                &TableName::new("d", "t"),
                def,
                &["a".into()],
                &Lossy::none()
            )
            .is_ok()
        );
    }
}
