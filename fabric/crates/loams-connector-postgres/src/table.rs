//! Tables: their columns and key from the catalog, and the readers that
//! turn their rows into Arrow batches within a budget.
//!
//! - Postgres reads with binary `COPY … TO STDOUT (FORMAT binary)` (the bulk
//!   path, D356): a whole-table stream inside the exported snapshot for the
//!   CDC snapshot, and key-ordered pages for batch reads.
//! - CockroachDB has no binary `COPY TO` (R0.20), so it pages with `SELECT …
//!   AS OF SYSTEM TIME`, decoding the same binary values from result rows.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use bytes::Bytes;
use futures_util::StreamExt;
use loams_connector_sdk::schema::{Lossy, NativeType, TypeMapper};
use loams_connector_sdk::{Budget, ConnectorError};
use tokio_postgres::Client;
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use tokio_postgres::types::{FromSql, Type};

use crate::config::TableName;
use crate::conn::classify;
use crate::rows::{RawRow, build_batch, row_cost};
use crate::types::{ColKind, PgTypeMapper, quote_ident, quote_literal};

/// One column as the catalog describes it.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    /// `format_type(atttypid, atttypmod)`.
    pub type_name: String,
    pub nullable: bool,
    pub kind: ColKind,
}

/// A table: its columns, key and Arrow schema.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: TableName,
    pub columns: Vec<Column>,
    /// The primary key (or the configured key), in key order.
    pub key: Vec<String>,
    pub schema: SchemaRef,
}

impl TableInfo {
    /// Build from catalog rows `(name, type, nullable)`; types the mapper
    /// refuses are errors (`SchemaIncompatible`: an unmappable or lossy
    /// column pauses the instance, R5.1).
    pub fn from_columns(
        name: TableName,
        cols: Vec<(String, String, bool)>,
        key: Vec<String>,
        lossy: &Lossy,
    ) -> Result<Self, ConnectorError> {
        let mapper = PgTypeMapper::new(lossy.clone());
        let mut fields = Vec::with_capacity(cols.len());
        let mut columns = Vec::with_capacity(cols.len());
        for (col, type_name, nullable) in cols {
            let native = NativeType::new(format!("{name}.{col}"), type_name.clone(), nullable);
            let field = mapper.to_arrow(&native)?;
            let kind = ColKind::of(&field, &type_name).ok_or_else(|| {
                ConnectorError::schema_incompatible(format!(
                    "{name}.{col}: no value codec for {type_name}"
                ))
            })?;
            columns.push(Column {
                name: col,
                type_name,
                nullable,
                kind,
            });
            fields.push(field);
        }
        Ok(Self {
            name,
            columns,
            key,
            schema: Arc::new(Schema::new(fields)),
        })
    }

    pub fn kinds(&self) -> Vec<ColKind> {
        self.columns.iter().map(|c| c.kind).collect()
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// The column list, quoted.
    pub fn select_list(&self) -> String {
        self.columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Read a table's columns and primary key from the catalog. `key`
/// overrides the primary key when not empty.
pub async fn describe(
    client: &Client,
    table: &TableName,
    key: &[String],
    lossy: &Lossy,
) -> Result<TableInfo, ConnectorError> {
    let rows = client
        .query(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod)::text, NOT a.attnotnull \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = $1::text::regclass AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&table.quoted()],
        )
        .await
        .map_err(|e| classify(&e, &format!("describing {table}")))?;
    if rows.is_empty() {
        return Err(ConnectorError::fatal(format!(
            "{table} has no columns or does not exist"
        )));
    }
    let cols = rows
        .iter()
        .map(|r| {
            (
                r.get::<_, String>(0),
                r.get::<_, String>(1),
                r.get::<_, bool>(2),
            )
        })
        .collect::<Vec<_>>();
    let key = if key.is_empty() {
        primary_key(client, table).await?
    } else {
        key.to_vec()
    };
    TableInfo::from_columns(table.clone(), cols, key, lossy)
}

/// The primary key's columns, in key order (`information_schema`, which
/// Postgres and CockroachDB both serve).
pub async fn primary_key(
    client: &Client,
    table: &TableName,
) -> Result<Vec<String>, ConnectorError> {
    let rows = client
        .query(
            "SELECT k.column_name::text FROM information_schema.table_constraints c \
             JOIN information_schema.key_column_usage k \
               ON k.constraint_name = c.constraint_name AND k.table_schema = c.table_schema \
              AND k.table_name = c.table_name \
             WHERE c.constraint_type = 'PRIMARY KEY' AND c.table_schema = $1 AND c.table_name = $2 \
             ORDER BY k.ordinal_position",
            &[&table.schema, &table.name],
        )
        .await
        .map_err(|e| classify(&e, &format!("reading {table}'s primary key")))?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Any value, undecoded: the readers decode by [`ColKind`].
struct RawRef<'a>(&'a [u8]);

impl<'a> FromSql<'a> for RawRef<'a> {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RawRef(raw))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

/// Take rows from the front of `pending` into one batch within `budget`
/// (rows and bytes, after `overhead` for the envelope), with the last row
/// taken (values past the table's columns, such as a page's key text, ride
/// along and are not in the batch). `Ok(None)` when `pending` is empty; a
/// single row larger than the budget is `Poison` (R4.7) and is dropped.
pub fn batch_within(
    table: &TableInfo,
    pending: &mut VecDeque<RawRow>,
    budget: Budget,
    overhead: usize,
) -> Result<Option<(RecordBatch, RawRow)>, ConnectorError> {
    if pending.is_empty() {
        return Ok(None);
    }
    let max_bytes = budget.max_bytes.saturating_sub(overhead);
    let mut n = 0;
    let mut cost = 0;
    for r in pending.iter() {
        let c = row_cost(r);
        if n >= budget.max_rows || (n > 0 && cost + c > max_bytes / 2) {
            break;
        }
        cost += c;
        n += 1;
    }
    let kinds = table.kinds();
    loop {
        let rows: Vec<RawRow> = pending.iter().take(n).cloned().collect();
        let batch = build_batch(&table.schema, &kinds, &rows)
            .map_err(|e| ConnectorError::poison(format!("{}: {e}", table.name)))?;
        if batch.get_array_memory_size() <= max_bytes {
            let last = pending.drain(..n).next_back().expect("n >= 1");
            return Ok(Some((batch, last)));
        }
        if n == 1 {
            pending.pop_front();
            return Err(ConnectorError::poison(format!(
                "{}: a row of {} bytes is larger than the batch budget of {} bytes",
                table.name,
                batch.get_array_memory_size(),
                budget.max_bytes
            )));
        }
        n /= 2;
    }
}

type RowStream = Pin<Box<BinaryCopyOutStream>>;

/// The whole table as one binary `COPY` stream (the CDC snapshot, inside
/// the exported snapshot's transaction).
pub struct CopyStream {
    stream: Option<RowStream>,
    ncols: usize,
}

impl CopyStream {
    pub async fn open(client: &Client, table: &TableInfo) -> Result<Self, ConnectorError> {
        let sql = format!(
            "COPY (SELECT {} FROM {}) TO STDOUT (FORMAT binary)",
            table.select_list(),
            table.name.quoted()
        );
        let copy = client
            .copy_out(sql.as_str())
            .await
            .map_err(|e| classify(&e, &format!("COPY {}", table.name)))?;
        let types = vec![Type::BYTEA; table.columns.len()];
        Ok(Self {
            stream: Some(Box::pin(BinaryCopyOutStream::new(copy, &types))),
            ncols: table.columns.len(),
        })
    }

    /// Read up to `max_rows` rows or about `max_cost` bytes into `pending`.
    /// `false` once the table is exhausted.
    pub async fn fill(
        &mut self,
        pending: &mut VecDeque<RawRow>,
        max_rows: usize,
        max_cost: usize,
    ) -> Result<bool, ConnectorError> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(false);
        };
        let mut rows = 0;
        let mut cost = 0;
        while rows < max_rows && cost < max_cost {
            match stream.next().await {
                None => {
                    self.stream = None;
                    return Ok(false);
                }
                Some(Err(e)) => return Err(classify(&e, "COPY")),
                Some(Ok(row)) => {
                    let mut raw = Vec::with_capacity(self.ncols);
                    for i in 0..self.ncols {
                        let v: Option<RawRef<'_>> = row
                            .try_get(i)
                            .map_err(|e| ConnectorError::poison(format!("COPY column {i}: {e}")))?;
                        raw.push(v.map(|r| Bytes::copy_from_slice(r.0)));
                    }
                    cost += row_cost(&raw);
                    rows += 1;
                    pending.push_back(raw);
                }
            }
        }
        Ok(true)
    }
}

impl std::fmt::Debug for CopyStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyStream")
            .field("open", &self.stream.is_some())
            .finish_non_exhaustive()
    }
}

/// Key-ordered pages: `WHERE (key) > (last) ORDER BY key LIMIT n`, with
/// the key's text appended to each row so the next page can start after it.
#[derive(Debug, Clone)]
pub struct Pager {
    /// CockroachDB: `SELECT … AS OF SYSTEM TIME <as_of>`; else binary COPY.
    pub cockroach_as_of: Option<String>,
}

impl Pager {
    /// The page query after `last` (the key's text values).
    pub fn page_sql(&self, table: &TableInfo, last: Option<&[String]>, limit: usize) -> String {
        let keys: Vec<String> = table.key.iter().map(|k| quote_ident(k)).collect();
        let key_text: Vec<String> = keys.iter().map(|k| format!("{k}::text")).collect();
        let mut sql = format!(
            "SELECT {}, {} FROM {}",
            table.select_list(),
            key_text.join(", "),
            table.name.quoted()
        );
        if let Some(ts) = &self.cockroach_as_of {
            sql.push_str(&format!(" AS OF SYSTEM TIME {}", quote_literal(ts)));
        }
        if let Some(last) = last {
            let lits: Vec<String> = table
                .key
                .iter()
                .zip(last)
                .map(|(k, v)| {
                    let ty = table.column(k).map_or("text", |c| c.type_name.as_str());
                    format!("{}::{ty}", quote_literal(v))
                })
                .collect();
            sql.push_str(&format!(
                " WHERE ({}) > ({})",
                keys.join(", "),
                lits.join(", ")
            ));
        }
        sql.push_str(&format!(" ORDER BY {} LIMIT {limit}", keys.join(", ")));
        if self.cockroach_as_of.is_none() {
            sql = format!("COPY ({sql}) TO STDOUT (FORMAT binary)");
        }
        sql
    }

    /// Fetch one page into `pending`. Each row carries its key's text
    /// after the table's columns. Returns the rows fetched and whether the
    /// page was full (more may follow).
    pub async fn fetch(
        &self,
        client: &Client,
        table: &TableInfo,
        last: Option<&[String]>,
        limit: usize,
        pending: &mut VecDeque<RawRow>,
    ) -> Result<(usize, bool), ConnectorError> {
        let sql = self.page_sql(table, last, limit);
        let width = table.columns.len() + table.key.len();
        let mut n = 0;
        if self.cockroach_as_of.is_some() {
            let rows = client
                .query_raw(sql.as_str(), std::iter::empty::<i32>())
                .await
                .map_err(|e| classify(&e, &format!("reading {}", table.name)))?;
            futures_util::pin_mut!(rows);
            while let Some(row) = rows.next().await {
                let row = row.map_err(|e| classify(&e, &format!("reading {}", table.name)))?;
                let mut values = Vec::with_capacity(width);
                for i in 0..width {
                    let v: Option<RawRef<'_>> = row
                        .try_get(i)
                        .map_err(|e| ConnectorError::poison(format!("column {i}: {e}")))?;
                    values.push(v.map(|r| Bytes::copy_from_slice(r.0)));
                }
                pending.push_back(values);
                n += 1;
            }
        } else {
            let copy = client
                .copy_out(sql.as_str())
                .await
                .map_err(|e| classify(&e, &format!("COPY {}", table.name)))?;
            let types = vec![Type::BYTEA; width];
            let stream = BinaryCopyOutStream::new(copy, &types);
            futures_util::pin_mut!(stream);
            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| classify(&e, &format!("COPY {}", table.name)))?;
                let mut values = Vec::with_capacity(width);
                for i in 0..width {
                    let v: Option<RawRef<'_>> = row
                        .try_get(i)
                        .map_err(|e| ConnectorError::poison(format!("column {i}: {e}")))?;
                    values.push(v.map(|r| Bytes::copy_from_slice(r.0)));
                }
                pending.push_back(values);
                n += 1;
            }
        }
        Ok((n, n == limit))
    }
}

/// The key text a paged row carries after the table's columns.
pub fn key_text(table: &TableInfo, row: &RawRow) -> Result<Vec<String>, ConnectorError> {
    row.iter()
        .skip(table.columns.len())
        .map(|k| {
            k.as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .ok_or_else(|| ConnectorError::fatal(format!("{}: a NULL key value", table.name)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> TableInfo {
        TableInfo::from_columns(
            TableName::parse("public.orders").expect("name"),
            vec![
                ("id".into(), "bigint".into(), false),
                ("region".into(), "text".into(), false),
                ("total".into(), "numeric(10,2)".into(), true),
            ],
            vec!["region".into(), "id".into()],
            &Lossy::none(),
        )
        .expect("table")
    }

    #[test]
    fn page_queries_use_copy_on_postgres_and_select_on_cockroach() {
        let t = info();
        let pg = Pager {
            cockroach_as_of: None,
        };
        assert_eq!(
            pg.page_sql(&t, None, 100),
            "COPY (SELECT \"id\", \"region\", \"total\", \"region\"::text, \"id\"::text FROM \"public\".\"orders\" \
             ORDER BY \"region\", \"id\" LIMIT 100) TO STDOUT (FORMAT binary)"
        );
        let crdb = Pager {
            cockroach_as_of: Some("1700000000000000000.0000000000".into()),
        };
        let sql = crdb.page_sql(&t, Some(&["eu".into(), "4'2".into()]), 10);
        assert!(sql.starts_with("SELECT "), "{sql}");
        assert!(!sql.contains("COPY"), "{sql}");
        assert!(
            sql.contains("AS OF SYSTEM TIME '1700000000000000000.0000000000'"),
            "{sql}"
        );
        assert!(
            sql.contains("WHERE (\"region\", \"id\") > ('eu'::text, '4''2'::bigint)"),
            "{sql}"
        );
    }

    #[test]
    fn unmappable_columns_pause() {
        let err = TableInfo::from_columns(
            TableName::parse("t").expect("name"),
            vec![("tags".into(), "text[]".into(), true)],
            vec![],
            &Lossy::none(),
        )
        .expect_err("arrays are unsupported");
        assert_eq!(
            err.class,
            loams_connector_sdk::ErrorClass::SchemaIncompatible
        );
    }

    #[test]
    fn batches_respect_the_budget_and_poison_oversized_rows() {
        let t = TableInfo::from_columns(
            TableName::parse("t").expect("name"),
            vec![("v".into(), "text".into(), true)],
            vec![],
            &Lossy::none(),
        )
        .expect("table");
        let mut pending: VecDeque<RawRow> = (0..100)
            .map(|i| vec![Some(Bytes::from(format!("row-{i}")))])
            .collect();
        let budget = Budget {
            max_rows: 30,
            max_bytes: 1 << 20,
        };
        let (b, last) = batch_within(&t, &mut pending, budget, 0)
            .expect("ok")
            .expect("batch");
        assert_eq!(b.num_rows(), 30);
        assert_eq!(last[0].as_deref(), Some(b"row-29".as_slice()));
        assert_eq!(pending.len(), 70);
        let small = Budget {
            max_rows: 1000,
            max_bytes: 4096,
        };
        let (b, _) = batch_within(&t, &mut pending, small, 0)
            .expect("ok")
            .expect("batch");
        assert!(b.get_array_memory_size() <= 4096 && b.num_rows() >= 1);
        let mut huge: VecDeque<RawRow> = vec![vec![Some(Bytes::from(vec![b'x'; 10_000]))]].into();
        let err = batch_within(&t, &mut huge, small, 0).expect_err("too big");
        assert_eq!(err.class, loams_connector_sdk::ErrorClass::Poison);
        assert!(huge.is_empty());
    }
}
