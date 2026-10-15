//! A described table and the SQL every reader builds from it: the select
//! list, keyset paging (`WHERE (key) > (last)` written out column by column,
//! since T-SQL has no row comparison), and budget-sized Arrow batches.

use std::collections::VecDeque;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use loams_connector_sdk::schema::Lossy;
use loams_connector_sdk::{Budget, ConnectorError};

use crate::config::TableName;
use crate::conn::{self, SqlClient};
use crate::types::{
    Cell, Column, SqlServerTypeMapper, arrow_schema, build_batch, cast_expr, quote_ident,
};

/// A table as a reader sees it.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: TableName,
    pub columns: Vec<Column>,
    /// The columns rows are ordered and paged by.
    pub key: Vec<String>,
    pub schema: SchemaRef,
}

impl TableInfo {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// `[a], [b], …`, in column order.
    pub fn select_list(&self) -> String {
        self.columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The positions of `cols` in the column list.
    pub fn indexes(&self, cols: &[String]) -> Result<Vec<usize>, ConnectorError> {
        cols.iter()
            .map(|k| {
                self.columns
                    .iter()
                    .position(|c| &c.name == k)
                    .ok_or_else(|| {
                        ConnectorError::fatal(format!("{} has no column {k}", self.name))
                    })
            })
            .collect()
    }

    /// The declared types of `cols`.
    pub fn declared(&self, cols: &[String]) -> Result<Vec<(String, String)>, ConnectorError> {
        self.indexes(cols).map(|ix| {
            ix.into_iter()
                .map(|i| {
                    (
                        self.columns[i].name.clone(),
                        self.columns[i].declared.clone(),
                    )
                })
                .collect()
        })
    }
}

/// Describe `name` for reading: columns, Arrow schema, and the key (the
/// configured one, else the primary key).
pub async fn describe(
    client: &mut SqlClient,
    name: &TableName,
    key: &[String],
    lossy: &Lossy,
) -> Result<TableInfo, ConnectorError> {
    let columns = conn::describe(client, name).await?;
    let key = if key.is_empty() {
        conn::primary_key(client, name).await?
    } else {
        key.to_vec()
    };
    let schema = arrow_schema(
        &name.to_string(),
        &columns,
        &SqlServerTypeMapper::new(lossy.clone()),
    )?;
    let t = TableInfo {
        name: name.clone(),
        columns,
        key,
        schema,
    };
    t.indexes(&t.key)?;
    Ok(t)
}

/// `(c1, c2, …) > (@Pn, @Pn+1, …)` written out:
/// `(c1 > v1) OR (c1 = v1 AND c2 > v2) OR …`, each value cast from text to
/// its column's type. `first` is the first parameter's number.
pub fn after(cols: &[(String, String)], first: usize) -> String {
    let p = |i: usize| cast_expr(&format!("@P{}", first + i), &cols[i].1);
    let terms: Vec<String> = (0..cols.len())
        .map(|i| {
            let mut t: Vec<String> = (0..i)
                .map(|j| format!("{} = {}", quote_ident(&cols[j].0), p(j)))
                .collect();
            t.push(format!("{} > {}", quote_ident(&cols[i].0), p(i)));
            format!("({})", t.join(" AND "))
        })
        .collect();
    format!("({})", terms.join(" OR "))
}

/// `ORDER BY [c1], [c2], …`.
pub fn order_by(cols: &[String]) -> String {
    format!(
        "ORDER BY {}",
        cols.iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// The rough size of a row, for budgets.
pub fn row_cost(r: &[Cell]) -> usize {
    r.iter().map(Cell::cost).sum::<usize>() + 16
}

/// The longest prefix of `pending` that fits `budget` as one Arrow batch,
/// and its last row; `None` when nothing is pending. A single row too large
/// for the budget is dropped as `Poison`.
pub fn batch_within(
    table: &TableInfo,
    pending: &mut VecDeque<Vec<Cell>>,
    budget: Budget,
    overhead: usize,
) -> Result<Option<(RecordBatch, Vec<Cell>)>, ConnectorError> {
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
    loop {
        let rows: Vec<Vec<Cell>> = pending.iter().take(n).cloned().collect();
        let batch = build_batch(&table.schema, &rows)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyset_paging_is_written_out_per_column() {
        let cols = vec![
            ("region".to_owned(), "nvarchar(8)".to_owned()),
            ("id".to_owned(), "bigint".to_owned()),
        ];
        assert_eq!(
            after(&cols, 2),
            "(([region] > CAST(@P2 AS nvarchar(8))) OR ([region] = CAST(@P2 AS nvarchar(8)) \
             AND [id] > CAST(@P3 AS bigint)))"
        );
        assert_eq!(order_by(&["a".into(), "b]".into()]), "ORDER BY [a], [b]]]");
    }
}
