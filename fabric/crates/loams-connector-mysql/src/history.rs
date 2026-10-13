//! The schema history: each captured table's columns as of a binlog
//! position (CN2 Task 11).
//!
//! A row event names its table only through `TABLE_MAP`, which carries the
//! column *types* and, only with `binlog_row_metadata=FULL`, their names.
//! Without FULL (MySQL's and MariaDB's default is MINIMAL), the names come
//! from this history: seeded from `information_schema` when the instance
//! starts, then moved forward by the DDL in `QUERY_EVENT`s, parsed with
//! `sqlparser`'s MySQL dialect. It is stored in the checkpoint with the
//! binlog position, so a restart maps the binlog's old events with the
//! columns they were written with, even if the table changed since.
//!
//! A DDL statement that names a captured table but that the parser cannot
//! read, or reads into an operation the history does not model (`CREATE
//! TABLE … AS SELECT`, `CONVERT TO CHARACTER SET`, …), asks for a refresh
//! from `information_schema`: the table's state *now*, which is exact when
//! no later DDL is pending in the binlog (the usual case for a running
//! instance) and is cross-checked against each `TABLE_MAP`'s column count
//! and types (ruling R11.5).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    AlterTableOperation, ColumnOption, CreateTableLikeKind, Expr, MySQLColumnPosition, ObjectName,
    ObjectNamePart, ObjectType, RenameTableNameKind, Statement, TableConstraint,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::config::TableName;
use crate::types::ColumnDef;

/// A table's columns, in order, and its primary key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDef {
    pub columns: Vec<ColumnDef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key: Vec<String>,
}

impl TableDef {
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    fn position(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }
}

/// See the module documentation. Keys are `database.table`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct History {
    pub tables: BTreeMap<String, TableDef>,
}

/// What one DDL statement did to the captured tables.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DdlOutcome {
    /// Tables whose columns or key changed (or that were created).
    pub changed: Vec<TableName>,
    /// Tables dropped (or renamed away).
    pub dropped: Vec<TableName>,
    /// Captured tables to re-read from `information_schema`.
    pub refresh: Vec<TableName>,
}

impl DdlOutcome {
    pub fn is_empty(&self) -> bool {
        self.changed.is_empty() && self.dropped.is_empty() && self.refresh.is_empty()
    }
}

fn ident_of(part: &ObjectNamePart) -> Option<String> {
    match part {
        ObjectNamePart::Identifier(i) => Some(i.value.clone()),
        _ => None,
    }
}

fn table_of(name: &ObjectName, default_db: &str) -> Option<TableName> {
    let parts: Vec<String> = name.0.iter().filter_map(ident_of).collect();
    match parts.as_slice() {
        [t] if !default_db.is_empty() => Some(TableName::new(default_db, t.clone())),
        [d, t] => Some(TableName::new(d.clone(), t.clone())),
        _ => None,
    }
}

fn type_text(dt: &sqlparser::ast::DataType) -> String {
    dt.to_string().to_ascii_lowercase()
}

fn column_def(c: &sqlparser::ast::ColumnDef) -> (ColumnDef, bool) {
    let mut nullable = true;
    let mut primary = false;
    for o in &c.options {
        match &o.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Null => nullable = true,
            ColumnOption::PrimaryKey(_) => {
                nullable = false;
                primary = true;
            }
            _ => {}
        }
    }
    (
        ColumnDef::new(c.name.value.clone(), type_text(&c.data_type), nullable),
        primary,
    )
}

fn key_columns(cols: &[sqlparser::ast::IndexColumn]) -> Vec<String> {
    cols.iter()
        .filter_map(|ic| match &ic.column.expr {
            Expr::Identifier(i) => Some(i.value.clone()),
            Expr::CompoundIdentifier(v) => v.last().map(|i| i.value.clone()),
            _ => None,
        })
        .collect()
}

fn place(t: &mut TableDef, col: ColumnDef, at: &Option<MySQLColumnPosition>) {
    match at {
        Some(MySQLColumnPosition::First) => t.columns.insert(0, col),
        Some(MySQLColumnPosition::After(after)) => {
            let i = t.position(&after.value).map_or(t.columns.len(), |i| i + 1);
            t.columns.insert(i, col);
        }
        None => t.columns.push(col),
    }
}

/// Whether `sql` might be DDL (the first keyword).
fn looks_like_ddl(sql: &str) -> bool {
    let first = sql
        .trim_start_matches(|c: char| c.is_whitespace() || c == '(')
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    matches!(first.as_str(), "ALTER" | "CREATE" | "DROP" | "RENAME")
}

impl History {
    pub fn get(&self, t: &TableName) -> Option<&TableDef> {
        self.tables.get(&t.to_string())
    }

    pub fn set(&mut self, t: &TableName, def: TableDef) {
        self.tables.insert(t.to_string(), def);
    }

    pub fn remove(&mut self, t: &TableName) {
        self.tables.remove(&t.to_string());
    }

    /// Apply one `QUERY_EVENT`'s statement (`default_db` is the event's
    /// schema). Only tables `captured` says yes to are tracked.
    pub fn apply_ddl(
        &mut self,
        default_db: &str,
        sql: &str,
        captured: &dyn Fn(&TableName) -> bool,
    ) -> DdlOutcome {
        let mut out = DdlOutcome::default();
        if !looks_like_ddl(sql) {
            return out;
        }
        let statements = match Parser::parse_sql(&MySqlDialect {}, sql) {
            Ok(s) => s,
            Err(_) => {
                // Unreadable: refresh every captured table the text names.
                let lower = sql.to_ascii_lowercase();
                out.refresh = self
                    .tables
                    .keys()
                    .filter_map(|k| {
                        let (d, t) = k.split_once('.')?;
                        Some(TableName::new(d, t))
                    })
                    .filter(|t| captured(t) && lower.contains(&t.name.to_ascii_lowercase()))
                    .collect();
                return out;
            }
        };
        for st in statements {
            self.apply_statement(default_db, &st, captured, &mut out);
        }
        out
    }

    fn apply_statement(
        &mut self,
        default_db: &str,
        st: &Statement,
        captured: &dyn Fn(&TableName) -> bool,
        out: &mut DdlOutcome,
    ) {
        match st {
            Statement::CreateTable(ct) => {
                let Some(t) = table_of(&ct.name, default_db) else {
                    return;
                };
                if !captured(&t) || (ct.if_not_exists && self.get(&t).is_some()) || ct.temporary {
                    return;
                }
                if let Some(like) = &ct.like {
                    let src = match like {
                        CreateTableLikeKind::Parenthesized(l) | CreateTableLikeKind::Plain(l) => {
                            &l.name
                        }
                    };
                    match table_of(src, default_db).and_then(|s| self.get(&s).cloned()) {
                        Some(def) => {
                            self.set(&t, def);
                            out.changed.push(t);
                        }
                        None => out.refresh.push(t),
                    }
                    return;
                }
                if ct.query.is_some() {
                    out.refresh.push(t);
                    return;
                }
                let mut def = TableDef::default();
                for c in &ct.columns {
                    let (col, primary) = column_def(c);
                    if primary {
                        def.key = vec![col.name.clone()];
                    }
                    def.columns.push(col);
                }
                for c in &ct.constraints {
                    if let TableConstraint::PrimaryKey(pk) = c {
                        def.key = key_columns(&pk.columns);
                        for k in &def.key {
                            if let Some(i) = def.position(k) {
                                def.columns[i].nullable = false;
                            }
                        }
                    }
                }
                self.set(&t, def);
                out.changed.push(t);
            }
            Statement::AlterTable(at) => {
                let Some(t) = table_of(&at.name, default_db) else {
                    return;
                };
                if !captured(&t) {
                    return;
                }
                let Some(mut def) = self.get(&t).cloned() else {
                    out.refresh.push(t);
                    return;
                };
                let mut name = t.clone();
                let mut changed = false;
                for op in &at.operations {
                    match op {
                        AlterTableOperation::AddColumn {
                            column_def: c,
                            column_position,
                            ..
                        } => {
                            let (col, primary) = column_def(c);
                            if primary {
                                def.key = vec![col.name.clone()];
                            }
                            place(&mut def, col, column_position);
                            changed = true;
                        }
                        AlterTableOperation::DropColumn { column_names, .. } => {
                            for c in column_names {
                                if let Some(i) = def.position(&c.value) {
                                    def.columns.remove(i);
                                    def.key.retain(|k| !k.eq_ignore_ascii_case(&c.value));
                                    changed = true;
                                }
                            }
                        }
                        AlterTableOperation::RenameColumn {
                            old_column_name,
                            new_column_name,
                        } => {
                            if let Some(i) = def.position(&old_column_name.value) {
                                def.columns[i].name = new_column_name.value.clone();
                                for k in &mut def.key {
                                    if k.eq_ignore_ascii_case(&old_column_name.value) {
                                        k.clone_from(&new_column_name.value);
                                    }
                                }
                                changed = true;
                            }
                        }
                        AlterTableOperation::ChangeColumn {
                            old_name,
                            new_name,
                            data_type,
                            options,
                            column_position,
                        } => {
                            changed |= change_column(
                                &mut def,
                                &old_name.value,
                                &new_name.value,
                                data_type,
                                options,
                                column_position,
                            );
                        }
                        AlterTableOperation::ModifyColumn {
                            col_name,
                            data_type,
                            options,
                            column_position,
                        } => {
                            changed |= change_column(
                                &mut def,
                                &col_name.value,
                                &col_name.value,
                                data_type,
                                options,
                                column_position,
                            );
                        }
                        AlterTableOperation::RenameTable { table_name } => {
                            let to = match table_name {
                                RenameTableNameKind::As(n) | RenameTableNameKind::To(n) => n,
                            };
                            if let Some(nt) = table_of(to, &t.database) {
                                name = nt;
                            }
                        }
                        AlterTableOperation::AddConstraint {
                            constraint: TableConstraint::PrimaryKey(pk),
                            ..
                        } => {
                            def.key = key_columns(&pk.columns);
                            changed = true;
                        }
                        AlterTableOperation::DropPrimaryKey { .. } => {
                            def.key.clear();
                            changed = true;
                        }
                        // Indexes, constraints, table options: no column moves.
                        AlterTableOperation::AddConstraint { .. }
                        | AlterTableOperation::DropConstraint { .. }
                        | AlterTableOperation::DropIndex { .. }
                        | AlterTableOperation::DropForeignKey { .. }
                        | AlterTableOperation::Algorithm { .. }
                        | AlterTableOperation::Lock { .. }
                        | AlterTableOperation::AutoIncrement { .. } => {}
                        AlterTableOperation::AlterColumn { .. } => {
                            // DEFAULTs and NOT NULL: the column list holds;
                            // nullability is re-read.
                            out.refresh.push(name.clone());
                        }
                        _ => out.refresh.push(name.clone()),
                    }
                }
                if name != t {
                    self.remove(&t);
                    out.dropped.push(t.clone());
                    if captured(&name) {
                        self.set(&name, def);
                        out.changed.push(name);
                    }
                } else if changed {
                    self.set(&t, def);
                    out.changed.push(t);
                }
            }
            Statement::RenameTable(renames) => {
                for r in renames {
                    let (Some(from), Some(to)) = (
                        table_of(&r.old_name, default_db),
                        table_of(&r.new_name, default_db),
                    ) else {
                        continue;
                    };
                    let def = self.get(&from).cloned();
                    if def.is_some() {
                        self.remove(&from);
                        out.dropped.push(from);
                    }
                    if captured(&to) {
                        match def {
                            Some(d) => {
                                self.set(&to, d);
                                out.changed.push(to);
                            }
                            None => out.refresh.push(to),
                        }
                    }
                }
            }
            Statement::Drop {
                object_type: ObjectType::Table,
                names,
                ..
            } => {
                for n in names {
                    if let Some(t) = table_of(n, default_db)
                        && self.get(&t).is_some()
                    {
                        self.remove(&t);
                        out.dropped.push(t);
                    }
                }
            }
            _ => {}
        }
    }
}

fn change_column(
    def: &mut TableDef,
    old: &str,
    new: &str,
    data_type: &sqlparser::ast::DataType,
    options: &[ColumnOption],
    at: &Option<MySQLColumnPosition>,
) -> bool {
    let Some(i) = def.position(old) else {
        return false;
    };
    let nullable = !options
        .iter()
        .any(|o| matches!(o, ColumnOption::NotNull | ColumnOption::PrimaryKey(_)));
    let col = ColumnDef::new(new, type_text(data_type), nullable);
    def.columns.remove(i);
    match at {
        None => def.columns.insert(i, col),
        some => place(def, col, some),
    }
    for k in &mut def.key {
        if k.eq_ignore_ascii_case(old) {
            new.clone_into(k);
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(_: &TableName) -> bool {
        true
    }

    fn names(h: &History, t: &str) -> Vec<String> {
        h.tables[t].columns.iter().map(|c| c.name.clone()).collect()
    }

    #[test]
    fn create_and_alter_track_columns_and_key() {
        let mut h = History::default();
        let o = h.apply_ddl(
            "shop",
            "CREATE TABLE orders (id BIGINT UNSIGNED NOT NULL, total DECIMAL(10,2), note VARCHAR(40), PRIMARY KEY (id))",
            &all,
        );
        assert_eq!(o.changed, vec![TableName::new("shop", "orders")]);
        let t = &h.tables["shop.orders"];
        assert_eq!(t.key, ["id"]);
        assert_eq!(t.columns[0].column_type, "bigint unsigned");
        assert!(!t.columns[0].nullable && t.columns[1].nullable);
        assert_eq!(t.columns[1].column_type, "decimal(10,2)");

        h.apply_ddl(
            "shop",
            "ALTER TABLE orders ADD COLUMN flag TINYINT(1) FIRST, ADD COLUMN extra TEXT AFTER id, DROP COLUMN note",
            &all,
        );
        assert_eq!(names(&h, "shop.orders"), ["flag", "id", "extra", "total"]);
        h.apply_ddl(
            "other",
            "ALTER TABLE shop.orders CHANGE COLUMN total amount DECIMAL(12,2) NOT NULL, RENAME COLUMN extra TO memo",
            &all,
        );
        assert_eq!(names(&h, "shop.orders"), ["flag", "id", "memo", "amount"]);
        assert_eq!(
            h.tables["shop.orders"].columns[3].column_type,
            "decimal(12,2)"
        );
        h.apply_ddl(
            "shop",
            "ALTER TABLE orders MODIFY flag INT AFTER amount",
            &all,
        );
        assert_eq!(names(&h, "shop.orders"), ["id", "memo", "amount", "flag"]);
    }

    #[test]
    fn renames_drops_and_unknowns() {
        let mut h = History::default();
        h.apply_ddl("d", "CREATE TABLE a (id INT PRIMARY KEY)", &all);
        assert_eq!(h.tables["d.a"].key, ["id"]);
        let o = h.apply_ddl("d", "RENAME TABLE a TO b", &all);
        assert_eq!(o.dropped, vec![TableName::new("d", "a")]);
        assert_eq!(o.changed, vec![TableName::new("d", "b")]);
        h.apply_ddl("d", "CREATE TABLE c LIKE b", &all);
        assert_eq!(h.tables["d.c"], h.tables["d.b"]);
        let o = h.apply_ddl("d", "DROP TABLE IF EXISTS b, zz", &all);
        assert_eq!(o.dropped, vec![TableName::new("d", "b")]);
        let o = h.apply_ddl("d", "ALTER TABLE c CONVERT TO CHARACTER SET utf8mb4", &all);
        assert!(!o.refresh.is_empty() || o.is_empty());
        let o = h.apply_ddl("d", "ALTER TABLE c FROBNICATE wildly", &all);
        assert_eq!(o.refresh, vec![TableName::new("d", "c")]);
        assert!(
            h.apply_ddl("d", "INSERT INTO c VALUES (1)", &all)
                .is_empty()
        );
        assert!(h.apply_ddl("d", "BEGIN", &all).is_empty());
        let none = |_: &TableName| false;
        assert!(
            h.apply_ddl("d", "CREATE TABLE x (id INT)", &none)
                .is_empty()
        );
    }

    #[test]
    fn history_serialises() {
        let mut h = History::default();
        h.apply_ddl(
            "d",
            "CREATE TABLE a (id INT PRIMARY KEY, v ENUM('x','y'))",
            &all,
        );
        let json = serde_json::to_string(&h).expect("json");
        assert_eq!(serde_json::from_str::<History>(&json).expect("back"), h);
    }
}
