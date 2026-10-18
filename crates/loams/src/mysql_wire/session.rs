//! The session statements MySQL drivers send at connect and around queries
//! (0.1.0 release e2e, B15).
//!
//! The wire surface is read-only: it runs one `SELECT` at a time over the
//! collection SQL engine. Drivers send more than that before the first query
//! and between queries, and refuse to connect when it fails:
//!
//! - PyMySQL: `SET NAMES utf8mb4`, `SET AUTOCOMMIT = 0`, `COMMIT`, `ROLLBACK`,
//!   `SHOW WARNINGS`;
//! - mysql-connector-python: `SET NAMES 'utf8mb4' COLLATE '…'`,
//!   `SET @@session.autocommit = OFF`;
//! - Connector/J: a comment-prefixed `SELECT @@session.auto_increment_increment
//!   AS auto_increment_increment, @@character_set_client AS …, …`, then
//!   `SET NAMES utf8mb4`, `SET character_set_results = NULL`, `SET autocommit=1`;
//! - go-sql-driver: `SET NAMES …` and `SET <param>=<value>` from the DSN,
//!   `SELECT @@max_allowed_packet`, `START TRANSACTION`, `COMMIT`;
//! - mysql2 (Node) and mysql_async: `SELECT @@…` reads of a few variables,
//!   `SELECT @@socket`.
//!
//! [`Session::answer`] recognises these and answers them from a per-connection
//! table of session variables: `SET` updates it (an `OK`), a `SELECT` of only
//! variables, literals and the session functions (`VERSION()`, `DATABASE()`,
//! `CONNECTION_ID()`, …) reads it, `SHOW VARIABLES` lists it, and the
//! transaction statements are accepted as no-ops (every statement is a
//! read-only autocommit `SELECT`). Anything else is not a session statement
//! and goes to the SQL engine as before.

use std::collections::BTreeMap;

use opensrv_mysql::ErrorKind;

/// The version every client sees, in the handshake and in `@@version`. A
/// MySQL 8.0 level, which is what current drivers and ORMs expect.
pub(super) const SERVER_VERSION: &str = "8.0.36-loams";

/// A session variable's value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Value {
    Int(i64),
    Text(String),
    Null,
}

/// What a session statement answers.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Answer {
    /// An `OK` packet.
    Ok,
    /// A result set: column names and rows.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    /// An error.
    Refuse(ErrorKind, String),
}

/// One connection's session variables.
#[derive(Debug)]
pub(super) struct Session {
    vars: BTreeMap<String, Value>,
    user_vars: BTreeMap<String, Value>,
    database: String,
    connection_id: u32,
}

/// The session variables a new connection starts with.
fn defaults() -> BTreeMap<String, Value> {
    use Value::{Int, Null, Text};
    let text = |s: &str| Text(s.to_owned());
    [
        ("auto_increment_increment", Int(1)),
        ("auto_increment_offset", Int(1)),
        ("autocommit", Int(1)),
        ("character_set_client", text("utf8mb4")),
        ("character_set_connection", text("utf8mb4")),
        ("character_set_database", text("utf8mb4")),
        ("character_set_results", text("utf8mb4")),
        ("character_set_server", text("utf8mb4")),
        ("character_set_system", text("utf8mb3")),
        ("collation_connection", text("utf8mb4_0900_ai_ci")),
        ("collation_database", text("utf8mb4_0900_ai_ci")),
        ("collation_server", text("utf8mb4_0900_ai_ci")),
        ("default_storage_engine", text("InnoDB")),
        ("foreign_key_checks", Int(1)),
        ("have_query_cache", text("NO")),
        ("init_connect", text("")),
        ("interactive_timeout", Int(28_800)),
        ("license", text("Apache-2.0")),
        ("lower_case_table_names", Int(0)),
        ("max_allowed_packet", Int(67_108_864)),
        ("max_execution_time", Int(0)),
        ("net_buffer_length", Int(16_384)),
        ("net_read_timeout", Int(30)),
        ("net_write_timeout", Int(60)),
        ("performance_schema", Int(0)),
        ("query_cache_size", Int(0)),
        ("query_cache_type", text("OFF")),
        ("read_only", Int(1)),
        ("socket", Null),
        ("sql_auto_is_null", Int(0)),
        ("sql_mode", text("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")),
        ("sql_select_limit", Int(i64::MAX)),
        ("system_time_zone", text("UTC")),
        ("time_zone", text("SYSTEM")),
        ("transaction_isolation", text("REPEATABLE-READ")),
        ("transaction_read_only", Int(0)),
        ("tx_isolation", text("REPEATABLE-READ")),
        ("tx_read_only", Int(0)),
        ("version", text(SERVER_VERSION)),
        ("version_comment", text("Loams MySQL wire (read-only)")),
        ("wait_timeout", Int(28_800)),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value))
    .collect()
}

impl Session {
    pub(super) fn new(database: impl Into<String>, connection_id: u32) -> Self {
        Self {
            vars: defaults(),
            user_vars: BTreeMap::new(),
            database: database.into(),
            connection_id,
        }
    }

    /// Whether `autocommit` is on, for the `OK` packets' status flags.
    pub(super) fn autocommit(&self) -> bool {
        self.vars.get("autocommit") != Some(&Value::Int(0))
    }

    /// The answer to `sql` when it is a session statement, `None` when it is
    /// not (the SQL engine runs it).
    pub(super) fn answer(&mut self, sql: &str) -> Option<Answer> {
        let tokens = tokenize(strip(sql))?;
        let first = tokens.first()?;
        match first.keyword().as_deref() {
            Some("SET") => Some(self.set(&tokens[1..])),
            Some("BEGIN") => only_words(&tokens[1..], &["WORK"]).then_some(Answer::Ok),
            Some("START") => (tokens.get(1).and_then(Token::keyword).as_deref()
                == Some("TRANSACTION"))
            .then_some(Answer::Ok),
            Some("COMMIT") => only_words(&tokens[1..], &["WORK", "AND", "NO", "CHAIN", "RELEASE"])
                .then_some(Answer::Ok),
            Some("ROLLBACK") => {
                only_words(&tokens[1..], &["WORK", "AND", "NO", "CHAIN", "RELEASE"])
                    .then_some(Answer::Ok)
            }
            Some("SHOW") => self.show(&tokens[1..]),
            Some("SELECT") => self.select(&tokens[1..]),
            _ => None,
        }
    }

    fn set(&mut self, tokens: &[Token]) -> Answer {
        let items = split_commas(tokens);
        // `SET [scope] TRANSACTION …` changes nothing on a read-only surface.
        let mut start = 0;
        if let Some(word) = tokens.first().and_then(Token::keyword) {
            if matches!(word.as_str(), "GLOBAL" | "PERSIST" | "PERSIST_ONLY") {
                return refuse_global();
            }
            if matches!(word.as_str(), "SESSION" | "LOCAL") {
                start = 1;
            }
        }
        if tokens.get(start).and_then(Token::keyword).as_deref() == Some("TRANSACTION") {
            return Answer::Ok;
        }
        let mut changes = Vec::new();
        for item in items {
            match self.assignment(item) {
                Ok(mut more) => changes.append(&mut more),
                Err(answer) => return answer,
            }
        }
        // All or nothing, like MySQL.
        for (target, value) in changes {
            match target {
                Target::System(name) => {
                    self.vars.insert(name, value);
                }
                Target::User(name) => {
                    self.user_vars.insert(name, value);
                }
            }
        }
        Answer::Ok
    }

    /// One `SET` item: what it changes.
    fn assignment(&self, item: &[Token]) -> Result<Vec<(Target, Value)>, Answer> {
        let malformed = || {
            Answer::Refuse(
                ErrorKind::ER_PARSE_ERROR,
                "this SET is not supported by the read-only MySQL wire".to_owned(),
            )
        };
        let mut item = item;
        let mut scope_given = false;
        if let Some(word) = item.first().and_then(Token::keyword) {
            match word.as_str() {
                "GLOBAL" | "PERSIST" | "PERSIST_ONLY" => return Err(refuse_global()),
                "SESSION" | "LOCAL" => {
                    item = &item[1..];
                    scope_given = true;
                }
                _ => {}
            }
        }
        let head = item.first().ok_or_else(malformed)?;
        match head.keyword().as_deref() {
            Some("NAMES") if !scope_given => {
                let charset = item.get(1).and_then(Token::text).ok_or_else(malformed)?;
                let charset = if charset.eq_ignore_ascii_case("DEFAULT") {
                    "utf8mb4".to_owned()
                } else {
                    charset
                };
                let collation = match item.get(2).and_then(Token::keyword).as_deref() {
                    Some("COLLATE") => item.get(3).and_then(Token::text).ok_or_else(malformed)?,
                    None => default_collation(&charset),
                    Some(_) => return Err(malformed()),
                };
                let text = |s: &str| Value::Text(s.to_owned());
                return Ok(vec![
                    (Target::system("character_set_client"), text(&charset)),
                    (Target::system("character_set_connection"), text(&charset)),
                    (Target::system("character_set_results"), text(&charset)),
                    (Target::system("collation_connection"), text(&collation)),
                ]);
            }
            Some("CHARSET") | Some("CHARACTER") if !scope_given => {
                let at = if head.keyword().as_deref() == Some("CHARACTER") {
                    if item.get(1).and_then(Token::keyword).as_deref() != Some("SET") {
                        return Err(malformed());
                    }
                    2
                } else {
                    1
                };
                let charset = item.get(at).and_then(Token::text).ok_or_else(malformed)?;
                let text = |s: &str| Value::Text(s.to_owned());
                let database = self
                    .vars
                    .get("character_set_database")
                    .cloned()
                    .unwrap_or_else(|| text("utf8mb4"));
                return Ok(vec![
                    (Target::system("character_set_client"), text(&charset)),
                    (Target::system("character_set_results"), text(&charset)),
                    (Target::system("character_set_connection"), database),
                ]);
            }
            _ => {}
        }
        // `name = value`, `@@[session.]name = value` or `@user = value`.
        let (target, rest) = match head {
            Token::Word(word) if word.starts_with("@@") => {
                let name = word[2..].to_ascii_lowercase();
                let name = match name.split_once('.') {
                    Some(("global" | "persist" | "persist_only", _)) => {
                        return Err(refuse_global());
                    }
                    Some(("session" | "local", name)) => name.to_owned(),
                    Some(_) => return Err(malformed()),
                    None => name,
                };
                (Target::System(name), &item[1..])
            }
            Token::Word(word) if word.starts_with('@') => {
                let name = word[1..].trim_matches('`').to_ascii_lowercase();
                (Target::User(name), &item[1..])
            }
            Token::Word(word) | Token::Quoted(word) => {
                (Target::System(word.to_ascii_lowercase()), &item[1..])
            }
            _ => return Err(malformed()),
        };
        let value = match rest {
            [Token::Punct('='), value @ ..]
            | [Token::Punct(':'), Token::Punct('='), value @ ..] => value,
            _ => return Err(malformed()),
        };
        let value = match (&target, value) {
            (Target::System(name), [Token::Word(w)]) if w.eq_ignore_ascii_case("DEFAULT") => {
                defaults().remove(name).unwrap_or(Value::Null)
            }
            // A value we cannot evaluate (an expression) is kept as typed:
            // nothing on this surface reads it.
            (_, value) => self
                .value(value)
                .unwrap_or_else(|| Value::Text(join(value))),
        };
        Ok(vec![(target, value)])
    }

    /// A literal, a keyword value or another variable.
    fn value(&self, tokens: &[Token]) -> Option<Value> {
        match tokens {
            [Token::Str(s)] => Some(Value::Text(s.clone())),
            [Token::Num(n)] => Some(number(n)),
            [Token::Punct('-'), Token::Num(n)] => Some(number(&format!("-{n}"))),
            [Token::Word(w)] if w.starts_with('@') => self.read(w).ok(),
            [Token::Word(w)] => Some(match w.to_ascii_uppercase().as_str() {
                "ON" | "TRUE" => Value::Int(1),
                "OFF" | "FALSE" => Value::Int(0),
                "NULL" => Value::Null,
                _ => Value::Text(w.clone()),
            }),
            _ => None,
        }
    }

    /// `@@[scope.]name` or `@name`.
    fn read(&self, word: &str) -> Result<Value, Answer> {
        if let Some(name) = word.strip_prefix("@@") {
            let lower = name.to_ascii_lowercase();
            let name = match lower.split_once('.') {
                Some(("global" | "session" | "local", name)) => name,
                _ => lower.as_str(),
            };
            return self.vars.get(name).cloned().ok_or_else(|| {
                Answer::Refuse(
                    ErrorKind::ER_UNKNOWN_SYSTEM_VARIABLE,
                    format!("Unknown system variable '{name}'"),
                )
            });
        }
        let name = word.trim_start_matches('@').trim_matches('`');
        Ok(self
            .user_vars
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or(Value::Null))
    }

    fn show(&self, tokens: &[Token]) -> Option<Answer> {
        let words: Vec<String> = tokens.iter().filter_map(Token::keyword).collect();
        let mut rest = tokens;
        if matches!(
            rest.first().and_then(Token::keyword).as_deref(),
            Some("SESSION" | "LOCAL" | "GLOBAL")
        ) {
            rest = &rest[1..];
        }
        match rest.first().and_then(Token::keyword).as_deref() {
            Some("VARIABLES") => {
                let pattern = match &rest[1..] {
                    [] => None,
                    [like, Token::Str(p)] if like.keyword().as_deref() == Some("LIKE") => {
                        Some(p.clone())
                    }
                    _ => return None,
                };
                let rows = self
                    .vars
                    .iter()
                    .filter(|(name, _)| pattern.as_deref().is_none_or(|p| like(p, name)))
                    .map(|(name, value)| {
                        let shown = match value {
                            Value::Int(n) => Value::Text(n.to_string()),
                            Value::Null => Value::Text(String::new()),
                            other => other.clone(),
                        };
                        vec![Value::Text(name.clone()), shown]
                    })
                    .collect();
                Some(Answer::Rows {
                    columns: vec!["Variable_name".to_owned(), "Value".to_owned()],
                    rows,
                })
            }
            Some("WARNINGS" | "ERRORS") if words.len() <= 3 => Some(Answer::Rows {
                columns: vec!["Level".to_owned(), "Code".to_owned(), "Message".to_owned()],
                rows: Vec::new(),
            }),
            _ => None,
        }
    }

    /// A `SELECT` with no `FROM` of variables, literals and session
    /// functions; `None` for anything else.
    fn select(&self, tokens: &[Token]) -> Option<Answer> {
        let mut tokens = tokens;
        // A trailing `LIMIT n` (n > 0) changes nothing on one row.
        if let [head @ .., limit, Token::Num(n)] = tokens
            && limit.keyword().as_deref() == Some("LIMIT")
        {
            if n.parse::<u64>().ok()? == 0 {
                return None;
            }
            tokens = head;
        }
        let mut columns = Vec::new();
        let mut row = Vec::new();
        let mut session = false;
        for item in split_commas(tokens) {
            let (expr, alias) = match item {
                [
                    expr @ ..,
                    as_,
                    Token::Word(alias) | Token::Quoted(alias) | Token::Str(alias),
                ] if as_.keyword().as_deref() == Some("AS") => (expr, Some(alias.clone())),
                _ => (item, None),
            };
            let value = match expr {
                [Token::Word(w)] if w.starts_with('@') => {
                    session = true;
                    match self.read(w) {
                        Ok(value) => value,
                        Err(answer) => return Some(answer),
                    }
                }
                [Token::Word(f), Token::Punct('('), Token::Punct(')')] => {
                    session = true;
                    self.function(f)?
                }
                [Token::Word(f)] if f.eq_ignore_ascii_case("CURRENT_USER") => {
                    session = true;
                    self.function(f)?
                }
                [Token::Str(s)] => Value::Text(s.clone()),
                [Token::Num(n)] => number(n),
                [Token::Punct('-'), Token::Num(n)] => number(&format!("-{n}")),
                [Token::Word(w)] if w.eq_ignore_ascii_case("NULL") => Value::Null,
                _ => return None,
            };
            columns.push(alias.unwrap_or_else(|| join(expr)));
            row.push(value);
        }
        // `SELECT 1` and the like are the SQL engine's.
        (session && !columns.is_empty()).then(|| Answer::Rows {
            columns,
            rows: vec![row],
        })
    }

    fn function(&self, name: &str) -> Option<Value> {
        Some(match name.to_ascii_uppercase().as_str() {
            "VERSION" => Value::Text(SERVER_VERSION.to_owned()),
            "DATABASE" | "SCHEMA" => Value::Text(self.database.clone()),
            "USER" | "CURRENT_USER" | "SESSION_USER" | "SYSTEM_USER" => {
                Value::Text("loams@localhost".to_owned())
            }
            "CONNECTION_ID" => Value::Int(i64::from(self.connection_id)),
            "LAST_INSERT_ID" | "ROW_COUNT" | "FOUND_ROWS" => Value::Int(0),
            _ => return None,
        })
    }
}

fn refuse_global() -> Answer {
    Answer::Refuse(
        ErrorKind::ER_SPECIFIC_ACCESS_DENIED_ERROR,
        "the read-only MySQL wire has no global variables to set".to_owned(),
    )
}

#[derive(Debug)]
enum Target {
    System(String),
    User(String),
}

impl Target {
    fn system(name: &str) -> Self {
        Self::System(name.to_owned())
    }
}

fn default_collation(charset: &str) -> String {
    match charset.to_ascii_lowercase().as_str() {
        "utf8mb4" => "utf8mb4_0900_ai_ci".to_owned(),
        "utf8" | "utf8mb3" => "utf8mb3_general_ci".to_owned(),
        "latin1" => "latin1_swedish_ci".to_owned(),
        "binary" => "binary".to_owned(),
        "ascii" => "ascii_general_ci".to_owned(),
        other => format!("{other}_general_ci"),
    }
}

fn number(n: &str) -> Value {
    n.parse::<i64>()
        .map(Value::Int)
        .unwrap_or_else(|_| Value::Text(n.to_owned()))
}

/// MySQL's `LIKE`, case-insensitively: `%` any run, `_` one character,
/// `\` escapes.
fn like(pattern: &str, text: &str) -> bool {
    fn go(p: &[char], t: &[char]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some(('%', rest)) => (0..=t.len()).any(|i| go(rest, &t[i..])),
            Some(('_', rest)) => !t.is_empty() && go(rest, &t[1..]),
            Some(('\\', rest)) if !rest.is_empty() => {
                !t.is_empty() && t[0].eq_ignore_ascii_case(&rest[0]) && go(&rest[1..], &t[1..])
            }
            Some((c, rest)) => !t.is_empty() && t[0].eq_ignore_ascii_case(c) && go(rest, &t[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    go(&p, &t)
}

/// `sql` without leading comments (Connector/J prefixes one), surrounding
/// whitespace and one trailing `;`.
fn strip(sql: &str) -> &str {
    let mut s = sql.trim();
    loop {
        if let Some(rest) = s.strip_prefix("/*") {
            match rest.find("*/") {
                Some(end) => s = rest[end + 2..].trim_start(),
                None => return s,
            }
        } else if s.starts_with("-- ") || s.starts_with('#') {
            match s.find('\n') {
                Some(end) => s = s[end + 1..].trim_start(),
                None => return "",
            }
        } else {
            break;
        }
    }
    let s = s.trim_end();
    s.strip_suffix(';').unwrap_or(s).trim_end()
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    /// A bare word: a keyword, an identifier, `@user` or `@@[scope.]name`.
    Word(String),
    /// A `` `quoted` `` identifier.
    Quoted(String),
    /// A `'string'` or `"string"`.
    Str(String),
    Num(String),
    Punct(char),
}

impl Token {
    /// The word, uppercased.
    fn keyword(&self) -> Option<String> {
        match self {
            Token::Word(w) => Some(w.to_ascii_uppercase()),
            _ => None,
        }
    }

    /// A word, a quoted identifier or a string, as text (a charset or a
    /// collation may be any of these).
    fn text(&self) -> Option<String> {
        match self {
            Token::Word(w) | Token::Quoted(w) | Token::Str(w) => Some(w.clone()),
            _ => None,
        }
    }
}

/// The tokens of `sql`; `None` when it has more than one statement or does
/// not lex (an unterminated string), which leaves it to the SQL engine.
fn tokenize(sql: &str) -> Option<Vec<Token>> {
    let chars: Vec<char> = sql.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '\'' || c == '"' {
            let mut text = String::new();
            i += 1;
            loop {
                let ch = *chars.get(i)?;
                if ch == '\\' {
                    match *chars.get(i + 1)? {
                        'n' => text.push('\n'),
                        't' => text.push('\t'),
                        '0' => text.push('\0'),
                        // MySQL keeps `\%` and `\_` as typed, for `LIKE`.
                        wild @ ('%' | '_') => {
                            text.push('\\');
                            text.push(wild);
                        }
                        other => text.push(other),
                    }
                    i += 2;
                } else if ch == c {
                    if chars.get(i + 1) == Some(&c) {
                        text.push(c);
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    text.push(ch);
                    i += 1;
                }
            }
            tokens.push(Token::Str(text));
        } else if c == '`' {
            let end = chars[i + 1..].iter().position(|&ch| ch == '`')? + i + 1;
            tokens.push(Token::Quoted(chars[i + 1..end].iter().collect()));
            i = end + 1;
        } else if c.is_ascii_digit() {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '.') {
                i += 1;
            }
            tokens.push(Token::Num(chars[start..i].iter().collect()));
        } else if c.is_alphanumeric() || c == '_' || c == '@' || c == '$' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric() || matches!(chars[i], '_' | '@' | '$' | '.' | '`'))
            {
                i += 1;
            }
            tokens.push(Token::Word(chars[start..i].iter().collect()));
        } else if c == ';' {
            // A second statement: not a session statement.
            return None;
        } else {
            tokens.push(Token::Punct(c));
            i += 1;
        }
    }
    Some(tokens)
}

/// `tokens` split at the commas outside parentheses.
fn split_commas(tokens: &[Token]) -> Vec<&[Token]> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, token) in tokens.iter().enumerate() {
        match token {
            Token::Punct('(') => depth += 1,
            Token::Punct(')') => depth = depth.saturating_sub(1),
            Token::Punct(',') if depth == 0 => {
                out.push(&tokens[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < tokens.len() {
        out.push(&tokens[start..]);
    }
    out
}

/// Whether every token is one of `words`.
fn only_words(tokens: &[Token], words: &[&str]) -> bool {
    tokens
        .iter()
        .all(|t| t.keyword().is_some_and(|k| words.contains(&k.as_str())))
}

/// `tokens` as text again: a column name or an unevaluated value.
fn join(tokens: &[Token]) -> String {
    let mut out = String::new();
    for token in tokens {
        let piece = match token {
            Token::Word(w) | Token::Num(w) => w.clone(),
            Token::Quoted(w) => format!("`{w}`"),
            Token::Str(s) => s.clone(),
            Token::Punct(c) => c.to_string(),
        };
        let glue = matches!(token, Token::Punct('(' | ')' | '.'))
            || out.ends_with(['(', '.', '@'])
            || out.is_empty();
        if !glue {
            out.push(' ');
        }
        out.push_str(&piece);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> Session {
        Session::new("e2e", 7)
    }

    fn ok(session: &mut Session, sql: &str) {
        assert_eq!(session.answer(sql), Some(Answer::Ok), "{sql}");
    }

    fn rows(session: &mut Session, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
        match session.answer(sql) {
            Some(Answer::Rows { columns, rows }) => (columns, rows),
            other => panic!("{sql}: {other:?}"),
        }
    }

    #[test]
    fn pymysql_connect_and_cursor_statements() {
        let mut s = session();
        ok(&mut s, "SET NAMES utf8mb4");
        ok(&mut s, "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci");
        ok(&mut s, "SET AUTOCOMMIT = 0");
        assert!(!s.autocommit());
        ok(&mut s, "SET AUTOCOMMIT = 1");
        assert!(s.autocommit());
        ok(&mut s, "BEGIN");
        ok(&mut s, "COMMIT");
        ok(&mut s, "ROLLBACK");
        let (columns, warnings) = rows(&mut s, "SHOW WARNINGS");
        assert_eq!(columns, ["Level", "Code", "Message"]);
        assert!(warnings.is_empty());
        let (_, got) = rows(&mut s, "SELECT @@collation_connection");
        assert_eq!(got, [[Value::Text("utf8mb4_general_ci".into())]]);
    }

    #[test]
    fn mysql_connector_python_connect_statements() {
        let mut s = session();
        ok(&mut s, "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci'");
        ok(&mut s, "SET @@session.autocommit = OFF");
        assert!(!s.autocommit());
        ok(&mut s, "SET @@session.time_zone = '+00:00'");
        ok(&mut s, "SET @@session.sql_mode = 'TRADITIONAL'");
        let (columns, got) = rows(&mut s, "SELECT @@session.time_zone, @@version");
        assert_eq!(columns, ["@@session.time_zone", "@@version"]);
        assert_eq!(
            got,
            [[
                Value::Text("+00:00".into()),
                Value::Text(SERVER_VERSION.into())
            ]]
        );
    }

    #[test]
    fn connector_j_connect_statements() {
        let mut s = session();
        let (columns, got) = rows(
            &mut s,
            "/* mysql-connector-j-8.4.0 (Revision: 1c3f5c149e0bfe31c7fbeb24e2d260cd890972c4) */\
             SELECT  @@session.auto_increment_increment AS auto_increment_increment, \
             @@character_set_client AS character_set_client, @@character_set_connection AS \
             character_set_connection, @@character_set_results AS character_set_results, \
             @@character_set_server AS character_set_server, @@collation_server AS \
             collation_server, @@collation_connection AS collation_connection, @@init_connect \
             AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS \
             license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet \
             AS max_allowed_packet, @@net_write_timeout AS net_write_timeout, \
             @@performance_schema AS performance_schema, @@sql_mode AS sql_mode, \
             @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, \
             @@transaction_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout",
        );
        assert_eq!(columns.len(), 19);
        assert_eq!(columns[0], "auto_increment_increment");
        assert_eq!(got[0][0], Value::Int(1));
        assert_eq!(got[0][11], Value::Int(67_108_864));
        ok(&mut s, "SET NAMES utf8mb4");
        ok(&mut s, "SET character_set_results = NULL");
        ok(&mut s, "SET autocommit=1");
        let (_, got) = rows(&mut s, "SELECT @@character_set_results");
        assert_eq!(got, [[Value::Null]]);
    }

    #[test]
    fn go_sql_driver_and_mysql2_statements() {
        let mut s = session();
        ok(&mut s, "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci");
        ok(&mut s, "SET sql_mode='ANSI_QUOTES', time_zone='UTC'");
        ok(&mut s, "START TRANSACTION");
        ok(&mut s, "START TRANSACTION READ ONLY");
        ok(&mut s, "COMMIT");
        let (_, got) = rows(&mut s, "SELECT @@max_allowed_packet");
        assert_eq!(got, [[Value::Int(67_108_864)]]);
        // mysql_async and mysql2.
        let (_, got) = rows(&mut s, "SELECT @@max_allowed_packet,@@wait_timeout");
        assert_eq!(got, [[Value::Int(67_108_864), Value::Int(28_800)]]);
        let (_, got) = rows(&mut s, "SELECT @@socket");
        assert_eq!(got, [[Value::Null]]);
        let (columns, got) = rows(&mut s, "select version(), database(), connection_id();");
        assert_eq!(columns, ["version()", "database()", "connection_id()"]);
        assert_eq!(
            got,
            [[
                Value::Text(SERVER_VERSION.into()),
                Value::Text("e2e".into()),
                Value::Int(7)
            ]]
        );
    }

    #[test]
    fn sqlalchemy_probes() {
        let mut s = session();
        let (_, got) = rows(&mut s, "SHOW VARIABLES LIKE 'sql_mode'");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0][0], Value::Text("sql_mode".into()));
        let (_, got) = rows(&mut s, "SHOW VARIABLES LIKE 'lower_case_table_names'");
        assert_eq!(got[0][1], Value::Text("0".into()));
        let (_, got) = rows(&mut s, "SELECT @@transaction_isolation");
        assert_eq!(got, [[Value::Text("REPEATABLE-READ".into())]]);
        let (_, got) = rows(&mut s, "SHOW VARIABLES LIKE 'character\\_set\\_%'");
        assert!(got.len() >= 5, "{got:?}");
        ok(
            &mut s,
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
        );
    }

    #[test]
    fn user_variables_and_defaults() {
        let mut s = session();
        ok(&mut s, "SET @x = 5, @name := 'loams'");
        let (_, got) = rows(&mut s, "SELECT @x, @name, @unset");
        assert_eq!(
            got,
            [[Value::Int(5), Value::Text("loams".into()), Value::Null]]
        );
        ok(&mut s, "SET wait_timeout = 10");
        ok(&mut s, "SET wait_timeout = DEFAULT");
        let (_, got) = rows(&mut s, "SELECT @@wait_timeout AS w LIMIT 1");
        assert_eq!(got, [[Value::Int(28_800)]]);
    }

    #[test]
    fn what_is_not_a_session_statement_is_left_to_the_engine() {
        let mut s = session();
        for sql in [
            "SELECT 1",
            "SELECT * FROM docs",
            "SELECT @@version FROM docs",
            "SELECT a, @@version FROM docs",
            "INSERT INTO x VALUES (1)",
            "SET NAMES utf8mb4; SELECT 1",
            "ROLLBACK TO SAVEPOINT a",
            "SHOW TABLES",
        ] {
            assert_eq!(s.answer(sql), None, "{sql}");
        }
    }

    #[test]
    fn globals_and_unknown_variables_are_refused() {
        let mut s = session();
        for sql in ["SET GLOBAL wait_timeout = 1", "SET @@global.autocommit = 0"] {
            assert!(
                matches!(
                    s.answer(sql),
                    Some(Answer::Refuse(
                        ErrorKind::ER_SPECIFIC_ACCESS_DENIED_ERROR,
                        _
                    ))
                ),
                "{sql}"
            );
        }
        assert!(matches!(
            s.answer("SELECT @@no_such_variable"),
            Some(Answer::Refuse(ErrorKind::ER_UNKNOWN_SYSTEM_VARIABLE, _))
        ));
        // A refused SET changes nothing.
        assert!(matches!(
            s.answer("SET autocommit = 0, GLOBAL wait_timeout = 1"),
            Some(Answer::Refuse(..))
        ));
        assert!(s.autocommit());
    }
}
