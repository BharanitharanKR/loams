//! Apache Cassandra over CQL (the `scylla` driver).
//!
//! **Source.** The Murmur3 token ring is cut into `splits` contiguous token
//! ranges (default 8), one split each (`tokens-<i>-of-<n>`). A split reads
//! `SELECT JSON * FROM ks.table WHERE token(pk…) >= ? AND token(pk…) <= ?`
//! a page at a time; its position is the driver's paging state (and whether
//! the range is finished), so a restart resumes at the page after the last
//! one the Fabric acknowledged. Each row is a `document` event: `subject`
//! is the row's primary key as JSON, and the id a digest of the row, so an
//! unchanged row re-read has the same id. Without `poll_interval_ms` the
//! table is read once.
//!
//! **Sink.** `INSERT INTO ks.table JSON ? DEFAULT UNSET` per event (an
//! upsert by primary key: a replay leaves one row), and for `loamsop = d`
//! `DELETE … WHERE <key> = fromJson(?) …`. Statements are prepared once and
//! marked idempotent.
//!
//! **Egress.** Contact points are resolved and checked by the SDK's egress
//! hook before the driver sees them, and a host filter applies the same
//! check to every peer the driver discovers.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, TimeUnit};
use async_trait::async_trait;
use base64::Engine as _;
use loams_connector_sdk::schema::{NativeType, SchemaError, TypeMapper};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EgressCheck,
    EnvelopeBuilder, EventBatch, Position, Sink, SinkAck, SinkFactory, SinkHandle, Source,
    SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::metadata::Peer;
use scylla::errors::{DbError, ExecutionError, NewSessionError, PrepareError, RequestAttemptError};
use scylla::policies::host_filter::HostFilter;
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::OnceCell;

use crate::config::{self, Config};
use crate::util::{digest16, is_delete, json_object, rows_of};

/// The manifest id.
pub const ID: &str = "cassandra";
/// The event name of a row.
pub const EVENT: &str = "document";

// ---------------------------------------------------------------- tokens --

/// The token ranges of `n` splits: contiguous, inclusive, covering the whole
/// Murmur3 ring `[i64::MIN, i64::MAX]` with no gap and no overlap.
pub fn token_ranges(n: u32) -> Vec<(i64, i64)> {
    let n = i128::from(n.max(1));
    let min = i128::from(i64::MIN);
    let span = i128::from(i64::MAX) - min + 1;
    (0..n)
        .map(|i| {
            let start = min + span * i / n;
            let end = min + span * (i + 1) / n - 1;
            (start as i64, end as i64)
        })
        .collect()
}

/// The split id of token range `i` of `n`.
pub fn range_split(i: u32, n: u32) -> String {
    format!("tokens-{i}-of-{n}")
}

/// A CQL identifier, quoted (names are stored case-sensitively).
pub fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// The primary key of a table, as `system_schema.columns` lists it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyColumns {
    pub partition: Vec<String>,
    pub clustering: Vec<String>,
}

impl KeyColumns {
    /// From `(column_name, kind, position)` rows.
    pub fn from_rows(rows: &[(String, String, i32)]) -> KeyColumns {
        let mut part: Vec<(i32, String)> = Vec::new();
        let mut clus: Vec<(i32, String)> = Vec::new();
        for (name, kind, pos) in rows {
            match kind.as_str() {
                "partition_key" => part.push((*pos, name.clone())),
                "clustering" => clus.push((*pos, name.clone())),
                _ => {}
            }
        }
        part.sort();
        clus.sort();
        KeyColumns {
            partition: part.into_iter().map(|(_, n)| n).collect(),
            clustering: clus.into_iter().map(|(_, n)| n).collect(),
        }
    }

    pub fn all(&self) -> impl Iterator<Item = &String> {
        self.partition.iter().chain(&self.clustering)
    }
}

/// The statements of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statements {
    pub select: String,
    pub insert: String,
    pub delete: String,
}

impl Statements {
    pub fn new(keyspace: &str, table: &str, keys: &KeyColumns) -> Statements {
        let t = format!("{}.{}", quote(keyspace), quote(table));
        let pk = keys
            .partition
            .iter()
            .map(|c| quote(c))
            .collect::<Vec<_>>()
            .join(", ");
        let wh = keys
            .all()
            .map(|c| format!("{} = fromJson(?)", quote(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        Statements {
            select: format!("SELECT JSON * FROM {t} WHERE token({pk}) >= ? AND token({pk}) <= ?"),
            insert: format!("INSERT INTO {t} JSON ? DEFAULT UNSET"),
            delete: format!("DELETE FROM {t} WHERE {wh}"),
        }
    }
}

// --------------------------------------------------------------- mapper --

/// CQL types to Arrow (Task 5's `TypeMapper`; the table is in the guide).
/// `decimal`, `varint`, `duration`, tuples and user types have no lossless
/// Arrow type and are `Unsupported`, never guessed (R5.2).
#[derive(Debug, Clone, Copy, Default)]
pub struct CqlTypeMapper;

fn cql_type(path: &str, ty: &str) -> Result<DataType, SchemaError> {
    let t = ty.trim();
    let unsupported = || SchemaError::Unsupported {
        path: path.to_owned(),
        native: ty.to_owned(),
    };
    if let Some(inner) = t.strip_prefix("frozen<").and_then(|r| r.strip_suffix('>')) {
        return cql_type(path, inner);
    }
    for prefix in ["list<", "set<"] {
        if let Some(inner) = t.strip_prefix(prefix).and_then(|r| r.strip_suffix('>')) {
            let item = cql_type(path, inner)?;
            return Ok(DataType::List(Arc::new(Field::new("item", item, true))));
        }
    }
    if let Some(inner) = t.strip_prefix("map<").and_then(|r| r.strip_suffix('>')) {
        let (k, v) = split_top(inner).ok_or_else(unsupported)?;
        let entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", cql_type(path, k)?, false),
                    Field::new("value", cql_type(path, v)?, true),
                ]
                .into(),
            ),
            false,
        );
        return Ok(DataType::Map(Arc::new(entries), false));
    }
    if let Some(inner) = t.strip_prefix("vector<").and_then(|r| r.strip_suffix('>')) {
        let (item, n) = split_top(inner).ok_or_else(unsupported)?;
        let n: i32 = n.trim().parse().map_err(|_| unsupported())?;
        return Ok(DataType::FixedSizeList(
            Arc::new(Field::new("item", cql_type(path, item)?, true)),
            n,
        ));
    }
    Ok(match t {
        "ascii" | "text" | "varchar" | "inet" => DataType::Utf8,
        "bigint" | "counter" => DataType::Int64,
        "int" => DataType::Int32,
        "smallint" => DataType::Int16,
        "tinyint" => DataType::Int8,
        "boolean" => DataType::Boolean,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "blob" => DataType::Binary,
        "uuid" | "timeuuid" => DataType::FixedSizeBinary(16),
        "timestamp" => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Nanosecond),
        _ => return Err(unsupported()),
    })
}

/// Split `a, b` at the top-level comma (not inside `<>`).
fn split_top(s: &str) -> Option<(&str, &str)> {
    let mut depth = 0i32;
    for (i, c) in s.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => return Some((s[..i].trim(), s[i + 1..].trim())),
            _ => {}
        }
    }
    None
}

fn cql_name(path: &str, dt: &DataType) -> Result<String, SchemaError> {
    Ok(match dt {
        DataType::Utf8 | DataType::LargeUtf8 => "text".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Int32 => "int".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Binary | DataType::LargeBinary => "blob".into(),
        DataType::FixedSizeBinary(16) => "uuid".into(),
        DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => {
            "timestamp".into()
        }
        DataType::Date32 => "date".into(),
        DataType::Time64(TimeUnit::Nanosecond) => "time".into(),
        DataType::List(f) => format!("list<{}>", cql_name(path, f.data_type())?),
        DataType::FixedSizeList(f, n) => format!("vector<{}, {n}>", cql_name(path, f.data_type())?),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => format!(
                "map<{}, {}>",
                cql_name(path, fields[0].data_type())?,
                cql_name(path, fields[1].data_type())?
            ),
            _ => {
                return Err(SchemaError::Invalid(format!(
                    "{path}: a map without key and value"
                )));
            }
        },
        other => {
            return Err(SchemaError::Unsupported {
                path: path.to_owned(),
                native: other.to_string(),
            });
        }
    })
}

impl TypeMapper for CqlTypeMapper {
    fn to_arrow(&self, native: &NativeType) -> Result<Field, SchemaError> {
        let name = native.path.rsplit('.').next().unwrap_or(&native.path);
        Ok(Field::new(
            name,
            cql_type(&native.path, &native.type_name)?,
            native.nullable,
        ))
    }

    fn from_arrow(&self, field: &Field) -> Result<NativeType, SchemaError> {
        Ok(NativeType::new(
            field.name().clone(),
            cql_name(field.name(), field.data_type())?,
            field.is_nullable(),
        ))
    }
}

// --------------------------------------------------------------- errors --

fn db_class(db: &DbError, what: &str, write: bool) -> ConnectorError {
    let name = format!("{what}: {}", db_name(db));
    match db {
        DbError::AuthenticationError | DbError::Unauthorized => ConnectorError::auth(name),
        DbError::RateLimitReached { .. } => ConnectorError::throttled(Duration::from_secs(1), name),
        DbError::SyntaxError | DbError::ConfigError | DbError::AlreadyExists { .. } => {
            ConnectorError::fatal(name)
        }
        DbError::Invalid | DbError::FunctionFailure { .. } if write => ConnectorError::poison(name),
        DbError::Invalid | DbError::FunctionFailure { .. } => ConnectorError::fatal(name),
        _ => ConnectorError::retryable(name),
    }
}

/// The error's kind, not its message (which can quote values).
fn db_name(db: &DbError) -> &'static str {
    match db {
        DbError::SyntaxError => "syntax error",
        DbError::Invalid => "invalid request",
        DbError::AlreadyExists { .. } => "already exists",
        DbError::FunctionFailure { .. } => "function failure",
        DbError::AuthenticationError => "authentication failed",
        DbError::Unauthorized => "unauthorized",
        DbError::ConfigError => "configuration error",
        DbError::Unavailable { .. } => "unavailable",
        DbError::Overloaded => "overloaded",
        DbError::IsBootstrapping => "bootstrapping",
        DbError::ReadTimeout { .. } => "read timeout",
        DbError::WriteTimeout { .. } => "write timeout",
        DbError::RateLimitReached { .. } => "rate limit reached",
        _ => "server error",
    }
}

fn attempt_class(e: &RequestAttemptError, what: &str, write: bool) -> ConnectorError {
    match e {
        RequestAttemptError::DbError(db, _) => db_class(db, what, write),
        RequestAttemptError::SerializationError(_) if write => {
            ConnectorError::poison(format!("{what}: the values do not serialize"))
        }
        _ => ConnectorError::retryable(format!("{what}: the request failed on the connection")),
    }
}

/// A driver error as an SDK error class.
pub fn exec_class(e: &ExecutionError, what: &str, write: bool) -> ConnectorError {
    match e {
        ExecutionError::LastAttemptError(a) => attempt_class(a, what, write),
        ExecutionError::PrepareError(p) => prepare_class(p, what),
        ExecutionError::BadQuery(_) if write => {
            ConnectorError::poison(format!("{what}: bad values"))
        }
        ExecutionError::BadQuery(_) => ConnectorError::fatal(format!("{what}: bad query")),
        _ => ConnectorError::retryable(format!("{what}: {}", short(e))),
    }
}

fn prepare_class(e: &PrepareError, what: &str) -> ConnectorError {
    match e {
        PrepareError::AllAttemptsFailed { first_attempt } => {
            attempt_class(first_attempt, what, false)
        }
        _ => ConnectorError::retryable(format!("{what}: preparing failed")),
    }
}

fn short(e: &ExecutionError) -> &'static str {
    match e {
        ExecutionError::RequestTimeout(_) => "timed out",
        ExecutionError::ConnectionPoolError(_) => "no connection",
        ExecutionError::EmptyPlan => "no node to send to",
        _ => "failed",
    }
}

fn session_class(e: &NewSessionError) -> ConnectorError {
    let text = e.to_string();
    if text.contains("Authentication") || text.contains("authentication") {
        ConnectorError::auth("connecting to Cassandra: authentication failed")
    } else {
        ConnectorError::retryable("connecting to Cassandra: no node answered")
    }
}

// -------------------------------------------------------------- session --

/// Refuses every peer the instance's egress check refuses.
struct EgressFilter(Arc<dyn EgressCheck>);

impl HostFilter for EgressFilter {
    fn accept(&self, peer: &Peer) -> bool {
        let addr = SocketAddr::new(peer.address.ip(), peer.address.port());
        self.0.check_addr(&addr).is_ok()
    }
}

/// Settings: `contact_points` (`host[:port]`, default port 9042),
/// `keyspace`, `table`, `user`, `password` (a secret name), `consistency`
/// (`local_quorum` by default, `quorum`, `one`, `local_one`, `all`).
#[derive(Debug, Clone)]
pub struct CassandraSettings {
    pub contact_points: Vec<String>,
    pub keyspace: String,
    pub table: String,
    pub consistency: Consistency,
    pub splits: u32,
    pub page_size: i32,
    pub poll_interval: Option<Duration>,
}

impl CassandraSettings {
    pub fn from_config(cfg: &Config) -> Result<Self, ConnectorError> {
        let contact_points = cfg.strings("contact_points")?;
        if contact_points.is_empty() {
            return Err(ConnectorError::fatal(
                "config: `contact_points` is required",
            ));
        }
        let consistency = match cfg.str("consistency")?.as_deref() {
            None | Some("local_quorum") => Consistency::LocalQuorum,
            Some("quorum") => Consistency::Quorum,
            Some("one") => Consistency::One,
            Some("local_one") => Consistency::LocalOne,
            Some("all") => Consistency::All,
            Some(_) => {
                return Err(ConnectorError::fatal(
                    "config: `consistency` must be one of local_quorum, quorum, one, local_one, all",
                ));
            }
        };
        Ok(Self {
            contact_points,
            keyspace: cfg.required_str("keyspace")?,
            table: cfg.required_str("table")?,
            consistency,
            splits: cfg.u64("splits")?.unwrap_or(8).clamp(1, 4096) as u32,
            page_size: cfg.u64("page_size")?.unwrap_or(500).clamp(1, 100_000) as i32,
            poll_interval: cfg.millis("poll_interval_ms")?,
        })
    }
}

/// A connected table: the session, its key columns and prepared statements.
#[derive(Debug)]
pub struct Table {
    pub session: Session,
    pub keys: KeyColumns,
    pub select: PreparedStatement,
    pub insert: PreparedStatement,
    pub delete: PreparedStatement,
}

/// Connect for `ctx`, egress-checking every contact point after DNS, and
/// prepare the table's statements.
pub async fn connect(
    ctx: &ConnectorContext,
    cfg: &Config,
    s: &CassandraSettings,
) -> Result<Table, ConnectorError> {
    let mut b = SessionBuilder::new().host_filter(Arc::new(EgressFilter(ctx.egress.clone())));
    let mut any = false;
    for cp in &s.contact_points {
        let (host, port) = crate::redis::split_host_port(cp, 9042)?;
        ctx.egress
            .check_host(&host, port)
            .map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let addrs = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|e| ConnectorError::retryable(format!("resolving {host}: {e}")))?;
        for a in addrs {
            if ctx.egress.check_addr(&a).is_ok() {
                b = b.known_node_addr(a);
                any = true;
            }
        }
    }
    if !any {
        return Err(ConnectorError::fatal(
            "no contact point resolved to an address the egress policy allows",
        ));
    }
    if let Some(user) = cfg.str("user")? {
        let pw = match config::secret(ctx, cfg, "password").await? {
            Some(p) => config::secret_text(&p, "password")?,
            None => String::new(),
        };
        b = b.user(user, pw);
    }
    let session = b.build().await.map_err(|e| session_class(&e))?;
    let rows = session
        .query_unpaged(
            "SELECT column_name, kind, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
            (s.keyspace.as_str(), s.table.as_str()),
        )
        .await
        .map_err(|e| exec_class(&e, "reading the table's columns", false))?
        .into_rows_result()
        .map_err(|_| ConnectorError::fatal("reading the table's columns: no rows"))?
        .rows::<(String, String, i32)>()
        .map_err(|_| ConnectorError::fatal("reading the table's columns: unexpected types"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ConnectorError::fatal("reading the table's columns: unexpected values"))?;
    let keys = KeyColumns::from_rows(&rows);
    let declared = cfg.strings("key")?;
    if !declared.is_empty() && declared != keys.all().cloned().collect::<Vec<_>>() {
        return Err(ConnectorError::fatal(format!(
            "config: `key` must list the primary key of {}.{} in order: {}",
            s.keyspace,
            s.table,
            keys.all().cloned().collect::<Vec<_>>().join(", ")
        )));
    }
    if keys.partition.is_empty() {
        return Err(ConnectorError::fatal(format!(
            "table {}.{} does not exist or has no partition key",
            s.keyspace, s.table
        )));
    }
    let st = Statements::new(&s.keyspace, &s.table, &keys);
    let prepare = |q: String| {
        let session = &session;
        async move {
            let mut p = session
                .prepare(q)
                .await
                .map_err(|e| prepare_class(&e, "preparing a statement"))?;
            p.set_consistency(s.consistency);
            p.set_is_idempotent(true);
            Ok::<_, ConnectorError>(p)
        }
    };
    let mut select = prepare(st.select).await?;
    select.set_page_size(s.page_size);
    let insert = prepare(st.insert).await?;
    let delete = prepare(st.delete).await?;
    Ok(Table {
        session,
        keys,
        select,
        insert,
        delete,
    })
}

/// Connects once, on first use, and shares the table between splits.
#[derive(Debug, Clone)]
pub struct TableCell {
    ctx: ConnectorContext,
    cfg: Config,
    settings: CassandraSettings,
    cell: Arc<OnceCell<Arc<Table>>>,
}

impl TableCell {
    pub fn new(ctx: ConnectorContext, cfg: Config, settings: CassandraSettings) -> Self {
        Self {
            ctx,
            cfg,
            settings,
            cell: Arc::new(OnceCell::new()),
        }
    }

    pub async fn get(&self) -> Result<Arc<Table>, ConnectorError> {
        self.cell
            .get_or_try_init(|| async {
                connect(&self.ctx, &self.cfg, &self.settings)
                    .await
                    .map(Arc::new)
            })
            .await
            .cloned()
    }
}

// --------------------------------------------------------------- source --

/// A token range's position (its opaque bytes are this, as JSON).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangePos {
    pub pass: u64,
    pub pages: u64,
    /// The driver's paging state for the next page, base64.
    pub paging: Option<String>,
    pub done: bool,
}

impl RangePos {
    pub fn to_position(&self, split: &str) -> Position {
        let display = if self.done {
            format!("{split} pass {} done after {} pages", self.pass, self.pages)
        } else {
            format!("{split} pass {} after {} pages", self.pass, self.pages)
        };
        Position::new(serde_json::to_vec(self).unwrap_or_default(), display)
    }

    pub fn from_position(p: &Position) -> Option<RangePos> {
        serde_json::from_slice(&p.opaque).ok()
    }

    /// The paging state to resume from.
    pub fn paging_state(&self) -> Result<PagingState, ConnectorError> {
        match &self.paging {
            None => Ok(PagingState::start()),
            Some(b) => base64::engine::general_purpose::STANDARD
                .decode(b)
                .map(PagingState::new_from_raw_bytes)
                .map_err(|_| ConnectorError::fatal("the checkpointed paging state does not parse")),
        }
    }

    /// Record the page just read.
    pub fn advance(&mut self, next: &PagingStateResponse) {
        self.pages += 1;
        match next {
            PagingStateResponse::HasMorePages { state } => {
                self.paging = state
                    .as_bytes_slice()
                    .map(|b| base64::engine::general_purpose::STANDARD.encode(b.as_ref()));
                self.done = self.paging.is_none();
            }
            _ => {
                self.paging = None;
                self.done = true;
            }
        }
    }
}

/// One split per token range.
#[derive(Debug)]
pub struct RangeEnumerator {
    n: u32,
}

#[async_trait]
impl SplitEnumerator for RangeEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(token_ranges(self.n)
            .into_iter()
            .enumerate()
            .map(|(i, (start, end))| {
                Split::new(
                    range_split(i as u32, self.n),
                    json!({ "start": start, "end": end }),
                )
            })
            .collect())
    }
}

#[derive(Debug)]
pub struct RangeSourceFactory {
    table: TableCell,
    envelope: EnvelopeBuilder,
}

impl SourceFactory for RangeSourceFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        let start = split.spec["start"]
            .as_i64()
            .ok_or_else(|| ConnectorError::fatal("split without a start token"))?;
        let end = split.spec["end"]
            .as_i64()
            .ok_or_else(|| ConnectorError::fatal("split without an end token"))?;
        Ok(Box::new(RangeSource {
            table: self.table.clone(),
            envelope: self.envelope.clone(),
            split: split.id.as_str().to_owned(),
            range: (start, end),
            pos: RangePos::default(),
            carry: Vec::new(),
            carry_next: None,
            finished_at: None,
        }))
    }
}

#[derive(Debug)]
struct RangeSource {
    table: TableCell,
    envelope: EnvelopeBuilder,
    split: String,
    range: (i64, i64),
    pos: RangePos,
    /// Rows of the current page not yet returned (reversed).
    carry: Vec<String>,
    carry_next: Option<PagingStateResponse>,
    finished_at: Option<Instant>,
}

/// The event a row (as `SELECT JSON` renders it) becomes.
pub fn row_event(
    envelope: &EnvelopeBuilder,
    resource: &str,
    keys: &KeyColumns,
    row_json: &str,
) -> CloudEvent {
    let key: serde_json::Map<String, Value> = match serde_json::from_str::<Value>(row_json) {
        Ok(Value::Object(o)) => keys
            .all()
            .filter_map(|k| o.get(k).map(|v| (k.clone(), v.clone())))
            .collect(),
        _ => serde_json::Map::new(),
    };
    envelope
        .event(
            EVENT,
            resource,
            format!(
                "{resource}/{}",
                digest16(&[resource.as_bytes(), row_json.as_bytes()])
            ),
        )
        .with_subject(Value::Object(key).to_string())
        .with_data("application/json", row_json.to_owned())
}

#[async_trait]
impl Source for RangeSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        if let Some(p) = pos.as_ref().and_then(RangePos::from_position) {
            self.pos = p;
            if self.pos.done {
                self.finished_at = Some(Instant::now());
            }
        }
        self.table.get().await?;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.pos.done && self.carry.is_empty() {
            match (self.table.settings.poll_interval, self.finished_at) {
                (Some(every), Some(at)) if at.elapsed() >= every => {
                    self.pos = RangePos {
                        pass: self.pos.pass + 1,
                        ..RangePos::default()
                    };
                    self.finished_at = None;
                }
                _ => return Ok(None),
            }
        }
        let table = self.table.get().await?;
        if self.carry.is_empty() {
            let mut select = table.select.clone();
            select.set_page_size(
                i32::try_from(budget.max_rows.max(1))
                    .unwrap_or(i32::MAX)
                    .min(self.table.settings.page_size),
            );
            let (result, next) = table
                .session
                .execute_single_page(&select, self.range, self.pos.paging_state()?)
                .await
                .map_err(|e| exec_class(&e, "reading a token range", false))?;
            let rows = result
                .into_rows_result()
                .map_err(|_| ConnectorError::fatal("SELECT JSON returned no rows result"))?;
            self.carry = rows
                .rows::<(String,)>()
                .map_err(|_| ConnectorError::fatal("SELECT JSON returned an unexpected column"))?
                .map(|r| r.map(|(j,)| j))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ConnectorError::fatal("SELECT JSON returned an unexpected value"))?;
            self.carry.reverse();
            self.carry_next = Some(next);
        }
        let resource = format!(
            "{}.{}",
            self.table.settings.keyspace, self.table.settings.table
        );
        let mut events = Vec::new();
        let mut bytes = 0usize;
        while let Some(row) = self.carry.last() {
            if events.len() >= budget.max_rows {
                break;
            }
            let ev = row_event(&self.envelope, &resource, &table.keys, row);
            let n = ev.size_bytes();
            if bytes + n > budget.max_bytes {
                if events.is_empty() {
                    self.carry.pop();
                    return Err(ConnectorError::poison(format!(
                        "a row of {resource} is {n} bytes, over the batch budget of {}",
                        budget.max_bytes
                    )));
                }
                break;
            }
            bytes += n;
            events.push(ev);
            self.carry.pop();
        }
        if self.carry.is_empty()
            && let Some(next) = self.carry_next.take()
        {
            self.pos.advance(&next);
            if self.pos.done {
                self.finished_at = Some(Instant::now());
            }
        }
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: self.pos.to_position(&self.split),
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// ----------------------------------------------------------------- sink --

#[derive(Debug)]
pub struct CassandraSinkFactory {
    table: TableCell,
}

impl SinkFactory for CassandraSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(CassandraSink {
            table: self.table.clone(),
        })))
    }
}

#[derive(Debug)]
struct CassandraSink {
    table: TableCell,
}

/// What one event writes: the row's JSON, or the key's values as JSON
/// texts for a delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowWrite {
    Upsert(String),
    Delete(Vec<String>),
}

/// Decode an event for `keys`, or `Poison`.
pub fn row_write(ev: &CloudEvent, keys: &KeyColumns) -> Result<RowWrite, ConnectorError> {
    let obj = json_object(ev)?;
    for k in keys.all() {
        if obj.get(k).is_none_or(Value::is_null) {
            return Err(ConnectorError::poison(format!(
                "event {}: the key column {k} is missing",
                ev.id
            )));
        }
    }
    if is_delete(ev) {
        Ok(RowWrite::Delete(
            keys.all().map(|k| obj[k].to_string()).collect(),
        ))
    } else {
        Ok(RowWrite::Upsert(Value::Object(obj).to_string()))
    }
}

#[async_trait]
impl Sink for CassandraSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let events = rows_of(batch)?;
        if events.is_empty() {
            return Ok(SinkAck::default());
        }
        let table = self.table.get().await?;
        let writes = events
            .iter()
            .map(|e| row_write(e, &table.keys))
            .collect::<Result<Vec<_>, _>>()?;
        // Up to 32 statements in flight; each is an idempotent upsert or
        // delete by primary key, so a retried batch converges.
        let mut set = tokio::task::JoinSet::new();
        for w in writes {
            while set.len() >= 32 {
                if let Some(r) = set.join_next().await {
                    r.map_err(|_| ConnectorError::fatal("a write task failed"))??;
                }
            }
            let t = table.clone();
            set.spawn(async move {
                let r = match &w {
                    RowWrite::Upsert(json) => {
                        t.session.execute_unpaged(&t.insert, (json.as_str(),)).await
                    }
                    RowWrite::Delete(vals) => {
                        t.session.execute_unpaged(&t.delete, vals.clone()).await
                    }
                };
                r.map(|_| ())
                    .map_err(|e| exec_class(&e, "writing a row", true))
            });
        }
        while let Some(r) = set.join_next().await {
            r.map_err(|_| ConnectorError::fatal("a write task failed"))??;
        }
        Ok(SinkAck {
            rows: events.len() as u64,
            bytes: events.iter().map(|e| e.size_bytes() as u64).sum(),
        })
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

// -------------------------------------------------------------- factory --

/// The `cassandra` factory.
#[derive(Debug, Clone, Copy, Default)]
pub struct CassandraFactory;

impl ConnectorFactory for CassandraFactory {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Config::of(ctx);
        let settings = CassandraSettings::from_config(&cfg)?;
        let n = settings.splits;
        Ok(Some((
            Box::new(RangeEnumerator { n }),
            Box::new(RangeSourceFactory {
                table: TableCell::new(ctx.clone(), cfg, settings),
                envelope: EnvelopeBuilder::new(ID, &ctx.instance),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = Config::of(ctx);
        let settings = CassandraSettings::from_config(&cfg)?;
        Ok(Some(Box::new(CassandraSinkFactory {
            table: TableCell::new(ctx.clone(), cfg, settings),
        })))
    }
}

#[cfg(test)]
mod tests {
    use loams_connector_sdk::{ErrorClass, InstanceKey};

    use super::*;

    /// `cassandra_token_range_partitions`, without a server: the ranges
    /// cover the whole ring, contiguously, with no overlap, for any count.
    #[test]
    fn cassandra_token_range_partitions_cover_the_ring() {
        for n in [1u32, 2, 3, 7, 8, 64, 1000] {
            let r = token_ranges(n);
            assert_eq!(r.len(), n as usize);
            assert_eq!(r[0].0, i64::MIN);
            assert_eq!(r[r.len() - 1].1, i64::MAX);
            for w in r.windows(2) {
                assert_eq!(w[0].1.checked_add(1), Some(w[1].0), "contiguous for {n}");
                assert!(w[0].0 <= w[0].1);
            }
        }
        assert_eq!(range_split(2, 8), "tokens-2-of-8");
    }

    #[test]
    fn statements_quote_identifiers_and_use_every_key_column() {
        let keys = KeyColumns::from_rows(&[
            ("b".into(), "partition_key".into(), 1),
            ("a".into(), "partition_key".into(), 0),
            ("ts".into(), "clustering".into(), 0),
            ("v".into(), "regular".into(), -1),
        ]);
        assert_eq!(keys.partition, vec!["a", "b"]);
        let st = Statements::new("ks", "My\"T", &keys);
        assert_eq!(
            st.select,
            r#"SELECT JSON * FROM "ks"."My""T" WHERE token("a", "b") >= ? AND token("a", "b") <= ?"#
        );
        assert_eq!(
            st.insert,
            r#"INSERT INTO "ks"."My""T" JSON ? DEFAULT UNSET"#
        );
        assert_eq!(
            st.delete,
            r#"DELETE FROM "ks"."My""T" WHERE "a" = fromJson(?) AND "b" = fromJson(?) AND "ts" = fromJson(?)"#
        );
    }

    /// `cassandra_paging_state_resumes`, without a server: the paging state
    /// survives the checkpoint byte for byte, and the last page ends the
    /// range.
    #[test]
    fn paging_state_survives_the_checkpoint() {
        let mut p = RangePos::default();
        assert_eq!(p.paging_state().expect("start"), PagingState::start());
        let state = PagingState::new_from_raw_bytes(vec![1u8, 2, 3, 255]);
        p.advance(&PagingStateResponse::HasMorePages {
            state: state.clone(),
        });
        let restored = RangePos::from_position(&p.to_position("tokens-0-of-1")).expect("roundtrip");
        assert_eq!(restored.paging_state().expect("state"), state);
        assert!(!restored.done);
        p.advance(&PagingStateResponse::NoMorePages);
        assert!(p.done && p.paging.is_none());
        assert_eq!(p.to_position("t").display, "t pass 0 done after 2 pages");
    }

    #[test]
    fn rows_become_documents_keyed_by_primary_key() {
        let keys = KeyColumns {
            partition: vec!["id".into()],
            clustering: vec!["ts".into()],
        };
        let env = EnvelopeBuilder::new(ID, &InstanceKey::new(1, "c"));
        let ev = row_event(
            &env,
            "ks.t",
            &keys,
            r#"{"id": 7, "ts": "2026-01-01", "v": "x"}"#,
        );
        assert_eq!(ev.subject.as_deref(), Some(r#"{"id":7,"ts":"2026-01-01"}"#));
        assert_eq!(ev.ty, "io.loams.dev.flow.cassandra.document.v1");
        assert_eq!(
            ev.id,
            row_event(
                &env,
                "ks.t",
                &keys,
                r#"{"id": 7, "ts": "2026-01-01", "v": "x"}"#
            )
            .id
        );
        let upsert = CloudEvent::new("e", "/s", "t")
            .with_data("application/json", r#"{"id":7,"ts":"x","v":1}"#);
        assert!(matches!(
            row_write(&upsert, &keys).expect("upsert"),
            RowWrite::Upsert(_)
        ));
        let del = upsert.clone().with_extension("loamsop", "d");
        assert_eq!(
            row_write(&del, &keys).expect("delete"),
            RowWrite::Delete(vec!["7".into(), "\"x\"".into()])
        );
        let no_key = CloudEvent::new("e", "/s", "t").with_data("application/json", r#"{"v":1}"#);
        assert_eq!(
            row_write(&no_key, &keys).expect_err("poison").class,
            ErrorClass::Poison
        );
    }

    #[test]
    fn cql_types_map_to_arrow_and_back() {
        let m = CqlTypeMapper;
        for (cql, dt) in [
            ("text", DataType::Utf8),
            ("bigint", DataType::Int64),
            (
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            ),
            (
                "frozen<list<int>>",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            ),
        ] {
            let f = m
                .to_arrow(&NativeType::new(format!("ks.t.{cql}"), cql, true))
                .expect(cql);
            assert_eq!(f.data_type(), &dt, "{cql}");
        }
        let map = m
            .to_arrow(&NativeType::new(
                "ks.t.m",
                "map<text, frozen<list<int>>>",
                true,
            ))
            .expect("map");
        assert_eq!(
            m.from_arrow(&map).expect("back").type_name,
            "map<text, list<int>>"
        );
        let vec = m
            .to_arrow(&NativeType::new("ks.t.e", "vector<float, 3>", true))
            .expect("vector");
        assert_eq!(
            m.from_arrow(&vec).expect("back").type_name,
            "vector<float, 3>"
        );
        for unsupported in ["decimal", "varint", "duration", "tuple<int, int>"] {
            assert!(matches!(
                m.to_arrow(&NativeType::new("ks.t.x", unsupported, true)),
                Err(SchemaError::Unsupported { .. })
            ));
        }
    }
}
