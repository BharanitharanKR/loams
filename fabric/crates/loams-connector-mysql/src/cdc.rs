//! Native MySQL and MariaDB CDC from the row binlog (OR-9, CN2 Task 11).
//!
//! One split per instance (`binlog`). The source reads the binlog as a
//! replica ([`crate::binlog`]) with the server id the config gives or the
//! instance derives, decodes row events into `change` events
//! ([`crate::decode`]), and on a fresh start snapshots the captured tables
//! ([`crate::snapshot`]): by default with DBLog watermarks interleaved with
//! the stream, or under a brief global read lock before it.
//!
//! The position ([`CdcPos`]) is the GTID set (MySQL), the GTID list
//! (MariaDB) or `file:pos` (GTIDs off) after the last whole transaction the
//! Fabric has, the changes of a transaction cut by the budget, the snapshot's
//! progress, and the schema history. The binlog dump has no
//! acknowledgement, so `committed` does nothing: the checkpoint *is* the
//! replica's position. A position the server no longer has (purged binlogs)
//! is `PositionLost`, and the instance stops with a re-snapshot hint
//! (Review Focus 6).

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::schema::SchemaPolicy;
use loams_connector_sdk::{
    Budget, CdcOp, CloudEvent, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch,
    Position, Source, SourceBatch, SourceFactory, Split, SplitEnumerator, SplitId, SplitState, ids,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::binlog::{BinlogStream, RawEvent, StartAt};
use crate::config::{Locking, MyConfig, TableName};
use crate::conn::{MyConn, connect};
use crate::decode::{Captured, Decoder, Emitted, FeedError, Mark, Partial, loamslsn};
use crate::gtid::{BinlogPos, GtidList, GtidSet};
use crate::history::History;
use crate::rows::pieces;
use crate::snapshot::{SnapshotPos, Window, locked_begin, locked_start, read_chunk, watermark_ddl};
use crate::table::{TableInfo, describe, describe_def, list_tables};

/// The CDC split's id.
pub const SPLIT: &str = "binlog";
/// The datacontenttype of snapshot rows (R5.5).
pub const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";
/// How long `next` waits for the binlog before answering "nothing now".
const POLL: Duration = Duration::from_millis(200);
/// How long a DBLog window may wait for its watermarks.
const WINDOW_TIMEOUT: Duration = Duration::from_secs(120);
/// How often lag is reported.
const LAG_EVERY: Duration = Duration::from_secs(5);

fn ser_history<S: Serializer>(h: &Arc<History>, s: S) -> Result<S::Ok, S::Error> {
    h.as_ref().serialize(s)
}

fn de_history<'de, D: Deserializer<'de>>(d: D) -> Result<Arc<History>, D::Error> {
    History::deserialize(d).map(Arc::new)
}

/// Where a CDC split is. Serialised as JSON in `Position::opaque`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CdcPos {
    pub binlog: BinlogPos,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partial: Option<Partial>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotPos>,
    #[serde(serialize_with = "ser_history", deserialize_with = "de_history")]
    pub history: Arc<History>,
}

impl CdcPos {
    pub fn from_mark(mark: Mark, snapshot: Option<SnapshotPos>) -> Self {
        Self {
            binlog: mark.binlog,
            partial: mark.partial,
            snapshot,
            history: mark.history,
        }
    }

    pub fn to_position(&self) -> Position {
        let mut display = self.binlog.to_string();
        if let Some(p) = &self.partial {
            display.push_str(&format!(" (+{} of {})", p.delivered, p.txn));
        }
        match &self.snapshot {
            Some(SnapshotPos::Watermark { tables, .. }) => display.push_str(&format!(
                ", snapshot of {} pending",
                tables.front().map_or("-", String::as_str)
            )),
            Some(SnapshotPos::Locked { table }) => {
                display.push_str(&format!(", locked snapshot at table {table}"));
            }
            None => {}
        }
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque)
            .map_err(|e| ConnectorError::fatal(format!("not a MySQL CDC position: {e}")))
    }
}

/// One split: the binlog.
#[derive(Debug, Default)]
pub struct MyCdcEnumerator;

#[async_trait]
impl SplitEnumerator for MyCdcEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(SPLIT, serde_json::json!({}))])
    }
}

/// Makes the CDC source.
#[derive(Debug)]
pub struct MyCdcFactory {
    cfg: MyConfig,
    ctx: ConnectorContext,
    connector: &'static str,
}

impl MyCdcFactory {
    pub fn new(cfg: MyConfig, ctx: ConnectorContext, connector: &'static str) -> Self {
        Self {
            cfg,
            ctx,
            connector,
        }
    }
}

impl SourceFactory for MyCdcFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(MyCdcSource::new(
            self.cfg.clone(),
            self.ctx.clone(),
            self.connector,
        )?))
    }
}

/// What waits to be delivered, with the position after it.
#[derive(Debug)]
enum Item {
    Event(CloudEvent, CdcPos),
    Arrow(EventBatch, CdcPos),
    Poison(String, CdcPos),
}

/// The DBLog snapshot's progress.
#[derive(Debug)]
struct Snap {
    tables: VecDeque<TableName>,
    after: Option<Vec<Value>>,
    announced: bool,
}

impl Snap {
    fn pos(&self) -> Option<SnapshotPos> {
        (!self.tables.is_empty()).then(|| SnapshotPos::Watermark {
            tables: self.tables.iter().map(ToString::to_string).collect(),
            after: self.after.clone(),
        })
    }
}

/// A snapshot under the global read lock, before the stream starts.
#[derive(Debug)]
struct Locked {
    reader: MyConn,
    start: BinlogPos,
    tables: Vec<TableName>,
    idx: usize,
    after: Option<Vec<Value>>,
    rows: u64,
    announced: bool,
}

/// The current binlog position of the server.
pub async fn server_position(conn: &mut MyConn) -> Result<BinlogPos, ConnectorError> {
    let status = match conn.query("SHOW BINARY LOG STATUS").await {
        Ok(rs) => rs,
        Err(e) if e.code() == Some(1064) => conn
            .query("SHOW MASTER STATUS")
            .await
            .map_err(|e| e.to_connector("reading the binlog position"))?,
        Err(e) => return Err(e.to_connector("reading the binlog position")),
    };
    let file = status.get(0, "File").ok_or_else(|| {
        ConnectorError::fatal("the server has no binary log: enable log_bin (see the runbook)")
    })?;
    let pos: u64 = status
        .get(0, "Position")
        .and_then(|p| p.parse().ok())
        .unwrap_or(4);
    if conn.mariadb {
        let state = conn
            .query("SELECT @@GLOBAL.gtid_binlog_pos")
            .await
            .map_err(|e| e.to_connector("reading gtid_binlog_pos"))?
            .scalar()
            .unwrap_or_default();
        return Ok(BinlogPos::Mariadb {
            state: GtidList::parse(&state).map_err(ConnectorError::fatal)?,
            file,
            pos,
        });
    }
    let rs = conn
        .query("SELECT @@GLOBAL.gtid_mode, @@GLOBAL.gtid_executed")
        .await
        .map_err(|e| e.to_connector("reading gtid_mode"))?;
    if rs.rows.first().and_then(|r| r[0].as_deref()) == Some(b"ON") {
        let executed = rs.get(0, "@@GLOBAL.gtid_executed").unwrap_or_default();
        Ok(BinlogPos::Mysql {
            executed: GtidSet::parse(&executed).map_err(ConnectorError::fatal)?,
            file,
            pos,
        })
    } else {
        Ok(BinlogPos::File { file, pos })
    }
}

/// The server logs full row images in ROW format (the runbook's settings).
pub async fn check_server(conn: &mut MyConn) -> Result<(), ConnectorError> {
    let rs = conn
        .query("SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image")
        .await
        .map_err(|e| e.to_connector("reading the binlog settings"))?;
    let v = |i: usize| {
        rs.rows
            .first()
            .and_then(|r| r[i].as_ref())
            .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase())
            .unwrap_or_default()
    };
    if !matches!(v(0).as_str(), "1" | "ON") {
        return Err(ConnectorError::fatal(
            "the server's binary log is off: set log_bin (see docs/runbooks/connectors/mysql-cdc.md)",
        ));
    }
    if v(1) != "ROW" {
        return Err(ConnectorError::fatal(format!(
            "binlog_format is {}: native CDC needs ROW",
            v(1)
        )));
    }
    if v(2) != "FULL" {
        return Err(ConnectorError::fatal(format!(
            "binlog_row_image is {}: native CDC needs FULL",
            v(2)
        )));
    }
    Ok(())
}

/// Whether the server still has the binlog from `pos` (MySQL's GTID purge
/// and files; MariaDB answers the dump itself with error 1236).
pub async fn check_not_purged(conn: &mut MyConn, pos: &BinlogPos) -> Result<(), ConnectorError> {
    let lost = |what: String| {
        ConnectorError::position_lost(format!(
            "{what}: delete and recreate the instance to take a new snapshot"
        ))
    };
    match pos {
        BinlogPos::Mysql { executed, .. } => {
            let purged = conn
                .query("SELECT @@GLOBAL.gtid_purged")
                .await
                .map_err(|e| e.to_connector("reading gtid_purged"))?
                .scalar()
                .unwrap_or_default();
            let purged = GtidSet::parse(&purged).map_err(ConnectorError::fatal)?;
            if !executed.contains_set(&purged) {
                return Err(lost(format!(
                    "the server purged binary logs this instance has not read (gtid_purged {purged})"
                )));
            }
        }
        BinlogPos::File { file, .. } => {
            let rs = conn
                .query("SHOW BINARY LOGS")
                .await
                .map_err(|e| e.to_connector("listing the binary logs"))?;
            let have = (0..rs.rows.len()).any(|i| rs.get(i, "Log_name").as_deref() == Some(file));
            if !have {
                return Err(lost(format!("the binary log {file} was purged")));
            }
        }
        BinlogPos::Mariadb { .. } => {}
    }
    Ok(())
}

/// The CDC source. See the module documentation.
pub struct MyCdcSource {
    cfg: MyConfig,
    ctx: ConnectorContext,
    envelope: EnvelopeBuilder,
    policy: SchemaPolicy,
    captured: Captured,
    owner: String,
    control: Option<MyConn>,
    stream: Option<BinlogStream>,
    decoder: Option<Decoder>,
    snap: Option<Snap>,
    window: Option<Window>,
    locked: Option<Locked>,
    carry: VecDeque<Item>,
    pending: Option<RawEvent>,
    /// An error met after events were gathered: those go out first, with
    /// the position of the last of them, and the error on the next call.
    deferred: Option<ConnectorError>,
    last_position: Option<CdcPos>,
    budget: Budget,
    last_lag: Option<Instant>,
}

impl std::fmt::Debug for MyCdcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MyCdcSource")
            .field("instance", &self.ctx.instance)
            .finish_non_exhaustive()
    }
}

impl MyCdcSource {
    pub fn new(
        cfg: MyConfig,
        ctx: ConnectorContext,
        connector: &'static str,
    ) -> Result<Self, ConnectorError> {
        let policy = SchemaPolicy::from_config(&ctx.config)?;
        let captured = if cfg.tables.is_empty() {
            Captured::Database {
                database: cfg.database.clone(),
                except: [
                    cfg.watermark_table.clone(),
                    TableName::new(&cfg.database, crate::sink::CHECKPOINTS),
                ]
                .into(),
            }
        } else {
            Captured::Tables(cfg.tables.iter().cloned().collect())
        };
        Ok(Self {
            envelope: EnvelopeBuilder::new(connector, &ctx.instance),
            owner: ctx.instance.to_string(),
            captured,
            cfg,
            ctx,
            policy,
            control: None,
            stream: None,
            decoder: None,
            snap: None,
            window: None,
            locked: None,
            carry: VecDeque::new(),
            pending: None,
            deferred: None,
            last_position: None,
            budget: Budget {
                max_rows: 1024,
                max_bytes: 1 << 20,
            },
            last_lag: None,
        })
    }

    async fn control(&mut self) -> Result<&mut MyConn, ConnectorError> {
        if self.control.as_ref().is_none_or(MyConn::is_broken) {
            self.control = Some(connect(&self.cfg, &self.ctx).await?);
        }
        Ok(self.control.as_mut().expect("set above"))
    }

    async fn captured_tables(&mut self) -> Result<Vec<TableName>, ConnectorError> {
        if !self.cfg.tables.is_empty() {
            return Ok(self.cfg.tables.clone());
        }
        let db = self.cfg.database.clone();
        let captured = self.captured.clone();
        let all = list_tables(self.control().await?, &db).await?;
        Ok(all.into_iter().filter(|t| captured.contains(t)).collect())
    }

    fn position_now(&self) -> Option<CdcPos> {
        let d = self.decoder.as_ref()?;
        Some(CdcPos::from_mark(
            d.position(),
            self.snap.as_ref().and_then(Snap::pos),
        ))
    }

    async fn start_stream(&mut self, from: &BinlogPos) -> Result<(), ConnectorError> {
        let at = match from {
            BinlogPos::Mysql { executed, .. } => StartAt::MysqlGtid(executed.clone()),
            BinlogPos::Mariadb { state, .. } if !state.is_empty() => {
                StartAt::Mariadb(state.clone())
            }
            BinlogPos::Mariadb { file, pos, .. } | BinlogPos::File { file, pos } => StartAt::File {
                file: file.clone(),
                pos: *pos,
            },
        };
        let conn = connect(&self.cfg, &self.ctx).await?;
        self.stream =
            Some(BinlogStream::start(conn, &at, self.cfg.server_id, self.cfg.heartbeat).await?);
        Ok(())
    }

    fn report_lag(&mut self, event_ts: Option<u32>) {
        if self.last_lag.is_some_and(|t| t.elapsed() < LAG_EVERY) {
            return;
        }
        self.last_lag = Some(Instant::now());
        let lag = match event_ts {
            Some(ts) if ts > 0 => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs());
                Duration::from_secs(now.saturating_sub(u64::from(ts)))
            }
            _ => Duration::ZERO,
        };
        self.ctx
            .observe
            .lag(&self.ctx.instance, Some(&SplitId::new(SPLIT)), lag);
    }

    /// Feed one event; refresh tables from `information_schema` once when
    /// the history and the binlog disagree.
    async fn feed(&mut self, raw: RawEvent) -> Result<(), ConnectorError> {
        self.pending = Some(raw);
        let mut refreshed: Option<Vec<TableName>> = None;
        loop {
            let raw = self.pending.as_ref().expect("set above");
            let d = self.decoder.as_mut().expect("streaming");
            match d.feed(raw) {
                Ok(emitted) => {
                    self.pending = None;
                    self.absorb(emitted).await?;
                    // DDL the history could not follow: re-read the tables
                    // as they are now (ruling R11.5).
                    let tables = self.decoder.as_mut().expect("streaming").take_refresh();
                    for t in tables {
                        let def = describe_def(self.control().await?, &t).await?;
                        let d = self.decoder.as_mut().expect("streaming");
                        if let Some(e) = d.apply_refresh(&t, def)? {
                            self.absorb(vec![e]).await?;
                        }
                    }
                    return Ok(());
                }
                Err(FeedError::Error(e)) => {
                    self.pending = None;
                    return Err(e);
                }
                Err(FeedError::Refresh(tables)) => {
                    if refreshed.as_ref() == Some(&tables) {
                        self.pending = None;
                        return Err(ConnectorError::schema_incompatible(format!(
                            "the binlog's columns for {} do not match information_schema even after a refresh",
                            tables
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", ")
                        )));
                    }
                    for t in &tables {
                        let def = describe_def(self.control().await?, t).await?;
                        self.decoder.as_mut().expect("streaming").refreshed(t, def);
                    }
                    refreshed = Some(tables);
                }
            }
        }
    }

    async fn absorb(&mut self, emitted: Vec<Emitted>) -> Result<(), ConnectorError> {
        for e in emitted {
            match e {
                Emitted::Event {
                    event,
                    table,
                    key,
                    mark,
                } => {
                    if let (Some(w), Some(k)) = (&mut self.window, &key) {
                        w.changed(&table, k);
                    }
                    let pos = CdcPos::from_mark(mark, self.snap.as_ref().and_then(Snap::pos));
                    self.carry.push_back(Item::Event(event, pos));
                }
                Emitted::Poison { message, mark } => {
                    let pos = CdcPos::from_mark(mark, self.snap.as_ref().and_then(Snap::pos));
                    self.carry.push_back(Item::Poison(message, pos));
                }
                Emitted::Watermark { value, mark } => {
                    let closes = self.window.as_mut().is_some_and(|w| w.watermark(&value));
                    if closes {
                        let w = self.window.take().expect("checked");
                        self.close_window(w, mark).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// The high watermark arrived: deliver what is left of the chunk.
    async fn close_window(&mut self, w: Window, mark: Mark) -> Result<(), ConnectorError> {
        let table = w.table.clone();
        let high = w.high.clone();
        let schema = w.schema.clone();
        let (last_key, exhausted) = (w.last_key.clone(), w.exhausted);
        let rows = w.remaining();
        let mark_after = Mark {
            partial: None,
            ..mark
        };
        let before = self.snap.as_ref().and_then(Snap::pos);
        // Advance the cursor; the last piece carries it.
        if let Some(s) = &mut self.snap {
            if exhausted || last_key.is_none() {
                s.tables.pop_front();
                s.after = None;
                s.announced = false;
            } else {
                s.after = last_key;
            }
        }
        let after = self.snap.as_ref().and_then(Snap::pos);
        let budget = self.budget;
        let parts = pieces(
            &schema,
            &rows,
            budget.max_rows,
            budget.max_bytes.saturating_sub(1024),
        );
        let count = parts.len();
        let (file, end) = {
            let (f, p) = mark_after.binlog.file_pos();
            (f.to_owned(), p)
        };
        for (i, (first, part)) in parts.into_iter().enumerate() {
            let snapshot = if i + 1 == count {
                after.clone()
            } else {
                before.clone()
            };
            let pos = CdcPos::from_mark(mark_after.clone(), snapshot);
            match part {
                Ok(batch) => {
                    let last = first + batch.num_rows() - 1;
                    let mut envelope = self
                        .envelope
                        .event(
                            "change",
                            &table.to_string(),
                            format!("{high}/r/{table}/{first}-{last}"),
                        )
                        .with_subject(table.to_string())
                        .with_cdc(CdcOp::Read, loamslsn(&file, end, 0));
                    envelope.datacontenttype = Some(ARROW_STREAM.to_owned());
                    self.carry
                        .push_back(Item::Arrow(EventBatch::Arrow { batch, envelope }, pos));
                }
                Err(e) => self
                    .carry
                    .push_back(Item::Poison(format!("{table}: {e}"), pos)),
            }
        }
        Ok(())
    }

    /// Start the next DBLog chunk (announcing a table it starts).
    async fn open_window(&mut self) -> Result<(), ConnectorError> {
        let Some(s) = &self.snap else {
            return Ok(());
        };
        let Some(table) = s.tables.front().cloned() else {
            self.snap = None;
            return Ok(());
        };
        let after = s.after.clone();
        let announced = s.announced;
        let key = self.cfg.key.clone();
        let lossy = self.policy.lossy.clone();
        let limit = self.cfg.chunk_size.min(self.budget.max_rows).max(1);
        let watermark = self.cfg.watermark_table.clone();
        let owner = self.owner.clone();
        let conn = self.control().await?;
        let Some(def) = describe_def(conn, &table).await? else {
            // Dropped since the snapshot was planned: nothing to read.
            if let Some(s) = &mut self.snap {
                s.tables.pop_front();
                s.after = None;
                s.announced = false;
                if s.tables.is_empty() {
                    self.snap = None;
                }
            }
            return Ok(());
        };
        let info: TableInfo = crate::table::info(&table, def, &key, &lossy)?;
        if !announced {
            let at = Position::new(Bytes::new(), format!("snapshot {}", self.owner));
            let d = self.decoder.as_mut().expect("streaming");
            if d.history().get(&table).is_none() {
                d.refreshed(&table, Some(info.def.clone()));
            }
            if let Some(ev) = d.announce(&table, &at)? {
                let pos = self.position_now().expect("streaming");
                self.carry.push_back(Item::Event(ev, pos));
            }
            if let Some(s) = &mut self.snap {
                s.announced = true;
            }
        }
        let conn = self.control().await?;
        let w = Window::open(conn, &watermark, &owner, &info, after.as_deref(), limit).await?;
        self.window = Some(w);
        Ok(())
    }

    /// Fill `out` from the carry within the budget; `Some` when a whole
    /// answer (an Arrow batch, or a poison error) is at the front.
    fn take_carry(
        &mut self,
        budget: Budget,
        events: &mut Vec<CloudEvent>,
        bytes: &mut usize,
        last: &mut Option<CdcPos>,
    ) -> Option<Result<SourceBatch, ConnectorError>> {
        while let Some(front) = self.carry.front() {
            match front {
                Item::Arrow(..) | Item::Poison(..) if !events.is_empty() => return None,
                Item::Arrow(..) => {
                    let Some(Item::Arrow(batch, pos)) = self.carry.pop_front() else {
                        unreachable!("matched above")
                    };
                    self.last_position = Some(pos.clone());
                    return Some(Ok(SourceBatch {
                        events: batch,
                        position: pos.to_position(),
                    }));
                }
                Item::Poison(..) => {
                    let Some(Item::Poison(message, pos)) = self.carry.pop_front() else {
                        unreachable!("matched above")
                    };
                    self.last_position = Some(pos);
                    return Some(Err(ConnectorError::poison(message)));
                }
                Item::Event(ev, _) => {
                    let size = ev.size_bytes();
                    if events.is_empty() && size > budget.max_bytes {
                        let Some(Item::Event(ev, pos)) = self.carry.pop_front() else {
                            unreachable!("matched above")
                        };
                        self.last_position = Some(pos);
                        return Some(Err(ConnectorError::poison(format!(
                            "change {} is {size} bytes, over the batch budget of {}",
                            ev.id, budget.max_bytes
                        ))));
                    }
                    if events.len() >= budget.max_rows || *bytes + size > budget.max_bytes {
                        return None;
                    }
                    let Some(Item::Event(ev, pos)) = self.carry.pop_front() else {
                        unreachable!("matched above")
                    };
                    *bytes += size;
                    *last = Some(pos);
                    events.push(ev);
                }
            }
        }
        None
    }

    async fn next_stream(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        self.budget = budget;
        if self.carry.is_empty()
            && let Some(e) = self.deferred.take()
        {
            return Err(e);
        }
        if self.deferred.is_none()
            && let Some(raw) = self.pending.take()
        {
            self.feed(raw).await?;
        }
        let mut events: Vec<CloudEvent> = Vec::new();
        let mut bytes = 0usize;
        let mut last: Option<CdcPos> = None;
        let mut waited = false;
        loop {
            if let Some(answer) = self.take_carry(budget, &mut events, &mut bytes, &mut last) {
                return answer.map(Some);
            }
            if !self.carry.is_empty() || events.len() >= budget.max_rows || self.deferred.is_some()
            {
                break;
            }
            if self.window.is_none() && self.snap.is_some() && events.is_empty() {
                self.open_window().await?;
                if !self.carry.is_empty() {
                    continue;
                }
            }
            if let Some(w) = &self.window
                && w.opened.elapsed() > WINDOW_TIMEOUT
            {
                return Err(ConnectorError::fatal(format!(
                    "the watermark rows in {} never reached the binlog: is the table replicated (binlog-do-db, InnoDB)?",
                    self.cfg.watermark_table
                )));
            }
            let stream = self.stream.as_mut().expect("streaming");
            let raw = if waited || !events.is_empty() {
                match stream.try_recv() {
                    Some(r) => Some(r?),
                    None => None,
                }
            } else {
                waited = true;
                stream.recv(POLL).await?
            };
            let Some(raw) = raw else {
                self.report_lag(None);
                break;
            };
            let ts = raw.event.header().timestamp();
            if let Err(e) = self.feed(raw).await {
                // What came before the failing event (a refused DDL, say)
                // is delivered and checkpointed before the instance stops
                // or pauses on it.
                if events.is_empty() && self.carry.is_empty() {
                    return Err(e);
                }
                self.deferred = Some(e);
                break;
            }
            self.report_lag(Some(ts));
        }
        let position = if self.carry.is_empty() && self.deferred.is_none() {
            match self.position_now() {
                Some(p) => p,
                None => return Ok(None),
            }
        } else {
            match last {
                Some(p) => p,
                None => return Ok(None),
            }
        };
        if events.is_empty() && self.last_position.as_ref() == Some(&position) {
            return Ok(None);
        }
        self.last_position = Some(position.clone());
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: position.to_position(),
        }))
    }

    /// The locked snapshot, chunk by chunk, then the stream.
    async fn next_locked(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        loop {
            let l = self.locked.as_mut().expect("locked");
            if l.idx >= l.tables.len() {
                l.reader
                    .exec("COMMIT")
                    .await
                    .map_err(|e| e.to_connector("ending the snapshot"))?;
                let l = self.locked.take().expect("locked");
                self.start_stream(&l.start).await?;
                let pos = self.position_now().expect("decoder");
                self.last_position = Some(pos.clone());
                return Ok(Some(SourceBatch {
                    events: EventBatch::Events(Vec::new()),
                    position: pos.to_position(),
                }));
            }
            let table = l.tables[l.idx].clone();
            let snapshot = Some(SnapshotPos::Locked { table: l.idx });
            let mark = Mark {
                binlog: l.start.clone(),
                partial: None,
                history: self.decoder.as_ref().expect("decoder").history().clone(),
            };
            if !l.announced {
                l.announced = true;
                let at = Position::new(Bytes::new(), format!("snapshot {}", l.start));
                if let Some(ev) = self
                    .decoder
                    .as_mut()
                    .expect("decoder")
                    .announce(&table, &at)?
                {
                    return Ok(Some(SourceBatch {
                        events: EventBatch::Events(vec![ev]),
                        position: CdcPos::from_mark(mark, snapshot).to_position(),
                    }));
                }
                continue;
            }
            let info = describe(&mut l.reader, &table, &self.cfg.key, &self.policy.lossy).await?;
            let limit = self.cfg.chunk_size.min(budget.max_rows).max(1);
            let rows = read_chunk(&mut l.reader, &info, l.after.as_deref(), limit).await?;
            if rows.is_empty() {
                l.idx += 1;
                l.after = None;
                l.rows = 0;
                l.announced = false;
                continue;
            }
            // Take only what fits the budget; the rest is read again.
            let mut parts = pieces(
                &info.schema,
                &rows,
                budget.max_rows,
                budget.max_bytes.saturating_sub(1024),
            );
            let (first_idx, part) = parts.remove(0);
            let taken = match &part {
                Ok(b) => b.num_rows(),
                Err(_) => 1,
            };
            debug_assert_eq!(first_idx, 0);
            l.after = Some(crate::table::key_of(&info, &rows[taken - 1]));
            let first = l.rows;
            l.rows += taken as u64;
            let run = ids::run_id(&self.ctx.instance, &table.to_string(), &l.start.to_string());
            let (file, end) = {
                let (f, p) = l.start.file_pos();
                (f.to_owned(), p)
            };
            let pos = CdcPos::from_mark(mark, snapshot).to_position();
            let batch = part.map_err(|e| ConnectorError::poison(format!("{table}: {e}")))?;
            let mut envelope = self
                .envelope
                .event(
                    "change",
                    &table.to_string(),
                    format!("{run}/r/{table}/{first}-{}", l.rows - 1),
                )
                .with_subject(table.to_string())
                .with_cdc(CdcOp::Read, loamslsn(&file, end, 0));
            envelope.datacontenttype = Some(ARROW_STREAM.to_owned());
            return Ok(Some(SourceBatch {
                events: EventBatch::Arrow { batch, envelope },
                position: pos,
            }));
        }
    }
}

#[async_trait]
impl Source for MyCdcSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let resume = pos.as_ref().map(CdcPos::from_position).transpose()?;
        {
            let c = self.control().await?;
            check_server(c).await?;
        }
        let tables = self.captured_tables().await?;
        let watermark = self.cfg.watermark_table.clone();
        let wm_def = describe_def(self.control().await?, &watermark).await?;
        if self.cfg.snapshot
            && self.cfg.locking == Locking::Watermark
            && wm_def.is_none()
            && resume.as_ref().is_none_or(|p| p.snapshot.is_some())
        {
            return Err(ConnectorError::fatal(format!(
                "the watermark table {watermark} does not exist: create it as the runbook says ({}), \
                 or set snapshot.locking: global-read-lock",
                watermark_ddl(&watermark)
            )));
        }
        let fresh = !matches!(
            resume,
            Some(CdcPos {
                snapshot: None | Some(SnapshotPos::Watermark { .. }),
                ..
            })
        );
        let (start, partial, snapshot, mut history) = if fresh {
            let mut history = History::default();
            for t in &tables {
                if let Some(def) = describe_def(self.control().await?, t).await? {
                    history.set(t, def);
                }
            }
            if self.cfg.snapshot && self.cfg.locking == Locking::GlobalReadLock {
                // Lock, read the position, begin the consistent read, unlock.
                let mut reader = connect(&self.cfg, &self.ctx).await?;
                locked_start(&mut reader).await?;
                let start = server_position(&mut reader).await;
                let begun = locked_begin(&mut reader).await;
                let start = start?;
                begun?;
                if let Some(def) = &wm_def {
                    history.set(&watermark, def.clone());
                }
                let history = Arc::new(history);
                let mut d = self.new_decoder(start.clone(), None, history);
                for t in &tables {
                    d.seed(t).ok();
                }
                self.decoder = Some(d);
                self.locked = Some(Locked {
                    reader,
                    start,
                    tables,
                    idx: 0,
                    after: None,
                    rows: 0,
                    announced: false,
                });
                return Ok(());
            }
            let start = server_position(self.control().await?).await?;
            let snapshot = self.cfg.snapshot.then(|| SnapshotPos::Watermark {
                tables: tables.iter().map(ToString::to_string).collect(),
                after: None,
            });
            (start, None, snapshot, history)
        } else {
            let p = resume.expect("not fresh");
            check_not_purged(self.control().await?, &p.binlog).await?;
            let mut history = (*p.history).clone();
            // Tables the config gained since are read from the catalog now.
            for t in &tables {
                if history.get(t).is_none()
                    && let Some(def) = describe_def(self.control().await?, t).await?
                {
                    history.set(t, def);
                }
            }
            (p.binlog, p.partial, p.snapshot, history)
        };
        if let Some(def) = wm_def {
            history.set(&watermark, def);
        }
        let mut d = self.new_decoder(start.clone(), partial, Arc::new(history));
        let in_snapshot: BTreeSet<String> = match &snapshot {
            Some(SnapshotPos::Watermark { tables, .. }) => tables.iter().cloned().collect(),
            _ => BTreeSet::new(),
        };
        for t in &tables {
            // A table the snapshot has not started is announced when it does.
            if !in_snapshot.contains(&t.to_string()) || !fresh {
                d.seed(t)?;
            }
        }
        self.decoder = Some(d);
        self.snap = match snapshot {
            Some(SnapshotPos::Watermark { tables, after }) => Some(Snap {
                announced: after.is_some() || !fresh,
                tables: tables
                    .iter()
                    .map(|t| TableName::parse(t, &self.cfg.database))
                    .collect::<Result<_, _>>()?,
                after,
            }),
            _ => None,
        };
        self.start_stream(&start).await?;
        self.last_position = self.position_now();
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.locked.is_some() {
            return self.next_locked(budget).await;
        }
        if self.stream.is_none() {
            return Err(ConnectorError::fatal("the CDC source is not open"));
        }
        self.next_stream(budget).await
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        // A binlog dump has no acknowledgement: the checkpoint is the
        // replica's position.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.stream = None;
        self.window = None;
        self.locked = None;
        if let Some(c) = self.control.take() {
            c.quit().await;
        }
        Ok(())
    }
}

impl MyCdcSource {
    fn new_decoder(
        &self,
        start: BinlogPos,
        partial: Option<Partial>,
        history: Arc<History>,
    ) -> Decoder {
        Decoder::new(
            self.envelope.clone(),
            &self.policy,
            self.captured.clone(),
            self.cfg.watermark_table.clone(),
            self.owner.clone(),
            start,
            partial,
            history,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnDef;
    use serde_json::json;

    #[test]
    fn positions_roundtrip_with_history_and_snapshot() {
        let mut h = History::default();
        h.set(
            &TableName::new("d", "t"),
            crate::history::TableDef {
                columns: vec![ColumnDef::new("id", "bigint", false)],
                key: vec!["id".into()],
            },
        );
        let p = CdcPos {
            binlog: BinlogPos::Mysql {
                executed: GtidSet::parse("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9").expect("set"),
                file: "binlog.000003".into(),
                pos: 157,
            },
            partial: Some(Partial {
                txn: "3e11fa47-71ca-11e1-9e33-c80aa9429562:10".into(),
                delivered: 2,
            }),
            snapshot: Some(SnapshotPos::Watermark {
                tables: ["d.t".to_owned()].into(),
                after: Some(vec![json!(41)]),
            }),
            history: Arc::new(h),
        };
        let pos = p.to_position();
        assert_eq!(CdcPos::from_position(&pos).expect("back"), p);
        assert!(pos.display.contains("+2 of"), "{}", pos.display);
        assert!(pos.display.contains("snapshot of d.t"), "{}", pos.display);
        assert!(CdcPos::from_position(&Position::from_u64(3)).is_err());
    }

    use crate::config::Target;
    use crate::history::TableDef;
    use crate::snapshot::WindowState;
    use crate::synth::*;
    use loams_connector_sdk::{InstanceKey, Scrubber, StaticSecrets};
    use mysql_common::binlog::consts::EventType;
    use tokio::sync::mpsc;

    fn t() -> TableName {
        TableName::new("d", "t")
    }

    /// A CDC source mid-snapshot, its stream fed by the test: no server.
    fn source(tx_rows: &[(i64, &str)]) -> (MyCdcSource, mpsc::Sender<crate::binlog::StreamItem>) {
        let inst = InstanceKey::new(1, "i");
        let config =
            json!({"host": "h", "user": "u", "database": "d", "mode": "cdc", "tables": ["t"]});
        let ctx = ConnectorContext::new(
            inst.clone(),
            config.clone(),
            Arc::new(StaticSecrets::new()),
            Scrubber::new(),
        );
        let cfg = MyConfig::parse(Target::Mysql, &inst, &config).expect("config");
        let mut src = MyCdcSource::new(cfg, ctx, "mysql").expect("source");
        let mut h = History::default();
        let def = TableDef {
            columns: vec![
                ColumnDef::new("id", "bigint", false),
                ColumnDef::new("v", "varchar(40)", true),
            ],
            key: vec!["id".into()],
        };
        h.set(&t(), def.clone());
        h.set(
            &TableName::new("d", "_loams_watermarks"),
            TableDef {
                columns: vec![
                    ColumnDef::new("instance", "varchar(255)", false),
                    ColumnDef::new("mark", "varchar(64)", false),
                ],
                key: vec!["instance".into()],
            },
        );
        let pos = BinlogPos::Mysql {
            executed: GtidSet::default(),
            file: "binlog.000001".into(),
            pos: 4,
        };
        let mut d = src.new_decoder(pos, None, Arc::new(h));
        d.seed(&t()).expect("seed");
        src.decoder = Some(d);
        src.snap = Some(Snap {
            tables: [t()].into(),
            after: None,
            announced: true,
        });
        let info = crate::table::info(&t(), def, &[], &Default::default()).expect("info");
        src.window = Some(Window {
            low: "L".into(),
            high: "H".into(),
            state: WindowState::Pending,
            table: t(),
            schema: info.schema.clone(),
            rows: tx_rows
                .iter()
                .enumerate()
                .map(|(i, (k, v))| {
                    (
                        crate::snapshot::key_text(&[json!(k)]),
                        (i, vec![json!(k), json!(v)]),
                    )
                })
                .collect(),
            last_key: tx_rows.last().map(|(k, _)| vec![json!(k)]),
            exhausted: true,
            opened: Instant::now(),
        });
        let (tx, rx) = mpsc::channel(64);
        src.stream = Some(BinlogStream::from_receiver(rx));
        (src, tx)
    }

    /// DBLog in the source: a change before the low watermark leaves the
    /// chunk alone, one between the watermarks removes its row, and the
    /// rest of the chunk follows the changes as one Arrow batch whose
    /// position finishes the snapshot.
    #[tokio::test]
    async fn dblog_windows_reconcile_and_advance_the_cursor() {
        let (mut src, tx) = source(&[(1, "a"), (2, "b"), (3, "c")]);
        let mut s = Synth::new();
        let cols = [ID, V40];
        let events = vec![
            s.gtid(1),
            s.query("d", "BEGIN"),
            s.table_map(42, "d", "t", &cols, &[]),
            s.rows(
                EventType::UPDATE_ROWS_EVENT,
                42,
                &cols,
                &[
                    &[Val::Int(1), Val::Str(b"a")],
                    &[Val::Int(1), Val::Str(b"A")],
                ],
            ),
            s.xid(),
            s.gtid(2),
            s.query("d", "BEGIN"),
            s.table_map(50, "d", "_loams_watermarks", &WM_COLS, &[]),
            s.watermark(50, b"1/i", b"L"),
            s.xid(),
            s.gtid(3),
            s.query("d", "BEGIN"),
            s.table_map(42, "d", "t", &cols, &[]),
            s.rows(
                EventType::UPDATE_ROWS_EVENT,
                42,
                &cols,
                &[
                    &[Val::Int(2), Val::Str(b"b")],
                    &[Val::Int(2), Val::Str(b"B")],
                ],
            ),
            s.rows(
                EventType::WRITE_ROWS_EVENT,
                42,
                &cols,
                &[&[Val::Int(9), Val::Str(b"x")]],
            ),
            s.xid(),
            s.gtid(4),
            s.query("d", "BEGIN"),
            s.table_map(50, "d", "_loams_watermarks", &WM_COLS, &[]),
            s.watermark(50, b"1/i", b"H"),
            s.xid(),
        ];
        for e in events {
            tx.send(Ok(e)).await.expect("send");
        }
        let budget = Budget {
            max_rows: 100,
            max_bytes: 1 << 20,
        };
        let first = src.next(budget).await.expect("next").expect("a batch");
        let EventBatch::Events(evs) = &first.events else {
            panic!("changes first")
        };
        let ops: Vec<(&str, i64)> = evs
            .iter()
            .map(|e| {
                let d: Value = serde_json::from_slice(&e.data).expect("json");
                (
                    e.extensions["loamsop"].as_str(),
                    d["after"]["id"].as_i64().expect("id"),
                )
            })
            .collect();
        assert_eq!(ops, [("u", 1), ("u", 2), ("c", 9)]);
        let p = CdcPos::from_position(&first.position).expect("pos");
        assert!(p.snapshot.is_some(), "the chunk is not delivered yet");

        let second = src.next(budget).await.expect("next").expect("the chunk");
        let EventBatch::Arrow { batch, envelope } = &second.events else {
            panic!("the chunk second")
        };
        let ids: Vec<i64> = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("ids")
            .values()
            .to_vec();
        assert_eq!(ids, [1, 3]);
        assert_eq!(envelope.extensions["loamsop"], "r");
        assert_eq!(envelope.id, "H/r/d.t/0-1");
        let p = CdcPos::from_position(&second.position).expect("pos");
        assert_eq!(p.snapshot, None, "the last chunk finished the snapshot");

        // Nothing more: the position moves past the high watermark's
        // transaction, and the snapshot is over.
        let third = src.next(budget).await.expect("next").expect("a position");
        assert!(third.events.is_empty());
        let p = CdcPos::from_position(&third.position).expect("pos");
        let BinlogPos::Mysql { executed, .. } = &p.binlog else {
            panic!("flavour")
        };
        assert!(executed.contains(&UUID, None, 4));
        assert!(src.snap.is_none() && src.window.is_none());
        assert!(src.next(budget).await.expect("next").is_none());
    }

    use crate::conn::Login;
    use crate::conn::fake::{Reply, row, script};

    async fn login(port: u16) -> MyConn {
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp");
        let login = Login {
            user: "u".into(),
            password: None,
            database: None,
        };
        MyConn::handshake(tcp, "127.0.0.1", None, &login)
            .await
            .expect("login")
    }

    const SET: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5";

    #[tokio::test]
    async fn server_positions_by_flavour() {
        let (port, _) = script(
            "8.4.10",
            |sql| {
                if sql.starts_with("SHOW BINARY LOG STATUS") {
                    vec![Reply::Rows(
                        vec!["File", "Position"],
                        vec![row(&["binlog.000003", "157"])],
                    )]
                } else if sql.contains("gtid_mode") {
                    vec![Reply::Rows(
                        vec!["@@GLOBAL.gtid_mode", "@@GLOBAL.gtid_executed"],
                        vec![row(&["ON", SET])],
                    )]
                } else {
                    vec![Reply::Ok(0)]
                }
            },
            vec![],
        )
        .await;
        let p = server_position(&mut login(port).await).await.expect("pos");
        assert_eq!(
            p,
            BinlogPos::Mysql {
                executed: GtidSet::parse(SET).expect("set"),
                file: "binlog.000003".into(),
                pos: 157
            }
        );

        let (port, _) = script(
            "8.0.40",
            |sql| {
                if sql.starts_with("SHOW BINARY LOG STATUS") {
                    vec![Reply::Rows(
                        vec!["File", "Position"],
                        vec![row(&["binlog.000009", "4"])],
                    )]
                } else if sql.contains("gtid_mode") {
                    vec![Reply::Rows(
                        vec!["@@GLOBAL.gtid_mode", "@@GLOBAL.gtid_executed"],
                        vec![row(&["OFF", ""])],
                    )]
                } else {
                    vec![Reply::Ok(0)]
                }
            },
            vec![],
        )
        .await;
        let p = server_position(&mut login(port).await).await.expect("pos");
        assert_eq!(
            p,
            BinlogPos::File {
                file: "binlog.000009".into(),
                pos: 4
            }
        );

        // MariaDB: no SHOW BINARY LOG STATUS before 10.5.2, GTIDs always on.
        let (port, _) = script(
            "5.5.5-11.8.8-MariaDB",
            |sql| {
                if sql.starts_with("SHOW BINARY LOG STATUS") {
                    vec![Reply::Err(1064, "You have an error in your SQL syntax")]
                } else if sql.starts_with("SHOW MASTER STATUS") {
                    vec![Reply::Rows(
                        vec!["File", "Position"],
                        vec![row(&["mariadb-bin.000002", "1000"])],
                    )]
                } else if sql.contains("gtid_binlog_pos") {
                    vec![Reply::Rows(vec!["p"], vec![row(&["0-1-9,1-2-3"])])]
                } else {
                    vec![Reply::Ok(0)]
                }
            },
            vec![],
        )
        .await;
        let p = server_position(&mut login(port).await).await.expect("pos");
        assert_eq!(
            p,
            BinlogPos::Mariadb {
                state: GtidList::parse("0-1-9,1-2-3").expect("list"),
                file: "mariadb-bin.000002".into(),
                pos: 1000
            }
        );
    }

    #[tokio::test]
    async fn server_settings_are_checked() {
        for (row_vals, ok) in [
            (["1", "ROW", "FULL"], true),
            (["1", "MIXED", "FULL"], false),
            (["1", "ROW", "MINIMAL"], false),
            (["0", "ROW", "FULL"], false),
        ] {
            let (port, _) = script(
                "8.4.10",
                move |_| {
                    vec![Reply::Rows(
                        vec!["log_bin", "binlog_format", "binlog_row_image"],
                        vec![row(&row_vals)],
                    )]
                },
                vec![],
            )
            .await;
            let r = check_server(&mut login(port).await).await;
            assert_eq!(r.is_ok(), ok, "{row_vals:?}: {r:?}");
        }
    }

    /// Review Focus 6: a purged position stops the instance, with the hint.
    #[tokio::test]
    async fn purged_positions_are_lost() {
        let (port, _) = script(
            "8.4.10",
            |sql| {
                if sql.contains("gtid_purged") {
                    vec![Reply::Rows(
                        vec!["p"],
                        vec![row(&["3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"])],
                    )]
                } else if sql.starts_with("SHOW BINARY LOGS") {
                    vec![Reply::Rows(
                        vec!["Log_name", "File_size"],
                        vec![row(&["binlog.000004", "157"])],
                    )]
                } else {
                    vec![Reply::Ok(0)]
                }
            },
            vec![],
        )
        .await;
        let mut c = login(port).await;
        let gtid = BinlogPos::Mysql {
            executed: GtidSet::parse(SET).expect("set"),
            file: "binlog.000003".into(),
            pos: 4,
        };
        let e = check_not_purged(&mut c, &gtid).await.expect_err("purged");
        assert_eq!(e.class, loams_connector_sdk::ErrorClass::PositionLost);
        assert!(e.message.contains("new snapshot"), "{}", e.message);
        let kept = BinlogPos::Mysql {
            executed: GtidSet::parse("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-20").expect("set"),
            file: "binlog.000003".into(),
            pos: 4,
        };
        assert!(check_not_purged(&mut c, &kept).await.is_ok());
        let file = BinlogPos::File {
            file: "binlog.000003".into(),
            pos: 4,
        };
        let e = check_not_purged(&mut c, &file)
            .await
            .expect_err("purged file");
        assert_eq!(e.class, loams_connector_sdk::ErrorClass::PositionLost);
    }

    /// A scripted MySQL 8.4 with GTIDs, table `d.t (id, v)` and no
    /// watermark table, whose binlog dump sends `dump`.
    async fn scripted(dump: Vec<Vec<u8>>) -> (u16, crate::conn::fake::Seen) {
        script(
            "8.4.10",
            |sql| {
                if sql.starts_with("SELECT @@GLOBAL.log_bin") {
                    vec![Reply::Rows(
                        vec!["a", "b", "c"],
                        vec![row(&["1", "ROW", "FULL"])],
                    )]
                } else if sql.starts_with("SELECT COLUMN_NAME, COLUMN_TYPE") {
                    if sql.contains("X'74'") {
                        vec![
                            Reply::Rows(
                                vec!["COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE"],
                                vec![
                                    row(&["id", "bigint", "NO"]),
                                    row(&["v", "varchar(40)", "YES"]),
                                ],
                            ),
                            Reply::Rows(vec!["COLUMN_NAME"], vec![row(&["id"])]),
                        ]
                    } else {
                        vec![
                            Reply::Rows(vec!["COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE"], vec![]),
                            Reply::Rows(vec!["COLUMN_NAME"], vec![]),
                        ]
                    }
                } else if sql.starts_with("SHOW BINARY LOG STATUS") {
                    vec![Reply::Rows(
                        vec!["File", "Position"],
                        vec![row(&["binlog.000003", "157"])],
                    )]
                } else if sql.contains("gtid_mode") {
                    vec![Reply::Rows(
                        vec!["@@GLOBAL.gtid_mode", "@@GLOBAL.gtid_executed"],
                        vec![row(&["ON", SET])],
                    )]
                } else {
                    vec![Reply::Ok(0)]
                }
            },
            dump,
        )
        .await
    }

    fn scripted_source(port: u16, extra: Value) -> MyCdcSource {
        let inst = InstanceKey::new(1, "i");
        let mut config = json!({"host": "127.0.0.1", "port": port, "user": "u", "database": "d",
                                "mode": "cdc", "tables": ["t"]});
        for (k, v) in extra.as_object().expect("object") {
            config[k] = v.clone();
        }
        let ctx = ConnectorContext::new(
            inst.clone(),
            config.clone(),
            Arc::new(StaticSecrets::new()),
            Scrubber::new(),
        );
        let cfg = MyConfig::parse(Target::Mysql, &inst, &config).expect("config");
        MyCdcSource::new(cfg, ctx, "mysql").expect("source")
    }

    /// `open` checks the server, reads its GTID set, starts the dump from
    /// it, and the stream's transaction arrives as a change.
    #[tokio::test]
    async fn open_starts_at_the_server_position_and_streams() {
        let mut s = Synth::new();
        let cols = [ID, V40];
        let mut dump = Vec::new();
        s.gtid(6);
        dump.push(s.packet());
        s.query("d", "BEGIN");
        dump.push(s.packet());
        s.table_map(42, "d", "t", &cols, &[]);
        dump.push(s.packet());
        s.rows(
            EventType::WRITE_ROWS_EVENT,
            42,
            &cols,
            &[&[Val::Int(5), Val::Str(b"five")]],
        );
        dump.push(s.packet());
        s.xid();
        dump.push(s.packet());
        let (port, seen) = scripted(dump).await;
        let mut src = scripted_source(port, json!({"snapshot": "never"}));
        src.open(&Split::new(SPLIT, json!({})), None)
            .await
            .expect("open");
        let budget = Budget {
            max_rows: 100,
            max_bytes: 1 << 20,
        };
        let mut got = Vec::new();
        for _ in 0..20 {
            if let Some(b) = src.next(budget).await.expect("next")
                && let EventBatch::Events(evs) = b.events
            {
                got.extend(evs);
            }
            if !got.is_empty() {
                break;
            }
        }
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].id, format!("{UUID_TEXT}:6/0"));
        let d: Value = serde_json::from_slice(&got[0].data).expect("json");
        assert_eq!(d["after"], json!({"id": 5, "v": "five"}));
        let dump_cmd = seen
            .lock()
            .expect("seen")
            .iter()
            .find(|p| p[0] == 0x1e)
            .cloned()
            .expect("COM_BINLOG_DUMP_GTID");
        assert!(dump_cmd.windows(16).any(|w| w == UUID));
        src.close().await.expect("close");
    }

    /// A DBLog snapshot needs the watermark table; without it `open` says
    /// how to create it, or to lock instead.
    #[tokio::test]
    async fn a_missing_watermark_table_is_explained() {
        let (port, _) = scripted(vec![]).await;
        let mut src = scripted_source(port, json!({}));
        let e = src
            .open(&Split::new(SPLIT, json!({})), None)
            .await
            .expect_err("no watermark table");
        assert!(
            e.message
                .contains("CREATE TABLE IF NOT EXISTS `d`.`_loams_watermarks`"),
            "{}",
            e.message
        );
        assert!(e.message.contains("global-read-lock"), "{}", e.message);
    }

    /// A batch never exceeds the budget's rows; the rest waits in order.
    #[tokio::test]
    async fn changes_respect_the_budget() {
        let (mut src, tx) = source(&[]);
        src.snap = None;
        src.window = None;
        let mut s = Synth::new();
        let cols = [ID, V40];
        let rows: Vec<Vec<Val<'_>>> = (0..5).map(|i| vec![Val::Int(i), Val::Null]).collect();
        let refs: Vec<&[Val<'_>]> = rows.iter().map(Vec::as_slice).collect();
        for e in [
            s.gtid(1),
            s.query("d", "BEGIN"),
            s.table_map(42, "d", "t", &cols, &[]),
            s.rows(EventType::WRITE_ROWS_EVENT, 42, &cols, &refs),
            s.xid(),
        ] {
            tx.send(Ok(e)).await.expect("send");
        }
        let budget = Budget {
            max_rows: 2,
            max_bytes: 1 << 20,
        };
        let mut seen = Vec::new();
        let mut partials = Vec::new();
        while let Some(b) = src.next(budget).await.expect("next") {
            assert!(b.events.rows() <= 2);
            if let EventBatch::Events(evs) = &b.events {
                seen.extend(evs.iter().map(|e| e.id.clone()));
            }
            partials.push(CdcPos::from_position(&b.position).expect("pos").partial);
        }
        assert_eq!(
            seen,
            (0..5)
                .map(|i| format!("{UUID_TEXT}:1/{i}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            partials[0],
            Some(Partial {
                txn: format!("{UUID_TEXT}:1"),
                delivered: 2
            })
        );
        assert_eq!(partials.last(), Some(&None));
    }
}
