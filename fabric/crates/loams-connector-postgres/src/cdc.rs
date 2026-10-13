//! Native Postgres CDC (OR-9): a pgoutput slot with a snapshot handoff.
//!
//! One split per instance, named after the slot. On a fresh start the slot
//! is created with `EXPORT_SNAPSHOT`; the initial snapshot reads every
//! published table with binary `COPY` inside a `REPEATABLE READ`
//! transaction on that exported snapshot (Arrow batches, `loamsop = r`),
//! while the replication connection stays idle so the snapshot stays valid;
//! streaming then starts at the slot's consistent point. The snapshot sees
//! exactly the transactions committed before the consistent point and the
//! stream exactly those after it: no gap, no overlap.
//!
//! The position is [`CdcPos`]; `committed` sends a Standby Status Update
//! with its flush LSN, so the server releases WAL only once the Fabric has
//! the data (Review Focus 6). Keepalives and an idle heartbeat
//! (`pg_logical_emit_message`) move the position when the captured tables
//! are quiet. The slot is dropped by [`delete_instance`] (on
//! `DeleteInstance`), never by `close` (`StopInstance`).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use loams_connector_sdk::schema::{SchemaChange, SchemaPolicy};
use loams_connector_sdk::{
    Budget, CdcOp, CloudEvent, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch,
    Position, Source, SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use tokio_postgres::Client;

use crate::config::PgConfig;
use crate::conn::{classify, connect};
use crate::decode::{CdcPos, Decoder, Emitted, all_nullable};
use crate::replication::{Lsn, ReplConn, ReplEvent, ReplStream, format_lsn};
use crate::rows::RawRow;
use crate::slot::{self, SlotLag};
use crate::table::{CopyStream, TableInfo, batch_within, describe};
use crate::types::quote_literal;

/// The datacontenttype of the snapshot's Arrow batches (R5.5).
pub const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

/// A schema-change event for a table the snapshot starts (a new table).
fn new_table_event(envelope: &EnvelopeBuilder, t: &TableInfo, at: &CdcPos) -> CloudEvent {
    SchemaChange {
        table: t.name.to_string(),
        before: Arc::new(arrow_schema::Schema::empty()),
        after: all_nullable(&t.schema),
        at: at.to_position(),
    }
    .event(envelope)
}

/// One split: the slot.
#[derive(Debug)]
pub struct PgCdcEnumerator {
    slot: String,
}

impl PgCdcEnumerator {
    pub fn new(cfg: &PgConfig) -> Self {
        Self {
            slot: cfg.slot_name(),
        }
    }
}

#[async_trait]
impl SplitEnumerator for PgCdcEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(
            self.slot.clone(),
            serde_json::json!({ "slot": self.slot }),
        )])
    }
}

/// Makes the CDC source; holds the slot's lag for metrics.
pub struct PgCdcFactory {
    cfg: PgConfig,
    ctx: ConnectorContext,
    connector: &'static str,
    lag: Arc<Mutex<Option<SlotLag>>>,
}

impl std::fmt::Debug for PgCdcFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgCdcFactory")
            .field("connector", &self.connector)
            .field("slot", &self.cfg.slot_name())
            .finish_non_exhaustive()
    }
}

impl PgCdcFactory {
    pub fn new(cfg: PgConfig, ctx: ConnectorContext, connector: &'static str) -> Self {
        Self {
            cfg,
            ctx,
            connector,
            lag: Arc::new(Mutex::new(None)),
        }
    }

    /// The slot's last sampled lag (bytes, seconds, `max_slot_wal_keep_size`
    /// headroom), for Task 24's metrics.
    pub fn lag(&self) -> Option<SlotLag> {
        *self.lag.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl SourceFactory for PgCdcFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(PgCdcSource::new(
            self.cfg.clone(),
            self.ctx.clone(),
            self.connector,
            self.lag.clone(),
        )?))
    }
}

struct Snapshot {
    repl: ReplConn,
    reader: Client,
    consistent: Lsn,
    tables: Vec<TableInfo>,
    idx: usize,
    rows: u64,
    copy: Option<CopyStream>,
    copy_done: bool,
    pending: VecDeque<RawRow>,
    announced: bool,
}

struct Streaming {
    stream: ReplStream,
    decoder: Decoder,
}

enum Phase {
    Closed,
    Snapshot(Box<Snapshot>),
    Stream(Box<Streaming>),
}

/// The CDC source of one slot.
pub struct PgCdcSource {
    cfg: PgConfig,
    ctx: ConnectorContext,
    envelope: EnvelopeBuilder,
    policy: SchemaPolicy,
    slot: String,
    control: Option<Client>,
    phase: Phase,
    carry: VecDeque<Emitted>,
    last_position: Option<CdcPos>,
    last_activity: Instant,
    last_heartbeat: Instant,
    last_lag: Option<Instant>,
    lag: Arc<Mutex<Option<SlotLag>>>,
    custom_types: Vec<(u32, String)>,
}

impl std::fmt::Debug for PgCdcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgCdcSource")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

/// How long `next` waits for the stream before answering "nothing now".
const POLL: Duration = Duration::from_millis(200);
/// How often the slot's lag is sampled.
const LAG_EVERY: Duration = Duration::from_secs(30);

impl PgCdcSource {
    pub fn new(
        cfg: PgConfig,
        ctx: ConnectorContext,
        connector: &'static str,
        lag: Arc<Mutex<Option<SlotLag>>>,
    ) -> Result<Self, ConnectorError> {
        let policy = SchemaPolicy::from_config(&ctx.config)?;
        Ok(Self {
            envelope: EnvelopeBuilder::new(connector, &ctx.instance),
            slot: cfg.slot_name(),
            cfg,
            ctx,
            policy,
            control: None,
            phase: Phase::Closed,
            carry: VecDeque::new(),
            last_position: None,
            last_activity: Instant::now(),
            last_heartbeat: Instant::now(),
            last_lag: None,
            lag,
            custom_types: Vec::new(),
        })
    }

    fn control(&self) -> Result<&Client, ConnectorError> {
        self.control
            .as_ref()
            .ok_or_else(|| ConnectorError::fatal("the CDC source is not open"))
    }

    async fn start_stream(
        &mut self,
        repl: ReplConn,
        from: &CdcPos,
        seed: &[TableInfo],
    ) -> Result<(), ConnectorError> {
        let v14 = repl.server_version >= 140_000;
        let proto = if v14 { 2 } else { 1 };
        let (publication, _) = self.cfg.publication_name();
        let mut options = vec![
            ("proto_version", proto.to_string()),
            ("publication_names", publication),
        ];
        if v14 {
            options.push(("messages", "true".to_owned()));
            if self.cfg.streaming {
                options.push(("streaming", "on".to_owned()));
            }
        }
        let start = from.flush_lsn().unwrap_or(0);
        let stream = repl
            .start_replication(&self.slot, start, &options, self.cfg.status_interval)
            .await?;
        let mut decoder = Decoder::new(
            &self.slot,
            self.envelope.clone(),
            &self.policy,
            proto,
            from,
            self.cfg.stream_buffer_bytes,
        );
        for (oid, name) in &self.custom_types {
            decoder.learn_type(*oid, name);
        }
        for t in seed {
            decoder.seed(&t.name.to_string(), &t.schema);
        }
        self.phase = Phase::Stream(Box::new(Streaming { stream, decoder }));
        Ok(())
    }

    async fn describe_all(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        let control = self.control()?;
        let (publication, _) = self.cfg.publication_name();
        let mut out = Vec::new();
        for t in slot::publication_tables(control, &publication).await? {
            out.push(describe(control, &t, &[], &self.policy.lossy).await?);
        }
        Ok(out)
    }

    async fn next_snapshot(
        &mut self,
        budget: Budget,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        loop {
            let Phase::Snapshot(s) = &mut self.phase else {
                return Ok(None);
            };
            if s.idx >= s.tables.len() {
                // The snapshot is complete: end its transaction and stream
                // from the consistent point.
                s.reader
                    .batch_execute("COMMIT")
                    .await
                    .map_err(|e| classify(&e, "ending the snapshot"))?;
                let Phase::Snapshot(s) = std::mem::replace(&mut self.phase, Phase::Closed) else {
                    unreachable!("matched above");
                };
                let s = *s;
                let from = CdcPos::Stream {
                    flushed: s.consistent,
                    partial: None,
                };
                self.start_stream(s.repl, &from, &s.tables).await?;
                self.last_position = Some(from.clone());
                return Ok(Some(SourceBatch {
                    events: EventBatch::Events(Vec::new()),
                    position: from.to_position(),
                }));
            }
            let at = CdcPos::Snapshot {
                consistent: s.consistent,
                table: s.idx,
                rows: s.rows,
            };
            if !s.announced {
                s.announced = true;
                let ev = new_table_event(&self.envelope, &s.tables[s.idx], &at);
                return Ok(Some(SourceBatch {
                    events: EventBatch::Events(vec![ev]),
                    position: at.to_position(),
                }));
            }
            let table = s.tables[s.idx].clone();
            if s.copy.is_none() && !s.copy_done {
                s.copy = Some(CopyStream::open(&s.reader, &table).await?);
            }
            if s.pending.len() < budget.max_rows
                && let Some(copy) = s.copy.as_mut()
            {
                let room = budget.max_rows - s.pending.len();
                let more = copy
                    .fill(&mut s.pending, room, budget.max_bytes / 2)
                    .await?;
                if !more {
                    s.copy = None;
                    s.copy_done = true;
                }
            }
            let overhead = 512;
            match batch_within(&table, &mut s.pending, budget, overhead)? {
                Some((batch, _)) => {
                    let first = s.rows;
                    s.rows += batch.num_rows() as u64;
                    let id = format!(
                        "{}/{}/r/{}/{first}-{}",
                        self.slot,
                        format_lsn(s.consistent),
                        table.name,
                        s.rows - 1
                    );
                    let envelope = self
                        .envelope
                        .event("change", &table.name.to_string(), id)
                        .with_subject(table.name.to_string())
                        .with_cdc(CdcOp::Read, crate::decode::loamslsn(s.consistent, 0));
                    let mut envelope = envelope;
                    envelope.datacontenttype = Some(ARROW_STREAM.to_owned());
                    let position = CdcPos::Snapshot {
                        consistent: s.consistent,
                        table: s.idx,
                        rows: s.rows,
                    };
                    return Ok(Some(SourceBatch {
                        events: EventBatch::Arrow { batch, envelope },
                        position: position.to_position(),
                    }));
                }
                None if s.copy_done => {
                    s.idx += 1;
                    s.rows = 0;
                    s.announced = false;
                    s.copy = None;
                    s.copy_done = false;
                }
                None => {}
            }
        }
    }

    async fn maintain(&mut self) -> Result<(), ConnectorError> {
        let idle = self.last_activity.elapsed();
        if !self.cfg.heartbeat.is_zero()
            && idle >= self.cfg.heartbeat
            && self.last_heartbeat.elapsed() >= self.cfg.heartbeat
        {
            self.last_heartbeat = Instant::now();
            slot::heartbeat(self.control()?).await?;
        }
        if self.last_lag.is_none_or(|t| t.elapsed() >= LAG_EVERY) {
            self.last_lag = Some(Instant::now());
            if let Ok(Some(mut lag)) = slot::slot_lag(self.control()?, &self.slot).await {
                if let Phase::Stream(s) = &self.phase {
                    lag.lag_seconds = s.decoder.last_commit_time().map(SlotLag::lag_seconds_since);
                }
                *self.lag.lock().unwrap_or_else(|p| p.into_inner()) = Some(lag);
            }
        }
        Ok(())
    }

    async fn next_stream(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        self.maintain().await?;
        let Phase::Stream(s) = &mut self.phase else {
            return Ok(None);
        };
        if let Some(e) = s.decoder.blocked() {
            return Err(e);
        }
        let mut events: Vec<CloudEvent> = Vec::new();
        let mut bytes = 0usize;
        let mut last_after: Option<CdcPos> = None;
        let mut waited = false;
        loop {
            // Deliver what is decoded, within the budget.
            while let Some(e) = self.carry.front() {
                let size = e.event.size_bytes();
                if events.is_empty() && size > budget.max_bytes {
                    let e = self.carry.pop_front().expect("front");
                    return Err(ConnectorError::poison(format!(
                        "change {} is {size} bytes, over the batch budget of {}",
                        e.event.id, budget.max_bytes
                    )));
                }
                if events.len() >= budget.max_rows || bytes + size > budget.max_bytes {
                    break;
                }
                let e = self.carry.pop_front().expect("front");
                bytes += size;
                last_after = Some(e.after);
                events.push(e.event);
            }
            if !self.carry.is_empty() || events.len() >= budget.max_rows {
                break;
            }
            let ev = if waited || !events.is_empty() {
                match s.stream.try_recv() {
                    Some(r) => Some(r?),
                    None => None,
                }
            } else {
                waited = true;
                s.stream.recv(POLL).await?
            };
            let Some(ev) = ev else { break };
            self.last_activity = Instant::now();
            match ev {
                ReplEvent::Keepalive { wal_end } => {
                    s.decoder.keepalive(wal_end);
                }
                ReplEvent::Data { body, .. } => {
                    self.carry.extend(s.decoder.feed(body)?);
                }
            }
        }
        let position = if self.carry.is_empty() {
            s.decoder.position()
        } else {
            match last_after {
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
}

#[async_trait]
impl Source for PgCdcSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let control = connect(&self.cfg, &self.ctx).await?;
        self.custom_types = slot::custom_types(&control).await?;
        let (publication, owned) = self.cfg.publication_name();
        slot::ensure_publication(&control, &publication, owned, &self.cfg.tables).await?;
        self.control = Some(control);
        let resume = pos.as_ref().map(CdcPos::from_position).transpose()?;
        let tables = self.describe_all().await?;
        match resume {
            Some(from @ CdcPos::Stream { .. }) => {
                let control = self.control()?;
                if slot::slot_info(control, &self.slot).await?.is_none() {
                    return Err(ConnectorError::position_lost(format!(
                        "replication slot {} no longer exists; delete and recreate the instance to re-snapshot",
                        self.slot
                    )));
                }
                if let Some(info) = slot::slot_info(control, &self.slot).await?
                    && info.wal_status.as_deref() == Some("lost")
                {
                    return Err(ConnectorError::position_lost(format!(
                        "replication slot {} lost its WAL (max_slot_wal_keep_size); re-snapshot",
                        self.slot
                    )));
                }
                let repl = ReplConn::connect(&self.cfg, &self.ctx).await?;
                self.start_stream(repl, &from, &tables).await?;
                self.last_position = Some(from);
            }
            None | Some(CdcPos::Snapshot { .. }) => {
                // A fresh start, or a snapshot cut short: its exported
                // snapshot died with its connection, so start over.
                slot::drop_slot(self.control()?, &self.slot).await?;
                let mut repl = ReplConn::connect(&self.cfg, &self.ctx).await?;
                let created = repl.create_slot(&self.slot, self.cfg.snapshot).await?;
                match (&created.snapshot, self.cfg.snapshot) {
                    (Some(name), true) => {
                        let reader = connect(&self.cfg, &self.ctx).await?;
                        reader
                            .batch_execute(&format!(
                                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT {}",
                                quote_literal(name)
                            ))
                            .await
                            .map_err(|e| classify(&e, "importing the exported snapshot"))?;
                        self.phase = Phase::Snapshot(Box::new(Snapshot {
                            repl,
                            reader,
                            consistent: created.consistent_point,
                            tables,
                            idx: 0,
                            rows: 0,
                            copy: None,
                            copy_done: false,
                            pending: VecDeque::new(),
                            announced: false,
                        }));
                    }
                    _ => {
                        let from = CdcPos::Stream {
                            flushed: created.consistent_point,
                            partial: None,
                        };
                        self.start_stream(repl, &from, &tables).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        match self.phase {
            Phase::Closed => Err(ConnectorError::fatal("the CDC source is not open")),
            Phase::Snapshot(_) => self.next_snapshot(budget).await,
            Phase::Stream(_) => self.next_stream(budget).await,
        }
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        let pos = CdcPos::from_position(upto)?;
        if let (Some(lsn), Phase::Stream(s)) = (pos.flush_lsn(), &self.phase) {
            s.stream.set_flushed(lsn).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        match std::mem::replace(&mut self.phase, Phase::Closed) {
            Phase::Stream(s) => s.stream.close().await,
            Phase::Snapshot(s) => s.repl.close().await,
            Phase::Closed => {}
        }
        self.control = None;
        Ok(())
    }
}

/// `DeleteInstance`: drop the slot (ending any session that holds it) and a
/// Loams-owned publication. `StopInstance` keeps both, so a restart resumes.
pub async fn delete_instance(cfg: &PgConfig, ctx: &ConnectorContext) -> Result<(), ConnectorError> {
    let control = connect(cfg, ctx).await?;
    slot::drop_slot(&control, &cfg.slot_name()).await?;
    let (publication, owned) = cfg.publication_name();
    if owned {
        slot::drop_publication(&control, &publication).await?;
    }
    Ok(())
}
