//! The reference connector: a counter source (one counter per split) and a
//! map sink (events by id) with an optional transactional mode. It is the
//! smallest connector that exercises every part of the SDK's contract, and
//! each part records what the pipeline did to it so tests can check the
//! delivery mechanics.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::Instant;

use crate::budget::Budget;
use crate::checkpoint::Checkpoint;
use crate::envelope::{CloudEvent, EnvelopeBuilder};
use crate::error::{ConnectorError, ErrorClass};
use crate::position::{InstanceKey, Position, SplitId};
use crate::sink::{Sink, SinkAck, SinkHandle};
use crate::source::{EventBatch, Source, SourceBatch};
use crate::split::{SinkFactory, SourceFactory, Split, SplitEnumerator, SplitState};
use crate::txn::{TransactionalSink, TxnToken};
use crate::{ConnectorContext, ConnectorFactory};

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn error(class: &ErrorClass, message: &str) -> ConnectorError {
    ConnectorError::new(class.clone(), message)
}

// ---------------------------------------------------------------- source --

/// How the counter source behaves.
#[derive(Debug, Clone)]
pub struct CounterConfig {
    /// Events per split; `None` counts forever.
    pub events_per_split: Option<u64>,
    /// The payload size of each event.
    pub payload_bytes: usize,
    /// Return more than the budget (to test that the pipeline refuses it).
    pub ignore_budget: bool,
}

impl Default for CounterConfig {
    fn default() -> Self {
        Self {
            events_per_split: Some(100),
            payload_bytes: 16,
            ignore_budget: false,
        }
    }
}

/// One `next` call: when, on which split, with which budget, and what came
/// back (rows and bytes).
#[derive(Debug, Clone)]
pub struct NextCall {
    pub split: SplitId,
    pub at: Instant,
    pub budget: Budget,
    pub rows: usize,
    pub bytes: usize,
}

/// What the pipeline did to the counter sources.
#[derive(Debug, Default)]
pub struct SourceLog {
    pub opened: Vec<(SplitId, Option<u64>)>,
    pub committed: Vec<(SplitId, u64)>,
    pub closed: Vec<SplitId>,
    pub next_calls: Vec<NextCall>,
    pub emitted: HashMap<SplitId, u64>,
    failures: HashMap<SplitId, VecDeque<(ErrorClass, String)>>,
    open_failures: HashMap<SplitId, VecDeque<(ErrorClass, String)>>,
}

/// The shared state of a counter source's splits.
#[derive(Debug, Clone, Default)]
pub struct CounterHandle {
    pub log: Arc<Mutex<SourceLog>>,
    splits: Arc<Mutex<Vec<String>>>,
}

impl CounterHandle {
    pub fn new(splits: &[&str]) -> Self {
        Self {
            log: Arc::default(),
            splits: Arc::new(Mutex::new(splits.iter().map(|s| (*s).to_owned()).collect())),
        }
    }

    pub fn add_split(&self, id: &str) {
        lock(&self.splits).push(id.to_owned());
    }

    pub fn remove_split(&self, id: &str) {
        lock(&self.splits).retain(|s| s != id);
    }

    /// The next `next` calls on `split` fail with these errors, in order.
    pub fn fail_next(&self, split: &str, errors: impl IntoIterator<Item = (ErrorClass, String)>) {
        lock(&self.log)
            .failures
            .entry(SplitId::new(split))
            .or_default()
            .extend(errors);
    }

    /// The next `open` calls on `split` fail with these errors, in order.
    pub fn fail_open(&self, split: &str, errors: impl IntoIterator<Item = (ErrorClass, String)>) {
        lock(&self.log)
            .open_failures
            .entry(SplitId::new(split))
            .or_default()
            .extend(errors);
    }

    pub fn committed(&self, split: &str) -> Vec<u64> {
        lock(&self.log)
            .committed
            .iter()
            .filter(|(s, _)| s.as_str() == split)
            .map(|(_, n)| *n)
            .collect()
    }

    pub fn emitted(&self, split: &str) -> u64 {
        lock(&self.log)
            .emitted
            .get(&SplitId::new(split))
            .copied()
            .unwrap_or(0)
    }

    pub fn next_calls(&self) -> Vec<NextCall> {
        lock(&self.log).next_calls.clone()
    }

    pub fn opened(&self) -> Vec<(SplitId, Option<u64>)> {
        lock(&self.log).opened.clone()
    }

    pub fn closed(&self) -> Vec<SplitId> {
        lock(&self.log).closed.clone()
    }
}

/// Discovers the handle's current split list.
#[derive(Debug)]
pub struct CounterEnumerator {
    handle: CounterHandle,
}

impl CounterEnumerator {
    pub fn new(handle: CounterHandle) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl SplitEnumerator for CounterEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(lock(&self.handle.splits)
            .iter()
            .map(|s| Split::new(SplitId::new(s.clone()), serde_json::json!({})))
            .collect())
    }
}

/// Makes counter sources.
#[derive(Debug, Clone)]
pub struct CounterFactory {
    pub handle: CounterHandle,
    pub config: CounterConfig,
    pub envelope: EnvelopeBuilder,
}

impl CounterFactory {
    pub fn new(handle: CounterHandle, config: CounterConfig, instance: &InstanceKey) -> Self {
        Self {
            handle,
            config,
            envelope: EnvelopeBuilder::new("reference", instance),
        }
    }
}

impl SourceFactory for CounterFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(CounterSource {
            factory: self.clone(),
            split: None,
            next: 0,
        }))
    }
}

/// Counts: event `n` of split `s` has id `"<s>/<n>"`, so a re-read from the
/// same position yields the same ids (design §33 §6).
#[derive(Debug)]
pub struct CounterSource {
    factory: CounterFactory,
    split: Option<SplitId>,
    next: u64,
}

impl CounterSource {
    /// The event a counter source emits for `n` on `split`.
    pub fn event(
        envelope: &EnvelopeBuilder,
        split: &SplitId,
        n: u64,
        payload_bytes: usize,
    ) -> CloudEvent {
        let mut payload = format!("{{\"n\":{n}}}").into_bytes();
        payload.resize(payload_bytes.max(payload.len()), b' ');
        envelope
            .event("record", split.as_str(), format!("{split}/{n}"))
            .with_subject(split.as_str())
            .with_data("application/json", Bytes::from(payload))
    }
}

#[async_trait]
impl Source for CounterSource {
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let mut log = lock(&self.factory.handle.log);
        if let Some((class, msg)) = log
            .open_failures
            .get_mut(&split.id)
            .and_then(VecDeque::pop_front)
        {
            return Err(error(&class, &msg));
        }
        self.next = pos.as_ref().and_then(Position::as_u64).unwrap_or(0);
        self.split = Some(split.id.clone());
        log.opened
            .push((split.id.clone(), pos.and_then(|p| p.as_u64())));
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let split = self
            .split
            .clone()
            .ok_or_else(|| ConnectorError::fatal("next before open"))?;
        let cfg = &self.factory.config;
        let mut log = lock(&self.factory.handle.log);
        if let Some((class, msg)) = log.failures.get_mut(&split).and_then(VecDeque::pop_front) {
            log.next_calls.push(NextCall {
                split: split.clone(),
                at: Instant::now(),
                budget,
                rows: 0,
                bytes: 0,
            });
            return Err(error(&class, &msg));
        }
        let remaining = cfg
            .events_per_split
            .map_or(u64::MAX, |n| n.saturating_sub(self.next));
        let mut events = Vec::new();
        let mut bytes = 0;
        while (events.len() as u64) < remaining {
            let ev = Self::event(
                &self.factory.envelope,
                &split,
                self.next + events.len() as u64,
                cfg.payload_bytes,
            );
            let size = ev.size_bytes();
            let over = events.len() >= budget.max_rows || bytes + size > budget.max_bytes;
            if over && !(cfg.ignore_budget && events.len() < budget.max_rows + 5) {
                break;
            }
            bytes += size;
            events.push(ev);
        }
        log.next_calls.push(NextCall {
            split: split.clone(),
            at: Instant::now(),
            budget,
            rows: events.len(),
            bytes,
        });
        if events.is_empty() {
            return Ok(None);
        }
        self.next += events.len() as u64;
        *log.emitted.entry(split.clone()).or_default() += events.len() as u64;
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: Position::new(
                Bytes::copy_from_slice(&self.next.to_be_bytes()),
                format!("{split}@{}", self.next),
            ),
        }))
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        let split = self
            .split
            .clone()
            .ok_or_else(|| ConnectorError::fatal("committed before open"))?;
        let n = upto
            .as_u64()
            .ok_or_else(|| ConnectorError::fatal("not a counter position"))?;
        lock(&self.factory.handle.log).committed.push((split, n));
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(split) = self.split.take() {
            lock(&self.factory.handle.log).closed.push(split);
        }
        Ok(())
    }
}

// ------------------------------------------------------------------ sink --

/// The map sink's state: what it holds, and what the pipeline did to it.
#[derive(Debug, Default)]
pub struct MapState {
    /// Events by id: the target's contents.
    pub map: BTreeMap<String, CloudEvent>,
    /// How many times each id was applied to `map`.
    pub applied: HashMap<String, u32>,
    /// When each `write` call arrived (successful or not).
    pub write_calls: Vec<Instant>,
    pub flushes: u64,
    /// Transactional mode: the checkpoint committed with the data.
    pub checkpoint: Option<Checkpoint>,
    pub aborted: u32,
    pub commits: u32,
    staged: Vec<CloudEvent>,
    prepared: Option<(TxnToken, Vec<CloudEvent>, Checkpoint)>,
    committed_tokens: HashSet<String>,
    next_token: u64,
    failures: VecDeque<(ErrorClass, String)>,
    poison: HashSet<String>,
    delay: Option<Duration>,
}

/// A handle on a map sink's shared state.
#[derive(Debug, Clone, Default)]
pub struct MapHandle {
    pub state: Arc<Mutex<MapState>>,
}

impl MapHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// The next `write` calls fail with these errors, in order.
    pub fn fail_writes(&self, errors: impl IntoIterator<Item = (ErrorClass, String)>) {
        lock(&self.state).failures.extend(errors);
    }

    /// Any batch holding the event `id` is refused as `Poison`.
    pub fn poison(&self, id: &str) {
        lock(&self.state).poison.insert(id.to_owned());
    }

    /// Every `write` takes `d` (a slow sink).
    pub fn set_delay(&self, d: Option<Duration>) {
        lock(&self.state).delay = d;
    }

    /// Stage a transaction and prepare it without committing, as a crash
    /// after `prepare` leaves it.
    pub fn leave_prepared(&self, events: Vec<CloudEvent>, cp: Checkpoint) {
        let mut s = lock(&self.state);
        s.next_token += 1;
        let token = TxnToken {
            id: format!("txn-{}", s.next_token),
            opaque: Bytes::new(),
        };
        s.prepared = Some((token, events, cp));
    }

    pub fn ids(&self) -> Vec<String> {
        lock(&self.state).map.keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        lock(&self.state).map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn max_applied(&self) -> u32 {
        lock(&self.state)
            .applied
            .values()
            .copied()
            .max()
            .unwrap_or(0)
    }

    pub fn write_calls(&self) -> Vec<Instant> {
        lock(&self.state).write_calls.clone()
    }

    pub fn checkpoint(&self) -> Option<Checkpoint> {
        lock(&self.state).checkpoint.clone()
    }

    pub fn aborted(&self) -> u32 {
        lock(&self.state).aborted
    }
}

/// Makes map sinks over one shared state.
#[derive(Debug, Clone)]
pub struct MapSinkFactory {
    pub handle: MapHandle,
    pub transactional: bool,
}

impl SinkFactory for MapSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        let sink = MapSink {
            handle: self.handle.clone(),
            transactional: self.transactional,
        };
        Ok(if self.transactional {
            SinkHandle::Transactional(Box::new(sink))
        } else {
            SinkHandle::Plain(Box::new(sink))
        })
    }
}

/// Upserts events by id. In transactional mode `write` stages, and the data
/// reaches the map only with `commit`, together with the checkpoint.
#[derive(Debug)]
pub struct MapSink {
    handle: MapHandle,
    transactional: bool,
}

fn apply(s: &mut MapState, events: Vec<CloudEvent>) {
    for ev in events {
        *s.applied.entry(ev.id.clone()).or_default() += 1;
        s.map.insert(ev.id.clone(), ev);
    }
}

#[async_trait]
impl Sink for MapSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let delay = {
            let mut s = lock(&self.handle.state);
            s.write_calls.push(Instant::now());
            s.delay
        };
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        let mut s = lock(&self.handle.state);
        if let Some((class, msg)) = s.failures.pop_front() {
            return Err(error(&class, &msg));
        }
        let events = match batch {
            EventBatch::Events(v) => v,
            EventBatch::Arrow { envelope, .. } => vec![envelope],
        };
        if let Some(bad) = events.iter().find(|e| s.poison.contains(&e.id)) {
            return Err(ConnectorError::poison(format!(
                "event {} rejected by the reference sink",
                bad.id
            )));
        }
        let ack = SinkAck {
            rows: events.len() as u64,
            bytes: events.iter().map(|e| e.size_bytes() as u64).sum(),
        };
        if self.transactional {
            s.staged.extend(events);
        } else {
            apply(&mut s, events);
        }
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        lock(&self.handle.state).flushes += 1;
        Ok(SinkAck::default())
    }
}

#[async_trait]
impl TransactionalSink for MapSink {
    async fn recover(&mut self) -> Result<Option<Checkpoint>, ConnectorError> {
        let mut s = lock(&self.handle.state);
        s.staged.clear();
        // The token's recorded state: a prepared, uncommitted transaction is
        // aborted (its checkpoint never reached the target).
        if s.prepared.take().is_some() {
            s.aborted += 1;
        }
        Ok(s.checkpoint.clone())
    }

    async fn prepare(&mut self, cp: &Checkpoint) -> Result<TxnToken, ConnectorError> {
        let mut s = lock(&self.handle.state);
        s.next_token += 1;
        let token = TxnToken {
            id: format!("txn-{}", s.next_token),
            opaque: Bytes::new(),
        };
        let staged = std::mem::take(&mut s.staged);
        s.prepared = Some((token.clone(), staged, cp.clone()));
        Ok(token)
    }

    async fn commit(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        let mut s = lock(&self.handle.state);
        if s.committed_tokens.contains(&t.id) {
            return Ok(());
        }
        let Some((token, events, cp)) = s.prepared.take() else {
            return Err(ConnectorError::fatal(format!("{} is not prepared", t.id)));
        };
        if token != t {
            s.prepared = Some((token, events, cp));
            return Err(ConnectorError::fatal(format!(
                "{} is not the prepared transaction",
                t.id
            )));
        }
        if let Some(stored) = &s.checkpoint
            && cp.epoch < stored.epoch
        {
            return Err(ConnectorError::fatal(format!(
                "fenced: epoch {} is older than the committed epoch {}",
                cp.epoch, stored.epoch
            )));
        }
        apply(&mut s, events);
        s.checkpoint = Some(cp);
        s.commits += 1;
        s.committed_tokens.insert(t.id);
        Ok(())
    }

    async fn abort(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        let mut s = lock(&self.handle.state);
        if s.prepared.as_ref().is_some_and(|(token, _, _)| *token == t) {
            s.prepared = None;
        }
        s.staged.clear();
        s.aborted += 1;
        Ok(())
    }
}

// ------------------------------------------------------------- connector --

/// The reference connector as a [`ConnectorFactory`] (id `reference`).
/// Config: `{"splits": ["a", "b"], "events_per_split": 100, "payload_bytes":
/// 16, "transactional": false}`.
#[derive(Debug, Clone, Default)]
pub struct ReferenceConnector {
    pub source: CounterHandle,
    pub sink: MapHandle,
}

impl ReferenceConnector {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ConnectorFactory for ReferenceConnector {
    fn id(&self) -> &'static str {
        "reference"
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let splits: Vec<String> = match ctx.config.get("splits") {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|e| ConnectorError::fatal(format!("config.splits: {e}")))?,
            None => vec!["0".to_owned()],
        };
        {
            let mut current = lock(&self.source.splits);
            *current = splits;
        }
        let config = CounterConfig {
            events_per_split: ctx.config.get("events_per_split").and_then(|v| v.as_u64()),
            payload_bytes: ctx
                .config
                .get("payload_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(16) as usize,
            ignore_budget: false,
        };
        Ok(Some((
            Box::new(CounterEnumerator::new(self.source.clone())),
            Box::new(CounterFactory::new(
                self.source.clone(),
                config,
                &ctx.instance,
            )),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let transactional = ctx
            .config
            .get("transactional")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        Ok(Some(Box::new(MapSinkFactory {
            handle: self.sink.clone(),
            transactional,
        })))
    }
}
