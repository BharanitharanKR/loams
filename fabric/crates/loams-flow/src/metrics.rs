//! Connector metrics (CN2 plan Task 24).
//!
//! [`ConnectorMetrics`] is the registry of every running instance's counters
//! and gauges; [`ConnectorMetrics::render`] writes them in the Prometheus text
//! exposition format (0.0.4) for `loams-fabric flow`'s `/metrics` endpoint.
//! [`InstanceMetrics`] is one instance and direction: it implements the SDK's
//! [`ConnectorObserver`], so the runtime hands the same `Arc` to the
//! [`Pipeline`](loams_connector_sdk::Pipeline) and to the connector's
//! `ConnectorContext::observe`, and a fallback runtime's own metrics are folded
//! in with [`InstanceMetrics::apply_scrape`]. The Q359 activity counters
//! ([`crate::activity`]) are read from the same registry.
//!
//! Labels are bounded (`metrics_labels_bounded`): every series carries
//! `namespace`, `instance`, `connector`, `runtime`, `fallback` and
//! `direction`; `split` only for an instance registered with
//! [`SplitLabels::Bounded`], capped at [`MAX_SPLIT_LABELS`] values per instance
//! (the rest fold into [`OVERFLOW_SPLIT`]); `dir` and `class` take a fixed set
//! of values. A label value is never a free user string or a secret: instance
//! and connector names must be identifiers ([`InstanceLabels::new`] refuses
//! anything else), and a split id that is not one, or that contains a secret
//! the instance resolved, becomes [`REDACTED_SPLIT`].

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use loams_connector_sdk::{
    ConnectorObserver, ErrorClass, InstanceKey, LastError, PipelineState, PipelineStatus, Scrubber,
    SplitId,
};

use crate::manifest::RuntimeKind;
use crate::validate::Direction;

/// The most `split` label values one instance carries.
pub const MAX_SPLIT_LABELS: usize = 1_024;
/// The `split` value of every split past [`MAX_SPLIT_LABELS`].
pub const OVERFLOW_SPLIT: &str = "_overflow";
/// The `split` value of a split id that is not a label-safe identifier or that
/// contains one of the instance's resolved secrets.
pub const REDACTED_SPLIT: &str = "_redacted";
/// `lag_alert_seconds`' default (CN2 Task 24).
pub const DEFAULT_LAG_ALERT: Duration = Duration::from_secs(300);

pub const EVENTS_TOTAL: &str = "loams_flow_connector_events_total";
pub const BYTES_TOTAL: &str = "loams_flow_connector_bytes_total";
pub const API_CALLS_TOTAL: &str = "loams_flow_connector_api_calls_total";
pub const ERRORS_TOTAL: &str = "loams_flow_connector_errors_total";
pub const LAG_SECONDS: &str = "loams_flow_connector_lag_seconds";
pub const BUDGET_WAIT_SECONDS_TOTAL: &str = "loams_flow_connector_budget_wait_seconds_total";
pub const RESTARTS_TOTAL: &str = "loams_flow_connector_restarts_total";
pub const DLQ_TOTAL: &str = "loams_flow_connector_dlq_total";
pub const CDC_POSITION_RISK_RATIO: &str = "loams_flow_cdc_position_risk_ratio";
/// 1 while the instance runs, 0 once it failed or was fenced (R24.3).
pub const UP: &str = "loams_flow_connector_up";
/// The instance's `lag_alert_seconds`, exported beside the lag so the alert
/// compares each instance with its own threshold (R24.3).
pub const LAG_ALERT_SECONDS: &str = "loams_flow_connector_lag_alert_seconds";

/// Every metric family this module writes; the alerts and the dashboard may
/// name only these.
pub const METRIC_FAMILIES: &[&str] = &[
    EVENTS_TOTAL,
    BYTES_TOTAL,
    API_CALLS_TOTAL,
    ERRORS_TOTAL,
    LAG_SECONDS,
    BUDGET_WAIT_SECONDS_TOTAL,
    RESTARTS_TOTAL,
    DLQ_TOTAL,
    CDC_POSITION_RISK_RATIO,
    UP,
    LAG_ALERT_SECONDS,
];

/// A label value that is not a bounded identifier. The value itself is never
/// echoed: it could be a secret.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LabelError {
    #[error("label `{label}` ({len} bytes) is not an identifier: {reason}")]
    Invalid {
        label: &'static str,
        len: usize,
        reason: &'static str,
    },
}

/// `Ok` when `v` is an identifier of at most `max` bytes: ASCII letters,
/// digits and `extra`, starting with a letter or digit.
fn identifier(label: &'static str, v: &str, max: usize, extra: &str) -> Result<(), LabelError> {
    let err = |reason| {
        Err(LabelError::Invalid {
            label,
            len: v.len(),
            reason,
        })
    };
    if v.is_empty() {
        return err("empty");
    }
    if v.len() > max {
        return err("too long");
    }
    if !v.starts_with(|c: char| c.is_ascii_alphanumeric()) {
        return err("must start with a letter or digit");
    }
    if !v
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || extra.contains(c))
    {
        return err("only ASCII letters, digits and `-_.` are allowed");
    }
    Ok(())
}

/// The labels every series of one instance and direction carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceLabels {
    namespace: u64,
    instance: String,
    connector: String,
    runtime: RuntimeKind,
    fallback: bool,
    direction: Direction,
}

impl InstanceLabels {
    /// Refuses an instance or connector name that is not an identifier of at
    /// most 63 bytes (a DNS label's length, as CN1's instance names are).
    pub fn new(
        namespace: u64,
        instance: impl Into<String>,
        connector: impl Into<String>,
        runtime: RuntimeKind,
        fallback: bool,
        direction: Direction,
    ) -> Result<Self, LabelError> {
        let instance = instance.into();
        let connector = connector.into();
        identifier("instance", &instance, 63, "-_.")?;
        identifier("connector", &connector, 63, "-_.")?;
        Ok(Self {
            namespace,
            instance,
            connector,
            runtime,
            fallback,
            direction,
        })
    }

    pub fn namespace(&self) -> u64 {
        self.namespace
    }
    pub fn instance(&self) -> &str {
        &self.instance
    }
    pub fn connector(&self) -> &str {
        &self.connector
    }
    pub fn runtime(&self) -> RuntimeKind {
        self.runtime
    }
    pub fn fallback(&self) -> bool {
        self.fallback
    }
    pub fn direction(&self) -> Direction {
        self.direction
    }

    fn key(&self) -> SeriesKey {
        (
            self.namespace,
            self.instance.clone(),
            direction_slug(self.direction),
        )
    }

    /// `namespace="…",instance="…",…` — every value is an identifier, a
    /// number, a slug or a boolean, so nothing needs escaping.
    fn render(&self) -> String {
        format!(
            "namespace=\"{}\",instance=\"{}\",connector=\"{}\",runtime=\"{}\",fallback=\"{}\",direction=\"{}\"",
            self.namespace,
            self.instance,
            self.connector,
            self.runtime.slug(),
            self.fallback,
            direction_slug(self.direction),
        )
    }
}

fn direction_slug(d: Direction) -> &'static str {
    match d {
        Direction::Source => "source",
        Direction::Sink => "sink",
    }
}

/// Whether an instance's series carry a `split` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitLabels {
    /// Splits are few and stable (Kafka partitions, Kinesis shards, a
    /// replication slot): `split` is a label, capped at [`MAX_SPLIT_LABELS`].
    Bounded,
    /// Splits are many or short-lived (object prefixes, key ranges, scan
    /// segments): no `split` label.
    Unbounded,
}

type SeriesKey = (u64, String, &'static str);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

// ---------------------------------------------------------------- registry --

/// Every registered instance's metrics. Cheap to clone; clones share the
/// registry.
#[derive(Debug, Clone, Default)]
pub struct ConnectorMetrics {
    inner: Arc<Registry>,
}

#[derive(Debug, Default)]
struct Registry {
    live: Mutex<BTreeMap<SeriesKey, Arc<InstanceMetrics>>>,
    /// Deregistered instances whose last window the activity observer has not
    /// seen yet; [`ConnectorMetrics::take_retired`] empties it each window.
    retired: Mutex<Vec<Arc<InstanceMetrics>>>,
}

/// At most this many deregistered instances wait for their last activity
/// window; past it the oldest is dropped (no pump is running).
const MAX_RETIRED: usize = 4_096;

impl ConnectorMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an instance and direction, or return the one already
    /// registered under the same labels (a restart keeps its counters, so
    /// `restarts_total` and the rest stay monotone). Registering the same
    /// instance and direction with other labels (a new runtime choice)
    /// retires the old series.
    pub fn register(&self, labels: InstanceLabels, splits: SplitLabels) -> Arc<InstanceMetrics> {
        self.register_inner(labels, splits, None)
    }

    /// [`ConnectorMetrics::register`], with the scrubber the instance's
    /// `ConnectorContext` records resolved secrets in: a split id containing
    /// one never becomes a label.
    pub fn register_scrubbed(
        &self,
        labels: InstanceLabels,
        splits: SplitLabels,
        scrubber: Arc<Scrubber>,
    ) -> Arc<InstanceMetrics> {
        self.register_inner(labels, splits, Some(scrubber))
    }

    fn register_inner(
        &self,
        labels: InstanceLabels,
        splits: SplitLabels,
        scrubber: Option<Arc<Scrubber>>,
    ) -> Arc<InstanceMetrics> {
        let key = labels.key();
        let mut live = lock(&self.inner.live);
        if let Some(m) = live.get(&key)
            && m.labels == labels
            && m.splits == splits
        {
            return Arc::clone(m);
        }
        // Brought back before its last window was reported: keep counting.
        let revived = {
            let mut retired = lock(&self.inner.retired);
            retired
                .iter()
                .position(|m| m.labels == labels && m.splits == splits)
                .map(|i| retired.remove(i))
        };
        let m = revived.unwrap_or_else(|| Arc::new(InstanceMetrics::new(labels, splits, scrubber)));
        if let Some(old) = live.insert(key, Arc::clone(&m)) {
            self.retire(old);
        }
        m
    }

    /// Remove an instance's series (it was stopped or deleted). Its last
    /// partial window is still handed to the activity observer once.
    pub fn deregister(&self, labels: &InstanceLabels) {
        let removed = {
            let mut live = lock(&self.inner.live);
            match live.get(&labels.key()) {
                Some(m) if m.labels == *labels => live.remove(&labels.key()),
                _ => None,
            }
        };
        if let Some(m) = removed {
            self.retire(m);
        }
    }

    fn retire(&self, m: Arc<InstanceMetrics>) {
        let mut retired = lock(&self.inner.retired);
        if retired.len() >= MAX_RETIRED {
            retired.remove(0);
        }
        retired.push(m);
    }

    /// The registered instances, in label order.
    pub fn instances(&self) -> Vec<Arc<InstanceMetrics>> {
        lock(&self.inner.live).values().cloned().collect()
    }

    /// The deregistered instances not yet reported, emptying the list.
    pub(crate) fn take_retired(&self) -> Vec<Arc<InstanceMetrics>> {
        std::mem::take(&mut *lock(&self.inner.retired))
    }

    /// Every registered instance's series, in the Prometheus text exposition
    /// format 0.0.4.
    pub fn render(&self) -> String {
        let instances = self.instances();
        let snaps: Vec<(String, Snapshot)> = instances
            .iter()
            .map(|m| (m.labels.render(), m.snapshot()))
            .collect();
        let mut out = String::new();
        let mut family = |name: &str, kind: &str, help: &str, rows: &mut dyn FnMut(&mut String)| {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} {kind}");
            rows(&mut out);
        };
        let split_rows =
            |out: &mut String, name: &str, base: &str, per: &[(Option<String>, f64)]| {
                for (split, v) in per {
                    match split {
                        Some(s) => {
                            let _ = writeln!(out, "{name}{{{base},split=\"{s}\"}} {}", Num(*v));
                        }
                        None => {
                            let _ = writeln!(out, "{name}{{{base}}} {}", Num(*v));
                        }
                    }
                }
            };
        family(
            EVENTS_TOTAL,
            "counter",
            "Events the Fabric acknowledged from a source, or the target acknowledged from a sink.",
            &mut |out| {
                for (base, s) in &snaps {
                    split_rows(out, EVENTS_TOTAL, base, &s.events);
                }
            },
        );
        family(
            BYTES_TOTAL,
            "counter",
            "Bytes read from (dir=in) or written to (dir=out) the external system.",
            &mut |out| {
                for (base, s) in &snaps {
                    let with_dir = format!("{base},dir=\"in\"");
                    split_rows(out, BYTES_TOTAL, &with_dir, &s.bytes_in);
                    let _ = writeln!(out, "{BYTES_TOTAL}{{{base},dir=\"out\"}} {}", s.bytes_out);
                }
            },
        );
        family(
            API_CALLS_TOTAL,
            "counter",
            "Calls to the external system (reads, writes, commits).",
            &mut |out| {
                for (base, s) in &snaps {
                    let _ = writeln!(out, "{API_CALLS_TOTAL}{{{base}}} {}", s.api_calls);
                }
            },
        );
        family(
            ERRORS_TOTAL,
            "counter",
            "Errors by the SDK's error class.",
            &mut |out| {
                for (base, s) in &snaps {
                    for (class, n) in &s.errors {
                        let _ = writeln!(out, "{ERRORS_TOTAL}{{{base},class=\"{class}\"}} {n}");
                    }
                }
            },
        );
        family(
            LAG_SECONDS,
            "gauge",
            "How far behind the external system (sources) or the Fabric (sinks) the instance is.",
            &mut |out| {
                for (base, s) in &snaps {
                    split_rows(out, LAG_SECONDS, base, &s.lag);
                }
            },
        );
        family(
            LAG_ALERT_SECONDS,
            "gauge",
            "The instance's lag_alert_seconds: the lag alert's threshold.",
            &mut |out| {
                for (base, s) in &snaps {
                    let _ = writeln!(out, "{LAG_ALERT_SECONDS}{{{base}}} {}", s.lag_alert_seconds);
                }
            },
        );
        family(
            BUDGET_WAIT_SECONDS_TOTAL,
            "counter",
            "Time the instance's source tasks spent paused by backpressure, waiting for budget.",
            &mut |out| {
                for (base, s) in &snaps {
                    let _ = writeln!(
                        out,
                        "{BUDGET_WAIT_SECONDS_TOTAL}{{{base}}} {}",
                        Num(s.budget_wait.as_secs_f64())
                    );
                }
            },
        );
        family(
            RESTARTS_TOTAL,
            "counter",
            "Times the runtime restarted the instance.",
            &mut |out| {
                for (base, s) in &snaps {
                    let _ = writeln!(out, "{RESTARTS_TOTAL}{{{base}}} {}", s.restarts);
                }
            },
        );
        family(
            DLQ_TOTAL,
            "counter",
            "Records sent to the instance's dead-letter topic.",
            &mut |out| {
                for (base, s) in &snaps {
                    let _ = writeln!(out, "{DLQ_TOTAL}{{{base}}} {}", s.dlq);
                }
            },
        );
        family(
            CDC_POSITION_RISK_RATIO,
            "gauge",
            "Share of the upstream log's retention the position has used (slot WAL, binlog, oplog, CDC retention, stream trim), 0-1.",
            &mut |out| {
                for (base, s) in &snaps {
                    split_rows(out, CDC_POSITION_RISK_RATIO, base, &s.risk);
                }
            },
        );
        family(
            UP,
            "gauge",
            "1 while the instance runs, 0 once it failed or was fenced.",
            &mut |out| {
                for (base, s) in &snaps {
                    let _ = writeln!(out, "{UP}{{{base}}} {}", u8::from(s.up));
                }
            },
        );
        out
    }
}

/// A sample value: integers without a fraction, the rest as Rust writes
/// them (never in exponent form).
struct Num(f64);

impl fmt::Display for Num {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_finite() && self.0.fract() == 0.0 && self.0.abs() < 1e15 {
            write!(f, "{}", self.0 as i64)
        } else if self.0.is_nan() {
            f.write_str("NaN")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

// ---------------------------------------------------------------- instance --

/// The totals the activity observer is handed per window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Totals {
    pub(crate) events: u64,
    pub(crate) bytes_in: u64,
    pub(crate) bytes_out: u64,
    pub(crate) api_calls: u64,
}

/// What the render reads under one lock.
struct Snapshot {
    events: Vec<(Option<String>, f64)>,
    bytes_in: Vec<(Option<String>, f64)>,
    bytes_out: u64,
    api_calls: u64,
    errors: BTreeMap<&'static str, u64>,
    lag: Vec<(Option<String>, f64)>,
    lag_alert_seconds: u64,
    budget_wait: Duration,
    restarts: u64,
    dlq: u64,
    risk: Vec<(Option<String>, f64)>,
    up: bool,
}

#[derive(Debug, Default)]
struct Series {
    /// The split label values in use (at most MAX_SPLIT_LABELS).
    splits: BTreeSet<String>,
    events: BTreeMap<Option<String>, u64>,
    bytes_in: BTreeMap<Option<String>, u64>,
    lag: BTreeMap<Option<String>, f64>,
    risk: BTreeMap<Option<String>, f64>,
    errors: BTreeMap<&'static str, u64>,
    /// The last absolute value of each scraped fallback counter.
    scraped: BTreeMap<&'static str, f64>,
    /// The totals at the end of the last activity window.
    reported: Totals,
}

/// One instance and direction's metrics; the SDK's observer hook.
pub struct InstanceMetrics {
    labels: InstanceLabels,
    splits: SplitLabels,
    scrubber: Option<Arc<Scrubber>>,
    events: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    api_calls: AtomicU64,
    restarts: AtomicU64,
    dlq: AtomicU64,
    budget_wait_nanos: AtomicU64,
    lag_alert_seconds: AtomicU64,
    up: AtomicBool,
    series: Mutex<Series>,
}

impl fmt::Debug for InstanceMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstanceMetrics")
            .field("labels", &self.labels)
            .field("splits", &self.splits)
            .finish_non_exhaustive()
    }
}

/// CN1 Task 3's `InstanceStatus`, with CN2 Task 24's additions.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceStatus {
    pub state: PipelineState,
    /// Already scrubbed of secrets by the pipeline.
    pub last_error: Option<LastError>,
    /// The largest lag any split reported, if any did.
    pub lag_seconds: Option<f64>,
    pub restarts: u64,
    pub runtime_choice: RuntimeChoice,
}

/// The runtime an instance runs on, and whether that is its fallback (OR-4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeChoice {
    pub kind: RuntimeKind,
    pub fallback: bool,
}

impl InstanceMetrics {
    fn new(labels: InstanceLabels, splits: SplitLabels, scrubber: Option<Arc<Scrubber>>) -> Self {
        Self {
            labels,
            splits,
            scrubber,
            events: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            api_calls: AtomicU64::new(0),
            restarts: AtomicU64::new(0),
            dlq: AtomicU64::new(0),
            budget_wait_nanos: AtomicU64::new(0),
            lag_alert_seconds: AtomicU64::new(DEFAULT_LAG_ALERT.as_secs()),
            up: AtomicBool::new(true),
            series: Mutex::new(Series::default()),
        }
    }

    pub fn labels(&self) -> &InstanceLabels {
        &self.labels
    }

    /// The runtime restarted the instance.
    pub fn restarted(&self) {
        self.restarts.fetch_add(1, Ordering::Relaxed);
    }

    /// The instance's `lag_alert_seconds` (default 300).
    pub fn set_lag_alert(&self, threshold: Duration) {
        self.lag_alert_seconds
            .store(threshold.as_secs(), Ordering::Relaxed);
    }

    /// The instance's status for `GetInstance`, from its pipeline's (or a
    /// fallback supervisor's) status. Also sets the `up` gauge the "instance
    /// down" alert reads.
    pub fn status(&self, pipeline: &PipelineStatus) -> InstanceStatus {
        let up = !matches!(
            pipeline.state,
            PipelineState::Failed | PipelineState::Fenced
        );
        self.up.store(up, Ordering::Relaxed);
        let lag_seconds = lock(&self.series)
            .lag
            .values()
            .copied()
            .fold(None, |m: Option<f64>, v| Some(m.map_or(v, |m| m.max(v))));
        InstanceStatus {
            state: pipeline.state,
            last_error: pipeline.last_error.clone(),
            lag_seconds,
            restarts: self.restarts.load(Ordering::Relaxed),
            runtime_choice: RuntimeChoice {
                kind: self.labels.runtime,
                fallback: self.labels.fallback,
            },
        }
    }

    /// The totals since the last call: one activity window.
    pub(crate) fn take_window(&self) -> Totals {
        let now = Totals {
            events: self.events.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            api_calls: self.api_calls.load(Ordering::Relaxed),
        };
        let mut s = lock(&self.series);
        let last = std::mem::replace(&mut s.reported, now);
        Totals {
            events: now.events.saturating_sub(last.events),
            bytes_in: now.bytes_in.saturating_sub(last.bytes_in),
            bytes_out: now.bytes_out.saturating_sub(last.bytes_out),
            api_calls: now.api_calls.saturating_sub(last.api_calls),
        }
    }

    /// The `split` label for `id`, or `None` for an instance without split
    /// labels. Registers a new value while there is room.
    fn split_label(&self, s: &mut Series, id: Option<&SplitId>) -> Option<String> {
        if self.splits == SplitLabels::Unbounded {
            return None;
        }
        let id = id?.as_str();
        let secret = self.scrubber.as_ref().is_some_and(|sc| sc.scrub(id) != id);
        if secret
            || identifier("split", id, 128, "-_.:/").is_err()
            || id == OVERFLOW_SPLIT
            || id == REDACTED_SPLIT
        {
            return Some(REDACTED_SPLIT.to_owned());
        }
        if s.splits.contains(id) {
            return Some(id.to_owned());
        }
        if s.splits.len() < MAX_SPLIT_LABELS {
            s.splits.insert(id.to_owned());
            return Some(id.to_owned());
        }
        Some(OVERFLOW_SPLIT.to_owned())
    }

    fn count_error(&self, class: &ErrorClass) {
        *lock(&self.series).errors.entry(class.name()).or_default() += 1;
    }

    fn snapshot(&self) -> Snapshot {
        let s = lock(&self.series);
        let counter = |m: &BTreeMap<Option<String>, u64>, total: u64| {
            if self.splits == SplitLabels::Bounded && !m.is_empty() {
                // Splits plus whatever arrived without one (a scraped
                // fallback, an instance-wide report).
                m.iter().map(|(k, v)| (k.clone(), *v as f64)).collect()
            } else {
                vec![(None, total as f64)]
            }
        };
        let gauge =
            |m: &BTreeMap<Option<String>, f64>| m.iter().map(|(k, v)| (k.clone(), *v)).collect();
        Snapshot {
            events: counter(&s.events, self.events.load(Ordering::Relaxed)),
            bytes_in: counter(&s.bytes_in, self.bytes_in.load(Ordering::Relaxed)),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            api_calls: self.api_calls.load(Ordering::Relaxed),
            errors: s.errors.clone(),
            lag: gauge(&s.lag),
            lag_alert_seconds: self.lag_alert_seconds.load(Ordering::Relaxed),
            budget_wait: Duration::from_nanos(self.budget_wait_nanos.load(Ordering::Relaxed)),
            restarts: self.restarts.load(Ordering::Relaxed),
            dlq: self.dlq.load(Ordering::Relaxed),
            risk: gauge(&s.risk),
            up: self.up.load(Ordering::Relaxed),
        }
    }

    fn add_events(&self, split: Option<&SplitId>, rows: u64, bytes_in: u64) {
        self.events.fetch_add(rows, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
        if self.splits == SplitLabels::Bounded {
            let mut s = lock(&self.series);
            let label = self.split_label(&mut s, split);
            *s.events.entry(label.clone()).or_default() += rows;
            *s.bytes_in.entry(label).or_default() += bytes_in;
        }
    }

    /// Fold one scrape of a fallback runtime's own Prometheus metrics into
    /// this instance's counters: each mapped counter's samples whose labels
    /// match every `(name, value)` of `selector` are summed, and the increase
    /// since the last scrape is added (a value lower than the last one is a
    /// restarted process, counted from zero).
    pub fn apply_scrape(&self, text: &str, map: &FallbackMap, selector: &[(&str, &str)]) {
        let samples = parse_exposition(text);
        let sum = |names: &[&'static str]| -> Option<f64> {
            let mut found = false;
            let mut total = 0.0;
            for s in &samples {
                if names.contains(&s.name.as_str())
                    && selector
                        .iter()
                        .all(|(k, v)| s.labels.iter().any(|(lk, lv)| lk == k && lv == v))
                {
                    found = true;
                    total += s.value;
                }
            }
            found.then_some(total)
        };
        let delta = |key: &'static str, names: &[&'static str]| -> u64 {
            let Some(now) = sum(names) else {
                return 0;
            };
            let mut s = lock(&self.series);
            let last = s.scraped.insert(key, now).unwrap_or(0.0);
            let d = if now >= last { now - last } else { now };
            d.max(0.0) as u64
        };
        let events = delta("events", &map.events);
        let bytes_in = delta("bytes_in", &map.bytes_in);
        let bytes_out = delta("bytes_out", &map.bytes_out);
        let api_calls = delta("api_calls", &map.api_calls);
        self.add_events(None, events, bytes_in);
        self.bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
        self.api_calls.fetch_add(api_calls, Ordering::Relaxed);
    }
}

impl ConnectorObserver for InstanceMetrics {
    fn source_batch(&self, _instance: &InstanceKey, split: &SplitId, rows: u64, bytes: u64) {
        self.add_events(Some(split), rows, bytes);
    }

    fn sink_batch(&self, _instance: &InstanceKey, rows: u64, bytes: u64) {
        self.events.fetch_add(rows, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes, Ordering::Relaxed);
        if self.splits == SplitLabels::Bounded {
            *lock(&self.series).events.entry(None).or_default() += rows;
        }
    }

    fn api_call(&self, _instance: &InstanceKey) {
        self.api_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn retry(&self, _instance: &InstanceKey, class: &ErrorClass, _attempt: u32, _delay: Duration) {
        self.count_error(class);
    }

    /// The message is not kept: only the class becomes a label.
    fn error(&self, _instance: &InstanceKey, class: &ErrorClass, _message: &str) {
        self.count_error(class);
    }

    fn dead_letter(&self, _instance: &InstanceKey) {
        self.dlq.fetch_add(1, Ordering::Relaxed);
        self.count_error(&ErrorClass::Poison);
    }

    fn budget_wait(&self, _instance: &InstanceKey, waited: Duration) {
        let nanos = u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX);
        self.budget_wait_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    fn lag(&self, _instance: &InstanceKey, split: Option<&SplitId>, lag: Duration) {
        let mut s = lock(&self.series);
        let label = self.split_label(&mut s, split);
        s.lag.insert(label, lag.as_secs_f64());
    }

    fn position_risk(&self, _instance: &InstanceKey, split: Option<&SplitId>, ratio: f64) {
        let ratio = if ratio.is_nan() { 1.0 } else { ratio.max(0.0) };
        let mut s = lock(&self.series);
        let label = self.split_label(&mut s, split);
        s.risk.insert(label, ratio);
    }
}

// ------------------------------------------------------- fallback scrapes --

/// Which of a fallback runtime's own metrics feed which counter. The names
/// are those its Prometheus endpoint exposes (R24.5: Task 22 verifies them
/// against the pinned images).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FallbackMap {
    pub runtime: Option<RuntimeKind>,
    pub events: Vec<&'static str>,
    pub bytes_in: Vec<&'static str>,
    pub bytes_out: Vec<&'static str>,
    pub api_calls: Vec<&'static str>,
}

impl FallbackMap {
    /// `loams-connect` (Camel 4.22 with `camel-micrometer-prometheus`): one
    /// exchange per message, selected by the route id
    /// `loams-<namespace>-<instance>-<direction>`.
    pub fn camel() -> Self {
        Self {
            runtime: Some(RuntimeKind::Camel),
            events: vec!["camel_exchanges_total"],
            api_calls: vec!["camel_exchanges_total"],
            ..Self::default()
        }
    }

    /// Debezium Server 3.7 with its JMX metrics exported to Prometheus
    /// (`debezium_metrics_*`), selected by the server's `context` and `name`.
    pub fn debezium() -> Self {
        Self {
            runtime: Some(RuntimeKind::Debezium),
            events: vec!["debezium_metrics_TotalNumberOfEventsSeen"],
            ..Self::default()
        }
    }
}

/// One sample of a scraped exposition.
#[derive(Debug, Clone, PartialEq)]
struct Sample {
    name: String,
    labels: Vec<(String, String)>,
    value: f64,
}

/// The samples of a Prometheus text exposition; lines that do not parse are
/// skipped (a scrape is best effort, and the next one catches up).
fn parse_exposition(text: &str) -> Vec<Sample> {
    text.lines().filter_map(parse_line).collect()
}

fn parse_line(line: &str) -> Option<Sample> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let name_end = line
        .find(|c: char| c == '{' || c.is_whitespace())
        .unwrap_or(line.len());
    let name = &line[..name_end];
    let mut rest = &line[name_end..];
    let mut labels = Vec::new();
    if let Some(body) = rest.strip_prefix('{') {
        let mut chars = body.char_indices().peekable();
        let mut end = None;
        loop {
            // name
            while chars
                .peek()
                .is_some_and(|(_, c)| *c == ',' || c.is_whitespace())
            {
                chars.next();
            }
            match chars.peek() {
                Some((i, '}')) => {
                    end = Some(*i);
                    break;
                }
                None => break,
                _ => {}
            }
            let mut key = String::new();
            for (_, c) in chars.by_ref() {
                if c == '=' {
                    break;
                }
                key.push(c);
            }
            if chars.next().map(|(_, c)| c) != Some('"') {
                return None;
            }
            let mut value = String::new();
            let mut closed = false;
            while let Some((_, c)) = chars.next() {
                match c {
                    '\\' => match chars.next().map(|(_, c)| c) {
                        Some('n') => value.push('\n'),
                        Some(c) => value.push(c),
                        None => return None,
                    },
                    '"' => {
                        closed = true;
                        break;
                    }
                    c => value.push(c),
                }
            }
            if !closed {
                return None;
            }
            labels.push((key.trim().to_owned(), value));
        }
        rest = &body[end? + 1..];
    }
    let value = rest.split_whitespace().next()?;
    let value = match value {
        "+Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        v => v.parse().ok()?,
    };
    Some(Sample {
        name: name.to_owned(),
        labels,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exposition_lines() {
        let s = parse_line(r#"camel_exchanges_total{routeId="a,b",x="q\"uote"} 12 1700000000"#)
            .expect("parses");
        assert_eq!(s.name, "camel_exchanges_total");
        assert_eq!(
            s.labels,
            vec![
                ("routeId".to_owned(), "a,b".to_owned()),
                ("x".to_owned(), "q\"uote".to_owned())
            ]
        );
        assert_eq!(s.value, 12.0);
        assert_eq!(parse_line("up 1").map(|s| s.value), Some(1.0));
        assert!(parse_line("# TYPE up gauge").is_none());
        assert!(parse_line(r#"broken{a="x} 1"#).is_none());
    }

    #[test]
    fn numbers_render_without_exponent() {
        assert_eq!(Num(3.0).to_string(), "3");
        assert_eq!(Num(0.25).to_string(), "0.25");
        assert_eq!(Num(1e-7).to_string(), "0.0000001");
    }
}
