//! Performance budgets (CN2 plan Task 27).
//!
//! Three things live here:
//!
//! - **Measurement.** [`run_reference`] times the SDK's reference pipeline
//!   (counter source → in-memory Fabric): throughput in events and MiB per
//!   second, the p99 of each batch's time from read to Fabric ack, the peak
//!   in-flight memory against the instance's budget, and CPU per 10 000
//!   events. [`run_harness_bench`] times any [`ConnectorHarness`] against its
//!   real system, so a family's bench is its conformance harness plus a
//!   [`BenchPlan`]. Both produce [`Point`]s, one per batch size, gathered in
//!   a [`BenchReport`] that [`write_report`] stores for the scheduled
//!   `connectors-bench` job.
//! - **Budgets.** [`Baselines`] (`conformance/connectors/bench/baselines.toml`)
//!   holds, per connector and direction, the calibrated measurements of one
//!   runner class and build profile. [`check`] fails a run that is more than
//!   [`REGRESSION_BUDGET`] (15 %) worse on any metric, misses a batch size the
//!   baseline records, ran elsewhere, or broke the memory budget (a hard
//!   bound, Review Focus 5). An entry that is not calibrated yet never fails
//!   a run: the run is a calibration candidate. [`calibrate`] takes the median
//!   of at least `calibration_runs` runs, so one noisy run moves nothing.
//! - **Density.** [`derive_chart_budgets`] turns the SDK's default memory
//!   budget and the measured per-instance overhead into the `flow` pod's
//!   memory and instance count, which `deploy/helm/loams-connectors/values.yaml`
//!   must equal (`budget_values_match_measurements`).
//!
//! Nothing here hard-codes a number measured on a developer machine. The
//! reference pipeline's floors ([`reference_floor`]) are derived from its
//! configuration and catch a stalled pipeline, not a slow one; the 15 %
//! budget against baselines recorded on CI's runner class catches a slow one.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::testing::reference::{
    CounterConfig, CounterEnumerator, CounterFactory, CounterHandle,
};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorError, EventBatch, FabricAck, FabricError, FabricWriter,
    InstanceKey, Pipeline, PipelineConfig, PipelineState, Position, Source, SourceBatch,
    SourceFactory, Split,
};
use serde::{Deserialize, Serialize};

use crate::{ConnectorHarness, HarnessError};

/// A run more than this much worse than its baseline, on any metric, fails
/// the scheduled job (plan Task 27).
pub const REGRESSION_BUDGET: f64 = 0.15;

/// The three batch sizes of the reference bench (rows per batch).
pub const BATCH_SIZES: [usize; 3] = [1, 100, 10_000];

/// The baselines file, relative to the repository root.
pub const BASELINES_PATH: &str = "conformance/connectors/bench/baselines.toml";

/// Per-instance memory outside the pipeline's budget (client buffers,
/// connection state, the instance's tasks) assumed until a family records a
/// measurement. Deliberately generous: an over-estimate packs fewer
/// instances per pod, an under-estimate gets pods OOM-killed.
pub const CONSERVATIVE_INSTANCE_OVERHEAD_BYTES: u64 = 32 << 20;

/// Headroom over the largest measured per-instance overhead.
pub const OVERHEAD_HEADROOM: f64 = 0.25;

/// The `flow` process's own memory before any instance runs (runtime,
/// registry, metrics, TLS roots), assumed until FL1's `flow` role exists to
/// measure (ruling R27.4).
pub const PROCESS_BASE_BYTES: u64 = 256 << 20;

/// The `flow` pod's memory limit (and request: memory is not overcommitted).
pub const POD_MEMORY_LIMIT_BYTES: u64 = 4 << 30;

/// The `flow` pod's CPU request. There is no CPU limit (throttling a
/// connector stalls its pipeline instead of slowing it).
pub const POD_CPU_REQUEST_MILLIS: u64 = 1_000;

/// The reference pipeline's throughput floor, in events per second. The
/// pipeline does no I/O; one event costs well under 100 µs even in a debug
/// build on a loaded runner, so this leaves a wide margin, and it is still
/// two orders of magnitude above a pipeline that stalls one linger per batch
/// (5 events per second at batch size 1 with the default 200 ms linger).
pub const FLOOR_EVENTS_PER_SEC: f64 = 1_000.0;

/// The per-event allowance in the reference pipeline's p99 ceiling.
pub const FLOOR_PER_EVENT: Duration = Duration::from_micros(100);

// ------------------------------------------------------------------ points --

/// One batch size's measurements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Point {
    /// Rows per batch (the reference bench), or records per round (a
    /// harness bench, [`run_harness_bench`]).
    pub batch_rows: usize,
    /// Events delivered while measuring.
    pub events: u64,
    pub events_per_sec: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mib_per_sec: Option<f64>,
    /// The p99 latency in milliseconds: a batch's read to its Fabric ack
    /// (reference), or a round's start to settle (harness).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p99_ms: Option<f64>,
    /// The peak in-flight memory the pipeline's budget accounted for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_memory_bytes: Option<u64>,
    /// The instance's memory budget during the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_budget_bytes: Option<u64>,
    /// CPU time of the whole test process per 10 000 events, in
    /// milliseconds (Linux only; the harness's own work is included).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_ms_per_10k: Option<f64>,
}

/// One bench run of one connector in one direction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchReport {
    /// The manifest id (`systems.toml`'s key), or `reference`.
    pub connector: String,
    /// `source`, `sink` or `roundtrip`.
    pub direction: String,
    /// The real system the run measured.
    pub system: String,
    /// The runner class (`LOAMS_BENCH_RUNNER`, e.g. `ubuntu-24.04`); a
    /// baseline is compared only with runs on its own class.
    pub runner: String,
    /// `release` or `debug`.
    pub profile: String,
    /// Resident memory one more instance costs beyond its pipeline budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_overhead_bytes: Option<u64>,
    pub points: Vec<Point>,
}

impl BenchReport {
    /// A report for this process: the runner from `LOAMS_BENCH_RUNNER`
    /// (default `local`) and the build profile it was compiled with.
    pub fn new(connector: &str, direction: &str, system: &str, points: Vec<Point>) -> Self {
        Self {
            connector: connector.to_owned(),
            direction: direction.to_owned(),
            system: system.to_owned(),
            runner: std::env::var("LOAMS_BENCH_RUNNER")
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| "local".to_owned()),
            profile: build_profile().to_owned(),
            instance_overhead_bytes: None,
            points,
        }
    }

    /// `<connector>/<direction>`.
    pub fn key(&self) -> String {
        format!("{}/{}", self.connector, self.direction)
    }
}

/// The profile this crate was compiled with.
pub const fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

/// Whether bench tests run: `LOAMS_CONNECTORS_BENCH` and each of
/// `stack_vars` must be set (Global Constraints, "Tests skip without
/// services"). A family's bench test returns early, saying why, otherwise.
pub fn bench_enabled(test: &str, stack_vars: &[&str]) -> bool {
    let missing: Vec<&str> = std::iter::once("LOAMS_CONNECTORS_BENCH")
        .chain(stack_vars.iter().copied())
        .filter(|v| std::env::var(v).map_or(true, |x| x.is_empty()))
        .collect();
    if missing.is_empty() {
        true
    } else {
        eprintln!("{test}: skipped, {} not set", missing.join(", "));
        false
    }
}

/// Write `report` to `$LOAMS_BENCH_OUT/<connector>.<direction>.json`. Without
/// `LOAMS_BENCH_OUT` nothing is written and `None` comes back.
pub fn write_report(report: &BenchReport) -> std::io::Result<Option<PathBuf>> {
    let Some(dir) = std::env::var_os("LOAMS_BENCH_OUT").filter(|d| !d.is_empty()) else {
        return Ok(None);
    };
    let dir = PathBuf::from(dir);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{}.{}.json", report.connector, report.direction));
    let json = serde_json::to_string_pretty(report).map_err(std::io::Error::other)?;
    std::fs::write(&path, json)?;
    Ok(Some(path))
}

/// Every `*.json` report in `dir`, sorted by key.
pub fn read_reports(dir: &Path) -> Result<Vec<BenchReport>, String> {
    let mut out = Vec::new();
    let entries = std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    for e in entries.flatten() {
        let p = e.path();
        if p.extension().is_some_and(|x| x == "json") {
            let text = std::fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
            out.push(
                serde_json::from_str::<BenchReport>(&text)
                    .map_err(|e| format!("{}: {e}", p.display()))?,
            );
        }
    }
    out.sort_by_key(BenchReport::key);
    Ok(out)
}

// --------------------------------------------------------------- baselines --

/// `conformance/connectors/bench/baselines.toml`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Baselines {
    pub budget: BudgetSection,
    #[serde(default)]
    pub connector: Vec<BaselineEntry>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetSection {
    /// The regression budget, a fraction (0.15).
    pub regression: f64,
    /// Runs a calibration takes the median of.
    pub calibration_runs: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BaselineStatus {
    /// Calibrated: runs are checked against `point`.
    Recorded,
    /// The bench is written and runs; no calibration has been committed.
    Uncalibrated,
    /// No bench yet; `owner` names the task that writes it.
    Pending,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineEntry {
    pub id: String,
    pub direction: String,
    pub family: String,
    pub status: BaselineStatus,
    /// `<file>::<fn>` of the bench (written benches).
    #[serde(default)]
    pub bench: Option<String>,
    /// The owning task (pending benches).
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub runner: Option<String>,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub recorded_on: Option<String>,
    #[serde(default)]
    pub calibration_runs: Option<usize>,
    #[serde(default)]
    pub instance_overhead_bytes: Option<u64>,
    #[serde(default)]
    pub point: Vec<Point>,
    #[serde(default)]
    pub notes: Vec<String>,
}

impl BaselineEntry {
    pub fn key(&self) -> String {
        format!("{}/{}", self.id, self.direction)
    }
}

impl Baselines {
    /// Parse and validate: a recorded entry has points, a runner, a profile
    /// and a calibration date; a pending one an owner; keys are unique.
    pub fn parse(text: &str) -> Result<Self, String> {
        let b: Baselines = toml::from_str(text).map_err(|e| e.to_string())?;
        if !(b.budget.regression > 0.0 && b.budget.regression < 1.0) {
            return Err("budget.regression must be a fraction in (0, 1)".into());
        }
        if b.budget.calibration_runs == 0 {
            return Err("budget.calibration_runs must be positive".into());
        }
        let mut keys = BTreeSet::new();
        for e in &b.connector {
            if !keys.insert(e.key()) {
                return Err(format!("{}: listed twice", e.key()));
            }
            if !["source", "sink", "roundtrip"].contains(&e.direction.as_str()) {
                return Err(format!(
                    "{}: direction must be source, sink or roundtrip",
                    e.key()
                ));
            }
            match e.status {
                BaselineStatus::Recorded => {
                    if e.point.is_empty()
                        || e.runner.is_none()
                        || e.profile.is_none()
                        || e.recorded_on.is_none()
                    {
                        return Err(format!(
                            "{}: a recorded baseline needs points, runner, profile and recorded_on",
                            e.key()
                        ));
                    }
                }
                BaselineStatus::Uncalibrated => {
                    if e.bench.is_none() {
                        return Err(format!(
                            "{}: an uncalibrated baseline names its bench",
                            e.key()
                        ));
                    }
                }
                BaselineStatus::Pending => {
                    if e.owner.is_none() {
                        return Err(format!("{}: a pending baseline names its owner", e.key()));
                    }
                }
            }
        }
        Ok(b)
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::parse(&text).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// This checkout's baselines.
    pub fn repository() -> Result<Self, String> {
        Self::load(&loams_flow::manifest::repository_root().join(BASELINES_PATH))
    }

    pub fn entry(&self, id: &str, direction: &str) -> Option<&BaselineEntry> {
        self.connector
            .iter()
            .find(|e| e.id == id && e.direction == direction)
    }
}

// ------------------------------------------------------------------- check --

/// A measured quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Metric {
    EventsPerSec,
    MibPerSec,
    P99,
    PeakMemory,
    CpuPer10k,
    /// The peak exceeded the instance's memory budget: a hard bound.
    MemoryBudget,
}

impl Metric {
    fn higher_is_better(self) -> bool {
        matches!(self, Metric::EventsPerSec | Metric::MibPerSec)
    }
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Metric::EventsPerSec => "events/s",
            Metric::MibPerSec => "MiB/s",
            Metric::P99 => "p99 ms",
            Metric::PeakMemory => "peak memory bytes",
            Metric::CpuPer10k => "CPU ms per 10k events",
            Metric::MemoryBudget => "memory budget",
        })
    }
}

/// One metric of one batch size that is worse than the budget allows.
#[derive(Debug, Clone, PartialEq)]
pub struct Regression {
    pub batch_rows: usize,
    pub metric: Metric,
    pub baseline: f64,
    pub measured: f64,
    /// How much worse, as a fraction (0.16 = 16 % worse).
    pub change: f64,
}

/// What [`check`] found for one report.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub key: String,
    /// The baseline is recorded (otherwise the run is a candidate only).
    pub calibrated: bool,
    pub regressions: Vec<Regression>,
    /// Batch sizes the baseline records and the run did not measure.
    pub missing: Vec<usize>,
    /// Why the run could not be compared (another runner class or profile,
    /// no baseline entry).
    pub refused: Option<String>,
}

impl Verdict {
    pub fn passed(&self) -> bool {
        self.refused.is_none() && self.regressions.is_empty() && self.missing.is_empty()
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match (self.passed(), self.calibrated) {
            (true, true) => "within budget",
            (true, false) => "uncalibrated (a calibration candidate)",
            (false, _) => "FAILED",
        };
        writeln!(f, "{}: {state}", self.key)?;
        if let Some(r) = &self.refused {
            writeln!(f, "  refused: {r}")?;
        }
        for m in &self.missing {
            writeln!(f, "  batch {m}: not measured")?;
        }
        for r in &self.regressions {
            writeln!(
                f,
                "  batch {}: {} {:.3} vs baseline {:.3} ({:+.1} % worse)",
                r.batch_rows,
                r.metric,
                r.measured,
                r.baseline,
                r.change * 100.0
            )?;
        }
        Ok(())
    }
}

fn worse_by(metric: Metric, baseline: f64, measured: f64) -> f64 {
    if baseline <= 0.0 {
        return 0.0;
    }
    if metric.higher_is_better() {
        1.0 - measured / baseline
    } else {
        measured / baseline - 1.0
    }
}

/// Compare `report` with `entry` under `budget` (a fraction).
pub fn check(entry: &BaselineEntry, report: &BenchReport, budget: f64) -> Verdict {
    let mut v = Verdict {
        key: report.key(),
        calibrated: entry.status == BaselineStatus::Recorded,
        regressions: Vec::new(),
        missing: Vec::new(),
        refused: None,
    };
    // The memory budget is a hard bound, calibrated or not.
    for p in &report.points {
        if let (Some(peak), Some(limit)) = (p.peak_memory_bytes, p.memory_budget_bytes)
            && peak > limit
        {
            v.regressions.push(Regression {
                batch_rows: p.batch_rows,
                metric: Metric::MemoryBudget,
                baseline: limit as f64,
                measured: peak as f64,
                change: worse_by(Metric::MemoryBudget, limit as f64, peak as f64),
            });
        }
    }
    if !v.calibrated {
        return v;
    }
    if entry.runner.as_deref() != Some(report.runner.as_str()) {
        v.refused = Some(format!(
            "the baseline was recorded on runner {:?}, this run is on {:?}: recalibrate, or run on the baseline's runner class",
            entry.runner.as_deref().unwrap_or("?"),
            report.runner
        ));
        return v;
    }
    if entry.profile.as_deref() != Some(report.profile.as_str()) {
        v.refused = Some(format!(
            "the baseline is a {} build, this run a {} build",
            entry.profile.as_deref().unwrap_or("?"),
            report.profile
        ));
        return v;
    }
    let mut found = Vec::new();
    for base in &entry.point {
        let Some(run) = report
            .points
            .iter()
            .find(|p| p.batch_rows == base.batch_rows)
        else {
            v.missing.push(base.batch_rows);
            continue;
        };
        let pairs = [
            (
                Metric::EventsPerSec,
                Some(base.events_per_sec),
                Some(run.events_per_sec),
            ),
            (Metric::MibPerSec, base.mib_per_sec, run.mib_per_sec),
            (Metric::P99, base.p99_ms, run.p99_ms),
            (
                Metric::PeakMemory,
                base.peak_memory_bytes.map(|b| b as f64),
                run.peak_memory_bytes.map(|b| b as f64),
            ),
            (Metric::CpuPer10k, base.cpu_ms_per_10k, run.cpu_ms_per_10k),
        ];
        for (metric, b, m) in pairs {
            if let (Some(b), Some(m)) = (b, m) {
                let change = worse_by(metric, b, m);
                // A hair of tolerance so a run exactly at the budget passes.
                if change > budget + 1e-9 {
                    found.push(Regression {
                        batch_rows: base.batch_rows,
                        metric,
                        baseline: b,
                        measured: m,
                        change,
                    });
                }
            }
        }
    }
    v.regressions.extend(found);
    v
}

/// Check every report against its baseline. A report with no baseline entry
/// is refused: a bench must be listed before its numbers count.
pub fn check_all(baselines: &Baselines, reports: &[BenchReport]) -> Vec<Verdict> {
    reports
        .iter()
        .map(|r| match baselines.entry(&r.connector, &r.direction) {
            Some(e) => check(e, r, baselines.budget.regression),
            None => Verdict {
                key: r.key(),
                calibrated: false,
                regressions: Vec::new(),
                missing: Vec::new(),
                refused: Some(format!("no entry in {BASELINES_PATH}")),
            },
        })
        .collect()
}

// --------------------------------------------------------------- calibrate --

/// A calibrated baseline: the median of several runs.
#[derive(Debug, Clone, PartialEq)]
pub struct Calibrated {
    pub connector: String,
    pub direction: String,
    pub runner: String,
    pub profile: String,
    pub runs: usize,
    pub instance_overhead_bytes: Option<u64>,
    pub points: Vec<Point>,
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(f64::total_cmp);
    let n = xs.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 1 {
        xs[n / 2]
    } else {
        (xs[n / 2 - 1] + xs[n / 2]) / 2.0
    }
}

fn median_opt(xs: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let v: Option<Vec<f64>> = xs.collect();
    v.filter(|v| !v.is_empty()).map(median)
}

/// The median, per batch size and metric, of `runs` (at least `min_runs`, of
/// one connector and direction, on one runner class and build profile, with
/// the same batch sizes).
pub fn calibrate(runs: &[BenchReport], min_runs: usize) -> Result<Calibrated, String> {
    let first = runs.first().ok_or("no runs")?;
    if runs.len() < min_runs {
        return Err(format!(
            "{}: {} runs, a calibration needs {min_runs}",
            first.key(),
            runs.len()
        ));
    }
    let sizes = |r: &BenchReport| r.points.iter().map(|p| p.batch_rows).collect::<Vec<_>>();
    for r in runs {
        if r.key() != first.key() {
            return Err(format!("runs of {} and {} mixed", first.key(), r.key()));
        }
        if r.runner != first.runner || r.profile != first.profile {
            return Err(format!(
                "{}: runs from {}/{} and {}/{} mixed",
                first.key(),
                first.runner,
                first.profile,
                r.runner,
                r.profile
            ));
        }
        if sizes(r) != sizes(first) {
            return Err(format!(
                "{}: the runs measured different batch sizes",
                first.key()
            ));
        }
    }
    let points = (0..first.points.len())
        .map(|i| {
            let col = |f: &dyn Fn(&Point) -> Option<f64>| {
                median_opt(runs.iter().map(|r| f(&r.points[i])))
            };
            Point {
                batch_rows: first.points[i].batch_rows,
                events: median(runs.iter().map(|r| r.points[i].events as f64).collect()).round()
                    as u64,
                events_per_sec: median(runs.iter().map(|r| r.points[i].events_per_sec).collect()),
                mib_per_sec: col(&|p| p.mib_per_sec),
                p99_ms: col(&|p| p.p99_ms),
                peak_memory_bytes: col(&|p| p.peak_memory_bytes.map(|b| b as f64))
                    .map(|b| b.round() as u64),
                memory_budget_bytes: first.points[i].memory_budget_bytes,
                cpu_ms_per_10k: col(&|p| p.cpu_ms_per_10k),
            }
        })
        .collect();
    Ok(Calibrated {
        connector: first.connector.clone(),
        direction: first.direction.clone(),
        runner: first.runner.clone(),
        profile: first.profile.clone(),
        runs: runs.len(),
        instance_overhead_bytes: median_opt(
            runs.iter()
                .map(|r| r.instance_overhead_bytes.map(|b| b as f64)),
        )
        .map(|b| b.round() as u64),
        points,
    })
}

impl Calibrated {
    /// The `[[connector]]` entry to paste over the uncalibrated one.
    pub fn to_toml(&self, family: &str, bench: &str, recorded_on: &str) -> String {
        let mut s = format!(
            "[[connector]]\nid = {:?}\ndirection = {:?}\nfamily = {family:?}\nstatus = \"recorded\"\n\
             bench = {bench:?}\nrunner = {:?}\nprofile = {:?}\nrecorded_on = {recorded_on:?}\n\
             calibration_runs = {}\n",
            self.connector, self.direction, self.runner, self.profile, self.runs
        );
        if let Some(o) = self.instance_overhead_bytes {
            s.push_str(&format!("instance_overhead_bytes = {o}\n"));
        }
        for p in &self.points {
            s.push_str(&format!(
                "\n[[connector.point]]\nbatch_rows = {}\nevents = {}\nevents_per_sec = {:.1}\n",
                p.batch_rows, p.events, p.events_per_sec
            ));
            if let Some(x) = p.mib_per_sec {
                s.push_str(&format!("mib_per_sec = {x:.3}\n"));
            }
            if let Some(x) = p.p99_ms {
                s.push_str(&format!("p99_ms = {x:.3}\n"));
            }
            if let Some(x) = p.peak_memory_bytes {
                s.push_str(&format!("peak_memory_bytes = {x}\n"));
            }
            if let Some(x) = p.memory_budget_bytes {
                s.push_str(&format!("memory_budget_bytes = {x}\n"));
            }
            if let Some(x) = p.cpu_ms_per_10k {
                s.push_str(&format!("cpu_ms_per_10k = {x:.3}\n"));
            }
        }
        s
    }
}

// ------------------------------------------------------------------ chart --

/// The `budgets` block of `deploy/helm/loams-connectors/values.yaml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChartBudgets {
    pub max_batch_rows: u64,
    pub max_batch_bytes: u64,
    pub max_in_flight_batches: u64,
    pub memory_budget_bytes: u64,
    pub instance_overhead_bytes: u64,
    /// The overhead comes from a recorded measurement, not the default.
    pub calibrated: bool,
    pub process_base_bytes: u64,
    pub pod_memory_limit_bytes: u64,
    pub pod_memory_request_bytes: u64,
    pub pod_cpu_request_millis: u64,
    pub instances_per_pod: u64,
}

/// The chart's budgets: each instance costs its pipeline memory budget plus
/// the largest recorded per-instance overhead (with
/// [`OVERHEAD_HEADROOM`], rounded up to a MiB), or
/// [`CONSERVATIVE_INSTANCE_OVERHEAD_BYTES`] before any is recorded; a pod
/// holds as many as fit in its limit after the process's base.
pub fn derive_chart_budgets(cfg: &PipelineConfig, baselines: &Baselines) -> ChartBudgets {
    const MIB: u64 = 1 << 20;
    let measured = baselines
        .connector
        .iter()
        .filter(|e| e.status == BaselineStatus::Recorded)
        .filter_map(|e| e.instance_overhead_bytes)
        .max();
    let (overhead, calibrated) = match measured {
        Some(m) => {
            let with = (m as f64 * (1.0 + OVERHEAD_HEADROOM)).ceil() as u64;
            (with.div_ceil(MIB) * MIB, true)
        }
        None => (CONSERVATIVE_INSTANCE_OVERHEAD_BYTES, false),
    };
    let per_instance = cfg.memory_budget_bytes as u64 + overhead;
    let instances = POD_MEMORY_LIMIT_BYTES.saturating_sub(PROCESS_BASE_BYTES) / per_instance.max(1);
    ChartBudgets {
        max_batch_rows: cfg.max_batch_rows as u64,
        max_batch_bytes: cfg.max_batch_bytes as u64,
        max_in_flight_batches: cfg.max_in_flight_batches as u64,
        memory_budget_bytes: cfg.memory_budget_bytes as u64,
        instance_overhead_bytes: overhead,
        calibrated,
        process_base_bytes: PROCESS_BASE_BYTES,
        pod_memory_limit_bytes: POD_MEMORY_LIMIT_BYTES,
        pod_memory_request_bytes: POD_MEMORY_LIMIT_BYTES,
        pod_cpu_request_millis: POD_CPU_REQUEST_MILLIS,
        instances_per_pod: instances,
    }
}

#[derive(Deserialize)]
struct ValuesDoc {
    budgets: ValuesBudgets,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValuesBudgets {
    pipeline: ValuesPipeline,
    instance: ValuesInstance,
    flow_pod: ValuesPod,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValuesPipeline {
    max_batch_rows: u64,
    max_batch_bytes: u64,
    max_in_flight_batches: u64,
    memory_budget_bytes: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValuesInstance {
    overhead_bytes: u64,
    calibrated: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValuesPod {
    process_base_bytes: u64,
    memory_limit_bytes: u64,
    memory_request_bytes: u64,
    cpu_request_millis: u64,
    instances_per_pod: u64,
}

impl ChartBudgets {
    /// Read the `budgets` block of a values file (other keys are the
    /// chart's own and ignored).
    pub fn from_values_yaml(text: &str) -> Result<Self, String> {
        let d: ValuesDoc = serde_norway::from_str(text).map_err(|e| e.to_string())?;
        let b = d.budgets;
        Ok(Self {
            max_batch_rows: b.pipeline.max_batch_rows,
            max_batch_bytes: b.pipeline.max_batch_bytes,
            max_in_flight_batches: b.pipeline.max_in_flight_batches,
            memory_budget_bytes: b.pipeline.memory_budget_bytes,
            instance_overhead_bytes: b.instance.overhead_bytes,
            calibrated: b.instance.calibrated,
            process_base_bytes: b.flow_pod.process_base_bytes,
            pod_memory_limit_bytes: b.flow_pod.memory_limit_bytes,
            pod_memory_request_bytes: b.flow_pod.memory_request_bytes,
            pod_cpu_request_millis: b.flow_pod.cpu_request_millis,
            instances_per_pod: b.flow_pod.instances_per_pod,
        })
    }

    /// The `budgets` block, as `values.yaml` holds it.
    pub fn to_values_yaml(&self) -> String {
        format!(
            "budgets:\n  pipeline:\n    maxBatchRows: {}\n    maxBatchBytes: {}\n    \
             maxInFlightBatches: {}\n    memoryBudgetBytes: {}\n  instance:\n    \
             overheadBytes: {}\n    calibrated: {}\n  flowPod:\n    processBaseBytes: {}\n    \
             memoryLimitBytes: {}\n    memoryRequestBytes: {}\n    cpuRequestMillis: {}\n    \
             instancesPerPod: {}\n",
            self.max_batch_rows,
            self.max_batch_bytes,
            self.max_in_flight_batches,
            self.memory_budget_bytes,
            self.instance_overhead_bytes,
            self.calibrated,
            self.process_base_bytes,
            self.pod_memory_limit_bytes,
            self.pod_memory_request_bytes,
            self.pod_cpu_request_millis,
            self.instances_per_pod
        )
    }
}

// ---------------------------------------------------------- process stats --

/// CPU time of every live thread of this process (Linux
/// `/proc/self/task/*/schedstat`, nanoseconds), or `None` elsewhere.
pub fn process_cpu_time() -> Option<Duration> {
    let mut total = 0u64;
    for t in std::fs::read_dir("/proc/self/task").ok()?.flatten() {
        if let Ok(s) = std::fs::read_to_string(t.path().join("schedstat"))
            && let Some(ns) = s
                .split_whitespace()
                .next()
                .and_then(|n| n.parse::<u64>().ok())
        {
            total += ns;
        }
    }
    Some(Duration::from_nanos(total))
}

/// Resident memory of this process (Linux `/proc/self/status` `VmRSS`).
pub fn process_rss_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = s.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

fn cpu_per_10k(before: Option<Duration>, after: Option<Duration>, events: u64) -> Option<f64> {
    let (b, a) = (before?, after?);
    (events > 0).then(|| a.saturating_sub(b).as_secs_f64() * 1_000.0 * 10_000.0 / events as f64)
}

/// Nearest-rank p99 in milliseconds (with fewer than 100 samples, the
/// largest).
fn p99_ms(mut xs: Vec<Duration>) -> Option<f64> {
    if xs.is_empty() {
        return None;
    }
    xs.sort();
    let rank = ((xs.len() as f64) * 0.99).ceil() as usize;
    Some(xs[rank.clamp(1, xs.len()) - 1].as_secs_f64() * 1_000.0)
}

// -------------------------------------------------------------- reference --

/// The reference pipeline's floor budgets for one batch size.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Floor {
    pub min_events_per_sec: f64,
    pub max_p99: Duration,
}

/// Derived, not measured: [`FLOOR_EVENTS_PER_SEC`], and a p99 ceiling of five
/// lingers plus [`FLOOR_PER_EVENT`] for each row of the batch.
pub fn reference_floor(batch_rows: usize, cfg: &PipelineConfig) -> Floor {
    Floor {
        min_events_per_sec: FLOOR_EVENTS_PER_SEC,
        max_p99: cfg.linger * 5 + FLOOR_PER_EVENT * u32::try_from(batch_rows).unwrap_or(u32::MAX),
    }
}

/// One run of the reference pipeline: `splits` counter sources, `events`
/// each, into the in-memory Fabric.
#[derive(Debug, Clone)]
pub struct ReferenceBench {
    pub batch_rows: usize,
    /// Events per split.
    pub events: u64,
    pub splits: usize,
    pub payload_bytes: usize,
    /// Slow the Fabric's acks (backpressure).
    pub fabric_delay: Option<Duration>,
    /// The SDK's defaults with `max_batch_rows = batch_rows`.
    pub config: PipelineConfig,
    pub timeout: Duration,
}

impl ReferenceBench {
    pub fn new(batch_rows: usize, events: u64) -> Self {
        Self {
            batch_rows,
            events,
            splits: 1,
            payload_bytes: 256,
            fabric_delay: None,
            config: PipelineConfig {
                max_batch_rows: batch_rows,
                ..PipelineConfig::default()
            },
            timeout: Duration::from_secs(600),
        }
    }
}

type Stamps = Arc<Mutex<HashMap<String, Instant>>>;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn first_id(b: &EventBatch) -> Option<String> {
    match b {
        EventBatch::Events(v) => v.first().map(|e| e.id.clone()),
        EventBatch::Arrow { envelope, .. } => Some(envelope.id.clone()),
    }
}

/// Stamps each batch a source returns with the time it was read.
struct TimedSources {
    inner: Arc<dyn SourceFactory>,
    stamps: Stamps,
}

struct TimedSource {
    inner: Box<dyn Source>,
    stamps: Stamps,
}

impl SourceFactory for TimedSources {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(TimedSource {
            inner: self.inner.create(split)?,
            stamps: self.stamps.clone(),
        }))
    }
}

#[async_trait]
impl Source for TimedSource {
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.inner.open(split, pos).await
    }
    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let r = self.inner.next(budget).await;
        if let Ok(Some(b)) = &r
            && let Some(id) = first_id(&b.events)
        {
            lock(&self.stamps).insert(id, Instant::now());
        }
        r
    }
    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        self.inner.committed(upto).await
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.inner.close().await
    }
}

/// Records each batch's read-to-ack latency.
struct TimedWriter {
    inner: MemoryFabric,
    stamps: Stamps,
    latencies: Arc<Mutex<Vec<Duration>>>,
}

#[async_trait]
impl FabricWriter for TimedWriter {
    async fn append(&self, b: &SourceBatch) -> Result<FabricAck, FabricError> {
        let r = FabricWriter::append(&self.inner, b).await;
        if r.is_ok()
            && let Some(id) = first_id(&b.events)
            && let Some(at) = lock(&self.stamps).remove(&id)
        {
            lock(&self.latencies).push(at.elapsed());
        }
        r
    }
}

/// Run the reference pipeline once and measure it.
pub async fn run_reference(b: &ReferenceBench) -> Result<Point, String> {
    let inst = InstanceKey::new(1, "bench");
    let names: Vec<String> = (0..b.splits.max(1)).map(|i| format!("s{i}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let handle = CounterHandle::new(&refs);
    let fabric = MemoryFabric::new();
    fabric.set_append_delay(b.fabric_delay);
    let stamps: Stamps = Arc::default();
    let latencies: Arc<Mutex<Vec<Duration>>> = Arc::default();
    let counter = CounterConfig {
        events_per_split: Some(b.events),
        payload_bytes: b.payload_bytes,
        ignore_budget: false,
    };
    let total = b.events * names.len() as u64;

    let cpu0 = process_cpu_time();
    let t0 = Instant::now();
    let p = Pipeline::source(inst.clone(), b.config.clone())
        .enumerator(Box::new(CounterEnumerator::new(handle.clone())))
        .sources(Arc::new(TimedSources {
            inner: Arc::new(CounterFactory::new(handle.clone(), counter, &inst)),
            stamps: stamps.clone(),
        }))
        .writer(Arc::new(TimedWriter {
            inner: fabric.clone(),
            stamps,
            latencies: latencies.clone(),
        }))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .dead_letters(Arc::new(fabric.clone()))
        .discover_every(Duration::from_secs(3600))
        .epoch(1)
        .start()
        .await
        .map_err(|e| e.to_string())?;
    let deadline = t0 + b.timeout;
    while (fabric.len() as u64) < total {
        let st = p.status();
        if matches!(st.state, PipelineState::Failed | PipelineState::Fenced) {
            return Err(format!("the reference pipeline stopped: {st:?}"));
        }
        if Instant::now() > deadline {
            return Err(format!(
                "{} of {total} events after {:?}",
                fabric.len(),
                b.timeout
            ));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let elapsed = t0.elapsed().as_secs_f64().max(1e-9);
    let cpu1 = process_cpu_time();
    let stats = p.stats();
    p.stop().await;

    let bytes: usize = fabric.events().iter().map(CloudEvent::size_bytes).sum();
    let lat = std::mem::take(&mut *lock(&latencies));
    Ok(Point {
        batch_rows: b.batch_rows,
        events: total,
        events_per_sec: total as f64 / elapsed,
        mib_per_sec: Some(bytes as f64 / (1 << 20) as f64 / elapsed),
        p99_ms: p99_ms(lat),
        peak_memory_bytes: Some(stats.peak_in_flight_bytes as u64),
        memory_budget_bytes: Some(b.config.memory_budget_bytes as u64),
        cpu_ms_per_10k: cpu_per_10k(cpu0, cpu1, total),
    })
}

/// Resident memory one idle reference instance adds: `n` pipelines started
/// with nothing to read, the RSS before and after, divided by `n`. Noisy on
/// a shared machine; `connectors-bench` records it, and only calibrated
/// medians reach the chart.
pub async fn measure_instance_overhead(n: usize) -> Option<u64> {
    let n = n.max(1);
    let before = process_rss_bytes()?;
    let mut running = Vec::with_capacity(n);
    let fabric = MemoryFabric::new();
    for i in 0..n {
        let inst = InstanceKey::new(1, format!("overhead-{i}"));
        let handle = CounterHandle::new(&["s0"]);
        let counter = CounterConfig {
            events_per_split: Some(0),
            payload_bytes: 0,
            ignore_budget: false,
        };
        let p = Pipeline::source(inst.clone(), PipelineConfig::default())
            .enumerator(Box::new(CounterEnumerator::new(handle.clone())))
            .sources(Arc::new(CounterFactory::new(handle, counter, &inst)))
            .writer(Arc::new(fabric.clone()))
            .checkpoints(Arc::new(MemoryCheckpointStore::new()))
            .discover_every(Duration::from_secs(3600))
            .epoch(1)
            .start()
            .await
            .ok()?;
        running.push(p);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = process_rss_bytes()?;
    for p in running {
        p.stop().await;
    }
    Some(after.saturating_sub(before) / n as u64)
}

/// The reference bench at [`BATCH_SIZES`], as `connectors-bench` runs it.
pub async fn reference_report() -> Result<BenchReport, String> {
    // First, while the allocator has nothing freed to hand back: RSS never
    // shrinks, so a measurement after the runs would read memory they freed.
    let overhead = measure_instance_overhead(64).await;
    let mut points = Vec::new();
    for rows in BATCH_SIZES {
        let b = ReferenceBench::new(rows, (rows as u64 * 20).clamp(20_000, 1_000_000));
        points.push(run_reference(&b).await?);
    }
    let mut r = BenchReport::new("reference", "source", "in-memory Fabric", points);
    r.instance_overhead_bytes = overhead;
    Ok(r)
}

// ---------------------------------------------------------------- harness --

/// How a harness bench runs.
#[derive(Debug, Clone, PartialEq)]
pub struct BenchPlan {
    /// Records seeded per round, one measurement each.
    pub batch_sizes: Vec<usize>,
    /// Rounds per batch size (the p99's samples).
    pub rounds: usize,
    /// The settle timeout of each round.
    pub settle: Duration,
    /// The size of one seeded record, when the harness's is known (MiB/s).
    pub record_bytes: Option<usize>,
}

impl Default for BenchPlan {
    /// 10, 1 000 and 10 000 records per round, ten rounds each.
    fn default() -> Self {
        Self {
            batch_sizes: vec![10, 1_000, 10_000],
            rounds: 10,
            settle: Duration::from_secs(300),
            record_bytes: None,
        }
    }
}

impl BenchPlan {
    /// A seconds-long plan for testing the bench itself.
    pub fn smoke() -> Self {
        Self {
            batch_sizes: vec![2, 10, 50],
            rounds: 3,
            settle: Duration::from_secs(30),
            record_bytes: None,
        }
    }
}

/// Time a connector against its real system through its conformance
/// harness. Per batch size: reset; then `rounds` times, seed that many
/// records, start the connector (from its checkpoint), wait for it to
/// settle, and stop it. A round's latency is its start-to-settle time (a
/// restart from the checkpoint plus the delivery of the round's records);
/// throughput is the records over the sum of those times. Seeding is the
/// external system's work and is not timed. Every seeded record must be read
/// back at the end, so a fast but lossy run fails instead of measuring.
pub async fn run_harness_bench(
    h: &dyn ConnectorHarness,
    plan: &BenchPlan,
) -> Result<Vec<Point>, HarnessError> {
    let mut points = Vec::new();
    for &rows in &plan.batch_sizes {
        h.reset().await?;
        let mut rounds = Vec::with_capacity(plan.rounds);
        let mut seeded = Vec::new();
        let mut peak: Option<(usize, usize)> = None;
        let mut cpu = Duration::ZERO;
        let mut cpu_known = true;
        for _ in 0..plan.rounds.max(1) {
            seeded = h.seed(rows).await?.ids;
            let c0 = process_cpu_time();
            let t0 = Instant::now();
            h.start().await?;
            h.settle(plan.settle).await?;
            rounds.push(t0.elapsed());
            match (c0, process_cpu_time()) {
                (Some(a), Some(b)) => cpu += b.saturating_sub(a),
                _ => cpu_known = false,
            }
            if let Ok(m) = h.memory().await {
                let (p, b) = peak.unwrap_or((0, m.budget_bytes));
                peak = Some((p.max(m.peak_bytes), b.max(m.budget_bytes)));
            }
            h.stop().await?;
        }
        let observed: BTreeSet<String> = h.read_back().await?.into_iter().map(|o| o.id).collect();
        let lost = seeded.iter().filter(|id| !observed.contains(*id)).count();
        if lost > 0 {
            return Err(HarnessError::failed(format!(
                "batch {rows}: {lost} of {} seeded records were not delivered",
                seeded.len()
            )));
        }
        let events = seeded.len() as u64;
        let busy = rounds.iter().sum::<Duration>().as_secs_f64().max(1e-9);
        points.push(Point {
            batch_rows: rows,
            events,
            events_per_sec: events as f64 / busy,
            mib_per_sec: plan
                .record_bytes
                .map(|b| (events as f64 * b as f64) / (1 << 20) as f64 / busy),
            p99_ms: p99_ms(rounds),
            peak_memory_bytes: peak.map(|(p, _)| p as u64),
            memory_budget_bytes: peak.map(|(_, b)| b as u64),
            cpu_ms_per_10k: cpu_known
                .then(|| cpu.as_secs_f64() * 1_000.0 * 10_000.0 / events.max(1) as f64),
        });
    }
    h.reset().await?;
    Ok(points)
}

/// Group reports by key (several runs of each, e.g. one directory per
/// scheduled run) for [`calibrate`].
pub fn group_runs(reports: Vec<BenchReport>) -> BTreeMap<String, Vec<BenchReport>> {
    let mut m: BTreeMap<String, Vec<BenchReport>> = BTreeMap::new();
    for r in reports {
        m.entry(r.key()).or_default().push(r);
    }
    m
}
