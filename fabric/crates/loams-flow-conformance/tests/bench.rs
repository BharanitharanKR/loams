//! Performance budgets and runtime upkeep (CN2 Task 27): the regression
//! budget, the baselines and their calibration, the chart's budgets, the
//! reference pipeline's floor budgets, and the upkeep scripts.
// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::process::Command;
use std::time::Duration;

use loams_connector_sdk::PipelineConfig;
use loams_flow::manifest::repository_root;
use loams_flow_conformance::bench::{
    BaselineStatus, Baselines, BenchPlan, BenchReport, ChartBudgets, Metric, Point, ReferenceBench,
    calibrate, check, derive_chart_budgets, reference_floor, run_harness_bench, run_reference,
};
use loams_flow_conformance::reference::ReferenceHarness;

fn point(batch_rows: usize, eps: f64, p99: f64, mem: u64, cpu: f64) -> Point {
    Point {
        batch_rows,
        events: 10_000,
        events_per_sec: eps,
        mib_per_sec: Some(eps / 1_000.0),
        p99_ms: Some(p99),
        peak_memory_bytes: Some(mem),
        memory_budget_bytes: Some(64 << 20),
        cpu_ms_per_10k: Some(cpu),
    }
}

fn report(points: Vec<Point>) -> BenchReport {
    BenchReport {
        connector: "reference".into(),
        direction: "source".into(),
        system: "in-memory Fabric".into(),
        runner: "ubuntu-24.04".into(),
        profile: "release".into(),
        instance_overhead_bytes: Some(4 << 20),
        points,
    }
}

const BASELINES: &str = r#"
[budget]
regression = 0.15
calibration_runs = 5

[[connector]]
id = "reference"
direction = "source"
family = "sdk"
status = "recorded"
bench = "fabric/crates/loams-flow-conformance/src/bin/loams-connectors-bench.rs::reference"
runner = "ubuntu-24.04"
profile = "release"
recorded_on = "2026-10-13"
calibration_runs = 5
instance_overhead_bytes = 4194304

[[connector.point]]
batch_rows = 1
events = 10000
events_per_sec = 100000.0
mib_per_sec = 100.0
p99_ms = 10.0
peak_memory_bytes = 1000000
memory_budget_bytes = 67108864
cpu_ms_per_10k = 50.0

[[connector.point]]
batch_rows = 100
events = 10000
events_per_sec = 100000.0
mib_per_sec = 100.0
p99_ms = 10.0
peak_memory_bytes = 1000000
memory_budget_bytes = 67108864
cpu_ms_per_10k = 50.0

[[connector]]
id = "opensearch"
direction = "roundtrip"
family = "search"
status = "uncalibrated"
bench = "fabric/crates/loams-connector-search/tests/bench.rs::bench_opensearch"

[[connector]]
id = "valkey"
direction = "source"
family = "kv"
status = "pending"
owner = "CN2 Task 14"
"#;

/// Plan test: a throughput drop, or a rise in p99, memory or CPU, of more
/// than 15 % against the recorded baseline fails the scheduled job.
#[test]
fn bench_regression_budget_enforced() {
    let baselines = Baselines::parse(BASELINES).unwrap();
    assert_eq!(baselines.budget.regression, 0.15);
    let entry = baselines.entry("reference", "source").unwrap();
    assert_eq!(entry.status, BaselineStatus::Recorded);

    // Within budget: 14 % slower, 14 % more p99, memory and CPU.
    let ok = report(vec![
        point(1, 86_000.0, 11.4, 1_140_000, 57.0),
        point(100, 120_000.0, 8.0, 900_000, 40.0),
    ]);
    let v = check(entry, &ok, baselines.budget.regression);
    assert!(v.calibrated);
    assert!(v.passed(), "{v}");

    // 16 % slower at batch size 1 fails, on throughput only.
    let mut slow = report(vec![
        point(1, 84_000.0, 10.0, 1_000_000, 50.0),
        point(100, 100_000.0, 10.0, 1_000_000, 50.0),
    ]);
    slow.points[0].mib_per_sec = Some(100.0);
    let v = check(entry, &slow, 0.15);
    assert!(!v.passed());
    assert_eq!(v.regressions.len(), 1, "{v}");
    assert_eq!(v.regressions[0].metric, Metric::EventsPerSec);
    assert_eq!(v.regressions[0].batch_rows, 1);
    assert!((v.regressions[0].change - 0.16).abs() < 1e-9);

    // Each higher-is-worse metric is enforced on its own.
    for (bad, metric) in [
        (point(100, 100_000.0, 11.6, 1_000_000, 50.0), Metric::P99),
        (
            point(100, 100_000.0, 10.0, 1_160_000, 50.0),
            Metric::PeakMemory,
        ),
        (
            point(100, 100_000.0, 10.0, 1_000_000, 58.0),
            Metric::CpuPer10k,
        ),
    ] {
        let r = report(vec![point(1, 100_000.0, 10.0, 1_000_000, 50.0), bad]);
        let v = check(entry, &r, 0.15);
        let metrics: Vec<Metric> = v.regressions.iter().map(|r| r.metric).collect();
        assert_eq!(metrics, vec![metric], "{v}");
    }
    // MiB/s too.
    let mut r = report(vec![
        point(1, 100_000.0, 10.0, 1_000_000, 50.0),
        point(100, 100_000.0, 10.0, 1_000_000, 50.0),
    ]);
    r.points[1].mib_per_sec = Some(80.0);
    let v = check(entry, &r, 0.15);
    assert_eq!(v.regressions[0].metric, Metric::MibPerSec, "{v}");

    // A batch size the baseline records but the run did not measure fails:
    // a bench that silently stops measuring proves nothing.
    let partial = report(vec![point(1, 100_000.0, 10.0, 1_000_000, 50.0)]);
    let v = check(entry, &partial, 0.15);
    assert!(!v.passed());
    assert_eq!(v.missing, vec![100]);

    // A run on another runner class or build profile is not comparable.
    let mut other = ok.clone();
    other.runner = "a-laptop".into();
    let v = check(entry, &other, 0.15);
    assert!(!v.passed());
    assert!(v.refused.as_deref().unwrap().contains("runner"), "{v}");
    let mut debug = ok.clone();
    debug.profile = "debug".into();
    assert!(!check(entry, &debug, 0.15).passed());

    // The memory budget itself is a hard bound, not a 15 % budget.
    let mut over = ok.clone();
    over.points[0].peak_memory_bytes = Some((64 << 20) + 1);
    let v = check(entry, &over, 0.15);
    assert!(!v.passed());
    assert!(
        v.regressions
            .iter()
            .any(|r| r.metric == Metric::MemoryBudget),
        "{v}"
    );

    // An uncalibrated entry never fails a run: the run is a calibration
    // candidate.
    let unc = baselines.entry("opensearch", "roundtrip").unwrap();
    let mut os = ok.clone();
    os.connector = "opensearch".into();
    os.direction = "roundtrip".into();
    let v = check(unc, &os, 0.15);
    assert!(!v.calibrated);
    assert!(v.passed(), "{v}");

    // Reports survive their JSON form.
    let json = serde_json::to_string(&ok).unwrap();
    assert_eq!(serde_json::from_str::<BenchReport>(&json).unwrap(), ok);

    // A pending entry names its owner; a recorded one has points.
    assert_eq!(
        baselines
            .entry("valkey", "source")
            .unwrap()
            .owner
            .as_deref(),
        Some("CN2 Task 14")
    );
    assert!(
        Baselines::parse(
            "[budget]\nregression = 0.15\ncalibration_runs = 5\n[[connector]]\nid = \"x\"\n\
             direction = \"source\"\nfamily = \"sdk\"\nstatus = \"recorded\"\n"
        )
        .is_err(),
        "a recorded entry without points is refused"
    );
}

/// Calibration takes the median of at least `calibration_runs` runs on one
/// runner class, so one noisy run moves nothing.
#[test]
fn calibration_takes_the_median_of_enough_runs() {
    let runs: Vec<BenchReport> = [90.0, 100.0, 1_000.0, 95.0, 105.0]
        .iter()
        .map(|eps| report(vec![point(1, *eps, 1_000.0 / eps, 1_000, 10.0)]))
        .collect();
    let c = calibrate(&runs, 5).unwrap();
    assert_eq!(c.points.len(), 1);
    assert_eq!(c.points[0].events_per_sec, 100.0);
    assert_eq!(c.points[0].p99_ms, Some(10.0));
    assert_eq!(c.runner, "ubuntu-24.04");
    assert_eq!(c.instance_overhead_bytes, Some(4 << 20));

    assert!(calibrate(&runs[..4], 5).is_err(), "too few runs");
    let mut mixed = runs.clone();
    mixed[2].runner = "a-laptop".into();
    assert!(calibrate(&mixed, 5).is_err(), "runner classes mixed");
    let mut sizes = runs;
    sizes[0].points[0].batch_rows = 2;
    assert!(calibrate(&sizes, 5).is_err(), "batch sizes differ");
}

/// Plan test: the chart's budgets are what the measurements (or, before
/// calibration, the conservative defaults) derive.
#[test]
fn budget_values_match_measurements() {
    let baselines = Baselines::repository().unwrap();
    let derived = derive_chart_budgets(&PipelineConfig::default(), &baselines);
    let values =
        std::fs::read_to_string(repository_root().join("deploy/helm/loams-connectors/values.yaml"))
            .unwrap();
    let chart = ChartBudgets::from_values_yaml(&values).unwrap();
    assert_eq!(
        chart, derived,
        "regenerate the budgets block of values.yaml"
    );

    // The default memory budget is the SDK's, and the pod holds as many
    // instances as fit after the process's own base.
    assert_eq!(chart.memory_budget_bytes, 64 << 20);
    assert_eq!(
        chart.instances_per_pod,
        (chart.pod_memory_limit_bytes - chart.process_base_bytes)
            / (chart.memory_budget_bytes + chart.instance_overhead_bytes)
    );
    assert!(chart.instances_per_pod >= 1);
    assert_eq!(chart.pod_memory_request_bytes, chart.pod_memory_limit_bytes);
}

/// The per-instance overhead follows the largest recorded measurement, and
/// falls back to the conservative default until one is recorded.
#[test]
fn chart_budgets_follow_recorded_overhead() {
    let cfg = PipelineConfig::default();
    let none = Baselines::parse("[budget]\nregression = 0.15\ncalibration_runs = 5\n").unwrap();
    let d = derive_chart_budgets(&cfg, &none);
    assert!(!d.calibrated);
    assert_eq!(
        d.instance_overhead_bytes,
        loams_flow_conformance::bench::CONSERVATIVE_INSTANCE_OVERHEAD_BYTES
    );

    let mut recorded = Baselines::parse(BASELINES).unwrap();
    let d4 = derive_chart_budgets(&cfg, &recorded);
    assert!(d4.calibrated);
    // 4 MiB measured, rounded up to the next MiB with the 25 % headroom.
    assert_eq!(d4.instance_overhead_bytes, 5 << 20);
    recorded.connector[0].instance_overhead_bytes = Some(100 << 20);
    let d100 = derive_chart_budgets(&cfg, &recorded);
    assert_eq!(d100.instance_overhead_bytes, 125 << 20);
    assert!(d100.instances_per_pod < d4.instances_per_pod);

    // A smaller per-instance memory budget packs more instances.
    let small = PipelineConfig {
        memory_budget_bytes: 16 << 20,
        ..cfg
    };
    assert!(derive_chart_budgets(&small, &none).instances_per_pod > d.instances_per_pod);
}

/// The reference pipeline holds its memory budget at every batch size, with
/// a slow Fabric behind a fast source (Review Focus 5).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reference_pipeline_memory_bounded_at_each_batch_size() {
    for batch_rows in [1, 100, 10_000] {
        let mut b = ReferenceBench::new(batch_rows, (batch_rows as u64 * 3).max(200));
        b.config.max_batch_bytes = 64 << 10;
        b.config.memory_budget_bytes = 160 << 10;
        b.config.max_in_flight_batches = 3;
        b.fabric_delay = Some(Duration::from_millis(2));
        let p = run_reference(&b).await.unwrap();
        let peak = p.peak_memory_bytes.unwrap();
        assert!(peak > 0, "{p:?}");
        assert!(peak <= 160 << 10, "batch {batch_rows}: {p:?}");
        assert_eq!(p.memory_budget_bytes, Some(160 << 10));
        assert_eq!(p.events, b.events * b.splits as u64);
    }
}

/// The reference pipeline's floor budgets: derived from its configuration,
/// not measured on this machine. They catch a stalled pipeline (a linger or
/// a lost budget lease per batch), not a slow one; the 15 % regression
/// budget against calibrated baselines does that, in `connectors-bench`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reference_pipeline_meets_floor_budgets() {
    for batch_rows in [1, 100, 10_000] {
        let b = ReferenceBench::new(batch_rows, (batch_rows as u64 * 2).clamp(500, 20_000));
        let floor = reference_floor(batch_rows, &b.config);
        let p = run_reference(&b).await.unwrap();
        assert_eq!(p.batch_rows, batch_rows);
        assert!(
            p.events_per_sec >= floor.min_events_per_sec,
            "batch {batch_rows}: {p:?} under the floor {floor:?}"
        );
        let p99 = p.p99_ms.unwrap();
        assert!(
            p99 <= floor.max_p99.as_secs_f64() * 1_000.0,
            "batch {batch_rows}: {p:?} over the ceiling {floor:?}"
        );
        assert!(p.peak_memory_bytes.unwrap() <= b.config.memory_budget_bytes as u64);
        assert!(p.mib_per_sec.unwrap() > 0.0);
    }
    // The floor is far below a working pipeline and far above a stalled one:
    // one batch per linger would be 5 events/s at batch size 1.
    let cfg = PipelineConfig::default();
    let f = reference_floor(1, &cfg);
    assert!(f.min_events_per_sec > 1.0 / cfg.linger.as_secs_f64() * 10.0);
    assert!(f.max_p99 >= cfg.linger);
}

/// The harness bench drives any `ConnectorHarness`: the same code times a
/// family's harness against its real system in `connectors-bench`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn harness_bench_drives_any_harness() {
    let h = ReferenceHarness::new(false);
    let plan = BenchPlan::smoke();
    let points = run_harness_bench(&h, &plan).await.unwrap();
    assert_eq!(
        points.iter().map(|p| p.batch_rows).collect::<Vec<_>>(),
        plan.batch_sizes
    );
    for p in &points {
        assert_eq!(p.events, (p.batch_rows * plan.rounds) as u64);
        assert!(p.events_per_sec > 0.0, "{p:?}");
        assert!(p.p99_ms.is_some(), "{p:?}");
        let (peak, budget) = (p.peak_memory_bytes.unwrap(), p.memory_budget_bytes.unwrap());
        assert!(peak <= budget, "{p:?}");
    }
}

/// Every connector with a harness has a bench baseline: recorded,
/// uncalibrated (its bench is written and runs in `connectors-bench`), or
/// pending with an owning task. Every bench a baseline names exists.
#[test]
fn every_harness_has_a_bench_baseline() {
    let baselines = Baselines::repository().unwrap();
    let systems: toml::Table = toml::from_str(
        &std::fs::read_to_string(repository_root().join("conformance/connectors/systems.toml"))
            .unwrap(),
    )
    .unwrap();
    let systems = systems["systems"].as_table().unwrap();
    let with_harness: BTreeSet<&str> = systems
        .iter()
        .filter(|(_, e)| e.get("harness").is_some())
        .map(|(id, _)| id.as_str())
        .collect();
    let covered: BTreeSet<&str> = baselines.connector.iter().map(|e| e.id.as_str()).collect();
    let missing: Vec<&&str> = with_harness.difference(&covered).collect();
    assert!(missing.is_empty(), "no bench baseline for {missing:?}");

    for e in &baselines.connector {
        assert!(
            e.id == "reference" || systems.contains_key(&e.id),
            "{}: not a systems.toml entry",
            e.id
        );
        match e.status {
            BaselineStatus::Pending => assert!(
                e.owner
                    .as_deref()
                    .is_some_and(|o| o.starts_with("CN2 Task")),
                "{}: a pending bench names its owning task",
                e.id
            ),
            BaselineStatus::Uncalibrated | BaselineStatus::Recorded => {
                let bench = e
                    .bench
                    .as_deref()
                    .unwrap_or_else(|| panic!("{}: a written bench names its test", e.id));
                let (file, func) = bench.split_once("::").unwrap();
                let src = std::fs::read_to_string(repository_root().join(file))
                    .unwrap_or_else(|_| panic!("{}: {file} is missing", e.id));
                assert!(
                    src.contains(&format!("fn {func}")),
                    "{}: {func} is not in {file}",
                    e.id
                );
            }
        }
    }
}

fn python(args: &[&str]) -> std::process::Output {
    Command::new("python3")
        .args(args)
        .current_dir(repository_root())
        .output()
        .expect("python3 runs")
}

/// Plan test: the Camel bump procedure (fallbacks only) plans every step
/// and is green as a dry run; steps whose inputs Task 22 has not built are
/// reported as deferred, never silently skipped.
#[test]
fn camel_bump_dry_run_green() {
    let out = python(&[
        "scripts/connectors/camel_bump.py",
        "--to",
        "4.22.1",
        "--dry-run",
        "--json",
        "-",
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let plan: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(plan["from"], "4.22.1");
    assert_eq!(plan["to"], "4.22.1");
    let steps: Vec<&str> = plan["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["step"].as_str().unwrap())
        .collect();
    assert_eq!(
        steps,
        [
            "pins",
            "catalog-snapshot",
            "catalog-drift",
            "directions-match-catalog",
            "validate-fallback-routes",
            "fallback-suites",
            "camel-iggy-recheck",
        ]
    );
    for s in plan["steps"].as_array().unwrap() {
        let status = s["status"].as_str().unwrap();
        assert!(
            ["ready", "deferred", "manual"].contains(&status),
            "{s}: a dry run plans, it does not fail"
        );
        if status == "deferred" {
            assert!(s["owner"].as_str().unwrap().starts_with("CN"), "{s}");
        }
    }
    // The fallback suites step names every manifest with a Camel fallback.
    let suites = plan["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["step"] == "fallback-suites")
        .unwrap();
    let manifests: Vec<&str> = suites["manifests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m.as_str().unwrap())
        .collect();
    assert!(manifests.contains(&"activemq-jms"), "{manifests:?}");

    // A malformed version is refused before anything runs.
    let bad = python(&[
        "scripts/connectors/camel_bump.py",
        "--to",
        "4.x",
        "--dry-run",
    ]);
    assert_eq!(bad.status.code(), Some(2));
}

/// The crate upkeep script's own fixture test: findings are grouped per
/// family crate, advisories reach every family whose graph holds the crate,
/// and a release younger than 14 days is held back.
#[test]
fn crate_upkeep_self_test_green() {
    let out = python(&["scripts/connectors/crate_upkeep.py", "--self-test"]);
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `connectors-bench` and `crate-upkeep` run on their own schedules (and on
/// demand); `camel-bump` runs only by hand, with a version.
#[test]
fn upkeep_jobs_are_scheduled_or_manual() {
    let wf =
        std::fs::read_to_string(repository_root().join(".github/workflows/fabric.yml")).unwrap();
    let doc: serde_norway::Value = serde_norway::from_str(&wf).unwrap();
    let jobs = &doc["jobs"];
    let cond = |job: &str| {
        jobs[job]["if"]
            .as_str()
            .unwrap_or_else(|| panic!("{job} has an if"))
            .to_owned()
    };
    for (job, cron) in [
        ("connectors-bench", "5 5 * * 0"),
        ("crate-upkeep", "29 5 * * 1"),
    ] {
        let c = cond(job);
        assert!(c.contains(&format!("'{cron}'")), "{job}: {c}");
        assert!(
            wf.contains(&format!("- cron: \"{cron}\"")),
            "{job}: no {cron} cron"
        );
        assert!(c.contains("workflow_dispatch"), "{job}: {c}");
    }
    let c = cond("camel-bump");
    assert!(
        c.contains("workflow_dispatch") && c.contains("inputs.camel_version"),
        "{c}"
    );
    assert!(!c.contains("schedule"), "{c}");
    // The bench job enforces the budget: it ends in the check.
    let steps = format!("{:?}", jobs["connectors-bench"]["steps"]);
    assert!(steps.contains("loams-connectors-bench"), "{steps}");
    assert!(steps.contains("check"), "{steps}");
    // Crate upkeep may open issues, and nothing else.
    let perms = &jobs["crate-upkeep"]["permissions"];
    assert_eq!(perms["issues"].as_str(), Some("write"));
    assert_eq!(perms["contents"].as_str(), Some("read"));
}
