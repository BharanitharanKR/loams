//! `loams-connectors-bench`: the scheduled `connectors-bench` job's tool
//! (CN2 plan Task 27).
//!
//! ```text
//! loams-connectors-bench reference --out DIR
//!     Run the reference pipeline at the three batch sizes, and the reference
//!     harness through the harness bench, and write their reports.
//! loams-connectors-bench check --reports DIR [--baselines FILE]
//!     Compare every report with its baseline; exit 1 if any run is more than
//!     the regression budget worse, misses a batch size, broke its memory
//!     budget, ran on another runner class, or has no baseline entry.
//! loams-connectors-bench calibrate --runs DIR [--runs DIR …] [--recorded-on DATE]
//!     Print calibrated `[[connector]]` entries (the median of the runs, one
//!     directory of reports per scheduled run) for the baselines file.
//! loams-connectors-bench budgets [--baselines FILE]
//!     Print the `budgets` block `deploy/helm/loams-connectors/values.yaml`
//!     must hold.
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use loams_connector_sdk::PipelineConfig;
use loams_flow_conformance::bench::{
    self, Baselines, BenchPlan, BenchReport, calibrate, check_all, derive_chart_budgets,
    group_runs, read_reports, run_harness_bench,
};
use loams_flow_conformance::reference::ReferenceHarness;

fn usage() -> ExitCode {
    eprintln!(
        "usage: loams-connectors-bench reference --out DIR\n\
         \x20      loams-connectors-bench check --reports DIR [--baselines FILE]\n\
         \x20      loams-connectors-bench calibrate --runs DIR [--runs DIR ...] [--recorded-on DATE]\n\
         \x20      loams-connectors-bench budgets [--baselines FILE]"
    );
    ExitCode::from(2)
}

struct Args {
    cmd: String,
    out: Option<PathBuf>,
    reports: Option<PathBuf>,
    runs: Vec<PathBuf>,
    baselines: Option<PathBuf>,
    recorded_on: Option<String>,
}

fn parse() -> Option<Args> {
    let mut it = std::env::args().skip(1);
    let mut a = Args {
        cmd: it.next()?,
        out: None,
        reports: None,
        runs: Vec::new(),
        baselines: None,
        recorded_on: None,
    };
    while let Some(flag) = it.next() {
        let v = it.next()?;
        match flag.as_str() {
            "--out" => a.out = Some(v.into()),
            "--reports" => a.reports = Some(v.into()),
            "--runs" => a.runs.push(v.into()),
            "--baselines" => a.baselines = Some(v.into()),
            "--recorded-on" => a.recorded_on = Some(v),
            _ => return None,
        }
    }
    Some(a)
}

fn baselines(a: &Args) -> Result<Baselines, String> {
    match &a.baselines {
        Some(p) => Baselines::load(p),
        None => Baselines::repository(),
    }
}

fn write(dir: &std::path::Path, r: &BenchReport) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let path = dir.join(format!("{}.{}.json", r.connector, r.direction));
    let json = serde_json::to_string_pretty(r).map_err(|e| e.to_string())?;
    std::fs::write(&path, json).map_err(|e| e.to_string())?;
    println!("wrote {}", path.display());
    Ok(())
}

async fn reference(a: &Args) -> Result<(), String> {
    let out = a.out.as_deref().ok_or("--out is required")?;
    if bench::build_profile() != "release" {
        eprintln!("warning: a debug build; baselines are release builds");
    }
    let source = bench::reference_report().await?;
    write(out, &source)?;

    let h = ReferenceHarness::new(true);
    let points = run_harness_bench(&h, &BenchPlan::default())
        .await
        .map_err(|e| e.to_string())?;
    let roundtrip = BenchReport::new(
        "reference",
        "roundtrip",
        "in-memory Fabric, transactional map sink",
        points,
    );
    write(out, &roundtrip)
}

fn check(a: &Args) -> Result<bool, String> {
    let dir = a.reports.as_deref().ok_or("--reports is required")?;
    let b = baselines(a)?;
    let reports = read_reports(dir)?;
    if reports.is_empty() {
        return Err(format!("no reports in {}", dir.display()));
    }
    let verdicts = check_all(&b, &reports);
    let mut ok = true;
    for v in &verdicts {
        print!("{v}");
        ok &= v.passed();
    }
    if let Some(summary) = std::env::var_os("GITHUB_STEP_SUMMARY") {
        let mut s = String::from("### Connector performance budgets\n\n```\n");
        for v in &verdicts {
            s.push_str(&v.to_string());
        }
        s.push_str("```\n");
        let _ = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(summary)
            .and_then(|mut f| std::io::Write::write_all(&mut f, s.as_bytes()));
    }
    Ok(ok)
}

fn calibrate_cmd(a: &Args) -> Result<(), String> {
    if a.runs.is_empty() {
        return Err("--runs is required".into());
    }
    let b = baselines(a)?;
    let mut all = Vec::new();
    for d in &a.runs {
        all.extend(read_reports(d)?);
    }
    let date = a.recorded_on.clone().unwrap_or_else(|| "YYYY-MM-DD".into());
    for (key, runs) in group_runs(all) {
        let c = calibrate(&runs, b.budget.calibration_runs)?;
        let Some(e) = b.entry(&c.connector, &c.direction) else {
            return Err(format!("{key}: no entry in the baselines file"));
        };
        let bench = e.bench.clone().unwrap_or_default();
        println!("{}", c.to_toml(&e.family, &bench, &date));
    }
    Ok(())
}

fn main() -> ExitCode {
    let Some(a) = parse() else {
        return usage();
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let r = match a.cmd.as_str() {
        "reference" => rt.block_on(reference(&a)).map(|()| true),
        "check" => check(&a),
        "calibrate" => calibrate_cmd(&a).map(|()| true),
        "budgets" => baselines(&a).map(|b| {
            print!(
                "{}",
                derive_chart_budgets(&PipelineConfig::default(), &b).to_values_yaml()
            );
            true
        }),
        _ => return usage(),
    };
    match r {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
