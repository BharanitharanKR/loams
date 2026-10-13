//! The suite runner: [`run`], [`run_with`], and one module per [`Suite`].

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use loams_connector_sdk::Profile;
use loams_flow::ConnectorSpec;

use crate::services::{ComposeEstate, Estate, EstateError, ServiceKind, SystemsCatalog};
use crate::{
    ConnectorHarness, HarnessError, Observed, Seeded, Suite, SuiteOutcome, SuiteReport,
    SuiteResult, check_services,
};

mod auth;
mod backpressure;
mod bulk;
mod cdc;
mod contract;
mod dup;
mod exactly_once;
mod kill;
mod roundtrip;
mod schema;

/// The estate a run sees: the catalog, the compose services, the checkout's
/// root (fixture directories are relative to it) and the `LOAMS_*`
/// environment that says which stacks and accounts are up.
#[derive(Debug, Clone)]
pub struct RunEnv {
    pub root: PathBuf,
    pub catalog: SystemsCatalog,
    pub compose: ComposeEstate,
    pub vars: BTreeMap<String, String>,
}

impl RunEnv {
    /// This checkout's estate and the process environment.
    pub fn from_process() -> Result<Self, EstateError> {
        let estate = Estate::load_default()?;
        let vars = std::env::vars()
            .filter(|(k, v)| k.starts_with("LOAMS_") && !v.is_empty())
            .collect();
        Ok(Self::from_estate(estate, vars))
    }

    pub fn from_estate(estate: Estate, vars: BTreeMap<String, String>) -> Self {
        Self {
            root: estate.root,
            catalog: estate.catalog,
            compose: estate.compose,
            vars,
        }
    }

    fn set(&self, var: &str) -> bool {
        self.vars.get(var).is_some_and(|v| !v.is_empty())
    }

    /// Whether `s` can be reached in this run, and if not, why.
    pub fn available(&self, s: &ServiceKind) -> Result<(), String> {
        match s {
            ServiceKind::Container {
                compose_service, ..
            }
            | ServiceKind::Emulator {
                compose_service, ..
            } => {
                let stacks = self.compose.stacks_of(compose_service);
                if stacks.is_empty() {
                    return Err(format!(
                        "compose service {compose_service} is not in {}",
                        ComposeEstate::PATH
                    ));
                }
                if stacks.iter().any(|st| self.set(st.env_var())) {
                    Ok(())
                } else {
                    Err(format!(
                        "{compose_service} runs in {}; set {} once it is up",
                        stacks
                            .iter()
                            .map(|s| s.profile())
                            .collect::<Vec<_>>()
                            .join(" or "),
                        stacks
                            .iter()
                            .map(|s| s.env_var())
                            .collect::<Vec<_>>()
                            .join(" or ")
                    ))
                }
            }
            ServiceKind::Fixture { dir, .. } => {
                let path = self.root.join(dir);
                let recorded = std::fs::read_dir(&path).is_ok_and(|entries| {
                    entries
                        .filter_map(Result::ok)
                        .any(|e| e.path().extension().is_some_and(|x| x == "json"))
                });
                if recorded {
                    Ok(())
                } else {
                    Err(format!("no recording under {dir}"))
                }
            }
            ServiceKind::Nightly { env, .. } => {
                let missing: Vec<&str> = env.iter().copied().filter(|v| !self.set(v)).collect();
                if missing.is_empty() {
                    Ok(())
                } else {
                    Err(format!(
                        "nightly account not configured: {}",
                        missing.join(", ")
                    ))
                }
            }
        }
    }
}

/// Sizes and time limits.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Records per suite.
    pub records: usize,
    /// Records when `contract` runs another suite's check in small.
    pub smoke_records: usize,
    /// Rows for `bulk` (§33 §4 rule 3: 1 M).
    pub bulk_rows: u64,
    pub smoke_bulk_rows: u64,
    /// How long the connector may take to deliver everything.
    pub settle: Duration,
    /// How long an armed fault may take to trip.
    pub fault: Duration,
    /// The delay the backpressure suite applies to the target and the
    /// Fabric.
    pub throttle: Duration,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            records: 200,
            smoke_records: 20,
            bulk_rows: 1_000_000,
            smoke_bulk_rows: 10_000,
            settle: Duration::from_secs(120),
            fault: Duration::from_secs(30),
            throttle: Duration::from_millis(5),
        }
    }
}

impl RunOptions {
    fn smoke(&self) -> RunOptions {
        RunOptions {
            records: self.smoke_records,
            bulk_rows: self.smoke_bulk_rows,
            ..self.clone()
        }
    }
}

/// Why a suite did not pass.
#[derive(Debug)]
pub(crate) enum Fail {
    Failed(String),
    Skipped(String),
}

impl From<HarnessError> for Fail {
    fn from(e: HarnessError) -> Self {
        match e {
            HarnessError::Unsupported(m) => Fail::Skipped(format!("unsupported: {m}")),
            HarnessError::Unavailable(m) => Fail::Skipped(format!("service unavailable: {m}")),
            HarnessError::Failed(m) => Fail::Failed(m),
        }
    }
}

pub(crate) type Outcome = Result<Vec<String>, Fail>;

/// What every suite body is given.
pub(crate) struct Ctx<'a> {
    pub spec: &'a ConnectorSpec,
    pub profile: Option<&'a Profile>,
    pub h: &'a dyn ConnectorHarness,
    pub opts: &'a RunOptions,
    /// The suites this run executes on their own (contract does not repeat
    /// them in small).
    pub requested: &'a BTreeSet<Suite>,
}

impl Ctx<'_> {
    /// The delivery slug of the direction under test: the sink's when the
    /// connector has one, else the source's.
    pub fn delivery(&self) -> String {
        let doc = self.spec.to_json().unwrap_or_default();
        let caps = &doc["capabilities"];
        let dir = if caps.get("sink").is_some_and(|s| s.is_object()) {
            "sink"
        } else {
            "source"
        };
        caps["delivery"][dir]
            .as_str()
            .unwrap_or("at_least_once")
            .to_owned()
    }

    /// Whether the declared delivery allows no duplicate in the target.
    pub fn deduplicates(&self) -> bool {
        matches!(
            self.delivery().as_str(),
            "exactly_once" | "effectively_once"
        )
    }
}

/// Every seeded id is in the target, and nothing else is.
pub(crate) fn no_loss(seeded: &Seeded, observed: &[Observed]) -> Result<(), Fail> {
    let want: BTreeSet<&str> = seeded.ids.iter().map(String::as_str).collect();
    let got: BTreeSet<&str> = observed.iter().map(|o| o.id.as_str()).collect();
    let lost: Vec<&&str> = want.difference(&got).take(5).collect();
    let extra: Vec<&&str> = got.difference(&want).take(5).collect();
    if !lost.is_empty() {
        return Err(Fail::Failed(format!(
            "lost {} of {} records (first: {lost:?})",
            want.difference(&got).count(),
            want.len()
        )));
    }
    if !extra.is_empty() {
        return Err(Fail::Failed(format!(
            "the target holds records nobody seeded (first: {extra:?})"
        )));
    }
    Ok(())
}

/// No record has more than `max` copies.
pub(crate) fn dup_bound(observed: &[Observed], max: u32) -> Result<(), Fail> {
    let over: Vec<&Observed> = observed.iter().filter(|o| o.copies > max).collect();
    if over.is_empty() {
        Ok(())
    } else {
        Err(Fail::Failed(format!(
            "{} records exceed {max} cop{} (first: {} with {})",
            over.len(),
            if max == 1 { "y" } else { "ies" },
            over[0].id,
            over[0].copies
        )))
    }
}

/// Run `suites` against the real system behind `h`, in this checkout's
/// estate and the process environment. See [`run_with`].
pub async fn run(
    spec: &ConnectorSpec,
    profile: Option<&Profile>,
    h: &dyn ConnectorHarness,
    suites: &[Suite],
) -> SuiteReport {
    match RunEnv::from_process() {
        Ok(env) => run_with(&env, &RunOptions::default(), spec, profile, h, suites).await,
        Err(e) => all(spec, None, suites, |_| SuiteOutcome::Failed(e.to_string())),
    }
}

fn all(
    spec: &ConnectorSpec,
    system: Option<String>,
    suites: &[Suite],
    f: impl Fn(Suite) -> SuiteOutcome,
) -> SuiteReport {
    SuiteReport {
        connector: spec.id.clone(),
        system,
        results: dedup(suites)
            .into_iter()
            .map(|suite| SuiteResult {
                suite,
                outcome: f(suite),
                notes: Vec::new(),
            })
            .collect(),
    }
}

fn dedup(suites: &[Suite]) -> Vec<Suite> {
    let mut seen = BTreeSet::new();
    suites.iter().copied().filter(|s| seen.insert(*s)).collect()
}

/// Run `suites`. First the OR-2 check ([`check_services`]): a harness whose
/// services are not the manifest's real system fails every suite. Then the
/// services reachable in `env`: with none, every suite is skipped (and a
/// skipped suite is never green). Then each suite, after a reset.
pub async fn run_with(
    env: &RunEnv,
    opts: &RunOptions,
    spec: &ConnectorSpec,
    profile: Option<&Profile>,
    h: &dyn ConnectorHarness,
    suites: &[Suite],
) -> SuiteReport {
    let system = match check_services(&spec.id, profile, h.services(), &env.catalog) {
        Ok(system) => system,
        Err(problems) => {
            let msg = problems.join("; ");
            return all(spec, None, suites, |_| SuiteOutcome::Failed(msg.clone()));
        }
    };
    let mut unavailable = Vec::new();
    let up: Vec<&ServiceKind> = h
        .services()
        .iter()
        .filter(|s| match env.available(s) {
            Ok(()) => true,
            Err(why) => {
                unavailable.push(why);
                false
            }
        })
        .collect();
    if up.is_empty() {
        let msg = format!("no service is up: {}", unavailable.join("; "));
        return all(spec, Some(system), suites, |_| {
            SuiteOutcome::Skipped(msg.clone())
        });
    }
    let requested: BTreeSet<Suite> = suites.iter().copied().collect();
    let ctx = Ctx {
        spec,
        profile,
        h,
        opts,
        requested: &requested,
    };
    let mut results = Vec::new();
    for suite in dedup(suites) {
        let mut r = run_suite(&ctx, suite).await;
        r.notes.insert(0, format!("services: {}", describe(&up)));
        results.push(r);
    }
    SuiteReport {
        connector: spec.id.clone(),
        system: Some(system),
        results,
    }
}

fn describe(up: &[&ServiceKind]) -> String {
    up.iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Reset, run one suite's body, stop.
pub(crate) async fn run_suite(ctx: &Ctx<'_>, suite: Suite) -> SuiteResult {
    let outcome = match ctx.h.reset().await {
        Err(e) => Err(Fail::from(e)),
        Ok(()) => match suite {
            Suite::Contract => contract::run(ctx).await,
            Suite::Roundtrip => roundtrip::run(ctx).await,
            Suite::KillRestart => kill::run(ctx).await,
            Suite::DupCheck => dup::run(ctx).await,
            Suite::Bulk => bulk::run(ctx).await,
            Suite::Cdc => cdc::run(ctx).await,
            Suite::SchemaEvolution => schema::run(ctx).await,
            Suite::Backpressure => backpressure::run(ctx).await,
            Suite::ExactlyOnce => exactly_once::run(ctx).await,
            Suite::Auth => auth::run(ctx).await,
        },
    };
    // Best effort: a suite that failed mid-way may leave the connector up.
    let _ = ctx.h.kill(crate::KillTarget::Task).await;
    let (outcome, notes) = match outcome {
        Ok(notes) => (SuiteOutcome::Passed, notes),
        Err(Fail::Failed(m)) => (SuiteOutcome::Failed(m), Vec::new()),
        Err(Fail::Skipped(m)) => (SuiteOutcome::Skipped(m), Vec::new()),
    };
    SuiteResult {
        suite,
        outcome,
        notes,
    }
}
