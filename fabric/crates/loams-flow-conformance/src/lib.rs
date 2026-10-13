//! `loams-flow-conformance`: the conformance kit every Loams connector runs
//! (CN1's crate name, OR-5; CN2 plan Task 6).
//!
//! A family crate implements [`ConnectorHarness`] for each manifest it serves:
//! the harness starts the connector under test against **the real system**
//! (OR-2) — its image, its vendor's emulator, a fixture recorded from it, or a
//! real account in the nightly job — writes records into that system and
//! reads its state back. [`run`] then drives the suites of [`Suite`] through
//! it and returns a [`SuiteReport`].
//!
//! - [`services`] holds [`ServiceKind`] and the OR-2 truth,
//!   `conformance/connectors/systems.toml`: for each manifest, the real system
//!   and the services allowed to stand for it. A Loams module (House, the
//!   collections' Elasticsearch and Qdrant APIs, Loams Postgres, the
//!   Fabric's Iggy) never stands in for an external system; it may only be an
//!   extra target. [`services::check_services`] enforces it before any suite
//!   runs.
//! - [`capabilities`] maps each capability a manifest declares to the suite
//!   that proves it (`every_declared_capability_has_a_test`).
//! - [`fixture`] is the record-and-replay HTTP proxy for cloud and SaaS
//!   systems.
//! - [`crash`] injects a crash at each [`FaultPoint`] of the delivery path.
//! - [`reference`] is the kit's self-test harness over the SDK's reference
//!   connector (`loams_connector_sdk::testing::reference`).
//! - [`bench`] holds the performance budgets of CN2 Task 27: the reference
//!   pipeline's bench, the harness bench every family's bench is built on,
//!   the baselines and the 15 % regression budget, and the chart's density.
//!
//! A suite that could not run is **skipped**, and a skipped suite never
//! counts as green ([`SuiteReport::green`]): a connector is `preview` only
//! when every suite it declares passed (OR-23).

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;

pub mod bench;
pub mod capabilities;
pub mod crash;
pub mod fixture;
pub mod reference;
pub mod services;
mod suites;

pub use capabilities::{
    CapabilityCheck, capability_checks, declared_capabilities, required_suites,
    unchecked_capabilities,
};
pub use crash::{CrashingReader, CrashingSinkFactory, CrashingWriter, FaultSwitch};
pub use fixture::{Exchange, Fixture, FixtureError, FixtureProxy, ProxyHandle, ScrubRules};
pub use services::{
    ComposeEstate, Estate, ServiceDecl, ServiceKind, Stack, SystemEntry, SystemsCatalog,
    check_services, harness_problems,
};
pub use suites::{RunEnv, RunOptions, run, run_with};

/// The conformance suites (Shared contracts, "Conformance kit"). Their slugs
/// are the manifest's `conformance` slugs (CN2 Task 1 adds the last four).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Suite {
    /// Exercises each declared capability.
    Contract,
    /// Records into the system, through the connector, and back: none lost.
    Roundtrip,
    /// The task, the pod or the system is killed mid-run: none lost.
    KillRestart,
    /// Duplicates stay within the declared delivery after a restart.
    DupCheck,
    /// The Arrow bulk path at 1 M rows (D356).
    Bulk,
    /// Inserts, updates, deletes, DDL and resume from the slot or binlog.
    Cdc,
    /// An additive change evolves; an incompatible one pauses.
    SchemaEvolution,
    /// A slow target and a slow Fabric: bounded memory, no loss.
    Backpressure,
    /// A crash at every [`FaultPoint`]: the target equals the input exactly.
    ExactlyOnce,
    /// Each declared auth method the service supports.
    Auth,
}

impl Suite {
    pub const ALL: &'static [Suite] = &[
        Suite::Contract,
        Suite::Roundtrip,
        Suite::KillRestart,
        Suite::DupCheck,
        Suite::Bulk,
        Suite::Cdc,
        Suite::SchemaEvolution,
        Suite::Backpressure,
        Suite::ExactlyOnce,
        Suite::Auth,
    ];

    /// The manifest slug (`connector.schema.json`'s `conformance` items).
    pub const fn slug(self) -> &'static str {
        match self {
            Suite::Contract => "contract",
            Suite::Roundtrip => "roundtrip",
            Suite::KillRestart => "kill-restart",
            Suite::DupCheck => "dup-check",
            Suite::Bulk => "bulk",
            Suite::Cdc => "cdc",
            Suite::SchemaEvolution => "schema-evolution",
            Suite::Backpressure => "backpressure",
            Suite::ExactlyOnce => "exactly-once",
            Suite::Auth => "auth",
        }
    }

    pub fn from_slug(slug: &str) -> Option<Suite> {
        Suite::ALL.iter().copied().find(|s| s.slug() == slug)
    }

    /// The suites a manifest declares, by slug, so the kit reads both CN1's
    /// six slugs and CN2 Task 1's ten without matching on `loams_flow::Suite`.
    pub fn declared_by(spec: &loams_flow::ConnectorSpec) -> Vec<Suite> {
        spec.conformance
            .iter()
            .filter_map(|s| Suite::from_slug(s.slug()))
            .collect()
    }
}

impl fmt::Display for Suite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.slug())
    }
}

/// Where a crash is injected in the delivery path (Shared contracts).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultPoint {
    /// Source: the batch was read but never reached the Fabric.
    BeforeFabricAck,
    /// Source: the Fabric holds the batch; neither the checkpoint nor the
    /// upstream commit happened.
    AfterFabricAckBeforeUpstreamCommit,
    /// Exactly-once sink: data and checkpoint staged, not committed.
    AfterPrepare,
    /// Sink: the target committed; the Fabric offset (advisory) did not move.
    AfterCommitBeforeFabricCommit,
    /// Sink: part of a batch was written.
    MidBatch,
}

impl FaultPoint {
    pub const ALL: &'static [FaultPoint] = &[
        FaultPoint::BeforeFabricAck,
        FaultPoint::AfterFabricAckBeforeUpstreamCommit,
        FaultPoint::AfterPrepare,
        FaultPoint::AfterCommitBeforeFabricCommit,
        FaultPoint::MidBatch,
    ];
}

/// What a kill takes down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillTarget {
    /// The connector's tasks (a crashed `flow` process).
    Task,
    /// The pod the connector runs in (Kubernetes, Task 26).
    Pod,
    /// The external system itself (a container restart).
    System,
}

/// What [`ConnectorHarness::seed`] wrote: the ids the kit expects to observe
/// in the target, cumulative since the last reset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Seeded {
    pub ids: Vec<String>,
}

/// One record of the target's state: its id and how many copies the target
/// holds or applied (duplicates, for `dup-check` and `exactly-once`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed {
    pub id: String,
    pub copies: u32,
}

/// A throttle the backpressure suite applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Throttle {
    /// The target answers every write after this delay.
    Target(Duration),
    /// The Fabric acknowledges every append after this delay.
    Fabric(Duration),
    /// Throttling off.
    Off,
}

/// The peak in-flight memory of the connector and the budget it ran with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryReport {
    pub peak_bytes: usize,
    pub budget_bytes: usize,
}

/// A schema change the schema-evolution suite applies to the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaStep {
    /// A new nullable column (or field): must evolve.
    Additive,
    /// A type change that loses information: must pause.
    Incompatible,
}

/// What the connector did with a [`SchemaStep`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Evolved {
    /// The change was taken and records keep flowing.
    Evolved,
    /// The instance paused with this error (scrubbed).
    Paused { error: String },
}

/// What the harness saw in a CDC script: inserts, updates, deletes and DDL
/// observed as change events, and a resume from the slot or binlog after a
/// restart without loss.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CdcReport {
    pub inserts: bool,
    pub updates: bool,
    pub deletes: bool,
    pub ddl: bool,
    pub resumed_without_loss: bool,
}

/// What the bulk path delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BulkReport {
    pub rows: u64,
    /// The rows travelled as Arrow batches (no row decode).
    pub arrow: bool,
}

/// A harness failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HarnessError {
    /// The harness, or the service behind it, cannot do this (an auth method
    /// the emulator lacks, a fault point a source-only connector has not).
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The service is not reachable: the suite is skipped, never green.
    #[error("service unavailable: {0}")]
    Unavailable(String),
    /// The step itself failed.
    #[error("{0}")]
    Failed(String),
}

impl HarnessError {
    pub fn failed(msg: impl Into<String>) -> Self {
        HarnessError::Failed(msg.into())
    }
}

/// A connector under test, wired to its real system (Shared contracts).
///
/// The first five methods are the contract's. The others (ruling R6.2) are
/// how the kit starts, stops and waits for the connector, and how the
/// optional suites drive the system; their defaults report `Unsupported`, so
/// a harness implements only what its manifest declares.
#[async_trait]
pub trait ConnectorHarness: Send + Sync {
    /// The services this harness runs against (OR-2). Checked against
    /// `systems.toml` before any suite runs.
    fn services(&self) -> &[ServiceKind];
    /// Write `n` records into the real system (a source's input, or the
    /// Fabric topic a sink reads). Cumulative until [`Self::reset`].
    async fn seed(&self, n: usize) -> Result<Seeded, HarnessError>;
    /// Read the real system's state (a sink's target, or what a source
    /// delivered to the Fabric).
    async fn read_back(&self) -> Result<Vec<Observed>, HarnessError>;
    /// Kill the task, the pod or the system itself, as a crash would.
    async fn kill(&self, what: KillTarget) -> Result<(), HarnessError>;
    /// Arm a crash at `at`: the next time the delivery path reaches it, the
    /// connector stops dead there, as a crashed process would.
    async fn inject(&self, at: FaultPoint) -> Result<(), HarnessError>;

    /// Empty the system, the Fabric and the checkpoints; disarm faults.
    async fn reset(&self) -> Result<(), HarnessError>;
    /// Start (or restart after a kill) the connector, from its checkpoint,
    /// under a new lease epoch.
    async fn start(&self) -> Result<(), HarnessError>;
    /// Stop the connector gracefully.
    async fn stop(&self) -> Result<(), HarnessError>;
    /// Wait until everything seeded has been delivered and committed, or
    /// `timeout` passes (then `Failed`).
    async fn settle(&self, timeout: Duration) -> Result<(), HarnessError>;
    /// Wait until the armed fault has been reached; `false` on timeout.
    async fn tripped(&self, timeout: Duration) -> Result<bool, HarnessError>;

    async fn throttle(&self, _t: Throttle) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported("throttle".into()))
    }
    /// The connector's peak in-flight memory since the last start.
    async fn memory(&self) -> Result<MemoryReport, HarnessError> {
        Err(HarnessError::Unsupported("memory".into()))
    }
    async fn bulk(&self, _rows: u64) -> Result<BulkReport, HarnessError> {
        Err(HarnessError::Unsupported("bulk".into()))
    }
    async fn cdc(&self) -> Result<CdcReport, HarnessError> {
        Err(HarnessError::Unsupported("cdc".into()))
    }
    async fn evolve(&self, _step: SchemaStep) -> Result<Evolved, HarnessError> {
        Err(HarnessError::Unsupported("schema evolution".into()))
    }
    /// Connect with the auth method of this manifest slug and move a record.
    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(format!("auth {method}")))
    }
}

/// How one suite ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuiteOutcome {
    Passed,
    Failed(String),
    /// It did not run (no service, an unsupported step). Never green.
    Skipped(String),
}

impl SuiteOutcome {
    pub fn is_passed(&self) -> bool {
        matches!(self, SuiteOutcome::Passed)
    }
}

/// One suite's result, with notes (fault points or auth methods that did not
/// apply, the service used).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteResult {
    pub suite: Suite,
    pub outcome: SuiteOutcome,
    pub notes: Vec<String>,
}

/// What [`run`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteReport {
    pub connector: String,
    /// The real system the suites ran against.
    pub system: Option<String>,
    pub results: Vec<SuiteResult>,
}

impl SuiteReport {
    pub fn result(&self, suite: Suite) -> Option<&SuiteResult> {
        self.results.iter().find(|r| r.suite == suite)
    }

    pub fn passed(&self) -> Vec<Suite> {
        self.with(|o| o.is_passed())
    }

    pub fn failed(&self) -> Vec<Suite> {
        self.with(|o| matches!(o, SuiteOutcome::Failed(_)))
    }

    pub fn skipped(&self) -> Vec<Suite> {
        self.with(|o| matches!(o, SuiteOutcome::Skipped(_)))
    }

    fn with(&self, f: impl Fn(&SuiteOutcome) -> bool) -> Vec<Suite> {
        self.results
            .iter()
            .filter(|r| f(&r.outcome))
            .map(|r| r.suite)
            .collect()
    }

    /// Green only when there is at least one result and every one passed: a
    /// skipped suite never counts (OR-23, Global Constraints).
    pub fn green(&self) -> bool {
        !self.results.is_empty() && self.results.iter().all(|r| r.outcome.is_passed())
    }

    /// Whether this report proves `suites` for `preview`: each was run here
    /// and passed. A suite absent from the report, or skipped, does not count.
    pub fn proves(&self, suites: &[Suite]) -> bool {
        suites
            .iter()
            .all(|s| self.result(*s).is_some_and(|r| r.outcome.is_passed()))
    }
}

impl fmt::Display for SuiteReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{} against {}:",
            self.connector,
            self.system.as_deref().unwrap_or("no system")
        )?;
        for r in &self.results {
            match &r.outcome {
                SuiteOutcome::Passed => writeln!(f, "  {:<16} passed", r.suite.slug())?,
                SuiteOutcome::Failed(m) => writeln!(f, "  {:<16} FAILED: {m}", r.suite.slug())?,
                SuiteOutcome::Skipped(m) => writeln!(f, "  {:<16} skipped: {m}", r.suite.slug())?,
            }
        }
        Ok(())
    }
}
