//! `loams-connector-sdk`: the contract every first-party Loams connector is
//! written to (CN2 plan Task 4, Owner ruling OR-5).
//!
//! A source is a [`SplitEnumerator`] plus one [`Source`] per split; a sink is
//! a [`Sink`], optionally a [`TransactionalSink`]. The [`Pipeline`] drives
//! both with bounded budgets (backpressure), retries by [`ErrorClass`],
//! checkpoints fenced by a lease epoch, a dead-letter queue, a per-instance
//! rate limiter, and the CloudEvents envelope of design §33 §6. It reaches the
//! Fabric through [`FabricWriter`] and [`FabricReader`], which `loams-flow`
//! implements over `ingest` and Iggy and [`testing::memory`] implements in
//! memory.
//!
//! Delivery mechanics, the same in every family:
//! - **Source:** `next` → `FabricWriter::append` → on the ack, the checkpoint
//!   is saved (fenced) and `Source::committed` commits upstream. A crash in
//!   between re-reads from the last checkpoint; `ingest` dedups by
//!   `(source, id)`.
//! - **Sink, at-least-once or effectively-once:** `FabricReader::poll` →
//!   `Sink::write` → `Sink::flush` → `FabricReader::commit`.
//! - **Sink, exactly-once:** on open, `recover()` and a seek to the recovered
//!   checkpoint; per batch `write` → `prepare(checkpoint)` → `commit(token)` →
//!   `FabricReader::commit` (advisory).

use std::fmt;
use std::sync::Arc;

pub mod budget;
pub mod checkpoint;
pub mod delivery;
pub mod egress;
pub mod envelope;
pub mod error;
pub mod fabric;
pub mod http;
pub mod net;
pub mod observe;
pub mod pipeline;
pub mod position;
pub mod profile;
pub mod ratelimit;
pub mod retry;
pub mod secrets;
pub mod sink;
pub mod source;
pub mod split;
pub mod testing;
pub mod txn;

pub use budget::{Budget, BudgetLease, BudgetPool, BudgetStats};
pub use checkpoint::{Checkpoint, CheckpointStore};
pub use delivery::{DeadLetter, DeadLetterQueue, Delivery};
pub use egress::{AllowAll, DenyLocal, EgressCheck, EgressDenied};
pub use envelope::{CdcOp, CloudEvent, EnvelopeBuilder, EnvelopeError, ids};
pub use error::{BoxError, CheckpointError, ConnectorError, ErrorClass, FabricError};
pub use fabric::{FabricAck, FabricReader, FabricWriter};
pub use observe::{ConnectorObserver, NoopObserver};
pub use pipeline::{
    LastError, Pipeline, PipelineConfig, PipelineHandle, PipelineState, PipelineStatus,
    SinkPipeline, SourcePipeline,
};
pub use position::{InstanceKey, Position, SplitId};
pub use profile::{
    AsyncJob, AuthScheme, CursorSpec, HttpAuth, HttpMethod, HttpProfile, HttpRateLimit,
    HttpResource, JobPoll, ListOp, Profile, ProfileKind, RequestSpec, SystemRef, WebhookSpec,
    WriteOp,
};
pub use ratelimit::{Rate, RateLimit, RateLimiter};
pub use retry::{RetryDecision, RetryPolicy};
pub use secrets::{Scrubber, ScrubbingResolver, Secret, SecretRef, SecretResolver, StaticSecrets};
pub use sink::{Sink, SinkAck, SinkHandle};
pub use source::{EventBatch, Source, SourceBatch};
pub use split::{SinkFactory, SourceFactory, Split, SplitEnumerator, SplitState};
pub use txn::{TransactionalSink, TxnToken};

/// One per manifest id, registered by each family crate; `NativeRuntime`
/// (loams-flow) runs any registered factory.
pub trait ConnectorFactory: Send + Sync {
    fn id(&self) -> &'static str;
    #[allow(clippy::type_complexity)]
    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError>;
    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError>;
}

/// What a factory is given to build an instance's connector.
#[derive(Clone)]
pub struct ConnectorContext {
    pub instance: InstanceKey,
    pub config: serde_json::Value,
    pub secrets: Arc<dyn SecretResolver>,
    pub egress: Arc<dyn EgressCheck>,
    pub observe: Arc<dyn ConnectorObserver>,
    pub profile: Option<Profile>,
}

impl ConnectorContext {
    /// A context whose secrets are recorded in `scrubber` as they are
    /// resolved (pass the same scrubber to the pipeline), with no egress
    /// restriction and no observer. The runtime replaces both.
    pub fn new(
        instance: InstanceKey,
        config: serde_json::Value,
        secrets: Arc<dyn SecretResolver>,
        scrubber: Arc<Scrubber>,
    ) -> Self {
        Self {
            instance,
            config,
            secrets: Arc::new(ScrubbingResolver::new(secrets, scrubber)),
            egress: Arc::new(AllowAll),
            observe: Arc::new(NoopObserver),
            profile: None,
        }
    }
}

/// `Debug` never renders the config's values (an instance config holds
/// secret *names*, but a careless one could hold more) or the resolver.
impl fmt::Debug for ConnectorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorContext")
            .field("instance", &self.instance)
            .field("profile", &self.profile.as_ref().map(|p| p.id.as_str()))
            .finish_non_exhaustive()
    }
}
