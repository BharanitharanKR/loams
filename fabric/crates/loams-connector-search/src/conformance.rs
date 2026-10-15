//! The family's conformance harness (feature `conformance`; CN2 Task 6's
//! kit, ruling R15.6): `loams_connector_search::conformance::SearchHarness`,
//! which `conformance/connectors/systems.toml` names for `elasticsearch`,
//! `opensearch` and `qdrant`.
//!
//! The harness drives the whole chain against **the real system** (OR-2):
//! documents seeded into a source index (or collection) are read by the
//! family's source, appended to the SDK's in-memory Fabric (with `ingest`'s
//! `(source, id)` dedup), read back by the family's sink and written to a
//! target index on the same server, which [`ConnectorHarness::read_back`]
//! queries. So both directions are proven, and every fault point of the
//! delivery path but `AfterPrepare` (the sinks are not transactional) has a
//! place to trip. Each reset uses fresh index names, so runs never see each
//! other's data.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorFactory, EgressCheck, EventBatch, InstanceKey,
    Pipeline, PipelineConfig, PipelineHandle, PipelineState, Position, Scrubber, SinkFactory,
    SinkHandle, StaticSecrets,
};
use loams_flow_conformance::{
    ConnectorHarness, CrashingReader, CrashingSinkFactory, CrashingWriter, FaultPoint, FaultSwitch,
    HarnessError, KillTarget, MemoryReport, Observed, Seeded, ServiceKind, Throttle,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::client::{AuthSpec, Client};
use crate::config::BasicAuth;
use crate::elasticsearch::Elasticsearch;
use crate::qdrant::{Qdrant, point_id};

/// The consumer group the harness's sink reads.
pub const GROUP: &str = "flow-search-conformance";

const ES_SERVICES: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "elasticsearch",
    system: "Elasticsearch",
}];
const OPENSEARCH_SERVICES: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "opensearch",
    system: "OpenSearch",
}];
const AMAZON_OPENSEARCH_ENV: &[&str] = &[
    "LOAMS_AMAZON_OPENSEARCH_URL",
    "LOAMS_AWS_ACCESS_KEY_ID",
    "LOAMS_AWS_SECRET_ACCESS_KEY",
    "LOAMS_AWS_REGION",
];
const AMAZON_OPENSEARCH_SERVICES: &[ServiceKind] = &[ServiceKind::Nightly {
    env: AMAZON_OPENSEARCH_ENV,
    system: "OpenSearch",
}];
const QDRANT_SERVICES: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "qdrant",
    system: "Qdrant",
}];

/// The fixed test credentials of `deploy/fabric/compose.yaml` (throwaway CI
/// containers, never an account's secret), overridable by environment.
const ES_PASSWORD: &str = "loams-test-secret";
const QDRANT_KEY: &str = "loams-test-key";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum System {
    Elasticsearch,
    OpenSearch,
    AmazonOpenSearch,
    Qdrant,
}

impl System {
    fn is_qdrant(self) -> bool {
        self == System::Qdrant
    }
}

/// Whether a source checkpoint holds only finished passes (`pass_done`, in
/// both the PIT and the Qdrant scroll positions).
fn pass_finished(cp: Option<loams_connector_sdk::Checkpoint>) -> bool {
    cp.is_some_and(|cp| {
        !cp.splits.is_empty()
            && cp.splits.values().all(|p| {
                serde_json::from_slice::<Value>(&p.opaque)
                    .is_ok_and(|v| v["pass_done"].as_bool() == Some(true))
            })
    })
}

struct State {
    generation: u32,
    fabric: MemoryFabric,
    store: MemoryCheckpointStore,
    seeded: Vec<String>,
    epoch: u64,
    running: Vec<PipelineHandle>,
    peak: usize,
}

impl State {
    fn new(generation: u32) -> Self {
        Self {
            generation,
            fabric: MemoryFabric::new(),
            store: MemoryCheckpointStore::new(),
            seeded: Vec::new(),
            epoch: 0,
            running: Vec::new(),
            peak: 0,
        }
    }

    fn note_peak(&mut self) {
        for h in &self.running {
            self.peak = self.peak.max(h.stats().peak_in_flight_bytes);
        }
    }
}

/// See the module documentation.
pub struct SearchHarness {
    system: System,
    services: &'static [ServiceKind],
    url: String,
    /// The auth part of every connector config (secret names).
    auth: Value,
    /// Secret name → value, for the connectors' resolver.
    secrets: Vec<(String, String)>,
    run: String,
    config: PipelineConfig,
    /// The connector's own page and bulk sizes and the SDK's default budgets
    /// ([`SearchHarness::for_bench`]), not the suites' small ones.
    bench: bool,
    switch: Arc<FaultSwitch>,
    state: Mutex<State>,
}

impl std::fmt::Debug for SearchHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchHarness")
            .field("system", &self.system)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

fn env_or(var: &str, default: &str) -> String {
    std::env::var(var)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn failed(e: impl std::fmt::Display) -> HarnessError {
    HarnessError::failed(e.to_string())
}

/// A connection failure is the service being down: the suite is skipped.
fn unavailable_or_failed(e: loams_connector_sdk::ConnectorError) -> HarnessError {
    if e.class.is_transient() {
        HarnessError::Unavailable(e.message)
    } else {
        HarnessError::Failed(e.message)
    }
}

impl SearchHarness {
    fn new(
        system: System,
        services: &'static [ServiceKind],
        url: String,
        auth: Value,
        secrets: Vec<(String, String)>,
    ) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        Self {
            system,
            services,
            url,
            auth,
            secrets,
            run: format!("loams-conf-{}-{nanos}", std::process::id()),
            config: PipelineConfig {
                max_batch_rows: 16,
                max_batch_bytes: 256 << 10,
                linger: Duration::from_millis(5),
                max_in_flight_batches: 4,
                memory_budget_bytes: 4 << 20,
                checkpoint_interval: Duration::ZERO,
                ..PipelineConfig::default()
            },
            bench: false,
            switch: FaultSwitch::new(),
            state: Mutex::new(State::new(0)),
        }
    }

    /// Real Elasticsearch (`connectors-ci-only`), with security on and Basic
    /// auth (`LOAMS_ELASTICSEARCH_URL`, `LOAMS_ELASTICSEARCH_PASSWORD`).
    pub fn elasticsearch() -> Self {
        let password = env_or("LOAMS_ELASTICSEARCH_PASSWORD", ES_PASSWORD);
        Self::new(
            System::Elasticsearch,
            ES_SERVICES,
            env_or("LOAMS_ELASTICSEARCH_URL", "http://127.0.0.1:9200"),
            json!({"basic": {"username": "elastic", "password": "es-password"}}),
            vec![("es-password".to_owned(), password)],
        )
    }

    /// Real OpenSearch 3 (`connectors-p2`, security plugin off).
    pub fn opensearch() -> Self {
        Self::new(
            System::OpenSearch,
            OPENSEARCH_SERVICES,
            env_or("LOAMS_OPENSEARCH_URL", "http://127.0.0.1:9201"),
            json!({"api": "opensearch"}),
            Vec::new(),
        )
    }

    /// Amazon OpenSearch Service with SigV4 (the nightly job's account).
    pub fn amazon_opensearch(url: &str, region: &str, key_id: &str, secret: &str) -> Self {
        Self::new(
            System::AmazonOpenSearch,
            AMAZON_OPENSEARCH_SERVICES,
            url.to_owned(),
            json!({"api": "opensearch", "aws": {
                "region": region, "access_key_id": "aws-id", "secret_access_key": "aws-secret"
            }}),
            vec![
                ("aws-id".to_owned(), key_id.to_owned()),
                ("aws-secret".to_owned(), secret.to_owned()),
            ],
        )
    }

    /// Real Qdrant (`connectors-p2`), with an API key
    /// (`LOAMS_QDRANT_URL`, `LOAMS_QDRANT_API_KEY`).
    /// The connector's own page and bulk sizes and the SDK's default budgets,
    /// for the performance bench: the suites' 7-document pages, 50-document
    /// bulks and 16-row batches exercise paging and resumption, and are not
    /// what a bench should measure (a round of the 10,000 batch re-reads up to
    /// 100,000 documents).
    pub fn for_bench(mut self) -> Self {
        self.bench = true;
        self.config = PipelineConfig {
            checkpoint_interval: Duration::ZERO,
            ..PipelineConfig::default()
        };
        self
    }

    pub fn qdrant() -> Self {
        let key = env_or("LOAMS_QDRANT_API_KEY", QDRANT_KEY);
        Self::new(
            System::Qdrant,
            QDRANT_SERVICES,
            env_or("LOAMS_QDRANT_URL", "http://127.0.0.1:6333"),
            json!({"api_key": "qdrant-key"}),
            vec![("qdrant-key".to_owned(), key)],
        )
    }

    pub fn instance() -> InstanceKey {
        InstanceKey::new(1, "search-conformance")
    }

    fn resolver(&self) -> StaticSecrets {
        self.secrets.iter().fold(StaticSecrets::new(), |s, (k, v)| {
            s.with(k.clone(), v.clone())
        })
    }

    fn names(&self, generation: u32) -> (String, String) {
        (
            format!("{}-src-{generation}", self.run),
            format!("{}-dst-{generation}", self.run),
        )
    }

    /// A connector config: the server, the auth, and `extra`.
    fn config(&self, extra: Value) -> Value {
        let mut c = json!({ "url": self.url });
        for src in [&self.auth, &extra] {
            if let Value::Object(m) = src {
                for (k, v) in m {
                    c[k] = v.clone();
                }
            }
        }
        c
    }

    fn ctx(&self, config: Value) -> ConnectorContext {
        let mut ctx = ConnectorContext::new(
            Self::instance(),
            config,
            Arc::new(self.resolver()),
            Scrubber::new(),
        );
        ctx.egress = Arc::new(loams_connector_sdk::AllowAll) as Arc<dyn EgressCheck>;
        ctx
    }

    async fn admin(&self) -> Result<Client, HarnessError> {
        let auth = AuthSpec {
            basic: self.auth.get("basic").map(|_| BasicAuth {
                username: "elastic".to_owned(),
                password: "es-password".to_owned(),
            }),
            es_api_key: None,
            qdrant_api_key: self.system.is_qdrant().then(|| "qdrant-key".to_owned()),
            aws: self
                .auth
                .get("aws")
                .and_then(|a| serde_json::from_value(a.clone()).ok()),
        };
        Client::connect(
            &self.url,
            &auth,
            &self.resolver(),
            Arc::new(loams_connector_sdk::AllowAll),
            Duration::from_secs(30),
        )
        .await
        .map_err(failed)
    }

    fn source_config(&self, src: &str, page: Option<usize>) -> Value {
        let mut source = if self.system.is_qdrant() {
            json!({})
        } else {
            json!({"cursor_field": "seq"})
        };
        if let Some(page) = page {
            source["page_size"] = json!(page);
        }
        if self.system.is_qdrant() {
            self.config(json!({"collection": src, "source": source}))
        } else {
            self.config(json!({"index": src, "source": source}))
        }
    }

    fn sink_config(&self, dst: &str) -> Value {
        if self.system.is_qdrant() {
            self.config(json!({
                "collection": dst, "key": "/payload/id", "vector": "/vector", "payload": "/payload"
            }))
        } else if self.bench {
            self.config(json!({"index": dst, "key": "/id"}))
        } else {
            self.config(json!({"index": dst, "key": "/id", "bulk": {"max_docs": 50, "retry_backoff_ms": 50}}))
        }
    }

    fn factory(&self) -> Box<dyn ConnectorFactory> {
        match self.system {
            System::Qdrant => Box::new(Qdrant::new()),
            System::Elasticsearch => Box::new(Elasticsearch::new()),
            System::OpenSearch | System::AmazonOpenSearch => Box::new(Elasticsearch::opensearch()),
        }
    }

    // ----------------------------------------------------- the system --

    async fn create(&self, name: &str) -> Result<(), HarnessError> {
        let admin = self.admin().await?;
        let (target, body) = if self.system.is_qdrant() {
            (
                format!("/collections/{name}"),
                json!({"vectors": {"size": 4, "distance": "Cosine"}}),
            )
        } else {
            (
                format!("/{name}"),
                json!({"mappings": {"properties": {
                    "id": {"type": "keyword"}, "seq": {"type": "long"}, "body": {"type": "text"}
                }}}),
            )
        };
        admin
            .json(Method::PUT, &target, Some(&body))
            .await
            .map(drop)
            .map_err(unavailable_or_failed)
    }

    async fn drop_name(&self, name: &str) {
        if let Ok(admin) = self.admin().await {
            let target = if self.system.is_qdrant() {
                format!("/collections/{name}")
            } else {
                format!("/{name}")
            };
            let _ = admin
                .send(Method::DELETE, &target, Bytes::new(), None)
                .await;
        }
    }

    fn doc(k: usize) -> (String, Value) {
        let id = format!("doc-{k}");
        let doc = json!({"id": id, "seq": k, "body": format!("document {k}")});
        (id, doc)
    }

    fn vector(k: usize) -> Value {
        json!([1.0, (k % 7) as f64 + 0.5, (k % 3) as f64, 0.25])
    }

    /// Write documents `from..to` into `name`, visible to reads at once.
    async fn put_docs(&self, name: &str, from: usize, to: usize) -> Result<(), HarnessError> {
        let admin = self.admin().await?;
        if self.system.is_qdrant() {
            let points: Vec<Value> = (from..to)
                .map(|k| {
                    let (id, doc) = Self::doc(k);
                    json!({"id": point_id(&id).to_string(), "vector": Self::vector(k), "payload": doc})
                })
                .collect();
            admin
                .json(
                    Method::PUT,
                    &format!("/collections/{name}/points?wait=true"),
                    Some(&json!({ "points": points })),
                )
                .await
                .map_err(unavailable_or_failed)?;
            return Ok(());
        }
        let mut body = String::new();
        for k in from..to {
            let (id, doc) = Self::doc(k);
            body.push_str(&json!({"index": {"_index": name, "_id": id}}).to_string());
            body.push('\n');
            body.push_str(&doc.to_string());
            body.push('\n');
        }
        let resp = admin
            .send(
                Method::POST,
                "/_bulk?refresh=true",
                Bytes::from(body),
                Some("application/x-ndjson"),
            )
            .await
            .map_err(unavailable_or_failed)?;
        let answer: Value = serde_json::from_slice(resp.body()).map_err(failed)?;
        if !resp.status().is_success() || answer["errors"] == true {
            return Err(HarnessError::failed(format!(
                "seeding {name} failed: {}",
                crate::client::shorten(&answer.to_string(), 300)
            )));
        }
        Ok(())
    }

    /// The document ids in `name`, with their copy counts.
    async fn ids_in(&self, name: &str) -> Result<Vec<Observed>, HarnessError> {
        let admin = self.admin().await?;
        let mut out = std::collections::BTreeMap::<String, u32>::new();
        if self.system.is_qdrant() {
            let mut offset = Value::Null;
            loop {
                let mut body = json!({"limit": 1000, "with_payload": true, "with_vector": false});
                if !offset.is_null() {
                    body["offset"] = offset.clone();
                }
                let a = admin
                    .json(
                        Method::POST,
                        &format!("/collections/{name}/points/scroll"),
                        Some(&body),
                    )
                    .await
                    .map_err(failed)?;
                for p in a["result"]["points"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                {
                    let id = p["payload"]["id"].as_str().unwrap_or("?").to_owned();
                    *out.entry(id).or_default() += 1;
                }
                offset = a["result"]["next_page_offset"].clone();
                if offset.is_null() {
                    break;
                }
            }
        } else {
            let _ = admin
                .send(
                    Method::POST,
                    &format!("/{name}/_refresh"),
                    Bytes::new(),
                    None,
                )
                .await;
            // A scroll, so every document is counted: one `_search` returns
            // at most `index.max_result_window` (10,000) hits, which the
            // bench's 10,000 batch passes from its second round.
            let mut a = admin
                .json(
                    Method::POST,
                    &format!("/{name}/_search?scroll=1m"),
                    Some(&json!({"size": 1000, "_source": ["id"], "query": {"match_all": {}}})),
                )
                .await
                .map_err(failed)?;
            loop {
                let hits = a["hits"]["hits"].as_array().cloned().unwrap_or_default();
                for h in &hits {
                    // One document per `_id`; a duplicate write would be a
                    // second document with the same `id` field.
                    let id = h["_source"]["id"].as_str().unwrap_or("?").to_owned();
                    *out.entry(id).or_default() += 1;
                }
                let Some(scroll) = a["_scroll_id"].as_str().map(str::to_owned) else {
                    break;
                };
                if hits.is_empty() {
                    let _ = admin
                        .json(
                            Method::DELETE,
                            "/_search/scroll",
                            Some(&json!({"scroll_id": scroll})),
                        )
                        .await;
                    break;
                }
                a = admin
                    .json(
                        Method::POST,
                        "/_search/scroll",
                        Some(&json!({"scroll": "1m", "scroll_id": scroll})),
                    )
                    .await
                    .map_err(failed)?;
            }
        }
        Ok(out
            .into_iter()
            .map(|(id, copies)| Observed { id, copies })
            .collect())
    }

    // ------------------------------------------------- direct checks --

    /// Read `n` seeded documents with pages of `page`, stop after two pages,
    /// resume a new source from the saved position, and check the result is
    /// every document once, in cursor order (`es_source_pit_search_after_resumes`,
    /// `qdrant_scroll_resumes`).
    pub async fn check_pit_resume(&self, n: usize, page: usize) -> Result<(), HarnessError> {
        self.reset().await?;
        let (src, _) = self.names(self.state.lock().await.generation);
        self.put_docs(&src, 0, n).await?;
        let ctx = self.ctx(self.source_config(&src, Some(page)));
        let (mut en, sources) = self
            .factory()
            .source(&ctx)
            .map_err(failed)?
            .ok_or_else(|| HarnessError::failed("no source"))?;
        let split = en.discover(&[]).await.map_err(failed)?.remove(0);
        let budget = Budget {
            max_rows: page,
            max_bytes: 1 << 20,
        };
        let mut seen: Vec<String> = Vec::new();
        let mut saved: Option<Position> = None;
        {
            let mut s = sources.create(&split).map_err(failed)?;
            s.open(&split, None).await.map_err(failed)?;
            for _ in 0..2 {
                if let Some(b) = s.next(budget).await.map_err(failed)? {
                    seen.extend(subjects(&b.events));
                    saved = Some(b.position);
                }
            }
            // Dropped without close, as a crashed task would be.
        }
        let mut s = sources.create(&split).map_err(failed)?;
        s.open(&split, saved).await.map_err(failed)?;
        while let Some(b) = s.next(budget).await.map_err(failed)? {
            seen.extend(subjects(&b.events));
        }
        s.close().await.map_err(failed)?;
        let want: Vec<String> = if self.system.is_qdrant() {
            let mut ids: Vec<String> = (0..n)
                .map(|k| point_id(&format!("doc-{k}")).to_string())
                .collect();
            ids.sort();
            ids
        } else {
            (0..n).map(|k| format!("doc-{k}")).collect()
        };
        if seen != want {
            return Err(HarnessError::failed(format!(
                "resumed read differs: got {} ids {:?}…, want {} in order",
                seen.len(),
                seen.iter().take(8).collect::<Vec<_>>(),
                want.len()
            )));
        }
        self.drop_name(&src).await;
        Ok(())
    }

    /// The Qdrant name of [`Self::check_pit_resume`].
    pub async fn check_scroll_resume(&self, n: usize, page: usize) -> Result<(), HarnessError> {
        self.check_pit_resume(n, page).await
    }

    async fn sink_write(&self, dst: &str, events: Vec<CloudEvent>) -> Result<(), HarnessError> {
        let ctx = self.ctx(self.sink_config(dst));
        let SinkHandle::Plain(mut sink) = self
            .factory()
            .sink(&ctx)
            .map_err(failed)?
            .ok_or_else(|| HarnessError::failed("no sink"))?
            .create()
            .map_err(failed)?
        else {
            return Err(HarnessError::failed("a transactional sink"));
        };
        sink.write(EventBatch::Events(events))
            .await
            .map_err(failed)?;
        sink.flush().await.map_err(failed)?;
        Ok(())
    }

    fn events(&self, n: usize) -> Vec<CloudEvent> {
        (0..n)
            .map(|k| {
                let (id, doc) = Self::doc(k);
                let data = if self.system.is_qdrant() {
                    json!({"payload": doc, "vector": Self::vector(k)})
                } else {
                    doc
                };
                CloudEvent::new(
                    format!("ce-{id}"),
                    "/conformance",
                    "io.loams.dev.flow.test.document.v1",
                )
                .with_data("application/json", data.to_string())
            })
            .collect()
    }

    /// Write `n` documents through the sink and read them back.
    pub async fn check_sink_roundtrip(&self, n: usize) -> Result<(), HarnessError> {
        self.reset().await?;
        let (_, dst) = self.names(self.state.lock().await.generation);
        self.sink_write(&dst, self.events(n)).await?;
        let got = self.ids_in(&dst).await?;
        self.drop_name(&dst).await;
        if got.len() != n || got.iter().any(|o| o.copies != 1) {
            return Err(HarnessError::failed(format!(
                "{} distinct documents back of {n}",
                got.len()
            )));
        }
        Ok(())
    }

    /// Write the same `n` events twice: still `n` points, each at the UUID v5
    /// of its key (`qdrant_upsert_idempotent_uuid_v5`).
    pub async fn check_upsert_idempotent(&self, n: usize) -> Result<(), HarnessError> {
        self.reset().await?;
        let (_, dst) = self.names(self.state.lock().await.generation);
        self.sink_write(&dst, self.events(n)).await?;
        self.sink_write(&dst, self.events(n)).await?;
        let got = self.ids_in(&dst).await?;
        if got.len() != n || got.iter().any(|o| o.copies != 1) {
            return Err(HarnessError::failed(format!(
                "after a replay: {} keys, copies {:?}",
                got.len(),
                got.iter().map(|o| o.copies).max()
            )));
        }
        if self.system.is_qdrant() {
            let admin = self.admin().await?;
            for k in [0, n / 2, n - 1] {
                let id = point_id(&format!("doc-{k}")).to_string();
                let a = admin
                    .json(
                        Method::GET,
                        &format!("/collections/{dst}/points/{id}"),
                        None,
                    )
                    .await
                    .map_err(failed)?;
                if a["result"]["payload"]["id"] != format!("doc-{k}") {
                    return Err(HarnessError::failed(format!("point {id} is not doc-{k}")));
                }
            }
        }
        self.drop_name(&dst).await;
        Ok(())
    }
}

fn subjects(batch: &EventBatch) -> Vec<String> {
    match batch {
        EventBatch::Events(evs) => evs.iter().filter_map(|e| e.subject.clone()).collect(),
        EventBatch::Arrow { .. } => Vec::new(),
    }
}

#[async_trait]
impl ConnectorHarness for SearchHarness {
    fn services(&self) -> &[ServiceKind] {
        self.services
    }

    async fn seed(&self, n: usize) -> Result<Seeded, HarnessError> {
        let mut s = self.state.lock().await;
        let (src, _) = self.names(s.generation);
        let from = s.seeded.len();
        self.put_docs(&src, from, from + n).await?;
        s.seeded
            .extend((from..from + n).map(|k| format!("doc-{k}")));
        Ok(Seeded {
            ids: s.seeded.clone(),
        })
    }

    async fn read_back(&self) -> Result<Vec<Observed>, HarnessError> {
        let generation = self.state.lock().await.generation;
        let (_, dst) = self.names(generation);
        self.ids_in(&dst).await
    }

    async fn kill(&self, what: KillTarget) -> Result<(), HarnessError> {
        match what {
            KillTarget::Task | KillTarget::Pod => {
                let mut s = self.state.lock().await;
                s.note_peak();
                for h in s.running.drain(..) {
                    h.abort();
                }
                Ok(())
            }
            KillTarget::System => Err(HarnessError::Unsupported(
                "the harness does not restart the server's container".into(),
            )),
        }
    }

    async fn inject(&self, at: FaultPoint) -> Result<(), HarnessError> {
        if at == FaultPoint::AfterPrepare {
            return Err(HarnessError::Unsupported(
                "the search sinks are not transactional".into(),
            ));
        }
        self.switch.arm(at);
        Ok(())
    }

    async fn reset(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        for h in s.running.drain(..) {
            h.abort();
        }
        let (old_src, old_dst) = self.names(s.generation);
        let generation = s.generation + 1;
        *s = State::new(generation);
        drop(s);
        self.switch.disarm();
        self.drop_name(&old_src).await;
        self.drop_name(&old_dst).await;
        let (src, dst) = self.names(generation);
        self.create(&src).await?;
        self.create(&dst).await?;
        Ok(())
    }

    async fn start(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        if !s.running.is_empty() {
            return Err(HarnessError::failed("already running"));
        }
        s.epoch += 1;
        let (src, dst) = self.names(s.generation);
        let factory = self.factory();
        let inst = Self::instance();
        // A source pass that finished (no `poll_interval`) has nothing left
        // to resume: starting again is the next run, a whole pass (the sink
        // upserts by key, so documents read again land on themselves). The
        // kit's bench restarts each round after seeding more; resuming the
        // finished pass would read nothing and never settle.
        if pass_finished(s.store.get(&inst)) {
            s.store = MemoryCheckpointStore::new();
        }
        let (enumerator, sources) = factory
            .source(&self.ctx(self.source_config(&src, (!self.bench).then_some(7))))
            .map_err(failed)?
            .ok_or_else(|| HarnessError::failed("no source"))?;
        let source = Pipeline::source(inst.clone(), self.config.clone())
            .enumerator(enumerator)
            .sources(Arc::from(sources))
            .writer(Arc::new(CrashingWriter::new(
                s.fabric.clone(),
                self.switch.clone(),
            )))
            .checkpoints(Arc::new(s.store.clone()))
            .dead_letters(Arc::new(s.fabric.clone()))
            .discover_every(Duration::from_secs(3600))
            .epoch(s.epoch)
            .start()
            .await
            .map_err(|e| failed(format!("source: {e}")))?;
        let sink: Arc<dyn SinkFactory> = Arc::from(
            factory
                .sink(&self.ctx(self.sink_config(&dst)))
                .map_err(failed)?
                .ok_or_else(|| HarnessError::failed("no sink"))?,
        );
        let sink = Pipeline::sink(inst, self.config.clone())
            .reader(Box::new(CrashingReader::new(
                s.fabric.reader(GROUP),
                self.switch.clone(),
            )))
            .sink(Arc::new(CrashingSinkFactory::new(
                sink,
                self.switch.clone(),
            )))
            .dead_letters(Arc::new(s.fabric.clone()))
            .epoch(s.epoch)
            .start()
            .await
            .map_err(|e| failed(format!("sink: {e}")))?;
        s.running = vec![source, sink];
        Ok(())
    }

    async fn stop(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        s.note_peak();
        for h in s.running.drain(..) {
            if h.status().state != PipelineState::Running {
                continue;
            }
            let _ = tokio::time::timeout(Duration::from_secs(10), h.stop()).await;
        }
        Ok(())
    }

    async fn settle(&self, timeout: Duration) -> Result<(), HarnessError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let (want, fabric_len, committed, generation) = {
                let mut s = self.state.lock().await;
                s.note_peak();
                for h in &s.running {
                    let st = h.status();
                    if matches!(st.state, PipelineState::Failed | PipelineState::Fenced) {
                        return Err(HarnessError::failed(format!(
                            "pipeline {:?}: {:?}",
                            st.state, st.last_error
                        )));
                    }
                }
                (
                    s.seeded.clone(),
                    s.fabric.len() as u64,
                    s.fabric.committed(GROUP),
                    s.generation,
                )
            };
            let (_, dst) = self.names(generation);
            let got: std::collections::BTreeSet<String> =
                self.ids_in(&dst).await?.into_iter().map(|o| o.id).collect();
            if want.iter().all(|id| got.contains(id)) && committed == fabric_len {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(HarnessError::failed(format!(
                    "not settled after {timeout:?}: target holds {} of {}, Fabric {fabric_len} committed {committed}",
                    got.len(),
                    want.len()
                )));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn tripped(&self, timeout: Duration) -> Result<bool, HarnessError> {
        Ok(self.switch.wait_tripped(timeout).await)
    }

    async fn throttle(&self, t: Throttle) -> Result<(), HarnessError> {
        let s = self.state.lock().await;
        match t {
            Throttle::Fabric(d) => s.fabric.set_append_delay(Some(d)),
            Throttle::Off => s.fabric.set_append_delay(None),
            Throttle::Target(_) => {
                return Err(HarnessError::Unsupported(
                    "the harness cannot slow the server itself".into(),
                ));
            }
        }
        Ok(())
    }

    async fn memory(&self) -> Result<MemoryReport, HarnessError> {
        let mut s = self.state.lock().await;
        s.note_peak();
        Ok(MemoryReport {
            peak_bytes: s.peak,
            budget_bytes: self.config.memory_budget_bytes,
        })
    }

    /// Move one document through the sink with the auth method `method`,
    /// and check the server refuses the same write without credentials.
    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        let (auth, secrets): (Value, Vec<(String, String)>) = match (self.system, method) {
            (System::Elasticsearch, "basic") => (self.auth.clone(), self.secrets.clone()),
            (System::Elasticsearch, "key") => {
                let admin = self.admin().await?;
                let key = admin
                    .json(
                        Method::POST,
                        "/_security/api_key",
                        Some(&json!({"name": format!("{}-auth", self.run), "expiration": "1h"})),
                    )
                    .await
                    .map_err(failed)?;
                let encoded = key["encoded"]
                    .as_str()
                    .ok_or_else(|| HarnessError::failed("no encoded API key"))?
                    .to_owned();
                (
                    json!({"api_key": "es-key"}),
                    vec![("es-key".to_owned(), encoded)],
                )
            }
            (System::Qdrant, "key") => (self.auth.clone(), self.secrets.clone()),
            (System::AmazonOpenSearch, "iam") => (self.auth.clone(), self.secrets.clone()),
            (System::OpenSearch, m) => {
                return Err(HarnessError::Unsupported(format!(
                    "auth {m}: the estate's OpenSearch runs without its security plugin; \
                     iam runs in the nightly job"
                )));
            }
            (_, m) => return Err(HarnessError::Unsupported(format!("auth {m}"))),
        };
        let generation = self.state.lock().await.generation;
        let (_, dst) = self.names(generation);
        let write = |auth: Value, secrets: Vec<(String, String)>| {
            let dst = dst.clone();
            async move {
                let mut cfg = self.sink_config(&dst);
                for k in ["basic", "api_key", "aws"] {
                    if let Value::Object(m) = &mut cfg {
                        m.remove(k);
                    }
                }
                if let (Value::Object(m), Value::Object(a)) = (&mut cfg, auth) {
                    m.extend(a);
                }
                let resolver = secrets.iter().fold(StaticSecrets::new(), |s, (k, v)| {
                    s.with(k.clone(), v.clone())
                });
                let mut ctx = ConnectorContext::new(
                    Self::instance(),
                    cfg,
                    Arc::new(resolver),
                    Scrubber::new(),
                );
                ctx.egress = Arc::new(loams_connector_sdk::AllowAll);
                let SinkHandle::Plain(mut sink) = self
                    .factory()
                    .sink(&ctx)?
                    .ok_or_else(|| loams_connector_sdk::ConnectorError::fatal("no sink"))?
                    .create()?
                else {
                    return Err(loams_connector_sdk::ConnectorError::fatal("transactional"));
                };
                sink.write(EventBatch::Events(self.events(1))).await
            }
        };
        write(auth, secrets).await.map_err(|e| {
            HarnessError::failed(format!("auth {method}: the write failed: {}", e.message))
        })?;
        match write(json!({}), Vec::new()).await {
            Err(e) if e.class == loams_connector_sdk::ErrorClass::Auth => {}
            other => {
                return Err(HarnessError::failed(format!(
                    "auth {method}: the server accepted a write without credentials ({:?})",
                    other.map(|a| a.rows)
                )));
            }
        }
        let got = self.ids_in(&dst).await?;
        if !got.iter().any(|o| o.id == "doc-0") {
            return Err(HarnessError::failed(format!(
                "auth {method}: the document is not in the target"
            )));
        }
        Ok(())
    }
}
