//! Prometheus remote write 1.0 (snappy-compressed protobuf), both ways.
//!
//! * **Receiver (source).** [`RemoteWriteReceiver`] serves
//!   `POST /v1/namespaces/{ns}/fabric/prometheus/{instance}/write` on
//!   `ingest`'s listener (the route itself is wired with `ingest`, FL1; until
//!   then the runtime mounts [`RemoteWriteReceiver::receive`], ruling
//!   R21.7). It answers **only after the Fabric acknowledged the events**:
//!   204 then, 503 (Prometheus retries) on a transient Fabric error, 429 with
//!   `Retry-After` when throttled, 400 (Prometheus drops) for a body that is
//!   not remote write 1.0. Each request becomes one `metric` event per
//!   series, its data `{labels, samples: [{value, timestamp}]}`; the id is a
//!   digest of the labels and samples, so a resent request is deduplicated
//!   by `ingest`. This is a push source: it has no splits, and the pull
//!   `Pipeline` does not drive it.
//! * **Sender (sink).** Metric events (the receiver's shape, or one sample as
//!   `{labels, value, timestamp}`) are sent to a remote-write URL in
//!   requests of at most `batch` series, labels sorted by name as the
//!   protocol requires. Prometheus accepts an identical sample twice, so a
//!   retried batch is harmless.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, Method, Response, StatusCode};
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder, ErrorClass,
    EventBatch, FabricWriter, InstanceKey, Position, Sink, SinkAck, SinkFactory, SinkHandle,
    SourceBatch, SourceFactory, SplitEnumerator,
};
use prost::Message;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;

use crate::client::{AuthSpec, BasicAuth, RestClient, classify, hex, with_context};
use crate::template::data_json;

/// `prometheus.WriteRequest` (remote write 1.0).
#[derive(Clone, PartialEq, prost::Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
    #[prost(message, repeated, tag = "3")]
    pub metadata: Vec<MetricMetadata>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    /// Milliseconds since the epoch.
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct MetricMetadata {
    #[prost(int32, tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub metric_family_name: String,
    #[prost(string, tag = "4")]
    pub help: String,
    #[prost(string, tag = "5")]
    pub unit: String,
}

/// The receiver's route for an instance.
pub fn route_path(namespace: u64, instance: &str) -> String {
    format!("/v1/namespaces/{namespace}/fabric/prometheus/{instance}/write")
}

/// Snappy (block format) protobuf bytes of a request.
pub fn encode(req: &WriteRequest) -> Result<Vec<u8>, ConnectorError> {
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .map_err(|e| ConnectorError::fatal(format!("snappy: {e}")))
}

/// Decode a remote-write body.
pub fn decode(body: &[u8]) -> Result<WriteRequest, String> {
    let raw = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| format!("not snappy: {e}"))?;
    WriteRequest::decode(raw.as_slice()).map_err(|e| format!("not a WriteRequest: {e}"))
}

fn sorted_labels(ts: &TimeSeries) -> Vec<(&str, &str)> {
    let mut l: Vec<_> = ts
        .labels
        .iter()
        .map(|l| (l.name.as_str(), l.value.as_str()))
        .collect();
    l.sort();
    l
}

/// `name{a="b",…}` of a series.
pub fn series_key(ts: &TimeSeries) -> String {
    let labels = sorted_labels(ts);
    let name = labels
        .iter()
        .find(|(k, _)| *k == "__name__")
        .map_or("", |(_, v)| *v);
    let rest: Vec<String> = labels
        .iter()
        .filter(|(k, _)| *k != "__name__")
        .map(|(k, v)| format!("{k}={v:?}"))
        .collect();
    format!("{name}{{{}}}", rest.join(","))
}

/// The `metric` event of one series.
pub fn series_event(builder: &EnvelopeBuilder, ts: &TimeSeries) -> CloudEvent {
    let labels = sorted_labels(ts);
    let mut digest = Sha256::new();
    for (k, v) in &labels {
        digest.update(k.as_bytes());
        digest.update([0]);
        digest.update(v.as_bytes());
        digest.update([0]);
    }
    for s in &ts.samples {
        digest.update(s.timestamp.to_be_bytes());
        digest.update(s.value.to_bits().to_be_bytes());
    }
    let name = labels
        .iter()
        .find(|(k, _)| *k == "__name__")
        .map_or("metric", |(_, v)| *v);
    let label_map: Map<String, Value> = labels
        .iter()
        .map(|(k, v)| ((*k).to_owned(), json!(v)))
        .collect();
    let samples: Vec<Value> = ts
        .samples
        .iter()
        .map(|s| {
            let value = serde_json::Number::from_f64(s.value)
                .map_or_else(|| json!(s.value.to_string()), Value::Number);
            json!({"value": value, "timestamp": s.timestamp})
        })
        .collect();
    let data = json!({"labels": label_map, "samples": samples});
    builder
        .event("metric", name, hex(&digest.finalize()[..16]))
        .with_subject(series_key(ts))
        .with_data(
            "application/json",
            serde_json::to_vec(&data).unwrap_or_default(),
        )
}

/// A sample's value from JSON: a number, or `NaN`/`+Inf`/`-Inf` text.
fn sample_value(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => match s.as_str() {
            "NaN" => Some(f64::NAN),
            "+Inf" | "Inf" => Some(f64::INFINITY),
            "-Inf" => Some(f64::NEG_INFINITY),
            other => other.parse().ok(),
        },
        _ => None,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// The series of one metric event.
pub fn event_series(ev: &CloudEvent) -> Result<TimeSeries, ConnectorError> {
    let data = data_json(ev)?;
    let bad = |why: &str| ConnectorError::poison(format!("event {}: {why}", ev.id));
    let labels = data["labels"]
        .as_object()
        .ok_or_else(|| bad("no `labels` object"))?;
    if !labels.contains_key("__name__") {
        return Err(bad("labels need `__name__`"));
    }
    let mut ts = TimeSeries::default();
    for (k, v) in labels {
        ts.labels.push(Label {
            name: k.clone(),
            value: v.as_str().map_or_else(|| v.to_string(), str::to_owned),
        });
    }
    ts.labels.sort_by(|a, b| a.name.cmp(&b.name));
    let one = |s: &Value| -> Result<Sample, ConnectorError> {
        Ok(Sample {
            value: sample_value(&s["value"])
                .ok_or_else(|| bad("a sample needs a numeric `value`"))?,
            timestamp: s["timestamp"].as_i64().unwrap_or_else(now_ms),
        })
    };
    match &data["samples"] {
        Value::Array(a) => {
            for s in a {
                ts.samples.push(one(s)?);
            }
        }
        _ => ts.samples.push(one(&data)?),
    }
    if ts.samples.is_empty() {
        return Err(bad("no samples"));
    }
    Ok(ts)
}

// --------------------------------------------------------------- receiver --

/// See the module documentation.
pub struct RemoteWriteReceiver {
    builder: EnvelopeBuilder,
    writer: Arc<dyn FabricWriter>,
    instance: InstanceKey,
}

impl std::fmt::Debug for RemoteWriteReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteWriteReceiver")
            .field("instance", &self.instance)
            .finish_non_exhaustive()
    }
}

fn answer(status: StatusCode, body: &str) -> Response<Bytes> {
    let mut r = Response::new(Bytes::from(body.to_owned()));
    *r.status_mut() = status;
    r
}

impl RemoteWriteReceiver {
    pub fn new(instance: &InstanceKey, writer: Arc<dyn FabricWriter>) -> Self {
        Self {
            builder: EnvelopeBuilder::new("prometheus", instance),
            writer,
            instance: instance.clone(),
        }
    }

    /// The route this receiver serves.
    pub fn path(&self) -> String {
        route_path(self.instance.namespace, &self.instance.name)
    }

    /// Handle one remote-write request (see the module documentation).
    pub async fn receive(&self, headers: &HeaderMap, body: &[u8]) -> Response<Bytes> {
        let get = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).unwrap_or("");
        if !get("content-encoding").eq_ignore_ascii_case("snappy") {
            return answer(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "remote write needs Content-Encoding: snappy",
            );
        }
        let ct = get("content-type");
        if !ct.starts_with("application/x-protobuf") || ct.contains("io.prometheus.write.v2") {
            return answer(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "remote write 1.0 (application/x-protobuf) only",
            );
        }
        let req = match decode(body) {
            Ok(r) => r,
            Err(e) => return answer(StatusCode::BAD_REQUEST, &e),
        };
        let events: Vec<CloudEvent> = req
            .timeseries
            .iter()
            .filter(|ts| !ts.samples.is_empty())
            .map(|ts| series_event(&self.builder, ts))
            .collect();
        if events.is_empty() {
            return answer(StatusCode::NO_CONTENT, "");
        }
        let batch = SourceBatch {
            events: EventBatch::Events(events),
            position: Position::new(Bytes::new(), "remote write"),
        };
        match self.writer.append(&batch).await {
            Ok(_) => answer(StatusCode::NO_CONTENT, ""),
            Err(e) => match e.class {
                ErrorClass::Throttled { after } => {
                    let mut r = answer(StatusCode::TOO_MANY_REQUESTS, "throttled");
                    if let Ok(v) = after.as_secs().max(1).to_string().parse() {
                        r.headers_mut().insert(http::header::RETRY_AFTER, v);
                    }
                    r
                }
                ErrorClass::Retryable { .. } => answer(StatusCode::SERVICE_UNAVAILABLE, "retry"),
                // Not a transient state: Prometheus would retry forever.
                _ => answer(StatusCode::BAD_REQUEST, "refused by the Fabric"),
            },
        }
    }
}

// ------------------------------------------------------------------ sink --

/// The sink's config.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PrometheusConfig {
    /// The remote-write URL, e.g. `http://prometheus:9090/api/v1/write`.
    pub url: String,
    #[serde(default)]
    pub basic: Option<BasicAuth>,
    /// A bearer token's secret name.
    #[serde(default)]
    pub bearer: Option<String>,
    #[serde(default = "default_batch")]
    pub batch: usize,
    #[serde(default = "default_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub pipeline: Option<Value>,
}

fn default_batch() -> usize {
    500
}

fn default_timeout() -> u64 {
    30_000
}

impl PrometheusConfig {
    pub fn parse(v: &Value) -> Result<Self, ConnectorError> {
        serde_json::from_value(v.clone())
            .map_err(|e| ConnectorError::fatal(format!("prometheus config: {e}")))
    }
}

/// The remote-write sink.
pub struct RemoteWriteSink {
    cfg: Arc<PrometheusConfig>,
    ctx: ConnectorContext,
    client: OnceCell<RestClient>,
}

impl std::fmt::Debug for RemoteWriteSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteWriteSink")
    }
}

impl RemoteWriteSink {
    pub fn new(cfg: Arc<PrometheusConfig>, ctx: &ConnectorContext) -> Self {
        Self {
            cfg,
            ctx: ctx.clone(),
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<&RestClient, ConnectorError> {
        self.client
            .get_or_try_init(|| async {
                let auth = match (&self.cfg.basic, &self.cfg.bearer) {
                    (Some(b), _) => AuthSpec::Basic(b.clone()),
                    (None, Some(t)) => AuthSpec::Bearer(t.clone()),
                    _ => AuthSpec::None,
                };
                RestClient::connect(
                    &self.cfg.url,
                    &auth,
                    &*self.ctx.secrets,
                    self.ctx.egress.clone(),
                    Duration::from_millis(self.cfg.request_timeout_ms),
                    "",
                )
                .await?
                .with_headers(&[
                    ("content-encoding", "snappy"),
                    ("x-prometheus-remote-write-version", "0.1.0"),
                    ("user-agent", "loams-flow-prometheus/1"),
                ])
            })
            .await
    }
}

#[async_trait]
impl Sink for RemoteWriteSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let EventBatch::Events(events) = batch else {
            return Err(ConnectorError::poison(
                "the remote-write sink takes metric events",
            ));
        };
        let series = events
            .iter()
            .map(event_series)
            .collect::<Result<Vec<_>, _>>()?;
        let client = self.client().await?;
        for chunk in series.chunks(self.cfg.batch.max(1)) {
            let body = encode(&WriteRequest {
                timeseries: chunk.to_vec(),
                metadata: Vec::new(),
            })?;
            self.ctx.observe.api_call(&self.ctx.instance);
            let resp = client
                .send(
                    Method::POST,
                    "",
                    Bytes::from(body),
                    Some("application/x-protobuf"),
                    &[],
                )
                .await?;
            if let Some(e) = classify(&resp) {
                return Err(with_context(e, "remote write"));
            }
        }
        Ok(SinkAck {
            rows: events.len() as u64,
            bytes: events.iter().map(|e| e.size_bytes() as u64).sum(),
        })
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

struct RemoteWriteSinkFactory {
    cfg: Arc<PrometheusConfig>,
    ctx: ConnectorContext,
}

impl std::fmt::Debug for RemoteWriteSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteWriteSinkFactory")
    }
}

impl SinkFactory for RemoteWriteSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(RemoteWriteSink::new(
            self.cfg.clone(),
            &self.ctx,
        ))))
    }
}

/// `runtime.ref: loams_connector_protocols::prometheus`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Prometheus;

impl Prometheus {
    /// The receiver of a source instance, appending to `writer`.
    pub fn receiver(ctx: &ConnectorContext, writer: Arc<dyn FabricWriter>) -> RemoteWriteReceiver {
        RemoteWriteReceiver::new(&ctx.instance, writer)
    }
}

impl ConnectorFactory for Prometheus {
    fn id(&self) -> &'static str {
        "prometheus"
    }

    /// A push source: see [`Prometheus::receiver`] (ruling R21.7).
    fn source(
        &self,
        _ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        Ok(None)
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Ok(Some(Box::new(RemoteWriteSinkFactory {
            cfg: Arc::new(PrometheusConfig::parse(&ctx.config)?),
            ctx: ctx.clone(),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series() -> TimeSeries {
        TimeSeries {
            labels: vec![
                Label {
                    name: "job".into(),
                    value: "api".into(),
                },
                Label {
                    name: "__name__".into(),
                    value: "up".into(),
                },
            ],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1_700_000_000_000,
            }],
        }
    }

    #[test]
    fn snappy_protobuf_round_trip() {
        let req = WriteRequest {
            timeseries: vec![series()],
            metadata: vec![],
        };
        let bytes = encode(&req).expect("encode");
        assert_eq!(decode(&bytes).expect("decode"), req);
        assert!(decode(b"not snappy").is_err());
    }

    #[test]
    fn events_are_stable_and_invertible() {
        let b = EnvelopeBuilder::new("prometheus", &InstanceKey::new(1, "prom"));
        let ev = series_event(&b, &series());
        assert_eq!(ev.ty, "io.loams.dev.flow.prometheus.metric.v1");
        assert_eq!(ev.subject.as_deref(), Some("up{job=\"api\"}"));
        assert_eq!(series_event(&b, &series()).id, ev.id);
        let back = event_series(&ev).expect("series");
        assert_eq!(back.labels[0].name, "__name__");
        assert_eq!(back.samples, series().samples);
        let one = CloudEvent::new("e", "/s", "t").with_data(
            "application/json",
            r#"{"labels":{"__name__":"x"},"value":"+Inf","timestamp":5}"#,
        );
        assert_eq!(
            event_series(&one).expect("one").samples[0].value,
            f64::INFINITY
        );
        let bad = CloudEvent::new("e", "/s", "t").with_data("application/json", r#"{"labels":{}}"#);
        assert_eq!(
            event_series(&bad).expect_err("no name").class,
            ErrorClass::Poison
        );
        assert_eq!(
            route_path(7, "prom"),
            "/v1/namespaces/7/fabric/prometheus/prom/write"
        );
    }
}
