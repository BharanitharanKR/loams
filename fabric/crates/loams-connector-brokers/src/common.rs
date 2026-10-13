//! What every broker connector shares: reading the instance config, resolving
//! secrets by name, the egress check for clients that dial by themselves,
//! the pending-acknowledgement ledger a source keeps between `next` and
//! `committed`, and the `Declared` delivery a factory reports for a config.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorError, EgressCheck, Position, Secret, SecretRef,
    SecretResolver,
};
use serde::de::DeserializeOwned;

/// Parse an instance config into a connector's typed config. Unknown keys are
/// refused by each config's `deny_unknown_fields`, so a misspelt setting is
/// an error rather than a silent default; the error names the key, never a
/// value (a value may be a secret someone pasted by mistake).
pub fn parse_config<T: DeserializeOwned>(
    connector: &str,
    config: &serde_json::Value,
) -> Result<T, ConnectorError> {
    let mut value = config.clone();
    // `pipeline.*` is the SDK's (`PipelineConfig::with_overrides`), not ours.
    if let Some(obj) = value.as_object_mut() {
        obj.remove("pipeline");
        obj.remove("evolution");
        obj.remove("lossy");
    }
    serde_json::from_value(value).map_err(|e| {
        ConnectorError::fatal(format!(
            "{connector} config: {}",
            scrub_serde_error(&e.to_string())
        ))
    })
}

/// serde's messages quote the offending value for some errors (`invalid
/// value: string "…"`). Keep the message up to the first quote.
fn scrub_serde_error(msg: &str) -> String {
    match msg.find(['"', '`']) {
        Some(i) if msg[..i].contains("unknown field") => {
            // `unknown field `x`, expected …`: the key is not a secret.
            msg.split(", expected").next().unwrap_or(msg).to_owned()
        }
        Some(i) => format!("{}…", msg[..i].trim_end()),
        None => msg.to_owned(),
    }
}

/// Resolve a secret *name* from the config through the instance's resolver
/// (which records it in the scrubber).
pub async fn resolve(
    secrets: &Arc<dyn SecretResolver>,
    name: &str,
) -> Result<Secret, ConnectorError> {
    secrets.resolve(&SecretRef::new(name)).await
}

/// A resolved secret as UTF-8 text.
pub async fn resolve_str(
    secrets: &Arc<dyn SecretResolver>,
    name: &str,
) -> Result<String, ConnectorError> {
    let s = resolve(secrets, name).await?;
    s.expose_str()
        .map(str::to_owned)
        .ok_or_else(|| ConnectorError::auth(format!("secret {name} is not UTF-8")))
}

/// The egress check for a client library that opens its own sockets (lapin,
/// async-nats, pulsar): the host is checked before DNS, then every address it
/// resolves to, before the client is built (R18.4, as R9.2 does for
/// librdkafka). A host that resolves to any refused address is refused.
pub async fn egress_precheck(
    host: &str,
    port: u16,
    egress: &dyn EgressCheck,
) -> Result<(), ConnectorError> {
    egress
        .check_host(host, port)
        .map_err(|e| ConnectorError::fatal(e.to_string()))?;
    let addrs: Vec<_> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| ConnectorError::retryable(format!("resolving {host}: {e}")))?
        .collect();
    if addrs.is_empty() {
        return Err(ConnectorError::retryable(format!(
            "{host} resolved to no address"
        )));
    }
    for addr in &addrs {
        egress
            .check_addr(addr)
            .map_err(|e| ConnectorError::fatal(format!("{host}:{port}: {e}")))?;
    }
    Ok(())
}

/// `host:port` from an address, with a default port.
pub fn host_port(addr: &str, default_port: u16) -> Result<(String, u16), ConnectorError> {
    let addr = addr.trim();
    if addr.is_empty() {
        return Err(ConnectorError::fatal("an empty broker address"));
    }
    // [v6]:port
    if let Some(rest) = addr.strip_prefix('[') {
        let (host, tail) = rest
            .split_once(']')
            .ok_or_else(|| ConnectorError::fatal("an unterminated IPv6 address"))?;
        let port = match tail.strip_prefix(':') {
            Some(p) => p
                .parse()
                .map_err(|_| ConnectorError::fatal("a broker port is not a number"))?,
            None => default_port,
        };
        return Ok((host.to_owned(), port));
    }
    match addr.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => Ok((
            h.to_owned(),
            p.parse()
                .map_err(|_| ConnectorError::fatal("a broker port is not a number"))?,
        )),
        _ => Ok((addr.to_owned(), default_port)),
    }
}

/// The host and port of a URL such as `amqp://user@host:5672/vhost`,
/// `nats://host`, `pulsar+ssl://host:6651` or `wss://host/path`, and whether
/// its scheme is a TLS one. User information is never returned.
pub fn url_host_port(url: &str, default_port: u16) -> Result<(String, u16), ConnectorError> {
    let rest = url
        .split_once("://")
        .map(|(_, r)| r)
        .ok_or_else(|| ConnectorError::fatal("a broker URL needs a scheme"))?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port_part = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    host_port(host_port_part, default_port)
}

/// True when the URL names user information (`user:pass@`). Configs carry
/// credentials as secret names, never in a URL (Global Constraints).
pub fn url_has_userinfo(url: &str) -> bool {
    url.split_once("://")
        .map(|(_, r)| r.split(['/', '?', '#']).next().unwrap_or_default())
        .is_some_and(|authority| authority.contains('@'))
}

/// What a factory says a config delivers: the manifest's declaration is the
/// strongest any config reaches; some configs reach less (core NATS, a
/// WebSocket source), and the factory says so (R18.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Declared {
    pub source_resumable: bool,
    pub source: loams_connector_sdk::Delivery,
    pub sink: loams_connector_sdk::Delivery,
}

/// Messages a source has handed to the pipeline but not yet acknowledged
/// upstream, keyed by the source's own sequence number (the number its
/// `Position` carries). `committed(upto)` takes every entry at or below it.
#[derive(Debug)]
pub struct Pending<T> {
    next: u64,
    entries: BTreeMap<u64, T>,
}

impl<T> Default for Pending<T> {
    fn default() -> Self {
        Self {
            next: 1,
            entries: BTreeMap::new(),
        }
    }
}

impl<T> Pending<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `item`; returns its sequence number (1, 2, …).
    pub fn push(&mut self, item: T) -> u64 {
        let seq = self.next;
        self.next += 1;
        self.entries.insert(seq, item);
        seq
    }

    /// The sequence number the next `push` will return.
    pub fn peek_next(&self) -> u64 {
        self.next
    }

    /// Remove and return every entry at or below `upto`, in order.
    pub fn take_upto(&mut self, upto: u64) -> Vec<(u64, T)> {
        let rest = self.entries.split_off(&(upto.saturating_add(1)));
        let taken = std::mem::replace(&mut self.entries, rest);
        taken.into_iter().collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Forget everything (the connection that owned the deliveries is gone;
    /// the broker redelivers them).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// A source position: the source's session (a connection generation, so a
/// position from a dropped connection never acknowledges on a new one) and
/// its sequence number within that session, with an optional
/// broker-assigned resume point (a JetStream stream sequence, a Pulsar
/// message id) shown in `display`.
pub fn session_position(session: u64, seq: u64, display: impl Into<String>) -> Position {
    let mut opaque = Vec::with_capacity(16);
    opaque.extend_from_slice(&session.to_be_bytes());
    opaque.extend_from_slice(&seq.to_be_bytes());
    Position::new(opaque, display)
}

/// The `(session, seq)` of a [`session_position`].
pub fn parse_session_position(p: &Position) -> Option<(u64, u64)> {
    let b = p.opaque.as_ref();
    if b.len() < 16 {
        return None;
    }
    let s = u64::from_be_bytes(b[..8].try_into().ok()?);
    let q = u64::from_be_bytes(b[8..16].try_into().ok()?);
    Some((s, q))
}

/// A process-unique session number for [`session_position`].
pub fn new_session() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or_default();
    // The time keeps sessions distinct across process restarts; the counter
    // keeps them distinct within one.
    (t << 12) ^ NEXT.fetch_add(1, Ordering::Relaxed)
}

/// How long a `next` call waits for the first message when none is buffered
/// (a source must return `None` promptly when nothing is available, so the
/// pipeline's stop and budget checks run).
pub const DEFAULT_POLL_WAIT: Duration = Duration::from_millis(200);

/// How many messages a source may take this call: the budget's rows, capped
/// by the connector's own batch setting.
pub fn take_rows(budget: Budget, max_batch: usize) -> usize {
    budget.max_rows.min(max_batch.max(1))
}

/// The resolver, egress check and instance name a connector keeps from its
/// context (the context itself is not `Send` across awaits in every family).
#[derive(Clone)]
pub struct Env {
    pub secrets: Arc<dyn SecretResolver>,
    pub egress: Arc<dyn EgressCheck>,
    pub envelope: loams_connector_sdk::EnvelopeBuilder,
}

impl Env {
    pub fn new(connector: &str, ctx: &ConnectorContext) -> Self {
        Self {
            secrets: ctx.secrets.clone(),
            egress: ctx.egress.clone(),
            envelope: loams_connector_sdk::EnvelopeBuilder::new(connector, &ctx.instance),
        }
    }
}

impl std::fmt::Debug for Env {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Env").finish_non_exhaustive()
    }
}

/// A fixed set of splits named by the config (queues, subjects, topics,
/// URLs): one split per name, whose id is the name, so a checkpoint is found
/// again after a restart.
#[derive(Debug, Clone)]
pub struct StaticSplits {
    names: Vec<String>,
}

impl StaticSplits {
    pub fn new(names: Vec<String>) -> Self {
        Self { names }
    }
}

#[async_trait::async_trait]
impl loams_connector_sdk::SplitEnumerator for StaticSplits {
    async fn discover(
        &mut self,
        _known: &[loams_connector_sdk::SplitState],
    ) -> Result<Vec<loams_connector_sdk::Split>, ConnectorError> {
        Ok(self
            .names
            .iter()
            .map(|n| loams_connector_sdk::Split::new(n.as_str(), serde_json::json!({ "name": n })))
            .collect())
    }
}

/// The name a [`StaticSplits`] split carries.
pub fn split_name(split: &loams_connector_sdk::Split) -> Result<String, ConnectorError> {
    split.spec["name"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| ConnectorError::fatal(format!("split {} has no name", split.id)))
}

/// The pipeline settings an instance runs with (`pipeline.*` over the SDK's
/// defaults), for connectors that size a broker-side window from them.
pub fn pipeline_config(
    config: &serde_json::Value,
) -> Result<loams_connector_sdk::PipelineConfig, ConnectorError> {
    loams_connector_sdk::PipelineConfig::default().with_overrides(config)
}

/// The broker-side window a source may hold unacknowledged (RabbitMQ's
/// prefetch, AMQP 1.0 link credit, MQTT's Receive Maximum): what the
/// pipeline can have in flight, `max_in_flight_batches` batches of at most
/// `min(max_batch_rows, max_batch)` messages, capped at `cap`. Messages are
/// acknowledged only after the Fabric acknowledges them, so once the window
/// is full the broker stops delivering: the budget is the backpressure, and
/// the source never buffers beyond it (R18.5).
pub fn window_for(
    pipeline: &loams_connector_sdk::PipelineConfig,
    max_batch: usize,
    explicit: Option<u32>,
    cap: u32,
) -> u32 {
    if let Some(n) = explicit {
        return n.clamp(1, cap);
    }
    let per_batch = pipeline.max_batch_rows.min(max_batch.max(1));
    let window = per_batch.saturating_mul(pipeline.max_in_flight_batches.max(1));
    u32::try_from(window).unwrap_or(u32::MAX).clamp(1, cap)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use loams_connector_sdk::{AllowAll, DenyLocal, ErrorClass};

    #[test]
    fn window_is_the_pipeline_budget() {
        let p = loams_connector_sdk::PipelineConfig::default();
        assert_eq!(window_for(&p, 1000, None, u32::from(u16::MAX)), 4000);
        assert_eq!(window_for(&p, 100_000, None, u32::from(u16::MAX)), 65_535);
        assert_eq!(window_for(&p, 1000, Some(10), 65_535), 10);
        let small = pipeline_config(&serde_json::json!({
            "pipeline": {"max_batch_rows": 50, "max_in_flight_batches": 2}
        }))
        .unwrap();
        assert_eq!(window_for(&small, 1000, None, 65_535), 100);
    }

    #[test]
    fn pending_takes_up_to_and_including() {
        let mut p = Pending::new();
        let a = p.push("a");
        let b = p.push("b");
        let c = p.push("c");
        assert_eq!((a, b, c), (1, 2, 3));
        assert_eq!(p.take_upto(2), vec![(1, "a"), (2, "b")]);
        assert_eq!(p.len(), 1);
        assert!(p.take_upto(2).is_empty());
        assert_eq!(p.take_upto(u64::MAX), vec![(3, "c")]);
        assert!(p.is_empty());
    }

    #[test]
    fn session_positions_round_trip() {
        let p = session_position(7, 42, "orders@42");
        assert_eq!(parse_session_position(&p), Some((7, 42)));
        assert_eq!(p.display, "orders@42");
        assert_eq!(parse_session_position(&Position::from_u64(1)), None);
    }

    #[test]
    fn addresses_and_urls() {
        assert_eq!(host_port("h:1", 2).unwrap(), ("h".into(), 1));
        assert_eq!(host_port("h", 2).unwrap(), ("h".into(), 2));
        assert_eq!(host_port("[::1]:9", 2).unwrap(), ("::1".into(), 9));
        assert_eq!(
            url_host_port("amqps://u@broker.example:5671/%2f", 5672).unwrap(),
            ("broker.example".into(), 5671)
        );
        assert_eq!(
            url_host_port("wss://x.example/feed?a=1", 443).unwrap(),
            ("x.example".into(), 443)
        );
        assert!(url_has_userinfo("amqp://user:pw@h/"));
        assert!(!url_has_userinfo("amqp://h/a@b"));
        assert!(url_host_port("no-scheme", 1).is_err());
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct Cfg {
        url: String,
    }

    #[test]
    fn config_errors_never_echo_values() {
        let e = parse_config::<Cfg>("t", &serde_json::json!({"url": 5})).unwrap_err();
        assert_eq!(e.class, ErrorClass::Fatal);
        let e = parse_config::<Cfg>(
            "t",
            &serde_json::json!({"url": "x", "password": "hunter2-secret"}),
        )
        .unwrap_err();
        assert!(e.message.contains("unknown field"), "{}", e.message);
        assert!(!e.message.contains("hunter2"), "{}", e.message);
        // `pipeline` belongs to the SDK and is not refused here.
        parse_config::<Cfg>("t", &serde_json::json!({"url": "x", "pipeline": {}})).unwrap();
    }

    #[tokio::test]
    async fn precheck_refuses_loopback_after_dns() {
        egress_precheck("localhost", 1, &AllowAll).await.unwrap();
        let e = egress_precheck("localhost", 1, &DenyLocal)
            .await
            .unwrap_err();
        assert_eq!(e.class, ErrorClass::Fatal);
    }
}
