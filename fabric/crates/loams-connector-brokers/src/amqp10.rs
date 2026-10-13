//! AMQP 1.0 (OR-22), with `fe2o3-amqp`: ActiveMQ Artemis and Classic
//! (`activemq-jms`) and Azure Service Bus (`azure-service-bus`).
//!
//! - **Source:** one split per address (a queue; on Service Bus also
//!   `<topic>/Subscriptions/<subscription>`). The receiver link holds its
//!   deliveries **unsettled**, and a delivery is settled with `accepted` only
//!   after the Fabric acknowledges it (`committed`). On Service Bus that is
//!   peek-lock: the lock is completed after the Fabric. Both ActiveMQ brokers
//!   force `rcv-settle-mode` first (spike §5.4), and nothing here relies on
//!   `second`. Link credit is the pipeline's budget (`common::window_for`):
//!   fe2o3 refills it only as deliveries are settled, so a slow Fabric stops
//!   the broker from sending. A crash drops the link with deliveries
//!   unsettled, and the broker redelivers them (`artemis_amqp10_redelivers_on_kill`,
//!   `servicebus_peek_lock_redelivery`).
//! - **Sink:** a sender link, unsettled; a batch is acknowledged when every
//!   message's outcome is `accepted`. `rejected` is poison; `released` and
//!   `modified` are retryable.
//! - **Auth:** ActiveMQ uses SASL PLAIN (or ANONYMOUS) and, for `mtls`, a
//!   client certificate on the TLS socket. Service Bus uses claims-based
//!   security on `$cbs` (`fe2o3-amqp-cbs`) with a SAS token from a connection
//!   string (`cs`) or an Entra ID token from the client-credentials grant
//!   (`aad`), renewed before it expires.
//! - The socket is the SDK's (`sdk::net::connect` / `connect_tls`), so the
//!   egress check runs after DNS on the address actually dialled.
//! - CloudEvents: application properties with the AMQP binding's
//!   `cloudEvents:` prefix on the way out; any recognised prefix on the way in.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::link::CreditMode;
use fe2o3_amqp::link::delivery::DeliveryInfo;
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::messaging::{
    ApplicationProperties, Body, Data, Message, MessageId, Outcome, Properties,
};
use fe2o3_amqp::types::primitives::{SimpleValue, Symbol, Timestamp, Value};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_cbs::client::CbsClient;
use fe2o3_amqp_cbs::token::CbsToken;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, Delivery, EventBatch,
    Position, Sink, SinkAck, SinkFactory, SinkHandle, Source, SourceBatch, SourceFactory, Split,
    SplitEnumerator,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::common::{
    self, Declared, Env, Pending, StaticSplits, new_session, parse_session_position,
    session_position, split_name, url_has_userinfo, url_host_port,
};
use crate::mapping::{self, Incoming};
use crate::tls::TlsSettings;

pub const ACTIVEMQ_ID: &str = "activemq-jms";
pub const SERVICE_BUS_ID: &str = "azure-service-bus";

/// The CloudEvents AMQP binding's application-property prefix.
pub const CE_PREFIX: &str = "cloudEvents:";

/// Which system the factory serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    ActiveMq,
    ServiceBus,
}

impl Flavor {
    pub fn id(self) -> &'static str {
        match self {
            Flavor::ActiveMq => ACTIVEMQ_ID,
            Flavor::ServiceBus => SERVICE_BUS_ID,
        }
    }
}

// ------------------------------------------------------------------ config --

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveMqConfig {
    /// `amqp://host:5672` or `amqps://host:5671`, without credentials.
    pub url: String,
    #[serde(default)]
    pub username: Option<String>,
    /// The secret name of the password (SASL PLAIN). Without a username,
    /// SASL ANONYMOUS.
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub tls: TlsSettings,
    #[serde(default)]
    pub source: Option<Amqp10Source>,
    #[serde(default)]
    pub sink: Option<Amqp10Sink>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceBusConfig {
    /// The secret name of a SAS connection string
    /// (`Endpoint=sb://…;SharedAccessKeyName=…;SharedAccessKey=…`, optionally
    /// `UseDevelopmentEmulator=true`): the manifest's `cs`.
    #[serde(default)]
    pub connection_string: Option<String>,
    /// The namespace host (`<ns>.servicebus.windows.net`) for Entra ID.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Entra ID client credentials: the manifest's `aad`.
    #[serde(default)]
    pub entra: Option<EntraConfig>,
    /// How long a SAS token lives before it is renewed (default 1 h).
    #[serde(default = "default_token_ttl_s")]
    pub token_ttl_s: u64,
    #[serde(default)]
    pub tls: TlsSettings,
    #[serde(default)]
    pub source: Option<Amqp10Source>,
    #[serde(default)]
    pub sink: Option<Amqp10Sink>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntraConfig {
    pub tenant_id: String,
    pub client_id: String,
    /// The secret name of the client secret.
    pub client_secret: String,
    /// The token endpoint's origin (default `https://login.microsoftonline.com`).
    #[serde(default = "default_authority")]
    pub authority: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Amqp10Source {
    /// One split per address.
    pub addresses: Vec<String>,
    /// Link credit; by default the pipeline's budget.
    #[serde(default)]
    pub credit: Option<u32>,
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,
    #[serde(default = "default_poll_wait_ms")]
    pub poll_wait_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Amqp10Sink {
    pub address: String,
    #[serde(default = "default_send_timeout_ms")]
    pub send_timeout_ms: u64,
}

fn default_max_batch() -> usize {
    1000
}
fn default_poll_wait_ms() -> u64 {
    common::DEFAULT_POLL_WAIT.as_millis() as u64
}
fn default_send_timeout_ms() -> u64 {
    30_000
}
fn default_token_ttl_s() -> u64 {
    3600
}
fn default_authority() -> String {
    "https://login.microsoftonline.com".to_owned()
}

/// A parsed config of either flavor.
#[derive(Debug, Clone)]
pub enum Amqp10Config {
    ActiveMq(ActiveMqConfig),
    ServiceBus(ServiceBusConfig),
}

impl Amqp10Config {
    pub fn parse(flavor: Flavor, config: &serde_json::Value) -> Result<Self, ConnectorError> {
        match flavor {
            Flavor::ActiveMq => {
                let c: ActiveMqConfig = common::parse_config(ACTIVEMQ_ID, config)?;
                if !(c.url.starts_with("amqp://") || c.url.starts_with("amqps://")) {
                    return Err(ConnectorError::fatal(
                        "activemq-jms: url must be amqp:// or amqps://",
                    ));
                }
                if url_has_userinfo(&c.url) {
                    return Err(ConnectorError::fatal(
                        "activemq-jms: credentials go in `username` and `password` (a secret name), not in the url",
                    ));
                }
                if c.password.is_some() && c.username.is_none() {
                    return Err(ConnectorError::fatal(
                        "activemq-jms: a password needs a username",
                    ));
                }
                check_dirs(ACTIVEMQ_ID, c.source.as_ref())?;
                Ok(Amqp10Config::ActiveMq(c))
            }
            Flavor::ServiceBus => {
                let c: ServiceBusConfig = common::parse_config(SERVICE_BUS_ID, config)?;
                match (&c.connection_string, &c.namespace, &c.entra) {
                    (Some(_), None, None) | (None, Some(_), Some(_)) => {}
                    _ => {
                        return Err(ConnectorError::fatal(
                            "azure-service-bus: give either `connection_string` (a secret name) or `namespace` with `entra`",
                        ));
                    }
                }
                check_dirs(SERVICE_BUS_ID, c.source.as_ref())?;
                Ok(Amqp10Config::ServiceBus(c))
            }
        }
    }

    fn source(&self) -> Option<&Amqp10Source> {
        match self {
            Amqp10Config::ActiveMq(c) => c.source.as_ref(),
            Amqp10Config::ServiceBus(c) => c.source.as_ref(),
        }
    }

    fn sink(&self) -> Option<&Amqp10Sink> {
        match self {
            Amqp10Config::ActiveMq(c) => c.sink.as_ref(),
            Amqp10Config::ServiceBus(c) => c.sink.as_ref(),
        }
    }

    fn id(&self) -> &'static str {
        match self {
            Amqp10Config::ActiveMq(_) => ACTIVEMQ_ID,
            Amqp10Config::ServiceBus(_) => SERVICE_BUS_ID,
        }
    }
}

fn check_dirs(id: &str, source: Option<&Amqp10Source>) -> Result<(), ConnectorError> {
    if source.is_some_and(|s| s.addresses.is_empty()) {
        return Err(ConnectorError::fatal(format!(
            "{id}: source.addresses is empty"
        )));
    }
    Ok(())
}

/// What an AMQP 1.0 config delivers: the broker holds unsettled deliveries
/// (a restart resumes without loss); the sink does not deduplicate.
pub fn declared() -> Declared {
    Declared {
        source_resumable: true,
        source: Delivery::AtLeastOnce,
        sink: Delivery::AtLeastOnce,
    }
}

// ------------------------------------------------------- Service Bus auth --

/// A Service Bus connection string's parts. `Debug` omits the key.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectionString {
    pub host: String,
    pub key_name: String,
    pub key: String,
    pub emulator: bool,
    pub entity_path: Option<String>,
}

impl std::fmt::Debug for ConnectionString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionString")
            .field("host", &self.host)
            .field("key_name", &self.key_name)
            .field("emulator", &self.emulator)
            .finish_non_exhaustive()
    }
}

impl ConnectionString {
    /// Parse `Endpoint=sb://host/;SharedAccessKeyName=…;SharedAccessKey=…`.
    /// Errors never quote the string (it holds the key).
    pub fn parse(s: &str) -> Result<Self, ConnectorError> {
        let mut host = None;
        let mut key_name = None;
        let mut key = None;
        let mut emulator = false;
        let mut entity_path = None;
        for part in s.split(';').map(str::trim).filter(|p| !p.is_empty()) {
            let Some((k, v)) = part.split_once('=') else {
                continue;
            };
            match k.trim().to_ascii_lowercase().as_str() {
                "endpoint" => {
                    let rest = v.split_once("://").map_or(v, |(_, r)| r);
                    // host[:port]: the emulator listens on a mapped port.
                    let h = rest.split('/').next().unwrap_or_default();
                    host = Some(h.to_owned());
                }
                "sharedaccesskeyname" => key_name = Some(v.to_owned()),
                "sharedaccesskey" => key = Some(v.to_owned()),
                "usedevelopmentemulator" => emulator = v.eq_ignore_ascii_case("true"),
                "entitypath" => entity_path = Some(v.to_owned()),
                _ => {}
            }
        }
        match (host, key_name, key) {
            (Some(host), Some(key_name), Some(key)) if !host.is_empty() => Ok(Self {
                host,
                key_name,
                key,
                emulator,
                entity_path,
            }),
            _ => Err(ConnectorError::auth(
                "azure-service-bus: the connection string needs Endpoint, SharedAccessKeyName and SharedAccessKey",
            )),
        }
    }
}

/// HMAC-SHA256 (RFC 2104) over `sha2`.
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(msg)
        .finalize();
    Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize()
        .into()
}

/// Percent-encoding as `encodeURIComponent` does it (unreserved characters
/// kept, uppercase hex), which is what Service Bus signs.
pub fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A Service Bus SAS token for `resource`, valid until `expiry` (seconds
/// since the epoch): `SharedAccessSignature sr=…&sig=…&se=…&skn=…`, where
/// `sig` is the base64 HMAC-SHA256 of `<encoded resource>\n<expiry>` with the
/// key's bytes.
pub fn sas_token(resource: &str, key_name: &str, key: &str, expiry: u64) -> String {
    let sr = uri_encode(resource);
    let to_sign = format!("{sr}\n{expiry}");
    let sig = base64::engine::general_purpose::STANDARD
        .encode(hmac_sha256(key.as_bytes(), to_sign.as_bytes()));
    format!(
        "SharedAccessSignature sr={sr}&sig={}&se={expiry}&skn={}",
        uri_encode(&sig),
        uri_encode(key_name)
    )
}

fn now_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// Fetch an Entra ID access token for Service Bus (client-credentials
/// grant, scope `https://servicebus.azure.net/.default`) through the SDK's
/// egress-checked HTTP client. Returns the token and its lifetime.
pub async fn entra_token(
    entra: &EntraConfig,
    env: &Env,
) -> Result<(String, Duration), ConnectorError> {
    let secret = common::resolve_str(&env.secrets, &entra.client_secret).await?;
    let url = format!(
        "{}/{}/oauth2/v2.0/token",
        entra.authority.trim_end_matches('/'),
        uri_encode(&entra.tenant_id)
    );
    let body = format!(
        "grant_type=client_credentials&client_id={}&client_secret={}&scope={}",
        uri_encode(&entra.client_id),
        uri_encode(&secret),
        uri_encode("https://servicebus.azure.net/.default")
    );
    let req = http::Request::post(url)
        .header(
            http::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(Bytes::from(body))
        .map_err(|_| ConnectorError::fatal("azure-service-bus: entra.authority is not a URL"))?;
    let resp = loams_connector_sdk::http::client(env.egress.clone())
        .send(req)
        .await?;
    if let Some(e) = loams_connector_sdk::http::error_for_response(&resp) {
        return Err(match resp.status().as_u16() {
            400 | 401 | 403 => ConnectorError::auth(format!(
                "azure-service-bus: Entra ID refused the client credentials ({})",
                resp.status()
            )),
            _ => e,
        });
    }
    #[derive(Deserialize)]
    struct TokenResponse {
        access_token: String,
        #[serde(default)]
        expires_in: Option<u64>,
    }
    let t: TokenResponse = serde_json::from_slice(resp.body()).map_err(|_| {
        ConnectorError::auth("azure-service-bus: Entra ID's token response is not JSON")
    })?;
    Ok((
        t.access_token,
        Duration::from_secs(t.expires_in.unwrap_or(3600)),
    ))
}

// ------------------------------------------------------------- connection --

/// An open connection and session (and, on Service Bus, the CBS client and
/// when its token runs out).
struct Conn {
    conn: ConnectionHandle<()>,
    session: SessionHandle<()>,
    cbs: Option<CbsClient>,
    renew_at: Option<SystemTime>,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conn").finish_non_exhaustive()
    }
}

fn amqp_err(context: &str, e: impl std::fmt::Display) -> ConnectorError {
    let text = e.to_string();
    let lower = text.to_ascii_lowercase();
    if lower.contains("unauthorized")
        || lower.contains("unauthorised")
        || lower.contains("sasl")
        || lower.contains("authentication")
        || lower.contains("security")
    {
        ConnectorError::auth(format!("{context}: {text}"))
    } else if lower.contains("not-found") || lower.contains("not found") {
        ConnectorError::fatal(format!("{context}: {text}"))
    } else {
        ConnectorError::retryable(format!("{context}: {text}"))
    }
}

/// The host, port, TLS flag and SASL profile of a config.
async fn endpoint(
    cfg: &Amqp10Config,
    env: &Env,
) -> Result<(String, u16, bool, SaslProfile, Option<ConnectionString>), ConnectorError> {
    match cfg {
        Amqp10Config::ActiveMq(c) => {
            let tls = c.url.starts_with("amqps://");
            let (host, port) = url_host_port(&c.url, if tls { 5671 } else { 5672 })?;
            let sasl = match (&c.username, &c.password) {
                (Some(u), Some(p)) => SaslProfile::Plain {
                    username: u.clone(),
                    password: common::resolve_str(&env.secrets, p).await?,
                },
                (Some(u), None) => SaslProfile::Plain {
                    username: u.clone(),
                    password: String::new(),
                },
                _ => SaslProfile::Anonymous,
            };
            Ok((host, port, tls, sasl, None))
        }
        Amqp10Config::ServiceBus(c) => {
            if let Some(name) = &c.connection_string {
                let cs = ConnectionString::parse(&common::resolve_str(&env.secrets, name).await?)?;
                let (host, port, tls) = if cs.emulator {
                    let (h, p) = common::host_port(&cs.host, 5672)?;
                    (h, p, false)
                } else {
                    let (h, p) = common::host_port(&cs.host, 5671)?;
                    (h, p, true)
                };
                Ok((host, port, tls, SaslProfile::Anonymous, Some(cs)))
            } else {
                let ns = c.namespace.clone().unwrap_or_default();
                let (host, port) = common::host_port(&ns, 5671)?;
                Ok((host, port, true, SaslProfile::Anonymous, None))
            }
        }
    }
}

async fn open(cfg: &Amqp10Config, env: &Env, container: &str) -> Result<Conn, ConnectorError> {
    let (host, port, tls, sasl, cs) = endpoint(cfg, env).await?;
    let settings = match cfg {
        Amqp10Config::ActiveMq(c) => &c.tls,
        Amqp10Config::ServiceBus(c) => &c.tls,
    };
    let builder = Connection::builder()
        .container_id(container.to_owned())
        .hostname(host.as_str())
        .sasl_profile(sasl);
    // The SDK dials (egress after DNS) and wraps TLS itself, so fe2o3 is
    // always told `amqp`: it has no TLS of its own in this build.
    let mut conn = if tls {
        let tls_config = settings.client_config(&env.secrets).await?;
        let stream =
            loams_connector_sdk::net::connect_tls(&host, port, env.egress.as_ref(), tls_config)
                .await?;
        builder
            .open_with_stream(stream)
            .await
            .map_err(|e| amqp_err("opening the AMQP connection", e))?
    } else {
        let stream = loams_connector_sdk::net::connect(&host, port, env.egress.as_ref()).await?;
        builder
            .open_with_stream(stream)
            .await
            .map_err(|e| amqp_err("opening the AMQP connection", e))?
    };
    let session = Session::begin(&mut conn)
        .await
        .map_err(|e| amqp_err("beginning the AMQP session", e))?;
    let mut c = Conn {
        conn,
        session,
        cbs: None,
        renew_at: None,
    };
    if let Amqp10Config::ServiceBus(sb) = cfg {
        let cbs = CbsClient::attach(&mut c.session)
            .await
            .map_err(|e| amqp_err("attaching to $cbs", e))?;
        c.cbs = Some(cbs);
        put_token(&mut c, sb, cs.as_ref(), &host, env).await?;
    }
    Ok(c)
}

/// Put (or renew) the claims for the namespace on `$cbs`.
async fn put_token(
    c: &mut Conn,
    sb: &ServiceBusConfig,
    cs: Option<&ConnectionString>,
    host: &str,
    env: &Env,
) -> Result<(), ConnectorError> {
    let audience = format!("sb://{host}/");
    let (token, token_type, ttl) = match (cs, &sb.entra) {
        (Some(cs), _) => {
            let ttl = Duration::from_secs(sb.token_ttl_s.max(60));
            let expiry = now_s() + ttl.as_secs();
            (
                sas_token(&audience, &cs.key_name, &cs.key, expiry),
                "servicebus.windows.net:sastoken",
                ttl,
            )
        }
        (None, Some(entra)) => {
            let (t, ttl) = entra_token(entra, env).await?;
            (t, "jwt", ttl)
        }
        (None, None) => return Err(ConnectorError::fatal("azure-service-bus: no credentials")),
    };
    let expires = SystemTime::now() + ttl;
    let ms = expires
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default();
    let cbs = c
        .cbs
        .as_mut()
        .expect("Service Bus connections have a CBS client");
    cbs.put_token(
        audience.as_str(),
        CbsToken::new(token, token_type, Timestamp::from_milliseconds(ms)),
    )
    .await
    .map_err(|e| ConnectorError::auth(format!("azure-service-bus: put-token refused: {e}")))?;
    // Renew at four fifths of the lifetime.
    c.renew_at = Some(SystemTime::now() + ttl.mul_f32(0.8));
    Ok(())
}

async fn renew_if_due(c: &mut Conn, cfg: &Amqp10Config, env: &Env) -> Result<(), ConnectorError> {
    let Amqp10Config::ServiceBus(sb) = cfg else {
        return Ok(());
    };
    if c.renew_at.is_some_and(|at| SystemTime::now() < at) {
        return Ok(());
    }
    let (host, _, _, _, cs) = endpoint(cfg, env).await?;
    put_token(c, sb, cs.as_ref(), &host, env).await
}

async fn shut(mut c: Conn) {
    if let Some(cbs) = c.cbs.take() {
        let _ = cbs.close().await;
    }
    let _ = c.session.close().await;
    let _ = c.conn.close().await;
}

/// ActiveMQ terminus capabilities: `queue`, so Artemis and Classic route an
/// address they create on attach as anycast (a queue) rather than as a
/// multicast address that drops messages nobody subscribed to yet. Service
/// Bus entities exist already and take no capability.
fn capabilities(cfg: &Amqp10Config) -> Option<Vec<Symbol>> {
    matches!(cfg, Amqp10Config::ActiveMq(_)).then(|| vec![Symbol::from("queue")])
}

fn terminus_source(cfg: &Amqp10Config, address: &str) -> fe2o3_amqp::types::messaging::Source {
    let b = fe2o3_amqp::types::messaging::Source::builder().address(address.to_owned());
    match capabilities(cfg) {
        Some(c) => b.capabilities(c).build(),
        None => b.build(),
    }
}

fn terminus_target(cfg: &Amqp10Config, address: &str) -> fe2o3_amqp::types::messaging::Target {
    let b = fe2o3_amqp::types::messaging::Target::builder().address(address.to_owned());
    match capabilities(cfg) {
        Some(c) => b.capabilities(c).build(),
        None => b.build(),
    }
}

// ------------------------------------------------------------------ source --

struct LiveReceiver {
    conn: Conn,
    receiver: Receiver,
    session: u64,
}

pub struct Amqp10SourceTask {
    cfg: Amqp10Config,
    src: Amqp10Source,
    credit: u32,
    env: Env,
    address: String,
    link_name: String,
    live: Option<LiveReceiver>,
    pending: Pending<DeliveryInfo>,
    held: Option<(DeliveryInfo, Message<Body<Value>>)>,
}

impl std::fmt::Debug for Amqp10SourceTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Amqp10Source")
            .field("address", &self.address)
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

/// An AMQP 1.0 message's text properties, content type and body bytes.
/// A received message's text application properties, content type, body
/// bytes and message id.
pub type MessageParts = (Vec<(String, String)>, Option<String>, Bytes, Option<String>);

pub fn message_parts(m: &Message<Body<Value>>) -> MessageParts {
    let mut headers = Vec::new();
    if let Some(ap) = &m.application_properties {
        for (k, v) in ap.iter() {
            if let Some(t) = simple_text(v) {
                headers.push((k.clone(), t));
            }
        }
    }
    let content_type = m
        .properties
        .as_ref()
        .and_then(|p| p.content_type.as_ref())
        .map(|s| s.0.clone());
    let message_id = m
        .properties
        .as_ref()
        .and_then(|p| p.message_id.as_ref())
        .map(message_id_text);
    let data = match &m.body {
        Body::Data(batch) => {
            let mut out = Vec::new();
            for d in batch.iter() {
                out.extend_from_slice(&d.0);
            }
            Bytes::from(out)
        }
        Body::Value(v) => match &v.0 {
            Value::String(s) => Bytes::from(s.clone().into_bytes()),
            Value::Binary(b) => Bytes::from(b.to_vec()),
            Value::Null => Bytes::new(),
            other => Bytes::from(format!("{other:?}").into_bytes()),
        },
        Body::Sequence(seqs) => Bytes::from(format!("{seqs:?}").into_bytes()),
        Body::Empty => Bytes::new(),
    };
    (headers, content_type, data, message_id)
}

fn simple_text(v: &SimpleValue) -> Option<String> {
    Some(match v {
        SimpleValue::String(s) => s.clone(),
        SimpleValue::Symbol(s) => s.0.clone(),
        SimpleValue::Bool(b) => b.to_string(),
        SimpleValue::Int(n) => n.to_string(),
        SimpleValue::Long(n) => n.to_string(),
        SimpleValue::Uint(n) => n.to_string(),
        SimpleValue::Ulong(n) => n.to_string(),
        SimpleValue::Timestamp(t) => t.milliseconds().to_string(),
        SimpleValue::Binary(b) => std::str::from_utf8(b).ok()?.to_owned(),
        _ => return None,
    })
}

fn message_id_text(id: &MessageId) -> String {
    match id {
        MessageId::String(s) => s.clone(),
        MessageId::Ulong(n) => n.to_string(),
        MessageId::Uuid(u) => format!("{u:?}"),
        MessageId::Binary(b) => base64::engine::general_purpose::STANDARD.encode(b),
    }
}

impl Amqp10SourceTask {
    async fn ensure(&mut self) -> Result<(), ConnectorError> {
        if let Some(l) = self.live.as_mut() {
            renew_if_due(&mut l.conn, &self.cfg, &self.env).await?;
            return Ok(());
        }
        self.pending.clear();
        self.held = None;
        let mut conn = open(&self.cfg, &self.env, &self.link_name).await?;
        let receiver = Receiver::builder()
            .name(self.link_name.clone())
            .source(terminus_source(&self.cfg, &self.address))
            .credit_mode(CreditMode::Auto(self.credit))
            .auto_accept(false)
            .attach(&mut conn.session)
            .await
            .map_err(|e| amqp_err("attaching the receiver", e))?;
        self.live = Some(LiveReceiver {
            conn,
            receiver,
            session: new_session(),
        });
        Ok(())
    }

    fn incoming(&self, session: u64, seq: u64, m: &Message<Body<Value>>) -> Incoming {
        let (headers, content_type, data, message_id) = message_parts(m);
        let fallback_id = match message_id {
            Some(id) => format!("{}/{id}", self.address),
            None => format!("{}/{session:x}/{seq}", self.address),
        };
        Incoming {
            headers,
            content_type,
            data,
            fallback_id,
            resource: self.address.clone(),
            subject: m.properties.as_ref().and_then(|p| p.subject.clone()),
        }
    }
}

#[async_trait]
impl Source for Amqp10SourceTask {
    async fn open(&mut self, split: &Split, _pos: Option<Position>) -> Result<(), ConnectorError> {
        self.address = split_name(split)?;
        self.ensure().await
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        self.ensure().await?;
        let rows = common::take_rows(budget, self.src.max_batch);
        let wait = Duration::from_millis(self.src.poll_wait_ms);
        let session = self.live.as_ref().map(|l| l.session).unwrap_or_default();
        let mut events = Vec::new();
        let mut bytes = 0usize;
        let mut last = None;
        while events.len() < rows {
            let (info, msg) = match self.held.take() {
                Some(h) => h,
                None => {
                    let live = self.live.as_mut().expect("ensured");
                    let w = if events.is_empty() {
                        wait
                    } else {
                        Duration::ZERO
                    };
                    match tokio::time::timeout(w, live.receiver.recv::<Body<Value>>()).await {
                        Err(_) => break,
                        Ok(Err(e)) => {
                            if let Some(l) = self.live.take() {
                                shut(l.conn).await;
                            }
                            return Err(amqp_err("receiving", e));
                        }
                        Ok(Ok(d)) => d.into_parts(),
                    }
                }
            };
            let seq = self.pending.peek_next();
            let ev = mapping::to_event(&self.env.envelope, self.incoming(session, seq, &msg));
            let size = ev.size_bytes();
            if size > budget.max_bytes && events.is_empty() {
                if let Some(l) = &self.live {
                    let _ = l.receiver.reject(info, None).await;
                }
                return Err(ConnectorError::poison(format!(
                    "{}: a {size}-byte message on {} exceeds the batch budget",
                    self.cfg.id(),
                    self.address
                )));
            }
            if bytes + size > budget.max_bytes {
                self.held = Some((info, msg));
                break;
            }
            bytes += size;
            let seq = self.pending.push(info);
            last = Some(seq);
            events.push(ev);
        }
        let Some(seq) = last else {
            return Ok(None);
        };
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: session_position(session, seq, format!("{}#{seq}", self.address)),
        }))
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        let Some((session, seq)) = parse_session_position(upto) else {
            return Ok(());
        };
        let Some(live) = &self.live else {
            return Ok(());
        };
        if live.session != session {
            return Ok(());
        }
        let infos: Vec<DeliveryInfo> = self
            .pending
            .take_upto(seq)
            .into_iter()
            .map(|(_, i)| i)
            .collect();
        if infos.is_empty() {
            return Ok(());
        }
        live.receiver
            .accept_all(infos)
            .await
            .map_err(|e| amqp_err("settling deliveries", e))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(l) = self.live.take() {
            let _ = l.receiver.close().await;
            shut(l.conn).await;
        }
        Ok(())
    }
}

struct SourceMaker {
    cfg: Amqp10Config,
    src: Amqp10Source,
    credit: u32,
    env: Env,
    instance: String,
}

impl SourceFactory for SourceMaker {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        let address = split_name(split)?;
        Ok(Box::new(Amqp10SourceTask {
            cfg: self.cfg.clone(),
            src: self.src.clone(),
            credit: self.credit,
            env: self.env.clone(),
            link_name: format!("loams-{}-{address}", self.instance),
            address,
            live: None,
            pending: Pending::new(),
            held: None,
        }))
    }
}

// -------------------------------------------------------------------- sink --

pub struct Amqp10SinkTask {
    cfg: Amqp10Config,
    sink: Amqp10Sink,
    env: Env,
    link_name: String,
    live: Option<(Conn, Sender)>,
}

impl std::fmt::Debug for Amqp10SinkTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Amqp10Sink")
            .field("address", &self.sink.address)
            .finish_non_exhaustive()
    }
}

/// The AMQP 1.0 message for an event: `message-id` = the event id, the
/// content type and subject in the properties, the attributes as
/// `cloudEvents:` application properties, and the data as one data section.
pub fn message(ev: &CloudEvent) -> Message<Data> {
    let mut props = Properties::builder()
        .message_id(MessageId::String(ev.id.clone()))
        .content_type(mapping::content_type(ev).to_owned());
    if let Some(s) = &ev.subject {
        props = props.subject(s.clone());
    }
    let mut ap = ApplicationProperties::builder();
    for (k, v) in mapping::binary_headers(ev, CE_PREFIX) {
        ap = ap.insert(k, SimpleValue::String(v));
    }
    Message::builder()
        .properties(props.build())
        .application_properties(ap.build())
        .data(Data(ev.data.to_vec().into()))
        .build()
}

#[async_trait]
impl Sink for Amqp10SinkTask {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let events = mapping::events(batch, self.cfg.id())?;
        if self.live.is_none() {
            let mut conn = open(&self.cfg, &self.env, &self.link_name).await?;
            let sender = Sender::builder()
                .name(self.link_name.clone())
                .target(terminus_target(&self.cfg, &self.sink.address))
                .attach(&mut conn.session)
                .await
                .map_err(|e| amqp_err("attaching the sender", e))?;
            self.live = Some((conn, sender));
        }
        let (conn, sender) = self.live.as_mut().expect("connected");
        renew_if_due(conn, &self.cfg, &self.env).await?;
        let timeout = Duration::from_millis(self.sink.send_timeout_ms);
        let mut futs = Vec::with_capacity(events.len());
        let mut ack = SinkAck::default();
        for ev in &events {
            let fut = match sender.send_batchable(message(ev)).await {
                Ok(f) => f,
                Err(e) => {
                    let err = amqp_err("sending", e);
                    if let Some((c, s)) = self.live.take() {
                        let _ = s.close().await;
                        shut(c).await;
                    }
                    return Err(err);
                }
            };
            futs.push(fut);
            ack.rows += 1;
            ack.bytes += ev.size_bytes() as u64;
        }
        for fut in futs {
            let outcome = tokio::time::timeout(timeout, fut)
                .await
                .map_err(|_| {
                    ConnectorError::retryable(format!("{}: no outcome in time", self.cfg.id()))
                })?
                .map_err(|e| amqp_err("waiting for the outcome", e))?;
            match outcome {
                Outcome::Accepted(_) => {}
                Outcome::Rejected(r) => {
                    return Err(ConnectorError::poison(format!(
                        "{}: the broker rejected a message: {:?}",
                        self.cfg.id(),
                        r.error.map(|e| e.condition)
                    )));
                }
                other => {
                    return Err(ConnectorError::retryable(format!(
                        "{}: the broker did not accept a message ({other:?})",
                        self.cfg.id()
                    )));
                }
            }
        }
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

struct SinkMaker {
    cfg: Amqp10Config,
    sink: Amqp10Sink,
    env: Env,
    instance: String,
}

impl SinkFactory for SinkMaker {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(Amqp10SinkTask {
            cfg: self.cfg.clone(),
            sink: self.sink.clone(),
            env: self.env.clone(),
            link_name: format!("loams-{}-{}-out", self.instance, self.sink.address),
            live: None,
        })))
    }
}

// ----------------------------------------------------------------- factory --

/// `loams_connector_brokers::amqp10::Amqp10`: one value per manifest.
#[derive(Debug, Clone, Copy)]
pub struct Amqp10 {
    flavor: Flavor,
}

impl Amqp10 {
    pub fn activemq() -> Self {
        Self {
            flavor: Flavor::ActiveMq,
        }
    }

    pub fn service_bus() -> Self {
        Self {
            flavor: Flavor::ServiceBus,
        }
    }

    pub fn flavor(&self) -> Flavor {
        self.flavor
    }
}

impl ConnectorFactory for Amqp10 {
    fn id(&self) -> &'static str {
        self.flavor.id()
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Amqp10Config::parse(self.flavor, &ctx.config)?;
        let Some(src) = cfg.source().cloned() else {
            return Ok(None);
        };
        let pipeline = common::pipeline_config(&ctx.config)?;
        let credit = common::window_for(&pipeline, src.max_batch, src.credit, 65_535);
        Ok(Some((
            Box::new(StaticSplits::new(src.addresses.clone())),
            Box::new(SourceMaker {
                cfg,
                src,
                credit,
                env: Env::new(self.flavor.id(), ctx),
                instance: ctx.instance.name.clone(),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = Amqp10Config::parse(self.flavor, &ctx.config)?;
        let Some(sink) = cfg.sink().cloned() else {
            return Ok(None);
        };
        Ok(Some(Box::new(SinkMaker {
            cfg,
            sink,
            env: Env::new(self.flavor.id(), ctx),
            instance: ctx.instance.name.clone(),
        })))
    }
}

pub fn activemq_factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(Amqp10::activemq())
}

pub fn service_bus_factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(Amqp10::service_bus())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use loams_connector_sdk::ErrorClass;
    use serde_json::json;

    #[test]
    fn hmac_matches_rfc_4231_case_2() {
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex: String = mac.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // A key longer than the block is hashed first (RFC 4231 case 6).
        let mac = hmac_sha256(
            &[0xaa; 131],
            b"Test Using Larger Than Block-Size Key - Hash Key First",
        );
        let hex: String = mac.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn sas_token_has_the_documented_shape() {
        let t = sas_token(
            "sb://ns.servicebus.windows.net/",
            "Root",
            "a2V5",
            1_700_000_000,
        );
        assert!(
            t.starts_with("SharedAccessSignature sr=sb%3A%2F%2Fns.servicebus.windows.net%2F&sig=")
        );
        assert!(t.ends_with("&se=1700000000&skn=Root"));
        let sig = t.split("&sig=").nth(1).unwrap().split('&').next().unwrap();
        // The signature is the base64 HMAC of "<sr>\n<se>", URI-encoded.
        let expected = base64::engine::general_purpose::STANDARD.encode(hmac_sha256(
            b"a2V5",
            b"sb%3A%2F%2Fns.servicebus.windows.net%2F\n1700000000",
        ));
        assert_eq!(sig, uri_encode(&expected));
    }

    #[test]
    fn connection_strings_parse_without_leaking() {
        let cs = ConnectionString::parse(
            "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
        )
        .unwrap();
        assert_eq!(cs.host, "localhost");
        assert!(cs.emulator);
        let cs = ConnectionString::parse(
            "Endpoint=sb://localhost:5676;SharedAccessKeyName=k;SharedAccessKey=v;UseDevelopmentEmulator=true",
        )
        .unwrap();
        assert_eq!(
            common::host_port(&cs.host, 5672).unwrap(),
            ("localhost".into(), 5676)
        );
        assert!(!format!("{cs:?}").contains("SAS_KEY_VALUE"));
        let cs = ConnectionString::parse(
            "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v;EntityPath=q",
        )
        .unwrap();
        assert_eq!(cs.host, "ns.servicebus.windows.net");
        assert_eq!(cs.entity_path.as_deref(), Some("q"));
        let e = ConnectionString::parse("Endpoint=sb://x;SharedAccessKey=hunter2").unwrap_err();
        assert_eq!(e.class, ErrorClass::Auth);
        assert!(!e.message.contains("hunter2"));
    }

    #[test]
    fn configs_are_checked() {
        assert!(Amqp10Config::parse(Flavor::ActiveMq, &json!({"url": "amqp://u:p@h"})).is_err());
        assert!(
            Amqp10Config::parse(
                Flavor::ActiveMq,
                &json!({"url": "amqp://h", "password": "x"})
            )
            .is_err()
        );
        Amqp10Config::parse(
            Flavor::ActiveMq,
            &json!({"url": "amqp://h:5672", "username": "loams", "password": "artemis-pw",
                    "source": {"addresses": ["orders"]}, "sink": {"address": "out"}}),
        )
        .unwrap();
        assert!(Amqp10Config::parse(Flavor::ServiceBus, &json!({})).is_err());
        assert!(
            Amqp10Config::parse(
                Flavor::ServiceBus,
                &json!({"connection_string": "sb-cs", "namespace": "x"})
            )
            .is_err()
        );
        Amqp10Config::parse(
            Flavor::ServiceBus,
            &json!({"namespace": "ns.servicebus.windows.net",
                    "entra": {"tenant_id": "t", "client_id": "c", "client_secret": "sb-secret"},
                    "source": {"addresses": ["q", "topic/Subscriptions/s"]}}),
        )
        .unwrap();
    }

    #[test]
    fn messages_carry_cloudevents_properties() {
        let ev = CloudEvent::new("evt-7", "/s", "t.v1")
            .with_subject("sub")
            .with_data("application/json", Bytes::from_static(b"{\"a\":1}"));
        let m = message(&ev);
        let m: Message<Body<Value>> = Message {
            header: m.header,
            delivery_annotations: m.delivery_annotations,
            message_annotations: m.message_annotations,
            properties: m.properties,
            application_properties: m.application_properties,
            body: Body::Data(vec![m.body].into()),
            footer: m.footer,
        };
        let (headers, ct, data, id) = message_parts(&m);
        assert_eq!(id.as_deref(), Some("evt-7"));
        assert_eq!(ct.as_deref(), Some("application/json"));
        assert_eq!(&data[..], b"{\"a\":1}");
        assert_eq!(mapping::header_id(&headers), Some("evt-7"));
        assert!(headers.iter().any(|(k, _)| k == "cloudEvents:type"));
        let env = loams_connector_sdk::EnvelopeBuilder::new(
            ACTIVEMQ_ID,
            &loams_connector_sdk::InstanceKey::new(1, "i"),
        );
        let back = mapping::to_event(
            &env,
            Incoming {
                headers,
                content_type: ct,
                data,
                fallback_id: "never".into(),
                resource: "q".into(),
                subject: None,
            },
        );
        assert_eq!(back.id, "evt-7");
        assert_eq!(back.ty, "t.v1");
        assert_eq!(back.subject.as_deref(), Some("sub"));
    }

    /// A one-shot token endpoint: answers `status` with `body`, and hands
    /// back the request it got.
    async fn token_endpoint(
        status: u16,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut req = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = s.read(&mut buf).await.unwrap();
                req.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&req).to_string();
                if let Some(i) = text.find("\r\n\r\n") {
                    let len = text
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if req.len() >= i + 4 + len {
                        break;
                    }
                }
                if n == 0 {
                    break;
                }
            }
            let resp = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            s.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&req).to_string()
        });
        (format!("http://{addr}"), task)
    }

    fn env_with(secret: &str) -> Env {
        let ctx = ConnectorContext::new(
            loams_connector_sdk::InstanceKey::new(1, "sb"),
            json!({}),
            Arc::new(
                loams_connector_sdk::StaticSecrets::new().with("sb-secret", secret.to_owned()),
            ),
            loams_connector_sdk::Scrubber::new(),
        );
        Env::new(SERVICE_BUS_ID, &ctx)
    }

    #[tokio::test]
    async fn entra_client_credentials_fetch_a_token() {
        let (authority, req) = token_endpoint(
            200,
            r#"{"token_type":"Bearer","expires_in":1800,"access_token":"eyJ.tok"}"#,
        )
        .await;
        let entra = EntraConfig {
            tenant_id: "tenant-1".into(),
            client_id: "app-1".into(),
            client_secret: "sb-secret".into(),
            authority,
        };
        let (token, ttl) = entra_token(&entra, &env_with("cl!ent secret"))
            .await
            .unwrap();
        assert_eq!(token, "eyJ.tok");
        assert_eq!(ttl, Duration::from_secs(1800));
        let req = req.await.unwrap();
        assert!(
            req.starts_with("POST /tenant-1/oauth2/v2.0/token "),
            "{req}"
        );
        assert!(req.contains("grant_type=client_credentials"));
        assert!(req.contains("client_id=app-1"));
        assert!(req.contains("client_secret=cl%21ent%20secret"));
        assert!(req.contains("scope=https%3A%2F%2Fservicebus.azure.net%2F.default"));
    }

    #[tokio::test]
    async fn a_refused_client_secret_is_an_auth_error() {
        let (authority, _req) = token_endpoint(401, r#"{"error":"invalid_client"}"#).await;
        let entra = EntraConfig {
            tenant_id: "t".into(),
            client_id: "c".into(),
            client_secret: "sb-secret".into(),
            authority,
        };
        let e = entra_token(&entra, &env_with("hunter2-secret"))
            .await
            .unwrap_err();
        assert_eq!(e.class, ErrorClass::Auth, "{e}");
        assert!(!e.message.contains("hunter2"));
    }

    #[test]
    fn declares_at_least_once() {
        assert_eq!(declared().sink, Delivery::AtLeastOnce);
        assert_eq!(Amqp10::activemq().id(), "activemq-jms");
        assert_eq!(Amqp10::service_bus().id(), "azure-service-bus");
    }
}
