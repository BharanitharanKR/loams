//! Connecting: the seed list, the primary, TLS and authentication.
//!
//! Every connection is opened by `loams_connector_sdk::net` (the host is
//! egress-checked before DNS and every address after it), including a
//! primary that a secondary's `hello` names: a server cannot steer the
//! connector to an address the instance's egress policy refuses (Review
//! Focus 9). All reads and writes go to the primary (plan ruling R16.2).

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use loams_connector_sdk::net;
use loams_connector_sdk::{ConnectorContext, ConnectorError, ErrorClass, SecretRef};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};

use crate::bson::{Bson, Document};
use crate::config::{AuthMechanism, MongoConfig, TlsConfig, parse_host};
use crate::doc;
use crate::scram;
use crate::wire::{Connection, Hello, Topology};

/// Open a connection to the deployment's primary (or the standalone server
/// or a mongos), authenticated.
pub async fn connect(
    cfg: &MongoConfig,
    ctx: &ConnectorContext,
) -> Result<Connection, ConnectorError> {
    let tls = match &cfg.tls {
        Some(t) => Some(tls_config(t, ctx).await?),
        None => None,
    };
    let mut last: Option<ConnectorError> = None;
    for (host, port) in &cfg.hosts {
        match connect_to(cfg, ctx, tls.clone(), host, *port).await {
            Ok(Reached::Ready(c)) => return Ok(c),
            Ok(Reached::Primary(p)) => {
                let (h, port) = parse_host(&p)?;
                match connect_to(cfg, ctx, tls.clone(), &h, port).await {
                    Ok(Reached::Ready(c)) => return Ok(c),
                    Ok(Reached::Primary(_)) => {
                        last = Some(ConnectorError::retryable(format!(
                            "{p} named as primary is not the primary yet"
                        )));
                    }
                    Err(e) if e.class.is_transient() => last = Some(e),
                    Err(e) => return Err(e),
                }
            }
            Err(e) if e.class.is_transient() => last = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| ConnectorError::retryable("no primary is reachable")))
}

enum Reached {
    Ready(Connection),
    /// A secondary that names this primary.
    Primary(String),
}

async fn connect_to(
    cfg: &MongoConfig,
    ctx: &ConnectorContext,
    tls: Option<Arc<rustls::ClientConfig>>,
    host: &str,
    port: u16,
) -> Result<Reached, ConnectorError> {
    let address = format!("{host}:{port}");
    let dial = async {
        let stream: Box<dyn crate::wire::Stream> = match &tls {
            Some(config) => {
                let name = cfg
                    .tls
                    .as_ref()
                    .and_then(|t| t.server_name.clone())
                    .unwrap_or_else(|| host.to_owned());
                let tcp = net::connect(host, port, ctx.egress.as_ref()).await?;
                let server = ServerName::try_from(name.clone()).map_err(|e| {
                    ConnectorError::fatal(format!("{name} is not a TLS server name: {e}"))
                })?;
                Box::new(
                    tokio_rustls::TlsConnector::from(config.clone())
                        .connect(server, tcp)
                        .await
                        .map_err(|e| {
                            ConnectorError::retryable(format!("TLS with {address}: {e}"))
                        })?,
                )
            }
            None => Box::new(net::connect(host, port, ctx.egress.as_ref()).await?),
        };
        Ok::<_, ConnectorError>(stream)
    };
    let stream = tokio::time::timeout(cfg.connect_timeout, dial)
        .await
        .map_err(|_| {
            ConnectorError::retryable(format!(
                "connecting to {address}: no answer within {:?}",
                cfg.connect_timeout
            ))
        })??;
    let mut conn = Connection::new(stream, address.clone(), cfg.connect_timeout);
    let sasl_user = match (cfg.auth_mechanism, &cfg.username) {
        (AuthMechanism::ScramSha256, Some(u)) => Some(format!("{}.{u}", cfg.auth_source)),
        _ => None,
    };
    let hello = conn.handshake(sasl_user.as_deref()).await?.clone();
    if let Some(want) = &cfg.replica_set
        && hello.set_name.as_deref() != Some(want.as_str())
        && !hello.mongos
    {
        return Err(ConnectorError::fatal(format!(
            "{address} is not a member of replica set {want}"
        )));
    }
    if hello.topology() == Topology::ReplicaSet && !hello.writable_primary {
        return match hello.primary {
            Some(p) if p != address => Ok(Reached::Primary(p)),
            _ => Err(ConnectorError::retryable(format!(
                "{address} is not the primary and knows of none (an election may be running)"
            ))),
        };
    }
    authenticate(&mut conn, cfg, ctx, &hello).await?;
    conn.set_timeout(cfg.request_timeout);
    Ok(Reached::Ready(conn))
}

async fn secret_str(
    ctx: &ConnectorContext,
    name: &str,
    what: &str,
) -> Result<String, ConnectorError> {
    let s = ctx.secrets.resolve(&SecretRef::new(name)).await?;
    s.expose_str()
        .map(str::to_owned)
        .ok_or_else(|| ConnectorError::fatal(format!("the {what} secret is not UTF-8")))
}

async fn authenticate(
    conn: &mut Connection,
    cfg: &MongoConfig,
    ctx: &ConnectorContext,
    hello: &Hello,
) -> Result<(), ConnectorError> {
    match cfg.auth_mechanism {
        AuthMechanism::None => Ok(()),
        AuthMechanism::X509 => {
            let mut cmd = doc! { "authenticate" => 1, "mechanism" => "MONGODB-X509" };
            if let Some(u) = &cfg.username {
                cmd.insert("user", u.clone());
            }
            conn.command("$external", cmd)
                .await
                .map(|_| ())
                .map_err(auth_error)
        }
        AuthMechanism::ScramSha256 => {
            let user = cfg.username.as_deref().expect("checked by the config");
            let name = cfg.password.as_deref().expect("checked by the config");
            if !hello.sasl_supported_mechs.is_empty()
                && !hello
                    .sasl_supported_mechs
                    .iter()
                    .any(|m| m == "SCRAM-SHA-256")
            {
                return Err(ConnectorError::auth(format!(
                    "the server offers {} for {user}, not SCRAM-SHA-256",
                    hello.sasl_supported_mechs.join(", ")
                )));
            }
            let password = secret_str(ctx, name, "password").await?;
            scram_sha256(conn, &cfg.auth_source, user, &password).await
        }
    }
}

/// A server `ok: 0` during authentication is an `Auth` failure, whatever its
/// code; a transport error stays retryable.
fn auth_error(e: ConnectorError) -> ConnectorError {
    if e.class.is_transient() && crate::wire::server_error(&e).is_none() {
        e
    } else {
        ConnectorError::new(ErrorClass::Auth, format!("authentication: {}", e.message))
    }
}

fn payload(d: &Document) -> Result<String, ConnectorError> {
    match d.get("payload") {
        Some(Bson::Binary { bytes, .. }) => String::from_utf8(bytes.clone())
            .map_err(|_| ConnectorError::auth("the SCRAM payload is not UTF-8")),
        Some(Bson::String(s)) => B64
            .decode(s)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .ok_or_else(|| ConnectorError::auth("the SCRAM payload is not base64 UTF-8")),
        _ => Err(ConnectorError::auth("the SCRAM reply has no payload")),
    }
}

/// The SCRAM-SHA-256 conversation over `saslStart`/`saslContinue`.
pub async fn scram_sha256(
    conn: &mut Connection,
    source: &str,
    user: &str,
    password: &str,
) -> Result<(), ConnectorError> {
    let mut c = scram::Conversation::new(user, password, &scram::random_nonce())
        .map_err(|e| ConnectorError::fatal(e.to_string()))?;
    let bin = |s: String| Bson::Binary {
        subtype: 0,
        bytes: s.into_bytes(),
    };
    let first = conn
        .command(
            source,
            doc! {
                "saslStart" => 1,
                "mechanism" => "SCRAM-SHA-256",
                "payload" => bin(c.client_first()),
                "autoAuthorize" => 1,
                "options" => doc! { "skipEmptyExchange" => true },
            },
        )
        .await
        .map_err(auth_error)?;
    let conversation = first
        .get("conversationId")
        .cloned()
        .ok_or_else(|| ConnectorError::auth("saslStart returned no conversationId"))?;
    let server_first = payload(&first)?;
    let client_final = c
        .client_final(&server_first)
        .map_err(|e| ConnectorError::auth(e.to_string()))?;
    let mut reply = conn
        .command(
            source,
            doc! {
                "saslContinue" => 1,
                "conversationId" => conversation.clone(),
                "payload" => bin(client_final),
            },
        )
        .await
        .map_err(auth_error)?;
    c.verify_server(&payload(&reply)?)
        .map_err(|e| ConnectorError::auth(e.to_string()))?;
    // Servers without `skipEmptyExchange` want one more, empty, round.
    let mut rounds = 0;
    while reply.get_bool("done") != Some(true) {
        rounds += 1;
        if rounds > 2 {
            return Err(ConnectorError::auth(
                "the SCRAM conversation did not finish",
            ));
        }
        reply = conn
            .command(
                source,
                doc! {
                    "saslContinue" => 1,
                    "conversationId" => conversation.clone(),
                    "payload" => bin(String::new()),
                },
            )
            .await
            .map_err(auth_error)?;
    }
    Ok(())
}

/// The rustls configuration: the instance's CA (or the webpki roots) and,
/// for `mtls`, the client certificate and key from their secrets.
async fn tls_config(
    t: &TlsConfig,
    ctx: &ConnectorContext,
) -> Result<Arc<rustls::ClientConfig>, ConnectorError> {
    let mut roots = rustls::RootCertStore::empty();
    match &t.ca_pem {
        Some(pem) => {
            for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
                let cert = cert.map_err(|e| ConnectorError::fatal(format!("tls.ca_pem: {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| ConnectorError::fatal(format!("tls.ca_pem: {e}")))?;
            }
            if roots.is_empty() {
                return Err(ConnectorError::fatal("tls.ca_pem holds no certificate"));
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| ConnectorError::fatal(format!("TLS: {e}")))?
        .with_root_certificates(roots);
    let config = match (&t.cert, &t.key) {
        (Some(cert), Some(key)) => {
            let cert_pem = secret_str(ctx, cert, "tls.cert").await?;
            let key_pem = secret_str(ctx, key, "tls.key").await?;
            let chain: Vec<CertificateDer<'static>> =
                CertificateDer::pem_slice_iter(cert_pem.as_bytes())
                    .collect::<Result<_, _>>()
                    .map_err(|_| {
                        ConnectorError::fatal("tls.cert is not a PEM certificate chain")
                    })?;
            if chain.is_empty() {
                return Err(ConnectorError::fatal("tls.cert holds no certificate"));
            }
            let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
                .map_err(|_| ConnectorError::fatal("tls.key is not a PEM private key"))?;
            builder.with_client_auth_cert(chain, key).map_err(|_| {
                ConnectorError::fatal("tls.cert and tls.key do not form a client identity")
            })?
        }
        _ => builder.with_no_client_auth(),
    };
    Ok(Arc::new(config))
}

/// A connection that reconnects after a transient failure. The source and
/// sink hold one each.
pub struct Client {
    cfg: MongoConfig,
    ctx: ConnectorContext,
    conn: Option<Connection>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("hosts", &self.cfg.hosts)
            .field("database", &self.cfg.database)
            .finish_non_exhaustive()
    }
}

impl Client {
    pub fn new(cfg: MongoConfig, ctx: ConnectorContext) -> Self {
        Self {
            cfg,
            ctx,
            conn: None,
        }
    }

    pub fn config(&self) -> &MongoConfig {
        &self.cfg
    }

    pub fn context(&self) -> &ConnectorContext {
        &self.ctx
    }

    /// The open connection, (re)connecting when there is none or it broke.
    pub async fn conn(&mut self) -> Result<&mut Connection, ConnectorError> {
        if self.conn.as_ref().is_none_or(Connection::is_broken) {
            self.conn = None;
            let c = connect(&self.cfg, &self.ctx).await?;
            self.conn = Some(c);
        }
        Ok(self.conn.as_mut().expect("set above"))
    }

    /// What the primary's `hello` said (connecting if needed).
    pub async fn hello(&mut self) -> Result<Hello, ConnectorError> {
        Ok(self.conn().await?.hello.clone())
    }

    /// Run a command; a transient failure drops the connection so the next
    /// call reconnects (and rediscovers the primary).
    pub async fn command(&mut self, db: &str, cmd: Document) -> Result<Document, ConnectorError> {
        let r = self.conn().await?.command(db, cmd).await;
        self.after(&r);
        r
    }

    /// As [`Client::command`], with document sequences.
    pub async fn command_with(
        &mut self,
        db: &str,
        cmd: Document,
        seqs: &[crate::wire::Sequence],
    ) -> Result<Document, ConnectorError> {
        let r = self.conn().await?.command_with(db, cmd, seqs).await;
        self.after(&r);
        r
    }

    fn after<T>(&mut self, r: &Result<T, ConnectorError>) {
        if let Err(e) = r
            && e.class.is_transient()
            && crate::wire::server_error(e).is_none_or(|s| {
                // A server that stepped down or is shutting down: find the
                // new primary. A write conflict keeps the connection.
                !matches!(
                    s.code,
                    crate::wire::codes::WRITE_CONFLICT
                        | crate::wire::codes::NO_SUCH_TRANSACTION
                        | crate::wire::codes::SNAPSHOT_TOO_OLD
                        | crate::wire::codes::CURSOR_NOT_FOUND
                )
            })
        {
            self.conn = None;
        }
    }

    /// Drop the connection (after a cancelled command, or on close).
    pub fn reset(&mut self) {
        self.conn = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::FakeServer;
    use loams_connector_sdk::{DenyLocal, InstanceKey, Scrubber, StaticSecrets};
    use serde_json::json;
    use std::sync::Mutex;

    fn ctx(secrets: StaticSecrets) -> ConnectorContext {
        ConnectorContext::new(
            InstanceKey::new(1, "i"),
            json!({}),
            Arc::new(secrets),
            Scrubber::new(),
        )
    }

    fn cfg(host: &str, extra: serde_json::Value) -> MongoConfig {
        let mut c = json!({"hosts": [host], "database": "app", "connect_timeout_ms": 2000});
        if let (Some(o), Some(e)) = (c.as_object_mut(), extra.as_object()) {
            for (k, v) in e {
                o.insert(k.clone(), v.clone());
            }
        }
        MongoConfig::parse(&c).expect("config")
    }

    fn primary() -> Document {
        doc! { "ok" => 1, "isWritablePrimary" => true, "setName" => "rs0", "maxWireVersion" => 25,
        "logicalSessionTimeoutMinutes" => 30 }
    }

    #[tokio::test]
    async fn connects_to_the_primary_a_secondary_names() {
        let p = FakeServer::start(|cmd| {
            if cmd.contains_key("hello") {
                primary()
            } else {
                doc! { "ok" => 1, "who" => "primary" }
            }
        })
        .await;
        let ph = p.host();
        let s = FakeServer::start(move |_| {
            doc! { "ok" => 1, "isWritablePrimary" => false, "secondary" => true, "setName" => "rs0",
            "primary" => ph.clone(), "maxWireVersion" => 25 }
        })
        .await;
        let mut c = connect(
            &cfg(&s.host(), json!({"replica_set": "rs0"})),
            &ctx(StaticSecrets::new()),
        )
        .await
        .expect("connect");
        assert_eq!(c.address(), p.host());
        let r = c.command("app", doc! { "ping" => 1 }).await.expect("ping");
        assert_eq!(r.get_str("who"), Some("primary"));
        // The wrong replica set is refused.
        let err = connect(
            &cfg(&p.host(), json!({"replica_set": "other"})),
            &ctx(StaticSecrets::new()),
        )
        .await
        .expect_err("wrong set");
        assert_eq!(err.class, ErrorClass::Fatal);
    }

    #[tokio::test]
    async fn every_connection_is_egress_checked() {
        let p = FakeServer::start(|_| primary()).await;
        let mut c = ctx(StaticSecrets::new());
        c.egress = Arc::new(DenyLocal);
        let err = connect(&cfg(&p.host(), json!({})), &c)
            .await
            .expect_err("loopback refused");
        assert!(err.message.contains("egress refused"), "{}", err.message);
    }

    /// The server half of SCRAM-SHA-256 for a known password, so the client
    /// is checked end to end (the RFC 7677 vectors check the arithmetic).
    #[tokio::test]
    async fn scram_sha_256_authenticates_and_a_wrong_password_is_auth() {
        use ring::{digest, hmac, pbkdf2};
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let state = Arc::new(Mutex::new(None::<(String, String)>));
        let (seen2, state2) = (seen.clone(), state.clone());
        let server = FakeServer::start(move |cmd| {
            let first = cmd.keys().next().unwrap_or("").to_owned();
            seen2.lock().expect("lock").push(first.clone());
            let text = |d: &Document| match d.get("payload") {
                Some(Bson::Binary { bytes, .. }) => String::from_utf8(bytes.clone()).expect("utf8"),
                _ => String::new(),
            };
            let salt = b"0123456789abcdef";
            let mut salted = [0u8; 32];
            pbkdf2::derive(
                pbkdf2::PBKDF2_HMAC_SHA256,
                std::num::NonZeroU32::new(4096).expect("n"),
                salt,
                b"right-password",
                &mut salted,
            );
            let mac = |k: &[u8], m: &[u8]| hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, k), m);
            match first.as_str() {
                "hello" => {
                    let mut h = primary();
                    h.insert("saslSupportedMechs", vec![Bson::from("SCRAM-SHA-256")]);
                    h
                }
                "saslStart" => {
                    let cf = text(cmd);
                    let bare = cf.trim_start_matches("n,,").to_owned();
                    let nonce = bare.split("r=").nth(1).expect("nonce").to_owned();
                    let sf = format!("r={nonce}SERVER,s={},i=4096", B64.encode(salt));
                    *state2.lock().expect("lock") = Some((bare, sf.clone()));
                    doc! { "ok" => 1, "conversationId" => 1, "done" => false,
                    "payload" => Bson::Binary { subtype: 0, bytes: sf.into_bytes() } }
                }
                "saslContinue" => {
                    let cfinal = text(cmd);
                    let (bare, sf) = state2.lock().expect("lock").clone().expect("started");
                    let (without, proof) = cfinal.rsplit_once(",p=").expect("proof");
                    let auth = format!("{bare},{sf},{without}");
                    let client_key = mac(&salted, b"Client Key");
                    let stored = digest::digest(&digest::SHA256, client_key.as_ref());
                    let sig = mac(stored.as_ref(), auth.as_bytes());
                    let want: Vec<u8> = client_key
                        .as_ref()
                        .iter()
                        .zip(sig.as_ref())
                        .map(|(a, b)| a ^ b)
                        .collect();
                    if B64.decode(proof).expect("b64") != want {
                        return doc! { "ok" => 0, "code" => 18, "codeName" => "AuthenticationFailed",
                        "errmsg" => "Authentication failed." };
                    }
                    let server_sig = mac(mac(&salted, b"Server Key").as_ref(), auth.as_bytes());
                    let v = format!("v={}", B64.encode(server_sig.as_ref()));
                    doc! { "ok" => 1, "conversationId" => 1, "done" => true,
                    "payload" => Bson::Binary { subtype: 0, bytes: v.into_bytes() } }
                }
                _ => doc! { "ok" => 1 },
            }
        })
        .await;
        let config = cfg(
            &server.host(),
            json!({"username": "loams", "password": "db-password"}),
        );
        let good = StaticSecrets::new().with("db-password", b"right-password".to_vec());
        connect(&config, &ctx(good)).await.expect("authenticated");
        assert_eq!(
            seen.lock().expect("lock").as_slice(),
            ["hello", "saslStart", "saslContinue"]
        );
        let bad = StaticSecrets::new().with("db-password", b"wrong-password".to_vec());
        let err = connect(&config, &ctx(bad)).await.expect_err("refused");
        assert_eq!(err.class, ErrorClass::Auth);
        assert!(!err.message.contains("wrong-password"));
    }

    #[tokio::test]
    async fn a_client_reconnects_after_a_transient_failure() {
        let calls = Arc::new(Mutex::new(0u32));
        let c2 = calls.clone();
        let server = FakeServer::start(move |cmd| {
            if cmd.contains_key("hello") {
                return primary();
            }
            let mut n = c2.lock().expect("lock");
            *n += 1;
            if *n == 1 {
                doc! { "ok" => 0, "code" => 10107, "codeName" => "NotWritablePrimary", "errmsg" => "x" }
            } else {
                doc! { "ok" => 1 }
            }
        })
        .await;
        let mut client = Client::new(cfg(&server.host(), json!({})), ctx(StaticSecrets::new()));
        let err = client
            .command("app", doc! { "ping" => 1 })
            .await
            .expect_err("stepdown");
        assert!(err.class.is_transient());
        assert!(client.conn.is_none(), "dropped to rediscover the primary");
        client
            .command("app", doc! { "ping" => 1 })
            .await
            .expect("reconnected");
    }
}
