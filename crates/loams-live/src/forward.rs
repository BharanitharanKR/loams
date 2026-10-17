//! Sessions across several Live nodes (design §45 §9, D693; LV1 plan Task
//! 24).
//!
//! Live nodes are stateless, and a load balancer spreads calls over them
//! without affinity, so a session's `ModifyQuerySet` or `Authenticate` can
//! land on a node that does not hold it. A session id starts with the id of
//! the node that owns it (`<node>-<32 hex digits>`, [`owner_of`]); every
//! node registers in the app directory's system keyspace under that id,
//! with the URL its peers reach it at, and refreshes the record every
//! [`heartbeat`](ForwardConfig::heartbeat) ([`Nodes`]):
//!
//! ```text
//! node = 0x04 ‖ node id  → NodeRecord (JSON: url, incarnation, authenticated, heartbeat_ms)
//! ```
//!
//! A node's records are not directory writes: they do not move the
//! directory's change counter, so they never restart an engine.
//!
//! **Forwarding.** A call for a session another node owns is sent to that
//! node's `LiveService` as it came: the same message, with the caller's own
//! credential (`Authorization`, `Cookie`), the `loams-live-app` header and
//! the `Origin`, plus [`FORWARDED_HEADER`] naming this node. The owner
//! authenticates and authorizes it itself, exactly as if the client had
//! called it, so a forward carries no authority of the node that sends it.
//! A forwarded call is never forwarded again: the owner answers it from its
//! own sessions. A node that authenticates its callers never forwards to
//! one that does not ([`NodeRecord::authenticated`]), so a forward cannot
//! turn an anonymous call into a developer's on a peer.
//!
//! **Gone.** An owner that is not registered, whose record is older than
//! [`node_ttl`](ForwardConfig::node_ttl), that does not answer, or that no
//! longer holds the session: `NOT_FOUND`, reason [`SESSION_GONE`]. The
//! client resumes its session on any node.
//!
//! **Draining** (`SIGTERM`): readiness goes false, the node's record is
//! removed, every stream closes with `UNAVAILABLE`, reason
//! [`NODE_DRAINING`], new calls are refused the same way, and in-flight
//! mutations get [`drain_grace`](ForwardConfig::drain_grace) to finish
//! ([`LiveHandle::drain`](crate::LiveHandle::drain)).

use std::time::Duration;

use buffa::Message;
use connectrpc::{ConnectError, RequestContext};
use loams_kv::{CommitMode, Store, TxnOptions};
use serde::{Deserialize, Serialize};

use crate::LiveError;
use crate::apps::APP_HEADER;
use crate::txn::lift;

/// The header a forwarded call carries, naming the node that forwarded it;
/// the receiving node answers from its own sessions and never forwards it
/// again.
pub const FORWARDED_HEADER: &str = "loams-live-forwarded";

/// The reason of a session call whose session no node holds any more (its
/// owner is gone, or ended it): `NOT_FOUND`; the client resumes.
pub const SESSION_GONE: &str = "session_gone";

/// The reason of a stream closed, or a call refused, by a node that is
/// shutting down: `UNAVAILABLE`; the client resumes on another node.
pub const NODE_DRAINING: &str = "node_draining";

/// How often a node refreshes its record.
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(2);

/// How old a node's record may be before its sessions count as gone.
pub const DEFAULT_NODE_TTL: Duration = Duration::from_secs(10);

/// How long a forwarded call may take.
pub const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a draining node waits for its in-flight mutations.
pub const DEFAULT_DRAIN_GRACE: Duration = Duration::from_secs(10);

/// The longest node id.
pub const MAX_NODE_ID_BYTES: usize = 64;

/// The request headers a forwarded call carries over.
const CARRIED: [&str; 4] = ["authorization", "cookie", APP_HEADER, "origin"];

/// The first byte of a node's key in the system keyspace (the directory's
/// own keys are `0x00`–`0x03`).
const NODE: u8 = 0x04;

fn node_key(node: &str) -> Vec<u8> {
    [&[NODE][..], node.as_bytes()].concat()
}

/// How a node takes part in a cluster of Live nodes (`[live] forward`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardConfig {
    /// The base URL other nodes forward this node's session calls to (its
    /// main port, `--live-advertise`); `None`: its own Live listener,
    /// `http://<listen address>`, which only a node on the same host
    /// reaches.
    pub advertise: Option<String>,
    /// Whether this node's port authenticates its callers (the engine's
    /// auth mode is on). Such a node forwards only to nodes that do too.
    pub authenticated: bool,
    /// How often this node refreshes its record ([`DEFAULT_HEARTBEAT`]).
    pub heartbeat: Duration,
    /// How old a record may be before the node counts as gone
    /// ([`DEFAULT_NODE_TTL`]).
    pub node_ttl: Duration,
    /// How long a forwarded call may take ([`DEFAULT_FORWARD_TIMEOUT`]).
    pub timeout: Duration,
    /// How long draining waits for in-flight mutations
    /// ([`DEFAULT_DRAIN_GRACE`]).
    pub drain_grace: Duration,
}

impl Default for ForwardConfig {
    fn default() -> Self {
        ForwardConfig {
            advertise: None,
            authenticated: false,
            heartbeat: DEFAULT_HEARTBEAT,
            node_ttl: DEFAULT_NODE_TTL,
            timeout: DEFAULT_FORWARD_TIMEOUT,
            drain_grace: DEFAULT_DRAIN_GRACE,
        }
    }
}

/// A node's record in the system keyspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// Where its peers forward to.
    pub url: String,
    /// This run of the node (a new one at every start).
    pub incarnation: String,
    /// Whether its port authenticates its callers.
    pub authenticated: bool,
    /// When it last refreshed the record (the store's clock, ms).
    pub heartbeat_ms: u64,
}

/// The node a session id names: everything before its last `-`, when what
/// follows is the 32 hex digits [`Sessions`](crate::Sessions) draws.
pub fn owner_of(session_id: &str) -> Option<&str> {
    let (node, random) = session_id.rsplit_once('-')?;
    (!node.is_empty() && random.len() == 32 && random.bytes().all(|b| b.is_ascii_hexdigit()))
        .then_some(node)
}

/// Refuses a node id that cannot prefix a session id: 1 to
/// [`MAX_NODE_ID_BYTES`] of ASCII letters, digits, `.`, `_` and `-`.
pub fn check_node_id(node: &str) -> Result<(), LiveError> {
    let ok = !node.is_empty()
        && node.len() <= MAX_NODE_ID_BYTES
        && node
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(LiveError::invalid(format!(
            "live node id {node:?}: 1 to {MAX_NODE_ID_BYTES} ASCII letters, digits, '.', '_' or '-'"
        )))
    }
}

/// Refuses an advertise URL that is not `http://` or `https://` with a host.
pub fn check_url(url: &str) -> Result<(), LiveError> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|e| LiveError::invalid(format!("live advertise URL {url:?}: {e}")))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(LiveError::invalid(format!(
            "live advertise URL {url:?}: expected http://<host>:<port> or https://<host>:<port>"
        )));
    }
    Ok(())
}

fn txn_options(op: &'static str) -> TxnOptions {
    TxnOptions {
        commit_mode: Some(CommitMode::TwoPc),
        ..TxnOptions::new(op)
    }
}

fn gone(message: impl Into<String>) -> LiveError {
    LiveError::SessionGone(message.into())
}

/// Where a session call goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// This node owns the session.
    Local,
    /// Node `node`, at `record.url`, owns it.
    Remote { node: String, record: NodeRecord },
}

/// This node's record and its view of the others (see the module docs).
#[derive(Debug)]
pub struct Nodes {
    store: Store,
    me: String,
    record: NodeRecord,
    config: ForwardConfig,
    http: Result<reqwest::Client, String>,
}

impl Nodes {
    /// Node `me`, reached at `url`, on the system keyspace's `store`.
    pub fn new(
        store: Store,
        me: &str,
        url: String,
        config: ForwardConfig,
    ) -> Result<Nodes, LiveError> {
        check_node_id(me)?;
        let url = url.trim_end_matches('/').to_string();
        check_url(&url)?;
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| format!("the Live forwarding HTTP client: {e}"));
        Ok(Nodes {
            store,
            me: me.to_string(),
            record: NodeRecord {
                url,
                incarnation: format!("{:032x}", rand::random::<u128>()),
                authenticated: config.authenticated,
                heartbeat_ms: 0,
            },
            config,
            http,
        })
    }

    /// This node's id.
    pub fn me(&self) -> &str {
        &self.me
    }

    /// The URL this node registers.
    pub fn url(&self) -> &str {
        &self.record.url
    }

    /// The settings.
    pub fn config(&self) -> &ForwardConfig {
        &self.config
    }

    async fn now_ms(&self) -> Result<u64, LiveError> {
        self.store
            .now()
            .await
            .map(loams_kv::Ts::physical_ms)
            .map_err(|e| LiveError::Internal(format!("a timestamp: {e}")))
    }

    fn fresh(&self, record: &NodeRecord, now_ms: u64) -> bool {
        let ttl = u64::try_from(self.config.node_ttl.as_millis()).unwrap_or(u64::MAX);
        now_ms.saturating_sub(record.heartbeat_ms) <= ttl
    }

    /// Registers this node. Refused while another process runs under the
    /// same id somewhere else (a fresh record of another incarnation at
    /// another URL): two nodes with one id would take each other's
    /// sessions' calls and journal checkpoints.
    pub async fn register(&self) -> Result<(), LiveError> {
        let now = self.now_ms().await?;
        let key = node_key(&self.me);
        let mine = NodeRecord {
            heartbeat_ms: now,
            ..self.record.clone()
        };
        let value = encode(&mine)?;
        let ttl = self.config.node_ttl;
        let me = self.me.clone();
        self.store
            .run(txn_options("live.nodes.register"), move |txn| {
                let (key, value, me) = (key.clone(), value.clone(), me.clone());
                let mine = mine.clone();
                Box::pin(async move {
                    let body = async {
                        if let Some(bytes) = txn.get(&key).await? {
                            let other = decode(&bytes)?;
                            let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
                            let fresh = now.saturating_sub(other.heartbeat_ms) <= ttl_ms;
                            if fresh
                                && other.incarnation != mine.incarnation
                                && other.url != mine.url
                            {
                                return Err(LiveError::FailedPrecondition(format!(
                                    "live node {me:?} already runs at {}; give each Live node \
                                     its own --live-node",
                                    other.url
                                )));
                            }
                        }
                        txn.put(&key, value).await?;
                        Ok(())
                    };
                    lift(body.await)
                })
            })
            .await?
            .value
    }

    /// Refreshes this node's record.
    pub async fn heartbeat(&self) -> Result<(), LiveError> {
        let now = self.now_ms().await?;
        let value = encode(&NodeRecord {
            heartbeat_ms: now,
            ..self.record.clone()
        })?;
        let key = node_key(&self.me);
        self.store
            .run(txn_options("live.nodes.heartbeat"), move |txn| {
                let (key, value) = (key.clone(), value.clone());
                Box::pin(async move {
                    txn.put(&key, value).await?;
                    Ok(Ok(()))
                })
            })
            .await?
            .value
    }

    /// Removes this node's record (when it is still this run's).
    pub async fn deregister(&self) -> Result<(), LiveError> {
        let key = node_key(&self.me);
        let incarnation = self.record.incarnation.clone();
        self.store
            .run(txn_options("live.nodes.deregister"), move |txn| {
                let (key, incarnation) = (key.clone(), incarnation.clone());
                Box::pin(async move {
                    let body = async {
                        if let Some(bytes) = txn.get(&key).await?
                            && decode(&bytes)?.incarnation == incarnation
                        {
                            txn.delete(&key).await?;
                        }
                        Ok(())
                    };
                    lift(body.await)
                })
            })
            .await?
            .value
    }

    /// The record of node `node`, when it is registered and fresh.
    pub async fn lookup(&self, node: &str) -> Result<Option<NodeRecord>, LiveError> {
        let key = node_key(node);
        let found = self
            .store
            .run(txn_options("live.nodes.get"), move |txn| {
                let key = key.clone();
                Box::pin(async move {
                    let body = async {
                        match txn.get(&key).await? {
                            Some(bytes) => decode(&bytes).map(Some),
                            None => Ok(None),
                        }
                    };
                    lift(body.await)
                })
            })
            .await?
            .value?;
        let now = self.now_ms().await?;
        Ok(found.filter(|r| self.fresh(r, now)))
    }

    /// Where a call for session `session_id` goes; `forwarded` when it came
    /// from another node. A session no node can answer for is
    /// [`SESSION_GONE`].
    pub async fn route(&self, session_id: &str, forwarded: bool) -> Result<Route, LiveError> {
        let Some(owner) = owner_of(session_id) else {
            return Err(gone(format!("session {session_id:?} is not open")));
        };
        if owner == self.me {
            return Ok(Route::Local);
        }
        if forwarded {
            return Err(gone(format!(
                "session {session_id:?} is not open on this node (a forwarded call is not \
                 forwarded again)"
            )));
        }
        let Some(record) = self.lookup(owner).await? else {
            return Err(gone(format!(
                "the node that owned session {session_id:?} is gone; resume the session"
            )));
        };
        if self.config.authenticated && !record.authenticated {
            tracing::warn!(
                node = owner,
                "not forwarding to a Live node that does not authenticate its callers"
            );
            return Err(gone(format!(
                "the node that owns session {session_id:?} cannot be reached from this node; \
                 resume the session"
            )));
        }
        Ok(Route::Remote {
            node: owner.to_string(),
            record,
        })
    }

    /// Sends `request` to `procedure` (`/loams.live.v1.LiveService/<Method>`)
    /// on the owner `to`, as the client sent it to this node (`ctx`'s
    /// credential, app and origin headers), and answers the owner's answer:
    /// its response, or its Connect error as it is. An owner that does not
    /// answer, or does not answer as a Live node, is [`SESSION_GONE`].
    pub async fn forward<Req, Resp>(
        &self,
        route: (&str, &NodeRecord),
        procedure: &str,
        ctx: &RequestContext,
        request: &Req,
    ) -> Result<Resp, ConnectError>
    where
        Req: Message,
        Resp: Message,
    {
        let (node, record) = route;
        let unreachable = |why: String| {
            tracing::warn!(node, why = %why, "a forwarded Live session call failed");
            crate::service::connect_error(&gone(
                "the node that owns the session did not answer; resume the session",
            ))
        };
        let http = self.http.as_ref().map_err(|e| unreachable(e.clone()))?;
        let mut call = http
            .post(format!("{}{procedure}", record.url))
            .header("content-type", "application/proto")
            .header("connect-protocol-version", "1")
            .header(FORWARDED_HEADER, &self.me)
            .body(request.encode_to_vec());
        for name in CARRIED {
            if let Some(value) = ctx.header(name) {
                call = call.header(name, value.clone());
            }
        }
        let answer = call.send().await.map_err(|e| unreachable(e.to_string()))?;
        let status = answer.status();
        let body = answer
            .bytes()
            .await
            .map_err(|e| unreachable(e.to_string()))?;
        if status.is_success() {
            return Resp::decode_from_slice(&body)
                .map_err(|e| unreachable(format!("an undecodable answer: {e}")));
        }
        serde_json::from_slice::<ConnectError>(&body)
            .map_err(|_| unreachable(format!("{status} without a Connect error")))
            .and_then(Err)
    }
}

fn encode(record: &NodeRecord) -> Result<Vec<u8>, LiveError> {
    serde_json::to_vec(record).map_err(|e| LiveError::Internal(format!("a node record: {e}")))
}

fn decode(bytes: &[u8]) -> Result<NodeRecord, LiveError> {
    serde_json::from_slice(bytes).map_err(|e| LiveError::Corrupt(format!("a node record: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_is_the_prefix_before_the_random_part() {
        let random = "0123456789abcdef0123456789ABCDEF";
        assert_eq!(owner_of(&format!("1-{random}")), Some("1"));
        assert_eq!(owner_of(&format!("live-a.2-{random}")), Some("live-a.2"));
        assert_eq!(owner_of("nobody"), None);
        assert_eq!(owner_of(&format!("-{random}")), None);
        assert_eq!(owner_of("1-0123"), None);
        assert_eq!(owner_of(&format!("1-{}", "g".repeat(32))), None);
    }

    #[test]
    fn node_ids_are_checked() {
        for ok in ["1", "live-a", "node_2.eu"] {
            assert!(check_node_id(ok).is_ok(), "{ok}");
        }
        for bad in ["", "a b", "a/b", "é", &"n".repeat(MAX_NODE_ID_BYTES + 1)] {
            assert!(check_node_id(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn advertise_urls_are_checked() {
        assert!(check_url("http://127.0.0.1:7710").is_ok());
        assert!(check_url("https://live-2.internal:7700").is_ok());
        assert!(check_url("live-2:7700").is_err());
        assert!(check_url("ftp://live-2").is_err());
    }
}
