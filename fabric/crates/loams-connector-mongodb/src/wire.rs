//! The MongoDB wire protocol as this crate speaks it: `OP_MSG` (opcode
//! 2013, MongoDB 3.6+) with a body section and document sequences, over any
//! stream the SDK's `net` module opened (plan ruling R16.1).
//!
//! A [`Connection`] runs one command at a time. If a command's future is
//! dropped mid-way (a cancelled `Source::next`) or times out, the connection
//! is marked broken and refuses further commands with a retryable error, so
//! its owner reconnects instead of reading a stale reply.

use std::time::Duration;

use loams_connector_sdk::{ConnectorError, ErrorClass};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::bson::{BsonError, Document};
use crate::doc;

pub const OP_MSG: i32 = 2013;
const HEADER_LEN: usize = 16;
/// The server's default `maxMessageSizeBytes` (48 MB) until `hello` says.
pub const DEFAULT_MAX_MESSAGE: usize = 48_000_000;
/// The default `maxBsonObjectSize` (16 MiB).
pub const DEFAULT_MAX_BSON: usize = 16 * 1024 * 1024;

const FLAG_CHECKSUM: u32 = 1;
const FLAG_MORE_TO_COME: u32 = 1 << 1;
const FLAG_EXHAUST_ALLOWED: u32 = 1 << 16;

/// A kind-1 section: a named run of documents (`documents`, `updates`,
/// `deletes`), which may exceed the 16 MiB limit of the body together.
#[derive(Debug, Clone, PartialEq)]
pub struct Sequence {
    pub identifier: String,
    pub documents: Vec<Document>,
}

/// A decoded `OP_MSG`.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub request_id: i32,
    pub response_to: i32,
    pub more_to_come: bool,
    pub body: Document,
    pub sequences: Vec<Sequence>,
}

/// A malformed frame.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    #[error("wire: {0}")]
    Frame(String),
    #[error(transparent)]
    Bson(#[from] BsonError),
}

/// Encode an `OP_MSG` with `body` and `sequences`.
pub fn encode(
    request_id: i32,
    response_to: i32,
    body: &Document,
    sequences: &[Sequence],
) -> Result<Vec<u8>, WireError> {
    let mut out = vec![0u8; HEADER_LEN];
    out.extend_from_slice(&0u32.to_le_bytes());
    out.push(0);
    out.extend_from_slice(&body.to_bytes()?);
    for s in sequences {
        out.push(1);
        let start = out.len();
        out.extend_from_slice(&[0; 4]);
        if s.identifier.as_bytes().contains(&0) {
            return Err(WireError::Frame(
                "a sequence identifier contains NUL".into(),
            ));
        }
        out.extend_from_slice(s.identifier.as_bytes());
        out.push(0);
        for d in &s.documents {
            out.extend_from_slice(&d.to_bytes()?);
        }
        let len = (out.len() - start) as i32;
        out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    }
    let len =
        i32::try_from(out.len()).map_err(|_| WireError::Frame("message over 2 GiB".into()))?;
    out[0..4].copy_from_slice(&len.to_le_bytes());
    out[4..8].copy_from_slice(&request_id.to_le_bytes());
    out[8..12].copy_from_slice(&response_to.to_le_bytes());
    out[12..16].copy_from_slice(&OP_MSG.to_le_bytes());
    Ok(out)
}

fn le_i32(b: &[u8]) -> i32 {
    i32::from_le_bytes(b[..4].try_into().expect("4 bytes"))
}

/// Decode one whole frame (header included).
pub fn decode(frame: &[u8]) -> Result<Message, WireError> {
    let bad = |m: &str| WireError::Frame(m.to_owned());
    if frame.len() < HEADER_LEN + 5 {
        return Err(bad("frame shorter than a header and flags"));
    }
    if le_i32(frame) as usize != frame.len() {
        return Err(bad("frame length does not match the header"));
    }
    let op = le_i32(&frame[12..]);
    if op != OP_MSG {
        return Err(WireError::Frame(format!("unexpected opcode {op}")));
    }
    let flags = u32::from_le_bytes(frame[16..20].try_into().expect("4"));
    // Bits 0–15 are required: an unknown one must be refused.
    if flags & 0xFFFF & !(FLAG_CHECKSUM | FLAG_MORE_TO_COME) != 0 {
        return Err(WireError::Frame(format!(
            "unknown required flag bits {flags:#x}"
        )));
    }
    let _ = FLAG_EXHAUST_ALLOWED;
    let end = if flags & FLAG_CHECKSUM != 0 {
        frame
            .len()
            .checked_sub(4)
            .filter(|e| *e >= 20)
            .ok_or_else(|| bad("checksum flag without a checksum"))?
    } else {
        frame.len()
    };
    let mut pos = 20;
    let mut body = None;
    let mut sequences = Vec::new();
    while pos < end {
        let kind = frame[pos];
        pos += 1;
        match kind {
            0 => {
                let (d, n) = Document::read_prefix(&frame[pos..end])?;
                if body.replace(d).is_some() {
                    return Err(bad("two body sections"));
                }
                pos += n;
            }
            1 => {
                if end - pos < 4 {
                    return Err(bad("truncated sequence"));
                }
                let size = le_i32(&frame[pos..]);
                let send = pos
                    .checked_add(usize::try_from(size).map_err(|_| bad("negative sequence size"))?)
                    .filter(|e| *e <= end && size >= 5)
                    .ok_or_else(|| bad("sequence past the frame"))?;
                let rest = &frame[pos + 4..send];
                let nul = rest
                    .iter()
                    .position(|b| *b == 0)
                    .ok_or_else(|| bad("unterminated sequence identifier"))?;
                let identifier = std::str::from_utf8(&rest[..nul])
                    .map_err(|_| bad("sequence identifier is not UTF-8"))?
                    .to_owned();
                let mut docs = Vec::new();
                let mut p = nul + 1;
                while p < rest.len() {
                    let (d, n) = Document::read_prefix(&rest[p..])?;
                    docs.push(d);
                    p += n;
                }
                sequences.push(Sequence {
                    identifier,
                    documents: docs,
                });
                pos = send;
            }
            k => return Err(WireError::Frame(format!("unknown section kind {k}"))),
        }
    }
    Ok(Message {
        request_id: le_i32(&frame[4..]),
        response_to: le_i32(&frame[8..]),
        more_to_come: flags & FLAG_MORE_TO_COME != 0,
        body: body.ok_or_else(|| bad("no body section"))?,
        sequences,
    })
}

/// Read one frame from `io`, refusing one over `max` bytes.
pub async fn read_frame<R: AsyncRead + Unpin + ?Sized>(
    io: &mut R,
    max: usize,
) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    io.read_exact(&mut len).await?;
    let n = i32::from_le_bytes(len);
    if n < HEADER_LEN as i32 || n as usize > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame length {n} out of range"),
        ));
    }
    let mut frame = vec![0u8; n as usize];
    frame[..4].copy_from_slice(&len);
    io.read_exact(&mut frame[4..]).await?;
    Ok(frame)
}

/// An error document the server returned (`ok: 0`, a write error, or a
/// write-concern error). Attached as the `source` of the [`ConnectorError`]
/// it is classified into, so callers can match on the code.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code_name} ({code}): {message}")]
pub struct ServerError {
    pub code: i32,
    pub code_name: String,
    pub message: String,
    pub labels: Vec<String>,
    /// For a write error: the index of the operation that failed.
    pub index: Option<usize>,
}

pub mod codes {
    pub const BAD_VALUE: i32 = 2;
    pub const HOST_UNREACHABLE: i32 = 6;
    pub const HOST_NOT_FOUND: i32 = 7;
    pub const UNAUTHORIZED: i32 = 13;
    pub const USER_NOT_FOUND: i32 = 11;
    pub const AUTHENTICATION_FAILED: i32 = 18;
    pub const NAMESPACE_NOT_FOUND: i32 = 26;
    pub const CURSOR_NOT_FOUND: i32 = 43;
    pub const NAMESPACE_EXISTS: i32 = 48;
    pub const MAX_TIME_MS_EXPIRED: i32 = 50;
    pub const STALE_SHARD_VERSION: i32 = 63;
    pub const WRITE_CONCERN_FAILED: i32 = 64;
    pub const NETWORK_TIMEOUT: i32 = 89;
    pub const SHUTDOWN_IN_PROGRESS: i32 = 91;
    pub const WRITE_CONFLICT: i32 = 112;
    pub const DOCUMENT_VALIDATION_FAILURE: i32 = 121;
    pub const FAILED_TO_SATISFY_READ_PREFERENCE: i32 = 133;
    pub const CAPPED_POSITION_LOST: i32 = 136;
    pub const STALE_EPOCH: i32 = 150;
    pub const PRIMARY_STEPPED_DOWN: i32 = 189;
    pub const RETRY_CHANGE_STREAM: i32 = 234;
    pub const SNAPSHOT_TOO_OLD: i32 = 239;
    pub const SNAPSHOT_UNAVAILABLE: i32 = 246;
    pub const NO_SUCH_TRANSACTION: i32 = 251;
    pub const INVALID_RESUME_TOKEN: i32 = 260;
    pub const EXCEEDED_TIME_LIMIT: i32 = 262;
    pub const CHANGE_STREAM_FATAL_ERROR: i32 = 280;
    pub const CHANGE_STREAM_HISTORY_LOST: i32 = 286;
    pub const INGRESS_RATE_LIMITED: i32 = 462;
    pub const SOCKET_EXCEPTION: i32 = 9001;
    pub const DUPLICATE_KEY: i32 = 11000;
    pub const NOT_WRITABLE_PRIMARY: i32 = 10107;
    pub const INTERRUPTED_AT_SHUTDOWN: i32 = 11600;
    pub const INTERRUPTED_DUE_TO_REPL_STATE_CHANGE: i32 = 11602;
    pub const STALE_CONFIG: i32 = 13388;
    pub const BSON_OBJECT_TOO_LARGE: i32 = 10334;
    pub const NOT_PRIMARY_NO_SECONDARY_OK: i32 = 13435;
    pub const NOT_PRIMARY_OR_SECONDARY: i32 = 13436;
}

impl ServerError {
    fn from_doc(d: &Document, index: Option<usize>) -> Self {
        Self {
            code: d.get_i64("code").unwrap_or(0) as i32,
            code_name: d.get_str("codeName").unwrap_or("UnknownError").to_owned(),
            message: d
                .get_str("errmsg")
                .unwrap_or("the server returned an error")
                .to_owned(),
            labels: d
                .get_array("errorLabels")
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
            index,
        }
    }

    pub fn has_label(&self, l: &str) -> bool {
        self.labels.iter().any(|x| x == l)
    }

    /// The SDK's class for this error. `write` marks a per-document write
    /// error, which is the document's fault (`Poison`) unless transient.
    pub fn class(&self, write: bool) -> ErrorClass {
        use codes::*;
        let transient_label = self.has_label("TransientTransactionError")
            || self.has_label("RetryableWriteError")
            || self.has_label("ResumableChangeStreamError");
        match self.code {
            AUTHENTICATION_FAILED | UNAUTHORIZED | USER_NOT_FOUND => ErrorClass::Auth,
            CHANGE_STREAM_HISTORY_LOST
            | CHANGE_STREAM_FATAL_ERROR
            | CAPPED_POSITION_LOST
            | INVALID_RESUME_TOKEN => ErrorClass::PositionLost,
            INGRESS_RATE_LIMITED => ErrorClass::Throttled {
                after: Duration::from_secs(1),
            },
            HOST_UNREACHABLE
            | HOST_NOT_FOUND
            | NETWORK_TIMEOUT
            | SHUTDOWN_IN_PROGRESS
            | PRIMARY_STEPPED_DOWN
            | EXCEEDED_TIME_LIMIT
            | SOCKET_EXCEPTION
            | NOT_WRITABLE_PRIMARY
            | INTERRUPTED_AT_SHUTDOWN
            | INTERRUPTED_DUE_TO_REPL_STATE_CHANGE
            | NOT_PRIMARY_NO_SECONDARY_OK
            | NOT_PRIMARY_OR_SECONDARY
            | STALE_SHARD_VERSION
            | STALE_EPOCH
            | STALE_CONFIG
            | RETRY_CHANGE_STREAM
            | FAILED_TO_SATISFY_READ_PREFERENCE
            | WRITE_CONFLICT
            | NO_SUCH_TRANSACTION
            | MAX_TIME_MS_EXPIRED
            | WRITE_CONCERN_FAILED
            | SNAPSHOT_TOO_OLD
            | SNAPSHOT_UNAVAILABLE
            | CURSOR_NOT_FOUND => ErrorClass::Retryable { after: None },
            _ if transient_label => ErrorClass::Retryable { after: None },
            DUPLICATE_KEY | DOCUMENT_VALIDATION_FAILURE | BSON_OBJECT_TOO_LARGE => {
                ErrorClass::Poison
            }
            _ if write => ErrorClass::Poison,
            _ => ErrorClass::Fatal,
        }
    }

    pub fn into_connector_error(self, what: &str, write: bool) -> ConnectorError {
        let class = self.class(write);
        ConnectorError::new(class, format!("{what}: {self}")).with_source(self)
    }
}

/// The server error behind a connector error, if it came from one.
pub fn server_error(e: &ConnectorError) -> Option<&ServerError> {
    e.source.as_ref()?.downcast_ref::<ServerError>()
}

/// Check a reply: `ok: 1`, no write error, no write-concern error.
pub fn check_reply(reply: Document, what: &str) -> Result<Document, ConnectorError> {
    if reply.get("ok").and_then(crate::bson::Bson::as_i64) != Some(1) {
        return Err(ServerError::from_doc(&reply, None).into_connector_error(what, false));
    }
    if let Some(errors) = reply.get_array("writeErrors")
        && let Some(first) = errors.first().and_then(crate::bson::Bson::as_document)
    {
        let index = first.get_i64("index").map(|i| i as usize);
        return Err(ServerError::from_doc(first, index).into_connector_error(what, true));
    }
    if let Some(wce) = reply.get_document("writeConcernError") {
        let mut e = ServerError::from_doc(wce, None);
        // A write-concern error is about replication, never the document.
        if e.code == 0 {
            e.code = codes::WRITE_CONCERN_FAILED;
        }
        let class = match e.class(false) {
            ErrorClass::Poison | ErrorClass::Fatal => ErrorClass::Retryable { after: None },
            c => c,
        };
        return Err(
            ConnectorError::new(class, format!("{what}: write concern: {e}")).with_source(e),
        );
    }
    Ok(reply)
}

/// What `hello` said about the server.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Hello {
    pub writable_primary: bool,
    pub secondary: bool,
    pub set_name: Option<String>,
    pub primary: Option<String>,
    pub hosts: Vec<String>,
    pub mongos: bool,
    pub max_wire_version: i64,
    pub max_bson_object_size: usize,
    pub max_message_size_bytes: usize,
    pub max_write_batch_size: usize,
    pub logical_session_timeout_minutes: Option<i64>,
    pub sasl_supported_mechs: Vec<String>,
}

/// The kind of deployment a connection reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topology {
    Standalone,
    ReplicaSet,
    Sharded,
}

impl Hello {
    pub fn parse(d: &Document) -> Self {
        let strings = |k: &str| -> Vec<String> {
            d.get_array(k)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        let size = |k: &str, default: usize| {
            d.get_i64(k)
                .and_then(|v| usize::try_from(v).ok())
                .unwrap_or(default)
        };
        Self {
            writable_primary: d
                .get_bool("isWritablePrimary")
                .or_else(|| d.get_bool("ismaster"))
                .unwrap_or(false),
            secondary: d.get_bool("secondary").unwrap_or(false),
            set_name: d.get_str("setName").map(str::to_owned),
            primary: d.get_str("primary").map(str::to_owned),
            hosts: strings("hosts"),
            mongos: d.get_str("msg") == Some("isdbgrid"),
            max_wire_version: d.get_i64("maxWireVersion").unwrap_or(0),
            max_bson_object_size: size("maxBsonObjectSize", DEFAULT_MAX_BSON),
            max_message_size_bytes: size("maxMessageSizeBytes", DEFAULT_MAX_MESSAGE),
            max_write_batch_size: size("maxWriteBatchSize", 100_000),
            logical_session_timeout_minutes: d.get_i64("logicalSessionTimeoutMinutes"),
            sasl_supported_mechs: strings("saslSupportedMechs"),
        }
    }

    pub fn topology(&self) -> Topology {
        if self.mongos {
            Topology::Sharded
        } else if self.set_name.is_some() {
            Topology::ReplicaSet
        } else {
            Topology::Standalone
        }
    }

    /// Multi-document transactions: a replica set on 4.0+ (wire 7) or a
    /// sharded cluster on 4.2+ (wire 8), with sessions.
    pub fn supports_transactions(&self) -> bool {
        self.logical_session_timeout_minutes.is_some()
            && match self.topology() {
                Topology::Standalone => false,
                Topology::ReplicaSet => self.max_wire_version >= 7,
                Topology::Sharded => self.max_wire_version >= 8,
            }
    }

    /// Change streams and snapshot reads need a replica set or a sharded
    /// cluster.
    pub fn supports_change_streams(&self) -> bool {
        self.topology() != Topology::Standalone && self.max_wire_version >= 6
    }

    /// Pre- and post-images and `showExpandedEvents` (6.0, wire 17).
    pub fn supports_expanded_events(&self) -> bool {
        self.max_wire_version >= 17
    }
}

/// Any stream a connection runs over (TCP or TLS from `sdk::net`, or a test
/// pipe).
pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

/// One connection to one server.
pub struct Connection {
    io: Box<dyn Stream>,
    address: String,
    next_request: i32,
    broken: bool,
    timeout: Duration,
    pub hello: Hello,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("address", &self.address)
            .field("broken", &self.broken)
            .finish_non_exhaustive()
    }
}

impl Connection {
    pub fn new(io: Box<dyn Stream>, address: impl Into<String>, timeout: Duration) -> Self {
        Self {
            io,
            address: address.into(),
            next_request: 1,
            broken: false,
            timeout,
            hello: Hello::default(),
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    /// Whether a command was abandoned or failed mid-frame: the connection
    /// must be dropped.
    pub fn is_broken(&self) -> bool {
        self.broken
    }

    /// Run `cmd` against `db` and check the reply.
    pub async fn command(&mut self, db: &str, cmd: Document) -> Result<Document, ConnectorError> {
        self.command_with(db, cmd, &[]).await
    }

    /// Run `cmd` with document sequences.
    pub async fn command_with(
        &mut self,
        db: &str,
        cmd: Document,
        sequences: &[Sequence],
    ) -> Result<Document, ConnectorError> {
        let what = cmd.keys().next().unwrap_or("command").to_owned();
        let reply = self.raw(db, cmd, sequences).await?;
        check_reply(reply, &what)
    }

    /// Run `cmd` and return the reply unchecked (`ok: 0` included).
    pub async fn raw(
        &mut self,
        db: &str,
        mut cmd: Document,
        sequences: &[Sequence],
    ) -> Result<Document, ConnectorError> {
        if self.broken {
            return Err(ConnectorError::retryable(format!(
                "the connection to {} was abandoned mid-command",
                self.address
            )));
        }
        let what = cmd.keys().next().unwrap_or("command").to_owned();
        cmd.insert("$db", db);
        let id = self.next_request;
        self.next_request = self.next_request.wrapping_add(1).max(1);
        let frame = encode(id, 0, &cmd, sequences)
            .map_err(|e| ConnectorError::poison(format!("{what}: {e}")))?;
        if frame.len() > self.hello.max_message_size_bytes.max(DEFAULT_MAX_MESSAGE) {
            return Err(ConnectorError::poison(format!(
                "{what}: a {} byte message is over the server's limit",
                frame.len()
            )));
        }
        // Broken until the whole reply is read: a drop or a timeout in
        // between leaves the stream mid-frame.
        self.broken = true;
        let max = self.hello.max_message_size_bytes.max(DEFAULT_MAX_MESSAGE);
        let address = self.address.clone();
        let io = &mut self.io;
        let round_trip = async {
            io.write_all(&frame).await?;
            io.flush().await?;
            loop {
                let reply = read_frame(io.as_mut(), max).await?;
                let msg = decode(&reply).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                if msg.response_to == id {
                    return Ok::<_, std::io::Error>(msg.body);
                }
            }
        };
        let body = match tokio::time::timeout(self.timeout, round_trip).await {
            Ok(Ok(body)) => body,
            Ok(Err(e)) => {
                return Err(ConnectorError::retryable(format!(
                    "{what} on {address}: {e}"
                )));
            }
            Err(_) => {
                return Err(ConnectorError::retryable(format!(
                    "{what} on {address}: no reply within {:?}",
                    self.timeout
                )));
            }
        };
        self.broken = false;
        Ok(body)
    }

    /// `hello`, recorded on the connection. `client` metadata is sent on a
    /// connection's first `hello` only, as the handshake specification asks.
    pub async fn handshake(&mut self, sasl_user: Option<&str>) -> Result<&Hello, ConnectorError> {
        let mut cmd = doc! {
            "hello" => 1,
            "helloOk" => true,
            "client" => doc! {
                "driver" => doc! { "name" => "loams-connector-mongodb", "version" => env!("CARGO_PKG_VERSION") },
                "os" => doc! { "type" => std::env::consts::OS },
            },
            "compression" => Vec::<crate::bson::Bson>::new(),
        };
        if let Some(u) = sasl_user {
            cmd.insert("saslSupportedMechs", u);
        }
        let reply = self.command("admin", cmd).await?;
        self.hello = Hello::parse(&reply);
        Ok(&self.hello)
    }

    /// Set the per-command timeout (a change stream's `getMore` waits).
    pub fn set_timeout(&mut self, t: Duration) {
        self.timeout = t;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bson::Bson;
    use crate::testkit::FakeServer;

    #[test]
    fn frames_round_trip_with_sequences() {
        let body = doc! { "insert" => "c", "$db" => "d" };
        let seq = Sequence {
            identifier: "documents".into(),
            documents: vec![doc! { "a" => 1 }, doc! { "a" => 2 }],
        };
        let frame = encode(7, 3, &body, std::slice::from_ref(&seq)).expect("encode");
        let msg = decode(&frame).expect("decode");
        assert_eq!(
            (msg.request_id, msg.response_to, msg.more_to_come),
            (7, 3, false)
        );
        assert_eq!(msg.body, body);
        assert_eq!(msg.sequences, vec![seq]);
    }

    #[test]
    fn malformed_frames_are_refused() {
        let frame = encode(1, 0, &doc! { "ping" => 1 }, &[]).expect("encode");
        for n in 0..frame.len() {
            assert!(decode(&frame[..n]).is_err());
        }
        let mut bad = frame.clone();
        bad[16] = 0x04; // an unknown required flag bit
        assert!(decode(&bad).is_err());
        let mut bad = frame.clone();
        bad[20] = 9; // an unknown section kind
        assert!(decode(&bad).is_err());
        let mut bad = frame;
        bad[12] = 1; // OP_REPLY
        assert!(decode(&bad).is_err());
    }

    #[test]
    fn errors_are_classified_by_code_and_label() {
        let e = |code: i32, labels: &[&str]| ServerError {
            code,
            code_name: "X".into(),
            message: "m".into(),
            labels: labels.iter().map(|s| (*s).to_owned()).collect(),
            index: None,
        };
        assert_eq!(e(18, &[]).class(false), ErrorClass::Auth);
        assert_eq!(e(286, &[]).class(false), ErrorClass::PositionLost);
        assert_eq!(e(260, &[]).class(false), ErrorClass::PositionLost);
        assert!(e(10107, &[]).class(false).is_transient());
        assert!(e(251, &[]).class(false).is_transient());
        assert!(
            e(99999, &["TransientTransactionError"])
                .class(false)
                .is_transient()
        );
        assert_eq!(e(11000, &[]).class(true), ErrorClass::Poison);
        assert_eq!(e(2, &[]).class(true), ErrorClass::Poison);
        assert_eq!(e(2, &[]).class(false), ErrorClass::Fatal);
        assert!(matches!(
            e(462, &[]).class(false),
            ErrorClass::Throttled { .. }
        ));
    }

    #[test]
    fn replies_are_checked() {
        assert!(check_reply(doc! { "ok" => 1.0 }, "x").is_ok());
        let err = check_reply(
            doc! { "ok" => 0.0, "code" => 26, "codeName" => "NamespaceNotFound", "errmsg" => "ns" },
            "find",
        )
        .expect_err("ok 0");
        assert_eq!(err.class, ErrorClass::Fatal);
        assert_eq!(server_error(&err).map(|s| s.code), Some(26));
        let err = check_reply(
            doc! { "ok" => 1, "n" => 0, "writeErrors" => vec![Bson::from(doc! {
                "index" => 3, "code" => 11000, "errmsg" => "dup"
            })] },
            "insert",
        )
        .expect_err("write error");
        assert_eq!(err.class, ErrorClass::Poison);
        assert_eq!(server_error(&err).and_then(|s| s.index), Some(3));
        let err = check_reply(
            doc! { "ok" => 1, "writeConcernError" => doc! { "code" => 100, "errmsg" => "w" } },
            "update",
        )
        .expect_err("wce");
        assert!(err.class.is_transient());
    }

    #[test]
    fn hello_says_what_the_deployment_can_do() {
        let rs = Hello::parse(&doc! {
            "isWritablePrimary" => true, "setName" => "rs0", "maxWireVersion" => 25,
            "logicalSessionTimeoutMinutes" => 30, "hosts" => vec![Bson::from("h:1")],
        });
        assert_eq!(rs.topology(), Topology::ReplicaSet);
        assert!(rs.supports_transactions() && rs.supports_change_streams());
        let standalone = Hello::parse(&doc! {
            "isWritablePrimary" => true, "maxWireVersion" => 25, "logicalSessionTimeoutMinutes" => 30,
        });
        assert_eq!(standalone.topology(), Topology::Standalone);
        assert!(!standalone.supports_transactions());
        assert!(!standalone.supports_change_streams());
        let mongos = Hello::parse(
            &doc! { "msg" => "isdbgrid", "maxWireVersion" => 25, "logicalSessionTimeoutMinutes" => 30 },
        );
        assert_eq!(mongos.topology(), Topology::Sharded);
        assert!(mongos.supports_transactions());
    }

    #[tokio::test]
    async fn a_connection_abandoned_mid_command_refuses_the_next() {
        let server = FakeServer::start(|cmd| {
            if cmd.contains_key("sleep") {
                std::thread::sleep(Duration::from_millis(300));
            }
            doc! { "ok" => 1, "echo" => cmd.keys().next().unwrap_or("").to_owned() }
        })
        .await;
        let mut c = server.connect(Duration::from_secs(5)).await;
        let r = c
            .command("admin", doc! { "ping" => 1 })
            .await
            .expect("ping");
        assert_eq!(r.get_str("echo"), Some("ping"));
        let cancelled = tokio::time::timeout(
            Duration::from_millis(50),
            c.command("admin", doc! { "sleep" => 1 }),
        )
        .await;
        assert!(cancelled.is_err(), "the command was cancelled");
        assert!(c.is_broken());
        let err = c
            .command("admin", doc! { "ping" => 1 })
            .await
            .expect_err("broken");
        assert!(err.class.is_transient());
    }
}
