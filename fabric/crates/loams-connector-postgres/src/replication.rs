//! A minimal logical-replication client: the walsender's startup and
//! authentication, simple queries (`CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT`,
//! `START_REPLICATION`) and the CopyBoth stream with Standby Status Updates.
//!
//! `pg_walstream` 0.9.0 (R0.6) decodes the pgoutput messages; its own
//! connection layer is not used (ruling R10.1): its default backend links
//! `aws-lc-rs` (a cmake build, and a second TLS stack beside the SDK's ring)
//! and dials the server itself, past `sdk::net::connect`'s egress check. This
//! is OR-24's fallback in the R0.6 sense — a CopyBoth client over
//! `postgres-protocol`'s frontend messages and SCRAM — kept to what the CDC
//! source needs.
//!
//! The stream is read on its own task into a bounded channel (so
//! `Source::next` stays cancel-safe), and a second task sends a Standby
//! Status Update every `status_interval` whatever the reader is doing, so a
//! paused pipeline (backpressure) does not trip `wal_sender_timeout`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use loams_connector_sdk::{ConnectorContext, ConnectorError, net};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use crate::config::PgConfig;
use crate::conn::{PgStream, class_of_sqlstate, password, tls_config};

/// A WAL position.
pub type Lsn = u64;

/// `X/Y`, as Postgres prints an LSN.
pub fn format_lsn(lsn: Lsn) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

/// Parse `X/Y`.
pub fn parse_lsn(s: &str) -> Option<Lsn> {
    let (hi, lo) = s.trim().split_once('/')?;
    Some((u64::from_str_radix(hi, 16).ok()? << 32) | u64::from_str_radix(lo, 16).ok()?)
}

/// A backend message: its tag and body.
#[derive(Debug, Clone)]
pub struct Frame {
    pub tag: u8,
    pub body: Bytes,
}

/// A server `ErrorResponse` as a connector error, classed by SQLSTATE.
pub fn error_from(body: &[u8], what: &str) -> ConnectorError {
    let (mut code, mut message) = (String::new(), String::new());
    let mut rest = body;
    while let Some((&field, tail)) = rest.split_first() {
        if field == 0 {
            break;
        }
        let end = tail.iter().position(|b| *b == 0).unwrap_or(tail.len());
        let value = String::from_utf8_lossy(&tail[..end]).into_owned();
        match field {
            b'C' => code = value,
            b'M' => message = value,
            _ => {}
        }
        rest = tail.get(end + 1..).unwrap_or(&[]);
    }
    ConnectorError::new(
        class_of_sqlstate(&code),
        format!("{what}: {message} ({code})"),
    )
}

async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut BytesMut,
) -> Result<Frame, ConnectorError> {
    loop {
        if buf.len() >= 5 {
            let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            if len < 4 {
                return Err(ConnectorError::fatal("replication: bad message length"));
            }
            if buf.len() > len {
                let tag = buf[0];
                let mut frame = buf.split_to(len + 1);
                frame.advance(5);
                return Ok(Frame {
                    tag,
                    body: frame.freeze(),
                });
            }
            buf.reserve(len + 1 - buf.len());
        }
        let n = r
            .read_buf(buf)
            .await
            .map_err(|e| ConnectorError::retryable(format!("replication: reading: {e}")))?;
        if n == 0 {
            return Err(ConnectorError::retryable("replication: connection closed"));
        }
    }
}

fn cstr(b: &mut Bytes) -> String {
    let end = b.iter().position(|x| *x == 0).unwrap_or(b.len());
    let s = String::from_utf8_lossy(&b[..end]).into_owned();
    b.advance((end + 1).min(b.len()));
    s
}

/// A replication connection before streaming.
pub struct ReplConn {
    stream: PgStream,
    buf: BytesMut,
    /// `server_version_num`-style, from the `server_version` parameter.
    pub server_version: u32,
}

/// `CREATE_REPLICATION_SLOT`'s answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotCreated {
    pub consistent_point: Lsn,
    pub snapshot: Option<String>,
}

/// `"17.2"` → 170002, `"9.6.1"` → 90601, `"16beta1"` → 160000.
pub fn version_num(s: &str) -> u32 {
    let nums: Vec<u32> = s
        .split(|c: char| !c.is_ascii_digit())
        .filter(|p| !p.is_empty())
        .take(3)
        .filter_map(|p| p.parse().ok())
        .collect();
    match nums.as_slice() {
        [major, rest @ ..] if *major >= 10 => major * 10_000 + rest.first().copied().unwrap_or(0),
        [major, minor, rest @ ..] => {
            major * 10_000 + minor * 100 + rest.first().copied().unwrap_or(0)
        }
        [major] => major * 10_000,
        [] => 0,
    }
}

impl ReplConn {
    /// Connect, authenticate and wait for the walsender to be ready.
    pub async fn connect(cfg: &PgConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let password = password(cfg, ctx).await?;
        let tcp = net::connect(&cfg.host, cfg.port, ctx.egress.as_ref()).await?;
        let stream = match tls_config(cfg)? {
            None => PgStream::Plain(tcp),
            Some(config) => {
                let mut tcp = tcp;
                let mut req = BytesMut::new();
                frontend::ssl_request(&mut req);
                write_all(&mut tcp, &req).await?;
                let mut answer = [0u8; 1];
                tcp.read_exact(&mut answer).await.map_err(|e| {
                    ConnectorError::retryable(format!("replication: SSL request: {e}"))
                })?;
                if answer[0] != b'S' {
                    return Err(ConnectorError::fatal("replication: the server refused TLS"));
                }
                let name =
                    rustls::pki_types::ServerName::try_from(cfg.host.clone()).map_err(|e| {
                        ConnectorError::fatal(format!("{} is not a TLS name: {e}", cfg.host))
                    })?;
                let tls = tokio_rustls::TlsConnector::from(config)
                    .connect(name, tcp)
                    .await
                    .map_err(|e| ConnectorError::retryable(format!("replication: TLS: {e}")))?;
                PgStream::Tls(Box::new(tls))
            }
        };
        Self::start(stream, &cfg.user, &cfg.database, password).await
    }

    /// The startup exchange over an open stream (tests use a plain loopback
    /// stream).
    pub async fn start(
        mut stream: PgStream,
        user: &str,
        database: &str,
        password: Option<Vec<u8>>,
    ) -> Result<Self, ConnectorError> {
        let mut out = BytesMut::new();
        frontend::startup_message(
            [
                ("user", user),
                ("database", database),
                ("replication", "database"),
                ("application_name", "loams"),
            ],
            &mut out,
        )
        .map_err(|e| ConnectorError::fatal(format!("replication: startup: {e}")))?;
        write_all(&mut stream, &out).await?;
        let mut conn = Self {
            stream,
            buf: BytesMut::with_capacity(8192),
            server_version: 0,
        };
        let need_password = || {
            ConnectorError::auth(
                "replication: the server asks for a password and none is configured",
            )
        };
        let mut scram: Option<ScramSha256> = None;
        loop {
            let f = conn.frame().await?;
            let mut body = f.body.clone();
            match f.tag {
                b'R' => {
                    let code = body.get_i32();
                    let mut out = BytesMut::new();
                    match code {
                        0 => continue,
                        3 => {
                            let p = password.as_deref().ok_or_else(need_password)?;
                            frontend::password_message(p, &mut out).map_err(io_fatal)?;
                        }
                        5 => {
                            let p = password.as_deref().ok_or_else(need_password)?;
                            let salt = [body[0], body[1], body[2], body[3]];
                            let hash = postgres_protocol::authentication::md5_hash(
                                user.as_bytes(),
                                p,
                                salt,
                            );
                            frontend::password_message(hash.as_bytes(), &mut out)
                                .map_err(io_fatal)?;
                        }
                        10 => {
                            let p = password.as_deref().ok_or_else(need_password)?;
                            let mut mechanisms = Vec::new();
                            while body.has_remaining() && body[0] != 0 {
                                mechanisms.push(cstr(&mut body));
                            }
                            if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                                return Err(ConnectorError::auth(format!(
                                    "replication: no supported SASL mechanism in {mechanisms:?}"
                                )));
                            }
                            let s = ScramSha256::new(p, ChannelBinding::unsupported());
                            frontend::sasl_initial_response("SCRAM-SHA-256", s.message(), &mut out)
                                .map_err(io_fatal)?;
                            scram = Some(s);
                        }
                        11 => {
                            let s = scram.as_mut().ok_or_else(|| {
                                ConnectorError::fatal("replication: SASL continue before start")
                            })?;
                            s.update(&body).map_err(|e| {
                                ConnectorError::auth(format!("replication: SCRAM: {e}"))
                            })?;
                            frontend::sasl_response(s.message(), &mut out).map_err(io_fatal)?;
                        }
                        12 => {
                            let s = scram.as_mut().ok_or_else(|| {
                                ConnectorError::fatal("replication: SASL final before start")
                            })?;
                            s.finish(&body).map_err(|e| {
                                ConnectorError::auth(format!("replication: SCRAM: {e}"))
                            })?;
                            continue;
                        }
                        other => {
                            return Err(ConnectorError::auth(format!(
                                "replication: unsupported authentication method {other}"
                            )));
                        }
                    }
                    write_all(&mut conn.stream, &out).await?;
                }
                b'S' => {
                    let name = cstr(&mut body);
                    let value = cstr(&mut body);
                    if name == "server_version" {
                        conn.server_version = version_num(&value);
                    }
                }
                b'K' | b'N' => {}
                b'Z' => return Ok(conn),
                b'E' => return Err(error_from(&f.body, "replication: login")),
                other => {
                    return Err(ConnectorError::fatal(format!(
                        "replication: unexpected message {:?} at startup",
                        other as char
                    )));
                }
            }
        }
    }

    async fn frame(&mut self) -> Result<Frame, ConnectorError> {
        read_frame(&mut self.stream, &mut self.buf).await
    }

    /// Run a simple query; the rows' text values.
    pub async fn simple_query(
        &mut self,
        sql: &str,
    ) -> Result<Vec<Vec<Option<String>>>, ConnectorError> {
        let mut out = BytesMut::new();
        frontend::query(sql, &mut out).map_err(io_fatal)?;
        write_all(&mut self.stream, &out).await?;
        let mut rows = Vec::new();
        let mut error = None;
        loop {
            let f = self.frame().await?;
            match f.tag {
                b'D' => {
                    let mut b = f.body.clone();
                    let n = b.get_i16();
                    let mut row = Vec::with_capacity(n.max(0) as usize);
                    for _ in 0..n {
                        let len = b.get_i32();
                        if len < 0 {
                            row.push(None);
                        } else {
                            let v = b.split_to(len as usize);
                            row.push(Some(String::from_utf8_lossy(&v).into_owned()));
                        }
                    }
                    rows.push(row);
                }
                b'E' => error = Some(error_from(&f.body, "replication")),
                b'Z' => {
                    return match error {
                        Some(e) => Err(e),
                        None => Ok(rows),
                    };
                }
                _ => {}
            }
        }
    }

    /// `CREATE_REPLICATION_SLOT … LOGICAL pgoutput`, exporting a snapshot
    /// when `export` (PG 15+ syntax from 15, the old keyword before).
    pub async fn create_slot(
        &mut self,
        slot: &str,
        export: bool,
    ) -> Result<SlotCreated, ConnectorError> {
        let sql = if self.server_version >= 150_000 {
            format!(
                "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT '{}')",
                crate::types::quote_ident(slot),
                if export { "export" } else { "nothing" }
            )
        } else {
            format!(
                "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput {}",
                crate::types::quote_ident(slot),
                if export {
                    "EXPORT_SNAPSHOT"
                } else {
                    "NOEXPORT_SNAPSHOT"
                }
            )
        };
        let rows = self.simple_query(&sql).await?;
        let row = rows.first().ok_or_else(|| {
            ConnectorError::fatal("replication: CREATE_REPLICATION_SLOT returned no row")
        })?;
        let consistent_point = row
            .get(1)
            .and_then(|v| v.as_deref())
            .and_then(parse_lsn)
            .ok_or_else(|| ConnectorError::fatal("replication: no consistent point"))?;
        Ok(SlotCreated {
            consistent_point,
            snapshot: row.get(2).cloned().flatten(),
        })
    }

    /// `START_REPLICATION SLOT … LOGICAL <start> (…)`, then stream.
    pub async fn start_replication(
        mut self,
        slot: &str,
        start: Lsn,
        options: &[(&str, String)],
        status_interval: Duration,
    ) -> Result<ReplStream, ConnectorError> {
        let opts = options
            .iter()
            .map(|(k, v)| {
                format!(
                    "{} {}",
                    crate::types::quote_ident(k),
                    crate::types::quote_literal(v)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} ({opts})",
            crate::types::quote_ident(slot),
            format_lsn(start)
        );
        let mut out = BytesMut::new();
        frontend::query(&sql, &mut out).map_err(io_fatal)?;
        write_all(&mut self.stream, &out).await?;
        loop {
            let f = self.frame().await?;
            match f.tag {
                b'W' => break,
                b'E' => return Err(error_from(&f.body, "START_REPLICATION")),
                b'N' => {}
                other => {
                    return Err(ConnectorError::fatal(format!(
                        "START_REPLICATION: unexpected message {:?}",
                        other as char
                    )));
                }
            }
        }
        Ok(ReplStream::spawn(
            self.stream,
            self.buf,
            start,
            status_interval,
        ))
    }

    /// End the session.
    pub async fn close(mut self) {
        let mut out = BytesMut::new();
        frontend::terminate(&mut out);
        let _ = self.stream.write_all(&out).await;
        let _ = self.stream.shutdown().await;
    }
}

fn io_fatal(e: std::io::Error) -> ConnectorError {
    ConnectorError::fatal(format!("replication: encoding: {e}"))
}

async fn write_all<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    b: &[u8],
) -> Result<(), ConnectorError> {
    w.write_all(b)
        .await
        .map_err(|e| ConnectorError::retryable(format!("replication: writing: {e}")))?;
    w.flush()
        .await
        .map_err(|e| ConnectorError::retryable(format!("replication: writing: {e}")))
}

/// What the stream delivers, in server order.
#[derive(Debug, Clone)]
pub enum ReplEvent {
    /// `XLogData`: the WAL start of this message and one pgoutput message.
    Data { start: Lsn, end: Lsn, body: Bytes },
    /// A keepalive: the server has sent everything before `wal_end`.
    Keepalive { wal_end: Lsn },
}

/// Microseconds since 2000-01-01 (the protocol's clock).
fn pg_now() -> i64 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64);
    micros - crate::types::PG_EPOCH_MICROS
}

/// A Standby Status Update in a CopyData frame.
pub fn status_update(received: Lsn, flushed: Lsn, reply: bool) -> BytesMut {
    let mut b = BytesMut::with_capacity(40);
    b.put_u8(b'd');
    b.put_i32(4 + 34);
    b.put_u8(b'r');
    b.put_u64(received);
    b.put_u64(flushed);
    b.put_u64(flushed);
    b.put_i64(pg_now());
    b.put_u8(u8::from(reply));
    b
}

struct Lsns {
    received: AtomicU64,
    flushed: AtomicU64,
}

/// A streaming replication session.
pub struct ReplStream {
    events: mpsc::Receiver<Result<ReplEvent, ConnectorError>>,
    writer: Arc<Mutex<WriteHalf<PgStream>>>,
    lsns: Arc<Lsns>,
    tasks: Vec<JoinHandle<()>>,
}

impl ReplStream {
    fn spawn(stream: PgStream, buf: BytesMut, start: Lsn, status_interval: Duration) -> Self {
        let (read, write) = tokio::io::split(stream);
        let writer = Arc::new(Mutex::new(write));
        let lsns = Arc::new(Lsns {
            received: AtomicU64::new(start),
            flushed: AtomicU64::new(start),
        });
        let (tx, rx) = mpsc::channel(1024);
        let reader = tokio::spawn(read_loop(read, buf, tx, writer.clone(), lsns.clone()));
        let w = writer.clone();
        let l = lsns.clone();
        let status = tokio::spawn(async move {
            let mut tick = tokio::time::interval(status_interval.max(Duration::from_millis(100)));
            tick.tick().await;
            loop {
                tick.tick().await;
                let msg = status_update(
                    l.received.load(Ordering::Acquire),
                    l.flushed.load(Ordering::Acquire),
                    false,
                );
                if write_all(&mut *w.lock().await, &msg).await.is_err() {
                    return;
                }
            }
        });
        Self {
            events: rx,
            writer,
            lsns,
            tasks: vec![reader, status],
        }
    }

    /// The next event, waiting at most `wait`; `Ok(None)` when none came.
    pub async fn recv(&mut self, wait: Duration) -> Result<Option<ReplEvent>, ConnectorError> {
        match tokio::time::timeout(wait, self.events.recv()).await {
            Err(_) => Ok(None),
            Ok(None) => Err(ConnectorError::retryable("replication: stream ended")),
            Ok(Some(r)) => r.map(Some),
        }
    }

    /// An event already received, without waiting.
    pub fn try_recv(&mut self) -> Option<Result<ReplEvent, ConnectorError>> {
        self.events.try_recv().ok()
    }

    /// The Fabric has everything before `lsn`: report it as flushed (and
    /// applied) now, so the server may release the WAL. Never goes back.
    pub async fn set_flushed(&self, lsn: Lsn) -> Result<(), ConnectorError> {
        let prev = self.lsns.flushed.fetch_max(lsn, Ordering::AcqRel);
        if lsn <= prev {
            return Ok(());
        }
        self.lsns.received.fetch_max(lsn, Ordering::AcqRel);
        let msg = status_update(self.lsns.received.load(Ordering::Acquire), lsn, false);
        write_all(&mut *self.writer.lock().await, &msg).await
    }

    pub fn flushed(&self) -> Lsn {
        self.lsns.flushed.load(Ordering::Acquire)
    }

    /// Stop streaming and close the connection.
    pub async fn close(self) {
        for t in &self.tasks {
            t.abort();
        }
        let mut w = self.writer.lock().await;
        let mut out = BytesMut::new();
        frontend::copy_done(&mut out);
        frontend::terminate(&mut out);
        let _ = w.write_all(&out).await;
        let _ = w.shutdown().await;
    }
}

impl Drop for ReplStream {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

async fn read_loop(
    mut read: ReadHalf<PgStream>,
    mut buf: BytesMut,
    tx: mpsc::Sender<Result<ReplEvent, ConnectorError>>,
    writer: Arc<Mutex<WriteHalf<PgStream>>>,
    lsns: Arc<Lsns>,
) {
    loop {
        let f = match read_frame(&mut read, &mut buf).await {
            Ok(f) => f,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        };
        let event = match f.tag {
            b'd' => {
                let mut b = f.body.clone();
                if !b.has_remaining() {
                    continue;
                }
                match b.get_u8() {
                    b'w' if b.remaining() >= 24 => {
                        let start = b.get_u64();
                        let end = b.get_u64();
                        let _time = b.get_i64();
                        lsns.received.fetch_max(start, Ordering::AcqRel);
                        ReplEvent::Data {
                            start,
                            end,
                            body: b,
                        }
                    }
                    b'k' if b.remaining() >= 17 => {
                        let wal_end = b.get_u64();
                        let _time = b.get_i64();
                        let reply = b.get_u8() != 0;
                        if reply {
                            let msg = status_update(
                                lsns.received.load(Ordering::Acquire),
                                lsns.flushed.load(Ordering::Acquire),
                                false,
                            );
                            if write_all(&mut *writer.lock().await, &msg).await.is_err() {
                                let _ = tx
                                    .send(Err(ConnectorError::retryable(
                                        "replication: status update failed",
                                    )))
                                    .await;
                                return;
                            }
                        }
                        ReplEvent::Keepalive { wal_end }
                    }
                    _ => continue,
                }
            }
            b'E' => {
                let _ = tx
                    .send(Err(error_from(&f.body, "replication stream")))
                    .await;
                return;
            }
            b'c' => {
                let _ = tx
                    .send(Err(ConnectorError::retryable(
                        "replication: the server ended the stream",
                    )))
                    .await;
                return;
            }
            _ => continue,
        };
        if tx.send(Ok(event)).await.is_err() {
            return;
        }
    }
}

impl std::fmt::Debug for ReplConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplConn").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ReplStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplStream").finish_non_exhaustive()
    }
}

#[cfg(test)]
pub(crate) mod fake {
    //! A loopback walsender for the client's unit tests: just enough of the
    //! protocol to log in, answer replication commands and stream frames.
    use super::*;
    use tokio::net::{TcpListener, TcpStream};

    pub fn msg(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![tag];
        v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    pub fn cstring(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    pub fn row_description(names: &[&str]) -> Vec<u8> {
        let mut b = (names.len() as i16).to_be_bytes().to_vec();
        for n in names {
            b.extend(cstring(n));
            b.extend_from_slice(&0i32.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&25i32.to_be_bytes());
            b.extend_from_slice(&(-1i16).to_be_bytes());
            b.extend_from_slice(&(-1i32).to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
        }
        msg(b'T', &b)
    }

    pub fn data_row(values: &[Option<&str>]) -> Vec<u8> {
        let mut b = (values.len() as i16).to_be_bytes().to_vec();
        for v in values {
            match v {
                None => b.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(s) => {
                    b.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    b.extend_from_slice(s.as_bytes());
                }
            }
        }
        msg(b'D', &b)
    }

    pub fn ready() -> Vec<u8> {
        msg(b'Z', b"I")
    }

    pub fn xlog(start: Lsn, body: &[u8]) -> Vec<u8> {
        let mut b = vec![b'w'];
        b.extend_from_slice(&start.to_be_bytes());
        b.extend_from_slice(&(start + 100).to_be_bytes());
        b.extend_from_slice(&0i64.to_be_bytes());
        b.extend_from_slice(body);
        msg(b'd', &b)
    }

    pub fn keepalive(wal_end: Lsn, reply: bool) -> Vec<u8> {
        let mut b = vec![b'k'];
        b.extend_from_slice(&wal_end.to_be_bytes());
        b.extend_from_slice(&0i64.to_be_bytes());
        b.push(u8::from(reply));
        msg(b'd', &b)
    }

    /// Read one frontend message (after startup).
    pub async fn read_msg(s: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut head = [0u8; 5];
        s.read_exact(&mut head).await.expect("head");
        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        s.read_exact(&mut body).await.expect("body");
        (head[0], body)
    }

    /// Accept one connection; do the cleartext-password login (checking
    /// `password`) and announce `version`.
    pub async fn login(l: &TcpListener, password: &str, version: &str) -> TcpStream {
        let (mut s, _) = l.accept().await.expect("accept");
        let mut len = [0u8; 4];
        s.read_exact(&mut len).await.expect("startup length");
        let mut rest = vec![0u8; u32::from_be_bytes(len) as usize - 4];
        s.read_exact(&mut rest).await.expect("startup");
        let text = String::from_utf8_lossy(&rest).into_owned();
        assert!(text.contains("replication\0database"), "{text:?}");
        s.write_all(&msg(b'R', &3i32.to_be_bytes()))
            .await
            .expect("auth");
        let (tag, body) = read_msg(&mut s).await;
        assert_eq!(tag, b'p');
        assert_eq!(body, cstring(password));
        let mut out = msg(b'R', &0i32.to_be_bytes());
        out.extend(msg(
            b'S',
            &[cstring("server_version"), cstring(version)].concat(),
        ));
        out.extend(ready());
        s.write_all(&out).await.expect("ready");
        s
    }
}

#[cfg(test)]
mod tests {
    use super::fake::*;
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn lsns_and_versions_parse() {
        assert_eq!(parse_lsn("16/B374D848"), Some(0x16_B374_D848));
        assert_eq!(format_lsn(0x16_B374_D848), "16/B374D848");
        assert_eq!(parse_lsn("nope"), None);
        assert_eq!(version_num("17.2"), 170_002);
        assert_eq!(version_num("16beta1"), 160_001);
        assert_eq!(version_num("9.6.1"), 90_601);
        assert_eq!(version_num("14.11 (Debian 14.11-1)"), 140_011);
    }

    #[test]
    fn error_responses_carry_sqlstate_and_message() {
        let body = [
            b"SERROR\0".as_slice(),
            b"C55006\0",
            b"Mreplication slot is active\0",
            b"\0",
        ]
        .concat();
        let e = error_from(&body, "START_REPLICATION");
        assert_eq!(
            e.class,
            loams_connector_sdk::ErrorClass::Retryable { after: None }
        );
        assert!(
            e.message.contains("replication slot is active (55006)"),
            "{}",
            e.message
        );
    }

    #[tokio::test]
    async fn logs_in_creates_a_slot_streams_and_reports_flush() {
        let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = l.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let mut s = login(&l, "pw", "17.2").await;
            // CREATE_REPLICATION_SLOT, PG 15+ syntax.
            let (tag, body) = read_msg(&mut s).await;
            assert_eq!(tag, b'Q');
            let q = String::from_utf8_lossy(&body).into_owned();
            assert!(
                q.contains("CREATE_REPLICATION_SLOT \"s1\" LOGICAL pgoutput (SNAPSHOT 'export')"),
                "{q}"
            );
            let mut out = row_description(&[
                "slot_name",
                "consistent_point",
                "snapshot_name",
                "output_plugin",
            ]);
            out.extend(data_row(&[
                Some("s1"),
                Some("0/1000"),
                Some("00000003-1"),
                Some("pgoutput"),
            ]));
            out.extend(msg(b'C', &cstring("CREATE_REPLICATION_SLOT")));
            out.extend(ready());
            s.write_all(&out).await.expect("slot");
            // START_REPLICATION.
            let (tag, body) = read_msg(&mut s).await;
            assert_eq!(tag, b'Q');
            let q = String::from_utf8_lossy(&body).into_owned();
            assert!(
                q.contains("START_REPLICATION SLOT \"s1\" LOGICAL 0/1000 (\"proto_version\" '2'"),
                "{q}"
            );
            let mut out = msg(b'W', &[0, 0, 0]);
            out.extend(xlog(0x1100, b"B-pgoutput-bytes"));
            out.extend(keepalive(0x1200, true));
            s.write_all(&out).await.expect("stream");
            // The keepalive asked for a reply; then the client's flush.
            let (tag, body) = read_msg(&mut s).await;
            assert_eq!((tag, body[0]), (b'd', b'r'));
            loop {
                let (tag, body) = read_msg(&mut s).await;
                assert_eq!((tag, body[0]), (b'd', b'r'));
                let flushed = u64::from_be_bytes(body[9..17].try_into().expect("flush"));
                if flushed == 0x1200 {
                    return;
                }
            }
        });
        let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let mut conn = ReplConn::start(PgStream::Plain(tcp), "loams", "db", Some(b"pw".to_vec()))
            .await
            .expect("login");
        assert_eq!(conn.server_version, 170_002);
        let slot = conn.create_slot("s1", true).await.expect("slot");
        assert_eq!(
            slot,
            SlotCreated {
                consistent_point: 0x1000,
                snapshot: Some("00000003-1".into())
            }
        );
        let mut stream = conn
            .start_replication(
                "s1",
                0x1000,
                &[
                    ("proto_version", "2".into()),
                    ("publication_names", "p".into()),
                ],
                Duration::from_secs(60),
            )
            .await
            .expect("start");
        match stream.recv(Duration::from_secs(5)).await.expect("event") {
            Some(ReplEvent::Data { start, body, .. }) => {
                assert_eq!(start, 0x1100);
                assert_eq!(&body[..], b"B-pgoutput-bytes");
            }
            other => panic!("{other:?}"),
        }
        match stream.recv(Duration::from_secs(5)).await.expect("event") {
            Some(ReplEvent::Keepalive { wal_end }) => assert_eq!(wal_end, 0x1200),
            other => panic!("{other:?}"),
        }
        stream.set_flushed(0x1200).await.expect("flush");
        assert_eq!(stream.flushed(), 0x1200);
        stream.set_flushed(0x1100).await.expect("never back");
        assert_eq!(stream.flushed(), 0x1200);
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("server")
            .expect("server ok");
    }

    #[tokio::test]
    async fn a_missing_password_is_an_auth_error() {
        let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = l.local_addr().expect("addr");
        let _server = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.expect("accept");
            let mut len = [0u8; 4];
            s.read_exact(&mut len).await.expect("len");
            let mut rest = vec![0u8; u32::from_be_bytes(len) as usize - 4];
            s.read_exact(&mut rest).await.expect("startup");
            s.write_all(&msg(b'R', &3i32.to_be_bytes()))
                .await
                .expect("auth");
            let _ = s.read(&mut [0u8; 1]).await;
        });
        let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let err = ReplConn::start(PgStream::Plain(tcp), "u", "d", None)
            .await
            .expect_err("refused");
        assert_eq!(err.class, loams_connector_sdk::ErrorClass::Auth);
    }
}
