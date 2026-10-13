//! A minimal MQTT 5 client (R18.3): one task owns the connection, keeps it
//! alive with PINGREQ, hands received PUBLISH packets to the source through
//! a bounded channel, writes the PUBACKs the source asks for (only after the
//! Fabric acknowledged), and matches the sink's QoS 1 publishes with their
//! PUBACKs.
//!
//! Flow control is the protocol's own: the CONNECT's Receive Maximum caps
//! the QoS 1 messages the server may have unacknowledged with this client,
//! so a source that is not acknowledging (a slow Fabric) stops the server
//! from sending (MQTT 5 §4.9).

use std::collections::HashMap;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::codec::{self, Connect, Packet, Publish};

/// Why the client stopped or an operation failed. Never carries a payload
/// or a credential.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClientError {
    #[error("MQTT connection refused (reason {0:#04x})")]
    Refused(u8),
    #[error("MQTT subscription refused (reason {0:#04x})")]
    SubscribeRefused(u8),
    #[error("MQTT: {0}")]
    Protocol(String),
    #[error("MQTT connection closed: {0}")]
    Closed(String),
}

impl ClientError {
    /// Reason codes 0x86 (bad user name or password) and 0x87 (not
    /// authorized) are authentication failures.
    pub fn is_auth(&self) -> bool {
        matches!(self, ClientError::Refused(0x86 | 0x87 | 0x8C))
            || matches!(self, ClientError::SubscribeRefused(0x87))
    }
}

/// What a connection is opened with.
#[derive(Debug, Clone)]
pub struct Options {
    pub connect: Connect,
    /// Topic filters to subscribe to at QoS 1 (a source); empty for a sink.
    pub subscribe: Vec<String>,
    /// The largest packet accepted from the server.
    pub max_packet: usize,
}

enum Cmd {
    Publish(Publish, oneshot::Sender<Result<u8, ClientError>>),
    Ack(u16),
    Disconnect,
}

/// An open MQTT session.
#[derive(Debug)]
pub struct Client {
    cmd: mpsc::UnboundedSender<Cmd>,
    incoming: mpsc::Receiver<Publish>,
    /// The server had kept the session (`clean_start = false`).
    pub session_present: bool,
    task: JoinHandle<()>,
    error: std::sync::Arc<std::sync::Mutex<Option<ClientError>>>,
}

impl std::fmt::Debug for Cmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cmd::Publish(p, _) => write!(f, "Publish({})", p.topic),
            Cmd::Ack(id) => write!(f, "Ack({id})"),
            Cmd::Disconnect => f.write_str("Disconnect"),
        }
    }
}

async fn read_packet<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut BytesMut,
    max: usize,
) -> Result<Packet, ClientError> {
    loop {
        if let Some(p) =
            codec::decode(buf, max).map_err(|e| ClientError::Protocol(e.to_string()))?
        {
            return Ok(p);
        }
        let n = r
            .read_buf(buf)
            .await
            .map_err(|e| ClientError::Closed(e.to_string()))?;
        if n == 0 {
            return Err(ClientError::Closed("end of stream".into()));
        }
    }
}

async fn write_packet<W: AsyncWrite + Unpin>(w: &mut W, p: &Packet) -> Result<(), ClientError> {
    let mut out = BytesMut::new();
    codec::encode(p, &mut out);
    w.write_all(&out)
        .await
        .map_err(|e| ClientError::Closed(e.to_string()))?;
    w.flush()
        .await
        .map_err(|e| ClientError::Closed(e.to_string()))
}

impl Client {
    /// CONNECT (and SUBSCRIBE) over `stream`, then run the connection in a
    /// task. The receive channel holds the server's Receive Maximum worth of
    /// messages plus room for QoS 0 ones.
    pub async fn connect<S>(stream: S, opts: Options) -> Result<Client, ClientError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut rd, mut wr) = tokio::io::split(stream);
        let mut buf = BytesMut::with_capacity(8 << 10);
        write_packet(&mut wr, &Packet::Connect(opts.connect.clone())).await?;
        let (session_present, keep_alive) =
            match read_packet(&mut rd, &mut buf, opts.max_packet).await? {
                Packet::ConnAck {
                    reason,
                    session_present,
                    server_keep_alive,
                    ..
                } => {
                    if reason >= 0x80 {
                        return Err(ClientError::Refused(reason));
                    }
                    (
                        session_present,
                        server_keep_alive.unwrap_or(opts.connect.keep_alive),
                    )
                }
                other => {
                    return Err(ClientError::Protocol(format!(
                        "expected CONNACK, got {other:?}"
                    )));
                }
            };
        let capacity = usize::from(opts.connect.receive_maximum.unwrap_or(u16::MAX)) + 64;
        let (in_tx, in_rx) = mpsc::channel(capacity);
        if !opts.subscribe.is_empty() {
            write_packet(
                &mut wr,
                &Packet::Subscribe {
                    packet_id: 1,
                    filters: opts.subscribe.iter().map(|f| (f.clone(), 1)).collect(),
                },
            )
            .await?;
            // With a kept session the server may resend unacknowledged
            // messages before the SUBACK: pass them on.
            loop {
                match read_packet(&mut rd, &mut buf, opts.max_packet).await? {
                    Packet::SubAck { reasons, .. } => {
                        if let Some(r) = reasons.iter().find(|r| **r >= 0x80) {
                            return Err(ClientError::SubscribeRefused(*r));
                        }
                        break;
                    }
                    Packet::Publish(p) => {
                        in_tx
                            .send(p)
                            .await
                            .map_err(|_| ClientError::Closed("receiver dropped".into()))?;
                    }
                    Packet::PingResp => {}
                    other => {
                        return Err(ClientError::Protocol(format!(
                            "expected SUBACK, got {other:?}"
                        )));
                    }
                }
            }
        }
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let error = std::sync::Arc::new(std::sync::Mutex::new(None));
        let task = tokio::spawn(run(
            rd,
            wr,
            buf,
            cmd_rx,
            in_tx,
            keep_alive,
            opts.max_packet,
            error.clone(),
        ));
        Ok(Client {
            cmd: cmd_tx,
            incoming: in_rx,
            session_present,
            task,
            error,
        })
    }

    /// The next received message, or `None` after `wait` with nothing. A
    /// closed connection is an error.
    pub async fn recv(&mut self, wait: Duration) -> Result<Option<Publish>, ClientError> {
        match tokio::time::timeout(wait, self.incoming.recv()).await {
            Err(_) => Ok(None),
            Ok(Some(p)) => Ok(Some(p)),
            Ok(None) => Err(self.failure()),
        }
    }

    /// Acknowledge a received QoS 1 message.
    pub fn ack(&self, packet_id: u16) -> Result<(), ClientError> {
        self.cmd
            .send(Cmd::Ack(packet_id))
            .map_err(|_| self.failure())
    }

    /// Publish at QoS 1; resolves with the PUBACK's reason code.
    pub async fn publish(
        &self,
        p: Publish,
    ) -> Result<oneshot::Receiver<Result<u8, ClientError>>, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.cmd
            .send(Cmd::Publish(p, tx))
            .map_err(|_| self.failure())?;
        Ok(rx)
    }

    /// Send DISCONNECT and stop.
    pub async fn disconnect(self) {
        let _ = self.cmd.send(Cmd::Disconnect);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !self.task.is_finished() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Whether the connection task has stopped.
    pub fn is_closed(&self) -> bool {
        self.task.is_finished()
    }

    fn failure(&self) -> ClientError {
        self.error
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .unwrap_or_else(|| ClientError::Closed("connection task ended".into()))
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Dropping without DISCONNECT is a crash as far as the server is
        // concerned: unacknowledged messages stay in the session.
        self.task.abort();
    }
}

#[allow(clippy::too_many_arguments)]
async fn run<R, W>(
    mut rd: R,
    mut wr: W,
    mut buf: BytesMut,
    mut cmd: mpsc::UnboundedReceiver<Cmd>,
    incoming: mpsc::Sender<Publish>,
    keep_alive: u16,
    max_packet: usize,
    error: std::sync::Arc<std::sync::Mutex<Option<ClientError>>>,
) where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let mut inflight: HashMap<u16, oneshot::Sender<Result<u8, ClientError>>> = HashMap::new();
    let mut next_id: u16 = 1;
    let ping_every = if keep_alive == 0 {
        Duration::from_secs(3600)
    } else {
        Duration::from_secs(u64::from(keep_alive)) / 2
    };
    let mut ping = tokio::time::interval(ping_every);
    ping.tick().await;
    let mut awaiting_pong: Option<tokio::time::Instant> = None;
    let result: Result<(), ClientError> = async {
        loop {
            tokio::select! {
                c = cmd.recv() => match c {
                    None | Some(Cmd::Disconnect) => {
                        let _ = write_packet(&mut wr, &Packet::Disconnect { reason: 0 }).await;
                        return Ok(());
                    }
                    Some(Cmd::Ack(id)) => {
                        write_packet(&mut wr, &Packet::PubAck { packet_id: id, reason: 0 }).await?;
                    }
                    Some(Cmd::Publish(mut p, tx)) => {
                        let mut id = next_id;
                        while inflight.contains_key(&id) || id == 0 {
                            id = id.wrapping_add(1);
                        }
                        next_id = id.wrapping_add(1);
                        p.qos = 1;
                        p.packet_id = Some(id);
                        write_packet(&mut wr, &Packet::Publish(p)).await?;
                        inflight.insert(id, tx);
                    }
                },
                p = read_packet(&mut rd, &mut buf, max_packet) => match p? {
                    Packet::Publish(p) => {
                        if incoming.send(p).await.is_err() {
                            return Err(ClientError::Closed("receiver dropped".into()));
                        }
                    }
                    Packet::PubAck { packet_id, reason } => {
                        if let Some(tx) = inflight.remove(&packet_id) {
                            let _ = tx.send(Ok(reason));
                        }
                    }
                    Packet::PingResp => awaiting_pong = None,
                    Packet::Disconnect { reason } => {
                        return Err(ClientError::Closed(format!("server sent DISCONNECT {reason:#04x}")));
                    }
                    Packet::SubAck { .. } => {}
                    other => return Err(ClientError::Protocol(format!("unexpected {other:?}"))),
                },
                _ = ping.tick() => {
                    if let Some(sent) = awaiting_pong
                        && sent.elapsed() > ping_every * 3
                    {
                        return Err(ClientError::Closed("no PINGRESP".into()));
                    }
                    write_packet(&mut wr, &Packet::PingReq).await?;
                    awaiting_pong.get_or_insert_with(tokio::time::Instant::now);
                }
            }
        }
    }
    .await;
    let err = result
        .err()
        .unwrap_or_else(|| ClientError::Closed("disconnected".into()));
    for (_, tx) in inflight.drain() {
        let _ = tx.send(Err(err.clone()));
    }
    *error.lock().unwrap_or_else(|p| p.into_inner()) = Some(err);
}
