//! A minimal WebSocket client (RFC 6455) for GraphQL subscriptions over
//! `graphql-transport-ws`: the opening handshake (with its
//! `Sec-WebSocket-Accept` check), masked client frames, text and binary
//! messages with continuation frames, ping/pong and close. It connects
//! through `sdk::net`, so the socket is egress-checked after DNS (ruling
//! R21.2); `tokio-tungstenite` is the brokers family's (Task 18) and would
//! dial outside the check. SHA-1 is written here for the accept check only
//! (RFC 3174; checked against its test vectors), never for security.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use bytes::{Buf, BytesMut};
use http::Uri;
use loams_connector_sdk::{ConnectorError, EgressCheck, net};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The handshake's GUID (RFC 6455 §1.3).
const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// SHA-1 (RFC 3174).
pub fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let mut msg = data.to_vec();
    let bits = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bits.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let t = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = t;
        }
        for (x, y) in h.iter_mut().zip([a, b, c, d, e]) {
            *x = x.wrapping_add(y);
        }
    }
    let mut out = [0u8; 20];
    for (i, x) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&x.to_be_bytes());
    }
    out
}

/// `Sec-WebSocket-Accept` for a key.
pub fn accept_for(key: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(sha1(format!("{key}{GUID}").as_bytes()))
}

/// Unpredictable-enough bytes for the handshake nonce and frame masks (not
/// a secret: RFC 6455 masks defeat proxy cache poisoning, not eavesdroppers).
fn nonce(counter: u64) -> [u8; 32] {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut d = Sha256::new();
    d.update(t.to_le_bytes());
    d.update(counter.to_le_bytes());
    d.update((std::ptr::addr_of!(counter) as usize).to_le_bytes());
    d.finalize().into()
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A message received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsMessage {
    Text(String),
    Binary(Vec<u8>),
}

/// An open WebSocket.
pub struct WsClient {
    io: Box<dyn Io>,
    buf: BytesMut,
    partial: Option<(u8, Vec<u8>)>,
    counter: u64,
    closed: bool,
}

impl std::fmt::Debug for WsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsClient")
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

/// Encode one client frame (always masked).
pub fn client_frame(opcode: u8, payload: &[u8], mask: [u8; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 14);
    out.push(0x80 | opcode);
    match payload.len() {
        n if n < 126 => out.push(0x80 | n as u8),
        n if n <= 0xFFFF => {
            out.push(0x80 | 126);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        n => {
            out.push(0x80 | 127);
            out.extend_from_slice(&(n as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(&mask);
    out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    out
}

/// Parse one frame from `buf`: (fin, opcode, payload, length used). Server
/// frames are unmasked; a masked one is accepted and unmasked.
pub fn parse_frame(buf: &[u8]) -> Option<(bool, u8, Vec<u8>, usize)> {
    if buf.len() < 2 {
        return None;
    }
    let fin = buf[0] & 0x80 != 0;
    let opcode = buf[0] & 0x0F;
    let masked = buf[1] & 0x80 != 0;
    let (len, mut at) = match buf[1] & 0x7F {
        126 => (u16::from_be_bytes([*buf.get(2)?, *buf.get(3)?]) as usize, 4),
        127 => {
            let b: [u8; 8] = buf.get(2..10)?.try_into().ok()?;
            (u64::from_be_bytes(b) as usize, 10)
        }
        n => (n as usize, 2),
    };
    let mask = if masked {
        let m: [u8; 4] = buf.get(at..at + 4)?.try_into().ok()?;
        at += 4;
        Some(m)
    } else {
        None
    };
    let data = buf.get(at..at + len)?;
    let payload = match mask {
        Some(m) => data.iter().enumerate().map(|(i, b)| b ^ m[i % 4]).collect(),
        None => data.to_vec(),
    };
    Some((fin, opcode, payload, at + len))
}

impl WsClient {
    /// Open `url` (`ws://` or `wss://`) with `protocol` and extra request
    /// headers (sent as given; credentials included).
    pub async fn connect(
        url: &str,
        protocol: &str,
        headers: &[(String, String)],
        egress: Arc<dyn EgressCheck>,
    ) -> Result<Self, ConnectorError> {
        let uri: Uri = url
            .parse()
            .map_err(|_| ConnectorError::fatal("the WebSocket URL is not valid"))?;
        let tls = match uri.scheme_str() {
            Some("wss") => true,
            Some("ws") => false,
            _ => return Err(ConnectorError::fatal("a WebSocket URL is ws:// or wss://")),
        };
        let host = uri
            .host()
            .ok_or_else(|| ConnectorError::fatal("no host"))?
            .trim_matches(['[', ']'])
            .to_owned();
        let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
        let mut io: Box<dyn Io> = if tls {
            Box::new(net::connect_tls(&host, port, &*egress, None).await?)
        } else {
            Box::new(net::connect(&host, port, &*egress).await?)
        };
        let key = base64::engine::general_purpose::STANDARD.encode(&nonce(0)[..16]);
        let path = uri.path_and_query().map_or("/", |p| p.as_str());
        let authority = uri
            .authority()
            .map_or(host.clone(), |a| a.as_str().to_owned());
        let mut req = format!(
            "GET {path} HTTP/1.1\r\nHost: {authority}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: {protocol}\r\n"
        );
        for (k, v) in headers {
            if k.contains(['\r', '\n']) || v.contains(['\r', '\n']) {
                return Err(ConnectorError::fatal(
                    "a header may not contain a line break",
                ));
            }
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str("\r\n");
        io.write_all(req.as_bytes())
            .await
            .map_err(|e| ConnectorError::retryable(format!("WebSocket handshake: {e}")))?;
        let mut buf = BytesMut::new();
        let end = loop {
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i;
            }
            if buf.len() > 16 * 1024 {
                return Err(ConnectorError::fatal(
                    "the WebSocket handshake answer is too long",
                ));
            }
            let n = io
                .read_buf(&mut buf)
                .await
                .map_err(|e| ConnectorError::retryable(format!("WebSocket handshake: {e}")))?;
            if n == 0 {
                return Err(ConnectorError::retryable(
                    "the server closed during the WebSocket handshake",
                ));
            }
        };
        let head = String::from_utf8_lossy(&buf[..end]).into_owned();
        buf.advance(end + 4);
        let mut lines = head.split("\r\n");
        let status = lines.next().unwrap_or_default();
        let code = status.split_whitespace().nth(1).unwrap_or("");
        if code != "101" {
            return Err(match code {
                "401" | "403" => {
                    ConnectorError::auth(format!("WebSocket upgrade refused: {status}"))
                }
                _ => ConnectorError::retryable(format!("WebSocket upgrade refused: {status}")),
            });
        }
        let header = |name: &str| {
            head.split("\r\n").skip(1).find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.trim()
                    .eq_ignore_ascii_case(name)
                    .then(|| v.trim().to_owned())
            })
        };
        if header("sec-websocket-accept").as_deref() != Some(accept_for(&key).as_str()) {
            return Err(ConnectorError::fatal(
                "the server's Sec-WebSocket-Accept is wrong",
            ));
        }
        if header("sec-websocket-protocol").as_deref() != Some(protocol) {
            return Err(ConnectorError::fatal(format!(
                "the server did not agree to {protocol}"
            )));
        }
        Ok(Self {
            io,
            buf,
            partial: None,
            counter: 1,
            closed: false,
        })
    }

    async fn send_frame(&mut self, opcode: u8, payload: &[u8]) -> Result<(), ConnectorError> {
        self.counter += 1;
        let n = nonce(self.counter);
        let frame = client_frame(opcode, payload, [n[0], n[1], n[2], n[3]]);
        self.io
            .write_all(&frame)
            .await
            .map_err(|e| ConnectorError::retryable(format!("WebSocket write: {e}")))
    }

    pub async fn send_text(&mut self, text: &str) -> Result<(), ConnectorError> {
        self.send_frame(0x1, text.as_bytes()).await
    }

    /// Send a close frame (best effort).
    pub async fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            let _ = self.send_frame(0x8, &1000u16.to_be_bytes()).await;
        }
    }

    /// The next message, or `None` once the server closed. Cancel-safe:
    /// bytes read stay buffered.
    pub async fn recv(&mut self) -> Result<Option<WsMessage>, ConnectorError> {
        loop {
            if let Some((fin, opcode, payload, used)) = parse_frame(&self.buf) {
                self.buf.advance(used);
                match opcode {
                    0x9 => self.send_frame(0xA, &payload).await?,
                    0xA => {}
                    0x8 => {
                        self.close().await;
                        return Ok(None);
                    }
                    0x0 => {
                        let Some((op, mut data)) = self.partial.take() else {
                            return Err(ConnectorError::retryable(
                                "a continuation frame with nothing to continue",
                            ));
                        };
                        data.extend_from_slice(&payload);
                        if fin {
                            return Ok(Some(message(op, data)?));
                        }
                        self.partial = Some((op, data));
                    }
                    0x1 | 0x2 if fin => return Ok(Some(message(opcode, payload)?)),
                    0x1 | 0x2 => self.partial = Some((opcode, payload)),
                    other => {
                        return Err(ConnectorError::retryable(format!(
                            "unknown WebSocket opcode {other}"
                        )));
                    }
                }
                continue;
            }
            if self.closed {
                return Ok(None);
            }
            let n = self
                .io
                .read_buf(&mut self.buf)
                .await
                .map_err(|e| ConnectorError::retryable(format!("WebSocket read: {e}")))?;
            if n == 0 {
                self.closed = true;
                return Ok(None);
            }
        }
    }
}

fn message(opcode: u8, data: Vec<u8>) -> Result<WsMessage, ConnectorError> {
    if opcode == 0x1 {
        String::from_utf8(data)
            .map(WsMessage::Text)
            .map_err(|_| ConnectorError::retryable("a text frame that is not UTF-8"))
    } else {
        Ok(WsMessage::Binary(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_vectors() {
        let h = |s: &[u8]| crate::client::hex(&sha1(s));
        assert_eq!(h(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(h(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            h(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
        // RFC 6455 §1.3's worked example.
        assert_eq!(
            accept_for("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn frames_round_trip() {
        let f = client_frame(0x1, b"hello", [1, 2, 3, 4]);
        let (fin, op, payload, used) = parse_frame(&f).expect("frame");
        assert!(fin);
        assert_eq!(
            (op, payload.as_slice(), used),
            (0x1, &b"hello"[..], f.len())
        );
        let big = vec![7u8; 70_000];
        let f = client_frame(0x2, &big, [9, 9, 9, 9]);
        assert_eq!(parse_frame(&f).expect("big").2, big);
        assert!(parse_frame(&f[..100]).is_none(), "incomplete");
    }
}
