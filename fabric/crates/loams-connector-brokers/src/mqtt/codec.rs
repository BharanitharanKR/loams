//! The MQTT 5.0 packets the connector speaks (OASIS MQTT Version 5.0, §2–§3):
//! CONNECT, CONNACK, PUBLISH, PUBACK, SUBSCRIBE, SUBACK, PINGREQ, PINGRESP
//! and DISCONNECT. QoS 2 is never requested (subscriptions ask for QoS 1, so
//! the server downgrades), and AUTH (enhanced authentication) is not used.
//!
//! [`decode`] takes one whole packet off the front of a buffer, or returns
//! `Ok(None)` when the buffer does not hold one yet, so a reader can call it
//! after every read.

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const CONNECT: u8 = 1;
pub const CONNACK: u8 = 2;
pub const PUBLISH: u8 = 3;
pub const PUBACK: u8 = 4;
pub const SUBSCRIBE: u8 = 8;
pub const SUBACK: u8 = 9;
pub const PINGREQ: u8 = 12;
pub const PINGRESP: u8 = 13;
pub const DISCONNECT: u8 = 14;

// Property identifiers (§2.2.2.2).
const P_PAYLOAD_FORMAT: u8 = 0x01;
const P_CONTENT_TYPE: u8 = 0x03;
const P_SESSION_EXPIRY: u8 = 0x11;
const P_ASSIGNED_CLIENT_ID: u8 = 0x12;
const P_SERVER_KEEP_ALIVE: u8 = 0x13;
const P_RECEIVE_MAXIMUM: u8 = 0x21;
const P_USER_PROPERTY: u8 = 0x26;
const P_MAXIMUM_PACKET_SIZE: u8 = 0x27;

/// A codec error. Its text never includes payloads or credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("malformed MQTT packet: {0}")]
    Malformed(&'static str),
    #[error("MQTT packet of {0} bytes exceeds the {1}-byte limit")]
    TooLarge(usize, usize),
    #[error("unsupported MQTT packet type {0}")]
    Unsupported(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Connect {
    pub client_id: String,
    pub clean_start: bool,
    pub keep_alive: u16,
    /// Seconds the server keeps the session after a disconnect.
    pub session_expiry: u32,
    /// How many unacknowledged QoS 1 messages the server may send us.
    pub receive_maximum: Option<u16>,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

/// `Debug` of a [`Packet`] would print a CONNECT's password; this one does
/// not.
impl Connect {
    pub fn redacted(&self) -> String {
        format!(
            "Connect {{ client_id: {:?}, clean_start: {}, keep_alive: {}, session_expiry: {}, password: {} }}",
            self.client_id,
            self.clean_start,
            self.keep_alive,
            self.session_expiry,
            if self.password.is_some() {
                "<redacted>"
            } else {
                "none"
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Publish {
    pub topic: String,
    pub qos: u8,
    pub packet_id: Option<u16>,
    pub dup: bool,
    pub retain: bool,
    pub payload_format: Option<u8>,
    pub content_type: Option<String>,
    pub user_properties: Vec<(String, String)>,
    pub payload: Bytes,
}

#[derive(Clone, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    ConnAck {
        session_present: bool,
        reason: u8,
        receive_maximum: Option<u16>,
        server_keep_alive: Option<u16>,
        assigned_client_id: Option<String>,
    },
    Publish(Publish),
    PubAck {
        packet_id: u16,
        reason: u8,
    },
    Subscribe {
        packet_id: u16,
        filters: Vec<(String, u8)>,
    },
    SubAck {
        packet_id: u16,
        reasons: Vec<u8>,
    },
    PingReq,
    PingResp,
    Disconnect {
        reason: u8,
    },
}

impl std::fmt::Debug for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Packet::Connect(c) => f.write_str(&c.redacted()),
            Packet::ConnAck {
                session_present,
                reason,
                ..
            } => write!(
                f,
                "ConnAck {{ session_present: {session_present}, reason: {reason:#04x} }}"
            ),
            Packet::Publish(p) => write!(
                f,
                "Publish {{ topic: {:?}, qos: {}, packet_id: {:?}, dup: {}, {} bytes }}",
                p.topic,
                p.qos,
                p.packet_id,
                p.dup,
                p.payload.len()
            ),
            Packet::PubAck { packet_id, reason } => {
                write!(
                    f,
                    "PubAck {{ packet_id: {packet_id}, reason: {reason:#04x} }}"
                )
            }
            Packet::Subscribe { packet_id, filters } => {
                write!(
                    f,
                    "Subscribe {{ packet_id: {packet_id}, filters: {filters:?} }}"
                )
            }
            Packet::SubAck { packet_id, reasons } => {
                write!(
                    f,
                    "SubAck {{ packet_id: {packet_id}, reasons: {reasons:?} }}"
                )
            }
            Packet::PingReq => f.write_str("PingReq"),
            Packet::PingResp => f.write_str("PingResp"),
            Packet::Disconnect { reason } => write!(f, "Disconnect {{ reason: {reason:#04x} }}"),
        }
    }
}

// ----------------------------------------------------------------- encoding --

fn put_vbi(buf: &mut BytesMut, mut n: usize) {
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if n == 0 {
            break;
        }
    }
}

fn put_str(buf: &mut BytesMut, s: &str) {
    put_bin(buf, s.as_bytes());
}

fn put_bin(buf: &mut BytesMut, b: &[u8]) {
    let len = b.len().min(u16::MAX as usize);
    buf.put_u16(len as u16);
    buf.put_slice(&b[..len]);
}

fn put_props(buf: &mut BytesMut, props: &BytesMut) {
    put_vbi(buf, props.len());
    buf.put_slice(props);
}

/// Append `packet`'s encoding to `out`.
pub fn encode(packet: &Packet, out: &mut BytesMut) {
    let mut body = BytesMut::new();
    let first: u8 = match packet {
        Packet::Connect(c) => {
            put_str(&mut body, "MQTT");
            body.put_u8(5);
            let mut flags = 0u8;
            if c.clean_start {
                flags |= 0x02;
            }
            if c.password.is_some() {
                flags |= 0x40;
            }
            if c.username.is_some() {
                flags |= 0x80;
            }
            body.put_u8(flags);
            body.put_u16(c.keep_alive);
            let mut props = BytesMut::new();
            if c.session_expiry > 0 {
                props.put_u8(P_SESSION_EXPIRY);
                props.put_u32(c.session_expiry);
            }
            if let Some(rm) = c.receive_maximum {
                props.put_u8(P_RECEIVE_MAXIMUM);
                props.put_u16(rm.max(1));
            }
            put_props(&mut body, &props);
            put_str(&mut body, &c.client_id);
            if let Some(u) = &c.username {
                put_str(&mut body, u);
            }
            if let Some(p) = &c.password {
                put_bin(&mut body, p);
            }
            CONNECT << 4
        }
        Packet::ConnAck {
            session_present,
            reason,
            receive_maximum,
            server_keep_alive,
            assigned_client_id,
        } => {
            body.put_u8(u8::from(*session_present));
            body.put_u8(*reason);
            let mut props = BytesMut::new();
            if let Some(rm) = receive_maximum {
                props.put_u8(P_RECEIVE_MAXIMUM);
                props.put_u16(*rm);
            }
            if let Some(k) = server_keep_alive {
                props.put_u8(P_SERVER_KEEP_ALIVE);
                props.put_u16(*k);
            }
            if let Some(id) = assigned_client_id {
                props.put_u8(P_ASSIGNED_CLIENT_ID);
                put_str(&mut props, id);
            }
            put_props(&mut body, &props);
            CONNACK << 4
        }
        Packet::Publish(p) => {
            put_str(&mut body, &p.topic);
            if p.qos > 0 {
                body.put_u16(p.packet_id.unwrap_or(1));
            }
            let mut props = BytesMut::new();
            if let Some(f) = p.payload_format {
                props.put_u8(P_PAYLOAD_FORMAT);
                props.put_u8(f);
            }
            if let Some(ct) = &p.content_type {
                props.put_u8(P_CONTENT_TYPE);
                put_str(&mut props, ct);
            }
            for (k, v) in &p.user_properties {
                props.put_u8(P_USER_PROPERTY);
                put_str(&mut props, k);
                put_str(&mut props, v);
            }
            put_props(&mut body, &props);
            body.put_slice(&p.payload);
            let mut flags = (p.qos & 0x03) << 1;
            if p.dup {
                flags |= 0x08;
            }
            if p.retain {
                flags |= 0x01;
            }
            (PUBLISH << 4) | flags
        }
        Packet::PubAck { packet_id, reason } => {
            body.put_u16(*packet_id);
            if *reason != 0 {
                body.put_u8(*reason);
            }
            PUBACK << 4
        }
        Packet::Subscribe { packet_id, filters } => {
            body.put_u16(*packet_id);
            put_vbi(&mut body, 0);
            for (f, qos) in filters {
                put_str(&mut body, f);
                body.put_u8(qos & 0x03);
            }
            (SUBSCRIBE << 4) | 0x02
        }
        Packet::SubAck { packet_id, reasons } => {
            body.put_u16(*packet_id);
            put_vbi(&mut body, 0);
            body.put_slice(reasons);
            SUBACK << 4
        }
        Packet::PingReq => PINGREQ << 4,
        Packet::PingResp => PINGRESP << 4,
        Packet::Disconnect { reason } => {
            if *reason != 0 {
                body.put_u8(*reason);
                put_vbi(&mut body, 0);
            }
            DISCONNECT << 4
        }
    };
    out.put_u8(first);
    put_vbi(out, body.len());
    out.put_slice(&body);
}

// ----------------------------------------------------------------- decoding --

/// The remaining length of a fixed header: `Some((len, header bytes))`, or
/// `None` when more bytes are needed.
fn peek_vbi(b: &[u8]) -> Result<Option<(usize, usize)>, CodecError> {
    let mut n = 0usize;
    let mut mult = 1usize;
    for (i, byte) in b.iter().enumerate().take(4) {
        n += usize::from(byte & 0x7f) * mult;
        if byte & 0x80 == 0 {
            return Ok(Some((n, i + 1)));
        }
        mult *= 128;
    }
    if b.len() >= 4 {
        return Err(CodecError::Malformed(
            "remaining length longer than 4 bytes",
        ));
    }
    Ok(None)
}

struct Reader {
    b: Bytes,
}

impl Reader {
    fn need(&self, n: usize) -> Result<(), CodecError> {
        if self.b.remaining() < n {
            Err(CodecError::Malformed("truncated"))
        } else {
            Ok(())
        }
    }
    fn u8(&mut self) -> Result<u8, CodecError> {
        self.need(1)?;
        Ok(self.b.get_u8())
    }
    fn u16(&mut self) -> Result<u16, CodecError> {
        self.need(2)?;
        Ok(self.b.get_u16())
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        self.need(4)?;
        Ok(self.b.get_u32())
    }
    fn vbi(&mut self) -> Result<usize, CodecError> {
        match peek_vbi(&self.b)? {
            Some((n, used)) => {
                self.b.advance(used);
                Ok(n)
            }
            None => Err(CodecError::Malformed("truncated variable byte integer")),
        }
    }
    fn bin(&mut self) -> Result<Bytes, CodecError> {
        let n = usize::from(self.u16()?);
        self.need(n)?;
        Ok(self.b.split_to(n))
    }
    fn string(&mut self) -> Result<String, CodecError> {
        let b = self.bin()?;
        String::from_utf8(b.to_vec()).map_err(|_| CodecError::Malformed("a string is not UTF-8"))
    }
    fn take(&mut self, n: usize) -> Result<Reader, CodecError> {
        self.need(n)?;
        Ok(Reader {
            b: self.b.split_to(n),
        })
    }
    fn rest(&mut self) -> Bytes {
        self.b.split_to(self.b.remaining())
    }
    fn is_empty(&self) -> bool {
        !self.b.has_remaining()
    }
}

#[derive(Default)]
struct Props {
    payload_format: Option<u8>,
    content_type: Option<String>,
    session_expiry: Option<u32>,
    receive_maximum: Option<u16>,
    server_keep_alive: Option<u16>,
    assigned_client_id: Option<String>,
    user: Vec<(String, String)>,
}

fn props(r: &mut Reader) -> Result<Props, CodecError> {
    let len = r.vbi()?;
    let mut p = r.take(len)?;
    let mut out = Props::default();
    while !p.is_empty() {
        let id = p.vbi()?;
        let id = u8::try_from(id).map_err(|_| CodecError::Malformed("property id"))?;
        match id {
            P_PAYLOAD_FORMAT => out.payload_format = Some(p.u8()?),
            P_CONTENT_TYPE => out.content_type = Some(p.string()?),
            P_SESSION_EXPIRY => out.session_expiry = Some(p.u32()?),
            P_RECEIVE_MAXIMUM => out.receive_maximum = Some(p.u16()?),
            P_SERVER_KEEP_ALIVE => out.server_keep_alive = Some(p.u16()?),
            P_ASSIGNED_CLIENT_ID => out.assigned_client_id = Some(p.string()?),
            P_USER_PROPERTY => {
                let k = p.string()?;
                let v = p.string()?;
                out.user.push((k, v));
            }
            // Skipped by type (§2.2.2.2).
            0x17 | 0x19 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2A => {
                p.u8()?;
            }
            0x22 | 0x23 => {
                p.u16()?;
            }
            0x02 | 0x18 | P_MAXIMUM_PACKET_SIZE => {
                p.u32()?;
            }
            0x0B => {
                p.vbi()?;
            }
            0x08 | 0x15 | 0x1A | 0x1C | 0x1F => {
                p.string()?;
            }
            0x09 | 0x16 => {
                p.bin()?;
            }
            _ => return Err(CodecError::Malformed("unknown property")),
        }
    }
    Ok(out)
}

/// Take one packet off the front of `buf`. `Ok(None)` means more bytes are
/// needed; nothing is consumed then. A packet larger than `max` is an error.
pub fn decode(buf: &mut BytesMut, max: usize) -> Result<Option<Packet>, CodecError> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let Some((len, header)) = peek_vbi(&buf[1..])? else {
        return Ok(None);
    };
    let total = 1 + header + len;
    if total > max {
        return Err(CodecError::TooLarge(total, max));
    }
    if buf.len() < total {
        return Ok(None);
    }
    let frame = buf.split_to(total).freeze();
    let first = frame[0];
    let mut r = Reader {
        b: frame.slice(1 + header..),
    };
    let kind = first >> 4;
    let flags = first & 0x0f;
    let packet = match kind {
        CONNECT => {
            if r.string()? != "MQTT" || r.u8()? != 5 {
                return Err(CodecError::Malformed("not an MQTT 5 CONNECT"));
            }
            let f = r.u8()?;
            let keep_alive = r.u16()?;
            let p = props(&mut r)?;
            let client_id = r.string()?;
            if f & 0x04 != 0 {
                // A will: its properties, topic and payload.
                props(&mut r)?;
                r.string()?;
                r.bin()?;
            }
            let username = if f & 0x80 != 0 {
                Some(r.string()?)
            } else {
                None
            };
            let password = if f & 0x40 != 0 {
                Some(r.bin()?.to_vec())
            } else {
                None
            };
            Packet::Connect(Connect {
                client_id,
                clean_start: f & 0x02 != 0,
                keep_alive,
                session_expiry: p.session_expiry.unwrap_or(0),
                receive_maximum: p.receive_maximum,
                username,
                password,
            })
        }
        CONNACK => {
            let ack_flags = r.u8()?;
            let reason = r.u8()?;
            let p = if r.is_empty() {
                Props::default()
            } else {
                props(&mut r)?
            };
            Packet::ConnAck {
                session_present: ack_flags & 0x01 != 0,
                reason,
                receive_maximum: p.receive_maximum,
                server_keep_alive: p.server_keep_alive,
                assigned_client_id: p.assigned_client_id,
            }
        }
        PUBLISH => {
            let qos = (flags >> 1) & 0x03;
            if qos == 3 {
                return Err(CodecError::Malformed("QoS 3"));
            }
            let topic = r.string()?;
            let packet_id = if qos > 0 { Some(r.u16()?) } else { None };
            let p = props(&mut r)?;
            Packet::Publish(Publish {
                topic,
                qos,
                packet_id,
                dup: flags & 0x08 != 0,
                retain: flags & 0x01 != 0,
                payload_format: p.payload_format,
                content_type: p.content_type,
                user_properties: p.user,
                payload: r.rest(),
            })
        }
        PUBACK => {
            let packet_id = r.u16()?;
            let reason = if r.is_empty() { 0 } else { r.u8()? };
            Packet::PubAck { packet_id, reason }
        }
        SUBSCRIBE => {
            let packet_id = r.u16()?;
            props(&mut r)?;
            let mut filters = Vec::new();
            while !r.is_empty() {
                let f = r.string()?;
                let o = r.u8()?;
                filters.push((f, o & 0x03));
            }
            Packet::Subscribe { packet_id, filters }
        }
        SUBACK => {
            let packet_id = r.u16()?;
            props(&mut r)?;
            Packet::SubAck {
                packet_id,
                reasons: r.rest().to_vec(),
            }
        }
        PINGREQ => Packet::PingReq,
        PINGRESP => Packet::PingResp,
        DISCONNECT => Packet::Disconnect {
            reason: if r.is_empty() { 0 } else { r.u8()? },
        },
        other => return Err(CodecError::Unsupported(other)),
    };
    Ok(Some(packet))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn round_trip(p: Packet) {
        let mut buf = BytesMut::new();
        encode(&p, &mut buf);
        let len = buf.len();
        // Partial buffers decode to nothing and consume nothing.
        for cut in 0..len {
            let mut part = BytesMut::from(&buf[..cut]);
            assert_eq!(
                decode(&mut part, 1 << 20).unwrap(),
                None,
                "{p:?} cut at {cut}"
            );
            assert_eq!(part.len(), cut);
        }
        let mut whole = buf.clone();
        whole.extend_from_slice(&[0xc0, 0x00]); // a PINGREQ after it
        assert_eq!(decode(&mut whole, 1 << 20).unwrap(), Some(p));
        assert_eq!(decode(&mut whole, 1 << 20).unwrap(), Some(Packet::PingReq));
        assert!(whole.is_empty());
    }

    #[test]
    fn packets_round_trip() {
        round_trip(Packet::Connect(Connect {
            client_id: "loams-1".into(),
            clean_start: false,
            keep_alive: 30,
            session_expiry: 3600,
            receive_maximum: Some(100),
            username: Some("u".into()),
            password: Some(b"pw".to_vec()),
        }));
        round_trip(Packet::ConnAck {
            session_present: true,
            reason: 0,
            receive_maximum: Some(10),
            server_keep_alive: Some(60),
            assigned_client_id: None,
        });
        round_trip(Packet::Publish(Publish {
            topic: "a/b".into(),
            qos: 1,
            packet_id: Some(7),
            dup: true,
            retain: false,
            payload_format: Some(1),
            content_type: Some("application/json".into()),
            user_properties: vec![
                ("ce_id".into(), "x".into()),
                ("ce_id".into(), "dup-key".into()),
            ],
            payload: Bytes::from(vec![7u8; 300]),
        }));
        round_trip(Packet::Publish(Publish {
            topic: "t".into(),
            payload: Bytes::from_static(b"q0"),
            ..Publish::default()
        }));
        round_trip(Packet::PubAck {
            packet_id: 7,
            reason: 0,
        });
        round_trip(Packet::PubAck {
            packet_id: 8,
            reason: 0x80,
        });
        round_trip(Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a/#".into(), 1), ("b".into(), 0)],
        });
        round_trip(Packet::SubAck {
            packet_id: 1,
            reasons: vec![1, 0],
        });
        round_trip(Packet::PingResp);
        round_trip(Packet::Disconnect { reason: 0 });
        round_trip(Packet::Disconnect { reason: 0x8e });
    }

    #[test]
    fn connect_matches_the_specification_example() {
        // §3.1.2.11's layout: "MQTT", level 5, flags, keep alive, properties,
        // then the payload.
        let mut buf = BytesMut::new();
        encode(
            &Packet::Connect(Connect {
                client_id: "c".into(),
                clean_start: true,
                keep_alive: 10,
                ..Connect::default()
            }),
            &mut buf,
        );
        assert_eq!(
            &buf[..],
            &[
                0x10, 14, 0, 4, b'M', b'Q', b'T', b'T', 5, 0x02, 0, 10, 0, 0, 1, b'c'
            ]
        );
    }

    #[test]
    fn variable_byte_integers() {
        for n in [0usize, 127, 128, 16_383, 16_384, 2_097_151, 268_435_455] {
            let mut b = BytesMut::new();
            put_vbi(&mut b, n);
            assert_eq!(peek_vbi(&b).unwrap(), Some((n, b.len())));
        }
        assert!(peek_vbi(&[0xff, 0xff, 0xff, 0xff, 0x01]).is_err());
    }

    #[test]
    fn oversized_and_malformed_packets_are_refused() {
        let mut buf = BytesMut::new();
        encode(
            &Packet::Publish(Publish {
                topic: "t".into(),
                payload: Bytes::from(vec![0u8; 1000]),
                ..Publish::default()
            }),
            &mut buf,
        );
        assert!(matches!(
            decode(&mut buf.clone(), 100),
            Err(CodecError::TooLarge(_, 100))
        ));
        let mut bad = BytesMut::from(&[0x36u8, 0][..]); // PUBLISH with QoS 3
        assert!(decode(&mut bad, 100).is_err());
        let mut auth = BytesMut::from(&[0xf0u8, 0][..]); // AUTH
        assert_eq!(decode(&mut auth, 100), Err(CodecError::Unsupported(15)));
    }

    #[test]
    fn connect_debug_never_shows_the_password() {
        let p = Packet::Connect(Connect {
            password: Some(b"hunter2".to_vec()),
            ..Connect::default()
        });
        assert!(!format!("{p:?}").contains("hunter2"));
    }
}
