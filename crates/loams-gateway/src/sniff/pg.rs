//! Postgres's opening packets (design §57 §7.2): `SSLRequest`,
//! `GSSENCRequest`, `CancelRequest`, a protocol-3 `StartupMessage`, or a TLS
//! record (direct TLS, `sslnegotiation=direct`).

use tokio::io::{AsyncRead, AsyncReadExt};

use super::SniffError;

/// `SSLRequest`'s code (`1234 << 16 | 5679`).
pub const SSL_REQUEST_CODE: u32 = 80_877_103;
/// `GSSENCRequest`'s code (`1234 << 16 | 5680`).
pub const GSSENC_REQUEST_CODE: u32 = 80_877_104;
/// `CancelRequest`'s code (`1234 << 16 | 5678`).
pub const CANCEL_REQUEST_CODE: u32 = 80_877_102;
/// The 8 bytes of an `SSLRequest`.
pub const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
/// A TLS handshake record's content type.
pub const TLS_HANDSHAKE: u8 = 0x16;
/// The largest plaintext startup packet looked at (D1020).
pub const MAX_STARTUP_LEN: usize = 10 * 1024;

/// What the client opened with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Opening {
    SslRequest,
    GssEncRequest,
    /// The whole 16-byte packet.
    Cancel([u8; 16]),
    /// A plaintext protocol-3 `StartupMessage` (refused, D1015).
    Startup,
    /// The first byte of a TLS record: direct TLS.
    Tls,
    Unknown,
}

/// Reads the opening into `buf` (which may already hold bytes) and
/// classifies it. Every byte read stays in `buf`. For an `SSLRequest` or a
/// `GSSENCRequest`, `buf.len() > 8` means the client sent data before the
/// server's answer.
pub async fn sniff_opening<R: AsyncRead + Unpin>(
    io: &mut R,
    buf: &mut Vec<u8>,
) -> Result<Opening, SniffError> {
    fill(io, buf, 1).await?;
    if buf[0] == TLS_HANDSHAKE {
        return Ok(Opening::Tls);
    }
    fill(io, buf, 8).await?;
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let code = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    Ok(match (len, code) {
        (8, SSL_REQUEST_CODE) => Opening::SslRequest,
        (8, GSSENC_REQUEST_CODE) => Opening::GssEncRequest,
        (16, CANCEL_REQUEST_CODE) => {
            fill(io, buf, 16).await?;
            let mut packet = [0; 16];
            packet.copy_from_slice(&buf[..16]);
            Opening::Cancel(packet)
        }
        (len, code) if code >> 16 == 3 && (8..=MAX_STARTUP_LEN).contains(&len) => Opening::Startup,
        _ => Opening::Unknown,
    })
}

/// Reads until `buf` holds at least `want` bytes.
async fn fill<R: AsyncRead + Unpin>(
    io: &mut R,
    buf: &mut Vec<u8>,
    want: usize,
) -> Result<(), SniffError> {
    while buf.len() < want {
        let mut chunk = [0u8; 512];
        let n = io.read(&mut chunk).await?;
        if n == 0 {
            return Err(SniffError::Eof);
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(())
}

/// A plaintext `ErrorResponse` with severity `FATAL`.
pub fn fatal_error(sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (field, value) in [
        (b'S', "FATAL"),
        (b'V', "FATAL"),
        (b'C', sqlstate),
        (b'M', message),
    ] {
        body.push(field);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(b'E');
    let len = u32::try_from(body.len() + 4).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&body);
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    async fn sniff(bytes: &[u8]) -> (Result<Opening, SniffError>, Vec<u8>) {
        let mut io = bytes;
        let mut buf = Vec::new();
        let r = sniff_opening(&mut io, &mut buf).await;
        (r, buf)
    }

    #[tokio::test]
    async fn classifies_every_opening() {
        assert_eq!(sniff(&SSL_REQUEST).await.0.unwrap(), Opening::SslRequest);
        let gss = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x30];
        assert_eq!(sniff(&gss).await.0.unwrap(), Opening::GssEncRequest);
        let mut cancel = vec![0, 0, 0, 16, 0x04, 0xd2, 0x16, 0x2e];
        cancel.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let (r, _) = sniff(&cancel).await;
        assert_eq!(
            r.unwrap(),
            Opening::Cancel(cancel.clone().try_into().unwrap())
        );
        let startup = [0, 0, 0, 9, 0, 3, 0, 0, 0];
        assert_eq!(sniff(&startup).await.0.unwrap(), Opening::Startup);
        assert_eq!(sniff(&[0x16, 3, 1]).await.0.unwrap(), Opening::Tls);
        assert_eq!(
            sniff(b"GET / HTTP/1.1\r\n").await.0.unwrap(),
            Opening::Unknown
        );
    }

    #[tokio::test]
    async fn oversized_startup_is_unknown() {
        let big = (MAX_STARTUP_LEN as u32 + 1).to_be_bytes();
        let startup = [big[0], big[1], big[2], big[3], 0, 3, 0, 0];
        assert_eq!(sniff(&startup).await.0.unwrap(), Opening::Unknown);
    }

    #[tokio::test]
    async fn short_opening_is_eof() {
        assert!(matches!(sniff(&[0, 0, 0]).await.0, Err(SniffError::Eof)));
    }

    #[tokio::test]
    async fn pipelined_bytes_are_kept() {
        let mut bytes = SSL_REQUEST.to_vec();
        bytes.extend_from_slice(b"extra");
        let (r, buf) = sniff(&bytes).await;
        assert_eq!(r.unwrap(), Opening::SslRequest);
        assert_eq!(buf.len(), 13);
    }

    #[test]
    fn error_response_is_well_formed() {
        let e = fatal_error("28000", "no");
        assert_eq!(e[0], b'E');
        let len = u32::from_be_bytes([e[1], e[2], e[3], e[4]]) as usize;
        assert_eq!(len + 1, e.len());
        assert!(e.ends_with(&[b'M', b'n', b'o', 0, 0]));
    }
}
