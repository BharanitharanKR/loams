//! The WebSocket upgrade request, checked before anything is served (design
//! §50 §7, plan DD1 Task 7).
//!
//! The head is parsed here rather than inside tungstenite so that every
//! refusal is a plain status line with no body and no `Access-Control-*`
//! header, including for `OPTIONS` and for requests that are not upgrades
//! (tungstenite closes those without answering). In order:
//! 1. a `GET` WebSocket upgrade (version 13, a well-formed key), else 403;
//! 2. `Host` exactly `127.0.0.1:<port>` (DNS rebinding), else 403;
//! 3. no `Origin` (browsers always send one), else 403;
//! 4. exactly one `Authorization` header, which the server then checks
//!    against its credentials (401 when missing or wrong).

use base64::Engine as _;

/// The most headers an upgrade request may carry.
const MAX_HEADERS: usize = 64;

/// Why an upgrade request is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// 403: not an upgrade, a foreign `Host`, an `Origin`, a malformed head.
    Forbidden,
    /// 401: no `Authorization` header (the server answers a wrong one the same way).
    Unauthorized,
}

/// A request that passed the checks that need no credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct Upgrade {
    /// `Sec-WebSocket-Key`, for the `Sec-WebSocket-Accept` reply.
    pub key: Vec<u8>,
    /// The `Authorization` header's value, OWS-trimmed.
    pub authorization: Vec<u8>,
}

impl std::fmt::Debug for Upgrade {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Upgrade").finish_non_exhaustive()
    }
}

/// Check the request head in `bytes` for a server on loopback `port`.
/// `Ok(None)`: the head is not complete yet. Bytes after the head (a client
/// that did not wait for the 101) are refused.
pub fn check(bytes: &[u8], port: u16) -> Result<Option<Upgrade>, Refusal> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut request = httparse::Request::new(&mut headers);
    let length = match request.parse(bytes) {
        Ok(httparse::Status::Complete(length)) => length,
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(_) => return Err(Refusal::Forbidden),
    };
    if length != bytes.len() {
        return Err(Refusal::Forbidden);
    }
    let headers = Headers(request.headers);

    // 1. A GET WebSocket upgrade, HTTP/1.1.
    if request.method != Some("GET") || request.version != Some(1) {
        return Err(Refusal::Forbidden);
    }
    if !headers.has_token("upgrade", b"websocket")
        || !headers.has_token("connection", b"upgrade")
        || headers.single("sec-websocket-version")? != Some(&b"13"[..])
    {
        return Err(Refusal::Forbidden);
    }
    let key = headers
        .single("sec-websocket-key")?
        .filter(|key| is_websocket_key(key))
        .ok_or(Refusal::Forbidden)?;

    // 2. The host the client dialed: loopback IPv4 and this port, nothing else.
    let expected_host = format!("127.0.0.1:{port}");
    if headers.single("host")? != Some(expected_host.as_bytes()) {
        return Err(Refusal::Forbidden);
    }

    // 3. No browser: every browser WebSocket carries Origin, and script can
    //    neither forge nor suppress it.
    if headers.any("origin") || headers.any("sec-websocket-origin") {
        return Err(Refusal::Forbidden);
    }

    // 4. One Authorization header, checked by the caller.
    let authorization = headers
        .single("authorization")
        .map_err(|_| Refusal::Unauthorized)?
        .ok_or(Refusal::Unauthorized)?;
    Ok(Some(Upgrade {
        key: key.to_vec(),
        authorization: authorization.to_vec(),
    }))
}

/// A `Sec-WebSocket-Key`: base64 of 16 bytes.
fn is_websocket_key(key: &[u8]) -> bool {
    key.len() == 24
        && base64::engine::general_purpose::STANDARD
            .decode(key)
            .is_ok_and(|decoded| decoded.len() == 16)
}

struct Headers<'h, 'b>(&'h [httparse::Header<'b>]);

impl<'h, 'b> Headers<'h, 'b> {
    fn values<'n>(&self, name: &'n str) -> impl Iterator<Item = &'b [u8]> + use<'h, 'b, 'n> {
        let headers: &'h [httparse::Header<'b>] = self.0;
        headers
            .iter()
            .filter(move |header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value.trim_ascii())
    }

    fn any(&self, name: &str) -> bool {
        self.values(name).next().is_some()
    }

    /// The header's value when it appears at most once; twice is refused.
    fn single(&self, name: &str) -> Result<Option<&'b [u8]>, Refusal> {
        let mut values = self.values(name);
        let first = values.next();
        if values.next().is_some() {
            return Err(Refusal::Forbidden);
        }
        Ok(first)
    }

    /// Whether a comma-separated header (any of its lines) lists `token`.
    fn has_token(&self, name: &str, token: &[u8]) -> bool {
        self.values(name).any(|value| {
            value
                .split(|byte| *byte == b',')
                .any(|item| item.trim_ascii().eq_ignore_ascii_case(token))
        })
    }
}

/// `101 Switching Protocols` for `key`.
pub fn accept_response(key: &[u8]) -> String {
    format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\n\r\n",
        tokio_tungstenite::tungstenite::handshake::derive_accept_key(key)
    )
}

/// 401 with no body.
pub const UNAUTHORIZED: &str = "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer\r\n\
                                Content-Length: 0\r\nConnection: close\r\n\r\n";
/// 403 with no body.
pub const FORBIDDEN: &str =
    "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
/// 429 with no body: the role's connection limit.
pub const TOO_MANY: &str =
    "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

#[cfg(test)]
mod tests {
    use super::*;

    const PORT: u16 = 50217;
    const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

    fn request(lines: &[&str]) -> Vec<u8> {
        let mut text = lines.join("\r\n");
        text.push_str("\r\n\r\n");
        text.into_bytes()
    }

    fn upgrade(extra: &[&str]) -> Vec<u8> {
        let host = format!("Host: 127.0.0.1:{PORT}");
        let key = format!("Sec-WebSocket-Key: {KEY}");
        let mut lines = vec![
            "GET / HTTP/1.1",
            host.as_str(),
            "Upgrade: websocket",
            "Connection: keep-alive, Upgrade",
            "Sec-WebSocket-Version: 13",
            key.as_str(),
        ];
        lines.extend_from_slice(extra);
        request(&lines)
    }

    #[test]
    fn a_good_upgrade_passes_with_its_authorization() {
        let checked = check(&upgrade(&["Authorization:  Bearer abc "]), PORT)
            .unwrap()
            .unwrap();
        assert_eq!(checked.key, KEY.as_bytes());
        assert_eq!(checked.authorization, b"Bearer abc");
    }

    #[test]
    fn a_partial_head_waits() {
        let bytes = upgrade(&[]);
        assert_eq!(check(&bytes[..bytes.len() - 2], PORT), Ok(None));
    }

    #[test]
    fn no_authorization_is_401_and_two_are_401() {
        assert_eq!(check(&upgrade(&[]), PORT), Err(Refusal::Unauthorized));
        assert_eq!(
            check(
                &upgrade(&["Authorization: Bearer a", "Authorization: Bearer b"]),
                PORT
            ),
            Err(Refusal::Unauthorized)
        );
    }

    #[test]
    fn browsers_foreign_hosts_and_non_upgrades_are_403() {
        let auth = "Authorization: Bearer abc";
        let forbidden = [
            upgrade(&[auth, "Origin: https://evil.example"]),
            upgrade(&[auth, "Origin: null"]),
            upgrade(&[auth, "Sec-WebSocket-Origin: https://evil.example"]),
            upgrade(&[auth, &format!("Host: 127.0.0.1:{PORT}")]),
            request(&[
                "GET / HTTP/1.1",
                "Host: evil.example",
                "Upgrade: websocket",
                "Connection: Upgrade",
                "Sec-WebSocket-Version: 13",
                &format!("Sec-WebSocket-Key: {KEY}"),
                auth,
            ]),
            request(&[
                "OPTIONS / HTTP/1.1",
                &format!("Host: 127.0.0.1:{PORT}"),
                auth,
            ]),
            request(&["GET / HTTP/1.1", &format!("Host: 127.0.0.1:{PORT}"), auth]),
            request(&[
                "GET / HTTP/1.0",
                &format!("Host: 127.0.0.1:{PORT}"),
                "Upgrade: websocket",
                "Connection: Upgrade",
                "Sec-WebSocket-Version: 13",
                &format!("Sec-WebSocket-Key: {KEY}"),
                auth,
            ]),
            String::from_utf8(upgrade(&[auth]))
                .unwrap()
                .replace(KEY, "short")
                .into_bytes(),
            b"\x16\x03\x01\x00\xa5\r\n\r\n".to_vec(),
        ];
        for bytes in forbidden {
            assert_eq!(
                check(&bytes, PORT),
                Err(Refusal::Forbidden),
                "{}",
                String::from_utf8_lossy(&bytes)
            );
        }
    }

    #[test]
    fn bytes_after_the_head_are_refused() {
        let mut bytes = upgrade(&["Authorization: Bearer abc"]);
        bytes.extend_from_slice(b"\x81\x05hello");
        assert_eq!(check(&bytes, PORT), Err(Refusal::Forbidden));
    }

    #[test]
    fn the_accept_key_follows_rfc_6455() {
        // RFC 6455 §1.3's example.
        assert!(accept_response(KEY.as_bytes()).contains("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
    }
}
