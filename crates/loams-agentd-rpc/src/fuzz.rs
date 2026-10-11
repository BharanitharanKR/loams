//! Invariants checked by the fuzz targets (`fuzz/fuzz_targets`) and by the
//! unit tests below. Each panics only when an invariant breaks.

use crate::{ClientFrame, ServerFrame};

/// Untrusted bytes as an ndjson payload, split into frames the way the server
/// and the client split them: decoding never panics, and every frame that
/// decodes re-encodes to a frame that decodes the same.
pub fn frame(data: &[u8]) {
    let Ok(payload) = std::str::from_utf8(data) else {
        return;
    };
    for line in payload
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        if let Ok(frame) = serde_json::from_str::<ClientFrame>(line) {
            let again: ClientFrame =
                serde_json::from_str(&serde_json::to_string(&frame).expect("encodes"))
                    .expect("a decoded client frame re-decodes");
            assert_eq!(
                serde_json::to_value(&again).expect("encodes"),
                serde_json::to_value(&frame).expect("encodes"),
                "client frame round trip"
            );
        }
        if let Ok(frame) = serde_json::from_str::<ServerFrame>(line) {
            let again: ServerFrame =
                serde_json::from_str(&serde_json::to_string(&frame).expect("encodes"))
                    .expect("a decoded server frame re-decodes");
            assert_eq!(
                serde_json::to_value(&again).expect("encodes"),
                serde_json::to_value(&frame).expect("encodes"),
                "server frame round trip"
            );
        }
    }
}

/// Untrusted bytes as an upgrade request head: the checks never panic, and
/// whatever passes them names the loopback host and carries one key.
pub fn handshake(data: &[u8]) {
    const PORT: u16 = 50217;
    if let Ok(Some(upgrade)) = crate::handshake::check(data, PORT) {
        assert_eq!(upgrade.key.len(), 24, "a checked key is 24 base64 bytes");
        let text = String::from_utf8_lossy(data).to_ascii_lowercase();
        assert!(
            text.contains("127.0.0.1:50217"),
            "a checked head names the host"
        );
        assert!(!text.contains("\norigin:"), "a checked head has no Origin");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn frame_invariants_hold_on_seeds() {
        for seed in [
            &b""[..],
            b"{}",
            b"{\"id\":1,\"method\":\"Hello\",\"params\":{\"protocol\":1}}\n{\"id\":1,\"cancel\":true}",
            b"{\"id\":18446744073709551615,\"method\":\"\"}",
            b"{\"id\":-1}",
            b"{\"id\":2,\"ok\":null,\"err\":\"x\",\"item\":[1],\"done\":true}",
            b"\xff\xfe",
            b"{\"id\":1,\"params\":{\"a\":1e400}}",
        ] {
            super::frame(seed);
        }
    }

    #[test]
    fn handshake_invariants_hold_on_seeds() {
        for seed in [
            &b""[..],
            b"GET / HTTP/1.1\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: 127.0.0.1:50217\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
              Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
              Authorization: Bearer x\r\n\r\n",
            b"\x16\x03\x01\x00\xa5\r\n\r\n",
        ] {
            super::handshake(seed);
        }
    }
}
