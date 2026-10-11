//! A fake keyspace-mode TiDB: PROXY v2 header, greeting (with or without
//! CLIENT_SSL), TLS, a switch to caching_sha2_password and full auth, then
//! OK for every command (its info names the fake).
//!
//! For the migration (Task 6) each connection keeps a session "state" (a
//! string) and a transaction flag: `SET STATE <s>` and `GET STATE` (in the
//! OK's info) change and read it, `BEGIN` and `COMMIT` set the
//! `SERVER_STATUS_IN_TRANS` flag of every OK, `SHOW SESSION_STATES` returns
//! the state as a one-row result set (ERR 8146 inside a transaction or when
//! the state is `locked`), and `SET SESSION_STATES '<s>'` restores it.
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use loams_sqlgate::codec::auth::{
    AuthMoreData, AuthSwitchRequest, CACHING_SHA2, NATIVE, double_sha256, verify_caching_sha2,
};
use loams_sqlgate::codec::command::{ErrPacket, OkPacket};
use loams_sqlgate::codec::handshake::{
    Capabilities as C, ClientHello, HandshakeV10, Limits, Nonce, TIDB_V8_5_8, decode_client_hello,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::pki::Pki;
use super::wire::Wire;

#[derive(Debug, Default, Clone)]
pub struct Seen {
    pub proxy_sources: Vec<SocketAddr>,
    pub logins: Vec<String>,
    pub databases: Vec<Option<String>>,
    pub plaintext_credentials: usize,
    pub commands: Vec<u8>,
    /// `SET SESSION_STATES` arguments received (unescaped).
    pub restored: Vec<String>,
    /// `SHOW SESSION_STATES` statements received.
    pub shows: usize,
}

pub struct FakeTidb {
    pub addr: SocketAddr,
    pub seen: Arc<Mutex<Seen>>,
}

pub struct FakeOpts {
    pub name: &'static str,
    pub ssl: bool,
    /// Flags left out of the greeting (a TiDB that drifted from the
    /// static profile).
    pub drop: C,
    pub user: &'static str,
    pub password: &'static [u8],
}

pub async fn spawn(opts: FakeOpts, pki: &Pki) -> FakeTidb {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let seen = Arc::new(Mutex::new(Seen::default()));
    let tls = tokio_rustls::TlsAcceptor::from(pki.upstream_server_config());
    let opts = Arc::new(opts);
    let s = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let (tls, opts, seen) = (tls.clone(), opts.clone(), s.clone());
            tokio::spawn(async move {
                let _ = serve(tcp, tls, &opts, &seen).await;
            });
        }
    });
    FakeTidb { addr, seen }
}

async fn read_proxy_v2(tcp: &mut tokio::net::TcpStream) -> Option<SocketAddr> {
    let mut head = [0u8; 16];
    tcp.read_exact(&mut head).await.ok()?;
    assert_eq!(&head[..12], b"\r\n\r\n\0\r\nQUIT\n", "PROXY v2 signature");
    assert_eq!(head[12], 0x21, "v2 PROXY command");
    let len = usize::from(u16::from_be_bytes([head[14], head[15]]));
    let mut body = vec![0u8; len];
    tcp.read_exact(&mut body).await.ok()?;
    match head[13] {
        0x11 => {
            let ip = std::net::Ipv4Addr::new(body[0], body[1], body[2], body[3]);
            Some(SocketAddr::new(
                ip.into(),
                u16::from_be_bytes([body[8], body[9]]),
            ))
        }
        0x21 => {
            let mut a = [0u8; 16];
            a.copy_from_slice(&body[..16]);
            Some(SocketAddr::new(
                std::net::Ipv6Addr::from(a).into(),
                u16::from_be_bytes([body[32], body[33]]),
            ))
        }
        f => panic!("unexpected PROXY family {f:#x}"),
    }
}

async fn serve(
    mut tcp: tokio::net::TcpStream,
    tls: tokio_rustls::TlsAcceptor,
    opts: &FakeOpts,
    seen: &Mutex<Seen>,
) -> Option<()> {
    let src = read_proxy_v2(&mut tcp).await?;
    seen.lock().expect("seen").proxy_sources.push(src);
    let nonce = Nonce::from_random(*b"fake-tidb-nonce-0001");
    let caps = if opts.ssl {
        TIDB_V8_5_8
    } else {
        TIDB_V8_5_8.without(C::SSL)
    }
    .without(opts.drop);
    let greeting = HandshakeV10 {
        server_version: "8.0.11-TiDB-v8.5.8-Loams".into(),
        connection_id: 1,
        nonce,
        capabilities: caps,
        charset: 46,
        status: 2,
        auth_plugin: NATIVE.into(),
    };
    let mut plain = Wire::new(tcp);
    plain.write(0, &greeting.encode()).await;
    let (_, first) = plain.read().await?;
    if let ClientHello::Response(_) = decode_client_hello(&first, &Limits::default()).ok()? {
        seen.lock().expect("seen").plaintext_credentials += 1;
        return None;
    }
    let (tcp, early) = plain.into_parts();
    let stream = tls
        .accept(loams_sqlgate::wire::Prefixed::new(early, tcp))
        .await
        .ok()?;
    let mut w = Wire::new(stream);
    let (_, r) = w.read().await?;
    let ClientHello::Response(response) = decode_client_hello(&r, &Limits::default()).ok()? else {
        return None;
    };
    {
        let mut s = seen.lock().expect("seen");
        s.logins.push(response.username.clone());
        s.databases.push(response.database.clone());
    }
    // Like TiDB: switch to the user's plugin with the same nonce.
    let mut data = nonce.as_bytes().to_vec();
    data.push(0);
    w.write(
        3,
        &AuthSwitchRequest {
            plugin: CACHING_SHA2.into(),
            data,
        }
        .encode(),
    )
    .await;
    let (_, scramble) = w.read().await?;
    let user_ok = response.username == opts.user
        && verify_caching_sha2(&double_sha256(opts.password), nonce.as_bytes(), &scramble);
    w.write(5, &AuthMoreData::PerformFullAuthentication.encode())
        .await;
    let (_, clear) = w.read().await?;
    let pw_ok = clear.strip_suffix(&[0]) == Some(opts.password);
    if !(user_ok && pw_ok) {
        w.write(
            7,
            &ErrPacket::new(1045, *b"28000", "Access denied").encode(),
        )
        .await;
        return None;
    }
    let ok_status = |info: &str, status: u16| {
        OkPacket {
            affected_rows: 0,
            last_insert_id: 0,
            status,
            warnings: 0,
            info: info.as_bytes().to_vec(),
        }
        .encode(C::PROTOCOL_41)
    };
    let ok = |info: &str| ok_status(info, 2);
    // Like TiDB: the database is checked after the password.
    match response.database.as_deref() {
        Some("missing") => {
            w.write(
                7,
                &ErrPacket::new(1049, *b"42000", "Unknown database 'missing'").encode(),
            )
            .await;
            return None;
        }
        Some("forbidden") => {
            let msg = "Access denied for user 'ri_writer'@'%' to database 'forbidden'";
            w.write(7, &ErrPacket::new(1044, *b"42000", msg).encode())
                .await;
            return None;
        }
        _ => {}
    }
    w.write(7, &ok("")).await;
    let deprecate_eof = response.capabilities.contains(C::DEPRECATE_EOF);
    let mut state = String::from("fresh");
    let mut in_txn = false;
    loop {
        let (_, cmd) = w.read().await?;
        let first = *cmd.first()?;
        seen.lock().expect("seen").commands.push(first);
        if first == 0x01 || cmd == b"\x03BYE" {
            return Some(());
        }
        let status = if in_txn { 3 } else { 2 };
        let sql = cmd
            .strip_prefix(b"\x03")
            .map(|q| String::from_utf8_lossy(q).into_owned());
        match sql.as_deref() {
            Some("BEGIN") => {
                in_txn = true;
                w.write(1, &ok_status(opts.name, 3)).await;
                continue;
            }
            Some("COMMIT") => {
                in_txn = false;
                w.write(1, &ok(opts.name)).await;
                continue;
            }
            Some("GET STATE") => {
                w.write(1, &ok_status(&state, status)).await;
                continue;
            }
            Some("SHOW SESSION_STATES") => {
                seen.lock().expect("seen").shows += 1;
                if in_txn || state == "locked" {
                    let msg =
                        "cannot migrate the current session: session has an active transaction";
                    w.write(1, &ErrPacket::new(8146, *b"HY000", msg).encode())
                        .await;
                    continue;
                }
                let lenenc = |v: &[u8]| [&[v.len() as u8][..], v].concat();
                let coldef = |name: &str| {
                    [
                        &lenenc(b"def")[..],
                        &lenenc(b""),
                        &lenenc(b""),
                        &lenenc(b""),
                        &lenenc(name.as_bytes()),
                        &lenenc(b""),
                        &[
                            0x0c, 0x3f, 0x00, 0xff, 0xff, 0xff, 0xff, 0xf5, 0, 0, 0, 0, 0,
                        ],
                    ]
                    .concat()
                };
                let mut seq = 1;
                let mut send = |p: Vec<u8>| {
                    let s = seq;
                    seq += 1;
                    (s, p)
                };
                let mut packets = vec![
                    send(vec![2]),
                    send(coldef("Session_states")),
                    send(coldef("Session_token")),
                ];
                if !deprecate_eof {
                    packets.push(send(vec![0xfe, 0, 0, 2, 0]));
                }
                packets.push(send([lenenc(state.as_bytes()), lenenc(b"null")].concat()));
                let mut end = ok("");
                end[0] = 0xfe;
                packets.push(send(if deprecate_eof {
                    end
                } else {
                    vec![0xfe, 0, 0, 2, 0]
                }));
                for (s, p) in packets {
                    w.write(s, &p).await;
                }
                continue;
            }
            Some(q) if q.starts_with("SET STATE ") => {
                state = q["SET STATE ".len()..].to_owned();
                w.write(1, &ok_status(opts.name, status)).await;
                continue;
            }
            Some(q) if q.starts_with("SET SESSION_STATES '") && q.ends_with('\'') => {
                let quoted = &q["SET SESSION_STATES '".len()..q.len() - 1];
                let mut restored = String::new();
                let mut chars = quoted.chars();
                while let Some(c) = chars.next() {
                    restored.push(if c == '\\' { chars.next()? } else { c });
                }
                state = restored.clone();
                seen.lock().expect("seen").restored.push(restored);
                w.write(1, &ok(opts.name)).await;
                continue;
            }
            _ => {}
        }
        if cmd == b"\x03SLEEP" {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            w.write(1, &ok("slept")).await;
            continue;
        }
        if cmd == b"\x03SLOW" {
            // One packet written in two halves 200 ms apart.
            let payload = ok(&format!("slow-{}", "x".repeat(200)));
            let mut frame = Vec::new();
            loams_sqlgate::codec::packet::encode(&payload, &mut 1, &mut frame);
            let half = frame.len() / 2;
            w.io.write_all(&frame[..half]).await.ok()?;
            w.io.flush().await.ok()?;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            w.io.write_all(&frame[half..]).await.ok()?;
            w.io.flush().await.ok()?;
            continue;
        }
        w.write(1, &ok_status(opts.name, status)).await;
    }
}
