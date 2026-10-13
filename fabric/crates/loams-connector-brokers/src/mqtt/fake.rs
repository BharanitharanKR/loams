//! An in-process MQTT 5 broker for the client's unit tests: persistent
//! sessions, QoS 1 delivery with Receive Maximum, redelivery of unacknowledged
//! messages (DUP) when a kept session reconnects, `+` and `#` filters. It
//! proves the connector's protocol handling without a server; the same tests
//! run against Eclipse Mosquitto in CI (`tests/mqtt.rs`), and only those
//! count toward the manifest's suites (OR-2).

// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use super::codec::{self, Packet, Publish};

#[derive(Default)]
struct Session {
    subs: Vec<String>,
    unacked: BTreeMap<u16, Publish>,
    queue: VecDeque<Publish>,
    next_id: u16,
    receive_max: u16,
    conn: Option<mpsc::UnboundedSender<Packet>>,
}

#[derive(Default)]
struct State {
    sessions: HashMap<String, Session>,
    /// Every message clients published, in order.
    published: Vec<Publish>,
}

pub struct FakeBroker {
    pub addr: SocketAddr,
    state: Arc<Mutex<State>>,
    _task: tokio::task::JoinHandle<()>,
}

pub fn matches(filter: &str, topic: &str) -> bool {
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
    for (i, part) in f.iter().enumerate() {
        match *part {
            "#" => return true,
            "+" => {
                if i >= t.len() {
                    return false;
                }
            }
            p => {
                if t.get(i) != Some(&p) {
                    return false;
                }
            }
        }
    }
    f.len() == t.len()
}

fn pump(s: &mut Session) {
    let Some(conn) = s.conn.clone() else {
        return;
    };
    while (s.unacked.len() as u16) < s.receive_max.max(1) {
        let Some(mut p) = s.queue.pop_front() else {
            break;
        };
        s.next_id = s.next_id.wrapping_add(1).max(1);
        let id = s.next_id;
        p.packet_id = Some(id);
        p.qos = 1;
        p.dup = false;
        s.unacked.insert(id, p.clone());
        let _ = conn.send(Packet::Publish(p));
    }
}

fn route(state: &mut State, p: &Publish) {
    for s in state.sessions.values_mut() {
        if s.subs.iter().any(|f| matches(f, &p.topic)) {
            s.queue.push_back(p.clone());
            pump(s);
        }
    }
}

impl FakeBroker {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let state = Arc::new(Mutex::new(State::default()));
        let st = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                tokio::spawn(serve(sock, st.clone()));
            }
        });
        Self {
            addr,
            state,
            _task: task,
        }
    }

    pub fn url(&self) -> String {
        format!("mqtt://{}", self.addr)
    }

    /// Publish as if another client had.
    pub fn inject(&self, p: Publish) {
        let mut st = self.state.lock().expect("state");
        st.published.push(p.clone());
        route(&mut st, &p);
    }

    pub fn published(&self) -> Vec<Publish> {
        self.state.lock().expect("state").published.clone()
    }

    pub fn unacked(&self, client_id: &str) -> usize {
        self.state
            .lock()
            .expect("state")
            .sessions
            .get(client_id)
            .map_or(0, |s| s.unacked.len())
    }

    pub fn queued(&self, client_id: &str) -> usize {
        self.state
            .lock()
            .expect("state")
            .sessions
            .get(client_id)
            .map_or(0, |s| s.queue.len())
    }
}

async fn serve(sock: tokio::net::TcpStream, state: Arc<Mutex<State>>) {
    let (mut rd, mut wr) = sock.into_split();
    let mut buf = BytesMut::new();
    let (tx, mut rx) = mpsc::unbounded_channel::<Packet>();
    let writer = tokio::spawn(async move {
        while let Some(p) = rx.recv().await {
            let mut out = BytesMut::new();
            codec::encode(&p, &mut out);
            if wr.write_all(&out).await.is_err() {
                break;
            }
        }
    });
    let mut client_id = None::<String>;
    let mut clean = true;
    loop {
        let packet = loop {
            match codec::decode(&mut buf, 1 << 24) {
                Ok(Some(p)) => break Some(p),
                Ok(None) => {}
                Err(_) => break None,
            }
            match rd.read_buf(&mut buf).await {
                Ok(0) | Err(_) => break None,
                Ok(_) => {}
            }
        };
        let Some(packet) = packet else {
            break;
        };
        let mut st = state.lock().expect("state");
        match packet {
            Packet::Connect(c) => {
                clean = c.clean_start;
                let present = !c.clean_start && st.sessions.contains_key(&c.client_id);
                if c.clean_start {
                    st.sessions.remove(&c.client_id);
                }
                let s = st.sessions.entry(c.client_id.clone()).or_default();
                s.receive_max = c.receive_maximum.unwrap_or(u16::MAX);
                s.conn = Some(tx.clone());
                let _ = tx.send(Packet::ConnAck {
                    session_present: present,
                    reason: 0,
                    receive_maximum: None,
                    server_keep_alive: None,
                    assigned_client_id: None,
                });
                // Redeliver what the last connection left unacknowledged.
                for p in s.unacked.values() {
                    let mut p = p.clone();
                    p.dup = true;
                    let _ = tx.send(Packet::Publish(p));
                }
                pump(s);
                client_id = Some(c.client_id);
            }
            Packet::Subscribe { packet_id, filters } => {
                if let Some(s) = client_id.as_ref().and_then(|id| st.sessions.get_mut(id)) {
                    for (f, _) in &filters {
                        if !s.subs.contains(f) {
                            s.subs.push(f.clone());
                        }
                    }
                }
                let _ = tx.send(Packet::SubAck {
                    packet_id,
                    reasons: filters.iter().map(|_| 1).collect(),
                });
            }
            Packet::Publish(p) => {
                if let Some(id) = p.packet_id {
                    let _ = tx.send(Packet::PubAck {
                        packet_id: id,
                        reason: 0,
                    });
                }
                st.published.push(p.clone());
                route(&mut st, &p);
            }
            Packet::PubAck { packet_id, .. } => {
                if let Some(s) = client_id.as_ref().and_then(|id| st.sessions.get_mut(id)) {
                    s.unacked.remove(&packet_id);
                    pump(s);
                }
            }
            Packet::PingReq => {
                let _ = tx.send(Packet::PingResp);
            }
            Packet::Disconnect { .. } => break,
            _ => break,
        }
    }
    let mut st = state.lock().expect("state");
    if let Some(id) = client_id {
        if clean {
            st.sessions.remove(&id);
        } else if let Some(s) = st.sessions.get_mut(&id) {
            s.conn = None;
        }
    }
    writer.abort();
}

#[test]
fn filters_match() {
    assert!(matches("a/#", "a/b/c"));
    assert!(matches("a/+/c", "a/b/c"));
    assert!(!matches("a/+", "a/b/c"));
    assert!(matches("a/b", "a/b"));
    assert!(!matches("a/b", "a/c"));
}
