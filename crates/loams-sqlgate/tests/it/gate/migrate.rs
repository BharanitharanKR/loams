//! Idle-session migration at the gate (plan SQ1 Task 6) against fake TiDB
//! members: a session moves between commands, keeps its state, waits out
//! a transaction or TiDB's refusal, and is closed with 1053 when the move
//! fails. `migrate_real.rs` runs the named tests against real TiDB.
use std::net::SocketAddr;
use std::time::Duration;

use loams_sqlgate::codec::command::ErrPacket;
use loams_sqlgate::upstream::{Close, UpstreamMember};
use rustls::pki_types::ServerName;

use super::{Harness, Options, harness};

fn member(addr: SocketAddr) -> UpstreamMember {
    UpstreamMember {
        addr,
        server_name: ServerName::try_from("tidb.test").expect("name"),
    }
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

fn restored(h: &Harness) -> Vec<String> {
    h.b.seen.lock().expect("seen").restored.clone()
}

fn shows(h: &Harness) -> usize {
    h.a.seen.lock().expect("seen").shows
}

#[tokio::test]
async fn idle_session_moves_to_another_member_with_its_state() {
    let h = harness(Options::default()).await;
    let mut c = h.tls("u_a", b"pa").await.expect("login");
    let state = r#"{"current-db":"w","user-vars":{"x":"it's a \\ backslash"}}"#;
    assert_eq!(c.query_info(&format!("SET STATE {state}")).await, "tidb-a");
    let before = h.activity.count("br_a");

    h.wake.close(Close::Migrate(vec![member(h.b.addr)]));
    eventually("the state restored on b", || restored(&h).len() == 1).await;
    assert_eq!(restored(&h), [state], "quoted and unquoted intact");

    assert_eq!(c.query_info("SELECT 1").await, "tidb-b");
    assert_eq!(c.query_info("GET STATE").await, state);
    // The migration's own statements are not the client's activity.
    assert_eq!(h.activity.count("br_a"), before + 2);
    // The old member got COM_QUIT.
    eventually("COM_QUIT on a", || {
        h.a.seen.lock().expect("seen").commands.last() == Some(&0x01)
    })
    .await;
    assert_eq!(h.wake.live(), 1, "one session, one lease");
}

#[tokio::test]
async fn session_in_transaction_moves_after_commit() {
    let h = harness(Options::default()).await;
    let mut c = h.tls("u_a", b"pa").await.expect("login");
    assert_eq!(c.query_info("BEGIN").await, "tidb-a");
    h.wake.close(Close::Migrate(vec![member(h.b.addr)]));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(c.query_info("SELECT 1").await, "tidb-a", "not moved");
    assert_eq!(shows(&h), 0, "TiDB was not even asked");
    assert_eq!(c.query_info("COMMIT").await, "tidb-a");
    eventually("the move after COMMIT", || restored(&h).len() == 1).await;
    assert_eq!(c.query_info("SELECT 1").await, "tidb-b");
}

#[tokio::test]
async fn session_tidb_will_not_move_stays_until_it_can() {
    let h = harness(Options::default()).await;
    let mut c = h.tls("u_a", b"pa").await.expect("login");
    assert_eq!(c.query_info("SET STATE locked").await, "tidb-a");
    h.wake.close(Close::Migrate(vec![member(h.b.addr)]));
    eventually("TiDB asked", || shows(&h) == 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(shows(&h), 1, "no retry while nothing changed");
    assert_eq!(c.query_info("SELECT 1").await, "tidb-a", "ERR 8146: stays");
    assert_eq!(c.query_info("SET STATE free").await, "tidb-a");
    eventually("the move once TiDB allows it", || restored(&h) == ["free"]).await;
    assert_eq!(c.query_info("SELECT 1").await, "tidb-b");
}

#[tokio::test]
async fn session_already_on_a_target_member_stays() {
    let h = harness(Options::default()).await;
    let mut c = h.tls("u_a", b"pa").await.expect("login");
    assert_eq!(c.query_info("SELECT 1").await, "tidb-a");
    h.wake
        .close(Close::Migrate(vec![member(h.b.addr), member(h.a.addr)]));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(c.query_info("SELECT 1").await, "tidb-a");
    assert_eq!(shows(&h), 0);
}

#[tokio::test]
async fn failed_migration_closes_the_session_with_1053() {
    let h = harness(Options::default()).await;
    let mut c = h.tls("u_a", b"pa").await.expect("login");
    assert_eq!(c.query_info("SELECT 1").await, "tidb-a");
    // Nothing listens there.
    let dead = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        l.local_addr().expect("addr")
    };
    h.wake.close(Close::Migrate(vec![member(dead)]));
    let (seq, p) = c.wire.read().await.expect("an ERR before the close");
    let e = ErrPacket::decode(&p).expect("ERR");
    assert_eq!((seq, e.code, &e.sql_state), (0, 1053, b"08S01"), "{e:?}");
    assert_eq!(c.wire.read().await, None, "then closed");
    eventually("the lease released", || h.wake.live() == 0).await;
    assert_eq!(h.gate.open_connections("br_a"), 0);
}

#[tokio::test]
async fn a_close_still_wins_over_a_pending_move() {
    let h = harness(Options::default()).await;
    let mut c = h.tls("u_a", b"pa").await.expect("login");
    assert_eq!(c.query_info("BEGIN").await, "tidb-a");
    h.wake.close(Close::Migrate(vec![member(h.b.addr)]));
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The member must go now: a session that never became movable is drained.
    h.wake.close(Close::WhenIdle);
    let (_, p) = c.wire.read().await.expect("ERR 1053");
    assert_eq!(ErrPacket::decode(&p).expect("ERR").code, 1053);
    assert_eq!(c.wire.read().await, None);
}

#[tokio::test]
async fn multi_statements_turned_on_after_login_survive_the_move() {
    let h = harness(Options::default()).await;
    let mut c = h.tls("u_a", b"pa").await.expect("login");
    // The test client logs in without CLIENT_MULTI_STATEMENTS, then turns
    // them on (mysql_set_server_option).
    let p = c.command(b"\x1b\x00\x00").await.expect("answer");
    assert_ne!(p[0], 0xff, "COM_SET_OPTION accepted");
    h.wake.close(Close::Migrate(vec![member(h.b.addr)]));
    eventually("the move", || restored(&h).len() == 1).await;
    eventually("COM_SET_OPTION ON replayed on b", || {
        h.b.seen.lock().expect("seen").commands.contains(&0x1b)
    })
    .await;
    assert_eq!(c.query_info("SELECT 1").await, "tidb-b");
}

#[tokio::test]
async fn states_travel_as_utf8_whatever_the_login_charset() {
    let h = harness(Options::default()).await;
    let mut c = h.tls("u_a", b"pa").await.expect("login");
    let state = r#"{"user-vars":{"city":"Zürich"}}"#;
    assert_eq!(c.query_info(&format!("SET STATE {state}")).await, "tidb-a");
    h.wake.close(Close::Migrate(vec![member(h.b.addr)]));
    eventually("the move", || restored(&h).len() == 1).await;
    assert_eq!(restored(&h), [state]);
    let names = h.b.seen.lock().expect("seen").queries.clone();
    let at = |q: &str| names.iter().position(|n| n == q);
    let (Some(set_names), Some(states)) = (
        at("SET NAMES utf8mb4"),
        names
            .iter()
            .position(|n| n.starts_with("SET SESSION_STATES")),
    ) else {
        panic!("statements on b: {names:?}");
    };
    assert!(set_names < states, "utf8mb4 before the states: {names:?}");
}
