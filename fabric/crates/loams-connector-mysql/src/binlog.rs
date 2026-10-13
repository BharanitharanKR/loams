//! The binlog dump: register as a replica, ask for the binlog from a
//! position, and read events on a task of their own into a bounded channel,
//! so `Source::next` stays cancel-safe and a slow pipeline pauses the read
//! (TCP backpressure) rather than buffering.
//!
//! MySQL resumes by GTID set (`COM_BINLOG_DUMP_GTID`), MariaDB by
//! `@slave_connect_state` and `@mariadb_slave_capability` (R0.7), and either
//! by `file:pos` when GTIDs are off. `@master_heartbeat_period` keeps an idle
//! stream alive. An error 1236 (purged binlogs, a GTID the server no longer
//! has) is `PositionLost`.

use std::time::Duration;

use loams_connector_sdk::{ConnectorError, ErrorClass};
use mysql_common::binlog::EventStreamReader;
use mysql_common::binlog::consts::{BinlogVersion, EventType};
use mysql_common::binlog::events::{Event, TransactionPayloadEvent};
use mysql_common::packets::ComRegisterSlave;
use mysql_common::packets::binlog_request::BinlogRequest;
use mysql_common::proto::MySerialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::conn::{MyConn, MyError};
use crate::gtid::{GtidList, GtidSet};
use crate::types::quote_str;

/// Where to start reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartAt {
    /// MySQL: every transaction outside this set.
    MysqlGtid(GtidSet),
    /// MariaDB: after the last GTID of each domain.
    Mariadb(GtidList),
    /// A file and offset (GTIDs off, or a MariaDB with no GTID yet).
    File { file: String, pos: u64 },
}

/// One event from the stream, and the end offset of the binlog event that
/// carried it (an event inside a compressed transaction payload carries the
/// payload's).
#[derive(Debug, Clone)]
pub struct RawEvent {
    pub event: Event,
    pub log_pos: u64,
}

/// What a stream yields.
pub type StreamItem = Result<RawEvent, ConnectorError>;

/// The largest decompressed transaction payload accepted (a guard against
/// a decompression bomb).
const MAX_PAYLOAD: u64 = 1 << 30;
/// The binlog event header's length (v4).
const HEADER: usize = 19;

/// An error from the dump connection as a [`ConnectorError`]; 1236 gets a
/// re-snapshot hint (Review Focus 6).
pub fn dump_error(e: &MyError) -> ConnectorError {
    let mut ce = e.to_connector("reading the binlog");
    if ce.class == ErrorClass::PositionLost {
        ce.message.push_str(
            "; the binlog this instance needs is gone (purged, or never written): \
             delete and recreate the instance to take a new snapshot",
        );
    }
    ce
}

/// `data`'s event size field matches its length (so `Event::read`, which
/// panics on short input, is safe).
fn whole_event(data: &[u8]) -> bool {
    data.len() >= HEADER
        && data
            .get(9..13)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_le_bytes)
            .is_some_and(|n| n as usize == data.len())
}

/// Decode one binlog packet's event, and the events of a compressed
/// transaction payload it may carry.
pub fn decode_packet(esr: &mut EventStreamReader, data: &[u8]) -> Result<Vec<RawEvent>, String> {
    if !whole_event(data) {
        return Err("a truncated binlog event".into());
    }
    let event = esr
        .read(data)
        .map_err(|e| format!("binlog event: {e}"))?
        .ok_or("an empty binlog event")?;
    let log_pos = u64::from(event.header().log_pos());
    if event.header().event_type_raw() != EventType::TRANSACTION_PAYLOAD_EVENT as u8 {
        return Ok(vec![RawEvent { event, log_pos }]);
    }
    let payload = event
        .read_event::<TransactionPayloadEvent<'_>>()
        .map_err(|e| format!("transaction payload: {e}"))?;
    let mut buf = Vec::new();
    {
        use std::io::Read as _;
        let reader = payload
            .decompressed()
            .map_err(|e| format!("transaction payload: {e}"))?;
        reader
            .take(MAX_PAYLOAD + 1)
            .read_to_end(&mut buf)
            .map_err(|e| format!("transaction payload: {e}"))?;
    }
    if buf.len() as u64 > MAX_PAYLOAD {
        return Err("a transaction payload over 1 GiB".into());
    }
    let mut out = Vec::new();
    let mut rest = buf.as_slice();
    while !rest.is_empty() {
        let size = rest
            .get(9..13)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_le_bytes)
            .ok_or("a truncated event in a transaction payload")? as usize;
        if size < HEADER || size > rest.len() {
            return Err("a truncated event in a transaction payload".into());
        }
        let inner = esr
            .read_decompressed(&rest[..size])
            .map_err(|e| format!("transaction payload event: {e}"))?
            .ok_or("an empty event in a transaction payload")?;
        out.push(RawEvent {
            event: inner,
            log_pos,
        });
        rest = &rest[size..];
    }
    Ok(out)
}

/// A running dump. See the module documentation.
#[derive(Debug)]
pub struct BinlogStream {
    rx: mpsc::Receiver<StreamItem>,
    task: JoinHandle<()>,
}

impl Drop for BinlogStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Events buffered between the reader task and the source.
const CHANNEL: usize = 512;

impl BinlogStream {
    /// Turn `conn` into a binlog dump from `at`.
    pub async fn start(
        mut conn: MyConn,
        at: &StartAt,
        server_id: u32,
        heartbeat: Duration,
    ) -> Result<Self, ConnectorError> {
        let setup = |e: MyError| e.to_connector("starting the binlog dump");
        conn.exec("SET @master_binlog_checksum = @@global.binlog_checksum")
            .await
            .map_err(setup)?;
        if !heartbeat.is_zero() {
            conn.exec(&format!(
                "SET @master_heartbeat_period = {}",
                heartbeat.as_nanos().min(u128::from(u32::MAX) * 1_000_000)
            ))
            .await
            .map_err(setup)?;
        }
        if let StartAt::Mariadb(state) = at {
            conn.exec(&format!(
                "SET @mariadb_slave_capability = 4, @slave_connect_state = {}, \
                 @slave_gtid_strict_mode = 0, @slave_gtid_ignore_duplicates = 0",
                quote_str(&state.to_string())
            ))
            .await
            .map_err(setup)?;
        } else if conn.mariadb {
            // A MariaDB replica that does not resume by GTID still declares
            // that it understands MariaDB's events.
            conn.exec("SET @mariadb_slave_capability = 4")
                .await
                .map_err(setup)?;
        }
        let mut buf = Vec::new();
        ComRegisterSlave::new(server_id).serialize(&mut buf);
        conn.command(&buf).await.map_err(setup)?;
        let ok = conn.read_packet().await.map_err(setup)?;
        conn.done();
        if ok.first() == Some(&0xff) {
            return Err(dump_error(&MyError::Protocol(
                "the server refused COM_REGISTER_SLAVE".into(),
            )));
        }
        let req = match at {
            StartAt::MysqlGtid(set) => BinlogRequest::new(server_id)
                .with_use_gtid(true)
                .with_sids(set.sids().map_err(ConnectorError::fatal)?),
            StartAt::Mariadb(_) => BinlogRequest::new(server_id).with_pos(4u64),
            StartAt::File { file, pos } => BinlogRequest::new(server_id)
                .with_filename(file.as_bytes().to_vec())
                .with_pos(*pos),
        };
        let mut buf = Vec::new();
        req.as_cmd().serialize(&mut buf);
        conn.command(&buf).await.map_err(setup)?;
        let (tx, rx) = mpsc::channel(CHANNEL);
        let task = tokio::spawn(read_loop(conn, tx));
        Ok(Self { rx, task })
    }

    /// The next event, waiting up to `wait`; `None` when none arrived.
    pub async fn recv(&mut self, wait: Duration) -> Result<Option<RawEvent>, ConnectorError> {
        match tokio::time::timeout(wait, self.rx.recv()).await {
            Err(_) => Ok(None),
            Ok(Some(r)) => r.map(Some),
            Ok(None) => Err(ConnectorError::retryable("the binlog stream ended")),
        }
    }

    /// An event already received, without waiting.
    pub fn try_recv(&mut self) -> Option<StreamItem> {
        match self.rx.try_recv() {
            Ok(r) => Some(r),
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                Some(Err(ConnectorError::retryable("the binlog stream ended")))
            }
        }
    }
}

#[cfg(test)]
impl BinlogStream {
    /// A stream fed by a test instead of a server.
    pub(crate) fn from_receiver(rx: mpsc::Receiver<StreamItem>) -> Self {
        Self {
            rx,
            task: tokio::spawn(async {}),
        }
    }
}

async fn read_loop(mut conn: MyConn, tx: mpsc::Sender<StreamItem>) {
    let mut esr = EventStreamReader::new(BinlogVersion::Version4);
    loop {
        let item = match conn.read_packet().await {
            Err(e) => Err(dump_error(&e)),
            Ok(p) => match p.first() {
                Some(0x00) => match decode_packet(&mut esr, &p[1..]) {
                    Ok(events) => {
                        for e in events {
                            if tx.send(Ok(e)).await.is_err() {
                                return;
                            }
                        }
                        continue;
                    }
                    Err(e) => Err(ConnectorError::fatal(e)),
                },
                Some(0xff) => {
                    let e = match crate::conn::parse_err(&p) {
                        Some(e) => MyError::Server(e),
                        None => MyError::Protocol("a malformed error packet".into()),
                    };
                    Err(dump_error(&e))
                }
                Some(0xfe) if p.len() < 9 => Err(ConnectorError::retryable(
                    "the server ended the binlog stream",
                )),
                _ => Err(ConnectorError::fatal("an unexpected packet in the binlog")),
            },
        };
        let stop = item.is_err();
        if tx.send(item).await.is_err() || stop {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncated_events_are_refused_not_panicked_on() {
        let mut esr = EventStreamReader::new(BinlogVersion::Version4);
        assert!(decode_packet(&mut esr, &[0u8; 10]).is_err());
        // A header claiming 100 bytes over 19 bytes of data.
        let mut h = vec![0u8; 19];
        h[9..13].copy_from_slice(&100u32.to_le_bytes());
        assert!(decode_packet(&mut esr, &h).is_err());
    }

    use crate::conn::fake::{Reply, script};
    use crate::conn::{Login, MyConn};
    use crate::synth::{Synth, UUID};
    use mysql_common::binlog::consts::EventType;

    async fn login(port: u16) -> MyConn {
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp");
        let login = Login {
            user: "u".into(),
            password: None,
            database: None,
        };
        MyConn::handshake(tcp, "127.0.0.1", None, &login)
            .await
            .expect("login")
    }

    fn rotate(s: &mut Synth) -> Vec<u8> {
        let mut b = vec![0u8];
        let mut body = 4u64.to_le_bytes().to_vec();
        body.extend(b"mariadb-bin.000002");
        b.extend(s.bytes(EventType::ROTATE_EVENT as u8, &body));
        b
    }

    /// R0.7: a MariaDB replica resumes by `@slave_connect_state` and a plain
    /// `COM_BINLOG_DUMP`; the server's 1236 is `PositionLost`.
    #[tokio::test]
    async fn mariadb_dump_resumes_by_connect_state() {
        let mut s = Synth::new();
        let mut err = vec![0xffu8];
        err.extend(1236u16.to_le_bytes());
        err.extend(b"#HY000Could not find GTID state requested by slave in any binlog files");
        let (port, seen) = script(
            "5.5.5-11.8.8-MariaDB-log",
            |_| vec![Reply::Ok(0)],
            vec![rotate(&mut s), err],
        )
        .await;
        let conn = login(port).await;
        assert!(conn.mariadb);
        let state = crate::gtid::GtidList::parse("0-1-100").expect("state");
        let mut stream = BinlogStream::start(
            conn,
            &StartAt::Mariadb(state),
            0x0123_4567,
            Duration::from_secs(5),
        )
        .await
        .expect("dump");
        let first = stream
            .recv(Duration::from_secs(5))
            .await
            .expect("an event")
            .expect("not idle");
        assert_eq!(
            first.event.header().event_type_raw(),
            EventType::ROTATE_EVENT as u8
        );
        let e = stream
            .recv(Duration::from_secs(5))
            .await
            .expect_err("the server's 1236");
        assert_eq!(e.class, ErrorClass::PositionLost);
        let seen = seen.lock().expect("seen").clone();
        let sql: Vec<String> = seen
            .iter()
            .filter(|p| p[0] == 0x03)
            .map(|p| String::from_utf8_lossy(&p[1..]).into_owned())
            .collect();
        assert!(
            sql.iter()
                .any(|q| q.contains("@mariadb_slave_capability = 4")
                    && q.contains(
                        "@slave_connect_state = CONVERT(X'302D312D313030' USING utf8mb4)"
                    )),
            "{sql:?}"
        );
        assert!(
            sql.iter()
                .any(|q| q.contains("@master_heartbeat_period = 5000000000"))
        );
        let register = seen
            .iter()
            .find(|p| p[0] == 0x15)
            .expect("COM_REGISTER_SLAVE");
        assert_eq!(&register[1..5], &0x0123_4567u32.to_le_bytes());
        let dump = seen.iter().find(|p| p[0] == 0x12).expect("COM_BINLOG_DUMP");
        assert_eq!(
            &dump[1..5],
            &4u32.to_le_bytes(),
            "position 4: the state decides"
        );
    }

    /// MySQL resumes with `COM_BINLOG_DUMP_GTID` and the GTID set.
    #[tokio::test]
    async fn mysql_dump_sends_the_gtid_set() {
        let (port, seen) = script("8.4.10", |_| vec![Reply::Ok(0)], vec![]).await;
        let conn = login(port).await;
        let set =
            crate::gtid::GtidSet::parse("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5").expect("set");
        let _stream = BinlogStream::start(conn, &StartAt::MysqlGtid(set), 77, Duration::ZERO)
            .await
            .expect("dump");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let dump = loop {
            if let Some(p) = seen.lock().expect("seen").iter().find(|p| p[0] == 0x1e) {
                break p.clone();
            }
            assert!(tokio::time::Instant::now() < deadline, "no dump command");
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(
            dump.windows(16).any(|w| w == UUID),
            "the set's UUID is sent"
        );
        let heartbeat = seen
            .lock()
            .expect("seen")
            .iter()
            .any(|p| String::from_utf8_lossy(p).contains("heartbeat"));
        assert!(!heartbeat, "a zero heartbeat is not set");
    }

    #[test]
    fn purged_positions_hint_at_a_resnapshot() {
        let e = dump_error(&MyError::Server(crate::conn::ServerErr {
            code: 1236,
            state: "HY000".into(),
            message: "Cannot replicate because the source purged required binary logs".into(),
        }));
        assert_eq!(e.class, ErrorClass::PositionLost);
        assert!(e.message.contains("new snapshot"), "{}", e.message);
    }
}
