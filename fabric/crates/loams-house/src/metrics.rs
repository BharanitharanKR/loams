//! The front's Prometheus metrics (HS1 Task 23, design §49 §15), on the admin
//! listener (`--admin-listen`, 8125, loopback only): `GET /metrics`.
//!
//! Served so far, from what the front already counts:
//!
//! * `loams_house_workers{state}`: the pool's workers by state (`idle`,
//!   `bound`, `leased`, `booting`, `pinned`);
//! * `loams_house_worker_kills_total{reason}`: workers retired, by the reasons
//!   of the Shared contracts;
//! * `loams_house_pipe_lag_records{ns,pipe}`: what each materialized view this
//!   front has run is behind its source (records of a stream, objects of an
//!   `S3Queue`), `pipe` being the view's `database.name` (ruling R23.2);
//! * `loams_house_pipe_committed_records_total{ns,pipe}` and
//!   `loams_house_pipe_committed_rows_total{ns,pipe}`: what this front
//!   committed for each;
//! * `loams_house_pipe_errors{ns,pipe}`: 1 while its last run failed;
//! * `loams_house_queue_depth`, `loams_house_admitted_queries` and
//!   `loams_house_admitted_memory_bytes`: §49 §12's admission (HS1 Task 21);
//!   the queue depth is what Task 24's autoscaler reads.
//!
//! The rest of §15's list (queries, bytes, commits, cache, compaction) comes
//! with the rest of Task 23.

use std::fmt::Write as _;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::admission::{Admission, WorkerPool};
use crate::catalog::Lake;
use crate::watchdog::ExitReason;

/// The `Content-Type` of the text exposition format.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
/// The most a request head to the admin listener may be.
const MAX_HEAD: usize = 8 * 1024;

/// A label value, escaped as the exposition format asks.
fn label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// The metrics, in the text exposition format.
pub fn render(pool: &WorkerPool, lake: Option<&Lake>, admission: &Admission) -> String {
    let mut out = String::new();
    let admitted = admission.stats();
    for (name, help, value) in [
        (
            "loams_house_queue_depth",
            "Statements waiting for admission (§49 §12), every namespace.",
            admitted.queued as u64,
        ),
        (
            "loams_house_admitted_queries",
            "Statements admitted and running, every namespace.",
            admitted.running as u64,
        ),
        (
            "loams_house_admitted_memory_bytes",
            "The admitted statements' max_memory_usage, summed.",
            admitted.memory,
        ),
    ] {
        family(&mut out, name, "gauge", help);
        let _ = writeln!(out, "{name} {value}");
    }
    let stats = pool.stats();
    family(
        &mut out,
        "loams_house_workers",
        "gauge",
        "Workers of this front's pool, by state.",
    );
    for (state, n) in [
        ("idle", stats.idle_unbound),
        ("bound", stats.idle_bound),
        ("leased", stats.leased),
        ("booting", stats.booting),
        ("pinned", stats.pinned),
    ] {
        let _ = writeln!(out, "loams_house_workers{{state=\"{state}\"}} {n}");
    }
    family(
        &mut out,
        "loams_house_worker_kills_total",
        "counter",
        "Workers retired, by reason.",
    );
    for reason in ExitReason::ALL {
        let _ = writeln!(
            out,
            "loams_house_worker_kills_total{{reason=\"{}\"}} {}",
            reason.as_str(),
            stats.kills_for(reason)
        );
    }
    let states = lake
        .map(|lake| lake.pipe_states().all())
        .unwrap_or_default();
    let series = |out: &mut String,
                  name: &str,
                  kind: &str,
                  help: &str,
                  value: &dyn Fn(&crate::pipes::PipeState) -> u64| {
        family(out, name, kind, help);
        for ((namespace, database, view), state) in &states {
            let _ = writeln!(
                out,
                "{name}{{ns=\"{namespace}\",pipe=\"{}\"}} {}",
                label(&format!("{database}.{view}")),
                value(state)
            );
        }
    };
    series(
        &mut out,
        "loams_house_pipe_lag_records",
        "gauge",
        "Records (objects, for S3Queue) a materialized view is behind its source, at this \
         front's last look.",
        &|s| s.lag_records,
    );
    series(
        &mut out,
        "loams_house_pipe_committed_records_total",
        "counter",
        "Records (objects, for S3Queue) this front committed for a materialized view.",
        &|s| s.committed_records,
    );
    series(
        &mut out,
        "loams_house_pipe_committed_rows_total",
        "counter",
        "Rows this front committed into a materialized view's target.",
        &|s| s.committed_rows,
    );
    series(
        &mut out,
        "loams_house_pipe_errors",
        "gauge",
        "1 while a materialized view's last run on this front failed.",
        &|s| u64::from(!s.last_error.is_empty()),
    );
    out
}

/// Serves `GET /metrics` on `listener` until the returned task is aborted.
pub fn serve(
    listener: TcpListener,
    pool: WorkerPool,
    lake: Option<Arc<Lake>>,
    admission: Arc<Admission>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            let pool = pool.clone();
            let lake = lake.clone();
            let admission = Arc::clone(&admission);
            tokio::spawn(async move {
                let _ = tokio::time::timeout(
                    Duration::from_secs(10),
                    answer(stream, &pool, lake.as_deref(), &admission),
                )
                .await;
            });
        }
    })
}

async fn answer(
    mut stream: TcpStream,
    pool: &WorkerPool,
    lake: Option<&Lake>,
    admission: &Admission,
) -> io::Result<()> {
    let mut head = Vec::with_capacity(512);
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        head.extend_from_slice(&buf[..n]);
        if head.len() > MAX_HEAD {
            return respond(
                &mut stream,
                "431 Request Header Fields Too Large",
                "text/plain",
                "",
            )
            .await;
        }
    }
    let line = head.split(|&b| b == b'\r').next().unwrap_or_default();
    let line = String::from_utf8_lossy(line);
    let mut parts = line.split_whitespace();
    let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let path = path.split('?').next().unwrap_or("");
    match (method, path) {
        ("GET", "/metrics") => {
            respond(
                &mut stream,
                "200 OK",
                CONTENT_TYPE,
                &render(pool, lake, admission),
            )
            .await
        }
        ("GET", _) => respond(&mut stream, "404 Not Found", "text/plain", "not found\n").await,
        _ => {
            respond(
                &mut stream,
                "405 Method Not Allowed",
                "text/plain",
                "GET only\n",
            )
            .await
        }
    }
}

async fn respond(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: \
         close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_are_escaped() {
        assert_eq!(label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }
}
