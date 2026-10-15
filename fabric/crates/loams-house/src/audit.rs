//! The House's audit (HS1 Task 20, §49 §14; controller ruling R20.5).
//!
//! Every statement a House is sent leaves one [`AuditEvent`]: who (the House
//! user and, through the engine's link, the Loams principal), where (the
//! namespace), what (the statement's kind, never its text: literals can be
//! secrets, Task 23's rule for operator logs) and how it ended (the
//! ClickHouse code, `0` for success). Refusals are recorded too, including
//! failed sign-ins, with the principal the request claimed.
//!
//! [`JsonLines`] appends one JSON object per line to a file (`--audit-log`;
//! a `--single-node` House writes `<data-dir>/audit.log`). [`Memory`] keeps
//! them for tests.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// One statement's record.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct AuditEvent {
    /// When, in Unix milliseconds.
    pub at_ms: u64,
    /// The surface: `http` or `api`.
    pub surface: &'static str,
    /// The House user that signed in (the link user for the engine), or the
    /// name the request claimed when sign-in failed.
    pub user: String,
    /// The Loams principal acted for, when the engine's link named one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// The namespace, once known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<u64>,
    /// The query id.
    pub query_id: String,
    /// The statement's kind (`Query`, `Insert`, `CreateTable`, …), once
    /// classified.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement: Option<String>,
    /// The ClickHouse code it ended with; `0` for success.
    pub code: i32,
}

impl AuditEvent {
    /// A record stamped now.
    pub fn now(surface: &'static str, query_id: &str) -> Self {
        Self {
            at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            surface,
            query_id: query_id.to_string(),
            ..Self::default()
        }
    }
}

/// Where audit records go.
pub trait AuditSink: Send + Sync + std::fmt::Debug {
    /// Records one statement. Never fails the statement: a sink that cannot
    /// write says so on stderr.
    fn record(&self, event: &AuditEvent);
}

/// One JSON object per line, appended to a file (mode 0600).
#[derive(Debug)]
pub struct JsonLines {
    path: PathBuf,
    file: Mutex<std::fs::File>,
}

impl JsonLines {
    /// Opens (or creates) `path` for appending.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        use std::os::unix::fs::OpenOptionsExt as _;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file: Mutex::new(file),
        })
    }
}

impl AuditSink for JsonLines {
    fn record(&self, event: &AuditEvent) {
        let Ok(mut line) = serde_json::to_vec(event) else {
            return;
        };
        line.push(b'\n');
        let mut file = self.file.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(err) = file.write_all(&line) {
            eprintln!(
                "loams-house: the audit log {} could not be written: {err}",
                self.path.display()
            );
        }
    }
}

/// Records kept in memory (tests).
#[derive(Debug, Default)]
pub struct Memory(Mutex<Vec<AuditEvent>>);

impl Memory {
    /// Everything recorded so far.
    pub fn events(&self) -> Vec<AuditEvent> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl AuditSink for Memory {
    fn record(&self, event: &AuditEvent) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(event.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_lines_append_without_statement_text() {
        let exe = std::env::current_exe().expect("the test binary");
        let dir = exe
            .parent()
            .expect("its directory")
            .join(format!("house-audit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("audit.log");
        let sink = JsonLines::open(&path).expect("opens");
        let mut event = AuditEvent::now("http", "q1");
        event.user = "engine".to_string();
        event.principal = Some("tailscale:alice@x.com".to_string());
        event.namespace = Some(1);
        event.statement = Some("Insert".to_string());
        sink.record(&event);
        event.code = 497;
        sink.record(&event);
        let text = std::fs::read_to_string(&path).expect("read");
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).expect("json"))
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["principal"], "tailscale:alice@x.com");
        assert_eq!(lines[1]["code"], 497);
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&path).expect("meta").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
