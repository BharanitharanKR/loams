//! The discovery file `agentd.json` (design §50 §5.4).
//!
//! The daemon publishes it atomically (owner-only) after its listener is
//! bound, and removes it on a clean exit. A crash leaves it behind, so a
//! reader trusts it only while its `pid` holds the single-instance lock;
//! anything else is stale and ignored. Clients still confirm with `Hello`.

use std::io;

pub use loams_agentd_proto::daemon::Discovery;

use crate::paths::Paths;

/// Write `discovery` to `agentd.json`, replacing any older file.
pub fn publish(paths: &Paths, discovery: &Discovery) -> io::Result<()> {
    let json = serde_json::to_vec_pretty(discovery).map_err(io::Error::other)?;
    crate::owner_only::write(&paths.discovery(), &json)
}

/// The live daemon's discovery: `None` when the file is missing or
/// unreadable, or when its pid does not hold `agentd.lock` (a crashed daemon).
pub fn read(paths: &Paths) -> Option<Discovery> {
    let bytes = std::fs::read(paths.discovery()).ok()?;
    let discovery: Discovery = serde_json::from_slice(&bytes).ok()?;
    let holder = crate::lock::holder(paths)?;
    (holder == discovery.pid.to_string()).then_some(discovery)
}

/// Remove `agentd.json` (a clean exit).
pub fn remove(paths: &Paths) -> io::Result<()> {
    match std::fs::remove_file(paths.discovery()) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_agentd_proto::daemon::DaemonMode;

    fn discovery(pid: u32) -> Discovery {
        Discovery {
            protocol: 1,
            version: "0.0.1".into(),
            pid,
            port: 50217,
            started_at_ms: 1,
            mode: DaemonMode::Child,
            connect_port: Some(50218),
        }
    }

    #[test]
    fn read_trusts_only_the_lock_holder() {
        let root = tempfile::tempdir().expect("temp dir");
        let paths = Paths::new(root.path()).expect("paths");
        let mine = discovery(std::process::id());
        publish(&paths, &mine).expect("publish");
        assert_eq!(read(&paths), None, "nobody holds the lock: stale");

        let lock = crate::lock::acquire(&paths).expect("lock");
        assert_eq!(read(&paths), Some(mine.clone()));
        publish(&paths, &discovery(std::process::id() + 1)).expect("publish");
        assert_eq!(read(&paths), None, "another pid's file is stale");

        publish(&paths, &mine).expect("publish");
        remove(&paths).expect("remove");
        assert_eq!(read(&paths), None);
        remove(&paths).expect("removing twice is fine");
        drop(lock);
    }

    #[test]
    fn read_ignores_a_malformed_file() {
        let root = tempfile::tempdir().expect("temp dir");
        let paths = Paths::new(root.path()).expect("paths");
        let _lock = crate::lock::acquire(&paths).expect("lock");
        std::fs::write(paths.discovery(), b"{not json").expect("write");
        assert_eq!(read(&paths), None);
    }
}
