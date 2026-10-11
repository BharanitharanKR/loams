//! The wall clock the WAL and the sequencer read (plan GT1 shared
//! contracts), so tests can fix time.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch.
pub trait Clock: Send + Sync + fmt::Debug {
    /// Now, in ms since the Unix epoch.
    fn now_unix_ms(&self) -> i64;
}

/// The system's wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
            })
    }
}
