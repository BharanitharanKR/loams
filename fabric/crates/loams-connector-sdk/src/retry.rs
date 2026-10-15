//! Retries by error class (CN2 Task 4): exponential backoff with full jitter,
//! honouring the system's `Retry-After`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::ErrorClass;

/// 100 ms → 30 s exponential, full jitter, 10 attempts, honours `Retry-After`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    pub initial: Duration,
    pub max: Duration,
    /// Attempts of one operation, the first included. `Throttled` waits do
    /// not count: a system asking Loams to slow down is not failing.
    pub max_attempts: u32,
    pub honour_retry_after: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            max_attempts: 10,
            honour_retry_after: true,
        }
    }
}

/// What to do after an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    /// Wait, then try again.
    RetryAfter(Duration),
    /// Out of attempts, or the class is not transient.
    GiveUp,
}

impl RetryPolicy {
    /// The backoff ceiling before `attempt` (1-based: the delay before the
    /// second attempt is `ceiling(1)`).
    pub fn ceiling(&self, attempt: u32) -> Duration {
        let factor = 1u32
            .checked_shl(attempt.saturating_sub(1))
            .unwrap_or(u32::MAX);
        self.initial.saturating_mul(factor).min(self.max)
    }

    /// Decide after `failures` failed attempts (counting the one that just
    /// failed) of an operation that returned `class`.
    pub fn decide(&self, class: &ErrorClass, failures: u32) -> RetryDecision {
        match class {
            ErrorClass::Throttled { after } => RetryDecision::RetryAfter(*after),
            ErrorClass::Retryable { after } => {
                if failures >= self.max_attempts {
                    return RetryDecision::GiveUp;
                }
                match after {
                    Some(after) if self.honour_retry_after => RetryDecision::RetryAfter(*after),
                    _ => RetryDecision::RetryAfter(full_jitter(self.ceiling(failures))),
                }
            }
            _ => RetryDecision::GiveUp,
        }
    }
}

/// A uniformly random duration in `[0, ceiling]` ("full jitter"). The
/// generator is splitmix64 over a process-wide counter seeded from the clock:
/// jitter needs spread, not unpredictability, so no RNG crate is pulled in.
fn full_jitter(ceiling: Duration) -> Duration {
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut seed = STATE.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    if seed == 0 {
        seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        STATE.store(seed, Ordering::Relaxed);
    }
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    let nanos = ceiling.as_nanos().min(u64::MAX as u128) as u64;
    if nanos == 0 {
        return Duration::ZERO;
    }
    Duration::from_nanos(z % (nanos + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_exponential_capped_and_jittered() {
        let p = RetryPolicy::default();
        assert_eq!(p.ceiling(1), Duration::from_millis(100));
        assert_eq!(p.ceiling(2), Duration::from_millis(200));
        assert_eq!(p.ceiling(9), Duration::from_millis(25_600));
        assert_eq!(p.ceiling(10), Duration::from_secs(30));
        assert_eq!(p.ceiling(64), Duration::from_secs(30));
        for failures in 1..10 {
            match p.decide(&ErrorClass::Retryable { after: None }, failures) {
                RetryDecision::RetryAfter(d) => assert!(d <= p.ceiling(failures)),
                RetryDecision::GiveUp => panic!("gave up at {failures}"),
            }
        }
        assert_eq!(
            p.decide(&ErrorClass::Retryable { after: None }, 10),
            RetryDecision::GiveUp
        );
        let after = Duration::from_secs(7);
        assert_eq!(
            p.decide(&ErrorClass::Retryable { after: Some(after) }, 1),
            RetryDecision::RetryAfter(after)
        );
        assert_eq!(
            p.decide(&ErrorClass::Throttled { after }, 50),
            RetryDecision::RetryAfter(after)
        );
        assert_eq!(p.decide(&ErrorClass::Fatal, 1), RetryDecision::GiveUp);
    }
}
