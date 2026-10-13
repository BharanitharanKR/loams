//! Rate limiting (CN2 Task 4): one token bucket per instance, for requests and
//! for bytes, shared by its splits and adjusted by `Throttled`.

use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

/// A rate: `amount` per `per`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rate {
    pub amount: f64,
    pub per: Duration,
}

impl Rate {
    pub fn per_second(amount: f64) -> Self {
        Self {
            amount,
            per: Duration::from_secs(1),
        }
    }

    fn per_sec(&self) -> f64 {
        self.amount / self.per.as_secs_f64().max(f64::MIN_POSITIVE)
    }
}

/// An instance's configured limits; `None` is unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RateLimit {
    pub requests: Option<Rate>,
    pub bytes: Option<Rate>,
}

/// The factor a `Throttled` multiplies the rate by, its floor, and how fast a
/// success brings it back.
const THROTTLE_FACTOR: f64 = 0.5;
const MIN_FACTOR: f64 = 1.0 / 16.0;
const RECOVERY: f64 = 1.05;

#[derive(Debug)]
struct Bucket {
    rate_per_sec: f64,
    capacity: f64,
    tokens: f64,
}

impl Bucket {
    fn new(rate: Rate) -> Self {
        let per_sec = rate.per_sec();
        // Burst: one period's worth, at least one unit.
        let capacity = rate.amount.max(1.0);
        Self {
            rate_per_sec: per_sec,
            capacity,
            tokens: capacity,
        }
    }

    fn refill(&mut self, elapsed: Duration, factor: f64) {
        self.tokens =
            (self.tokens + elapsed.as_secs_f64() * self.rate_per_sec * factor).min(self.capacity);
    }

    /// Time until `n` tokens are there (capped at the capacity, so a request
    /// bigger than the burst waits for a full bucket and then goes).
    fn wait_for(&self, n: f64, factor: f64) -> Duration {
        let n = n.min(self.capacity);
        if self.tokens >= n {
            Duration::ZERO
        } else {
            Duration::from_secs_f64((n - self.tokens) / (self.rate_per_sec * factor))
        }
    }

    fn take(&mut self, n: f64) {
        self.tokens -= n.min(self.capacity);
    }
}

#[derive(Debug)]
struct State {
    requests: Option<Bucket>,
    bytes: Option<Bucket>,
    factor: f64,
    blocked_until: Option<Instant>,
    last: Instant,
}

/// The token bucket of one instance.
#[derive(Debug)]
pub struct RateLimiter {
    state: Mutex<State>,
}

impl RateLimiter {
    pub fn new(limit: RateLimit) -> Self {
        Self {
            state: Mutex::new(State {
                requests: limit.requests.map(Bucket::new),
                bytes: limit.bytes.map(Bucket::new),
                factor: 1.0,
                blocked_until: None,
                last: Instant::now(),
            }),
        }
    }

    pub fn unlimited() -> Self {
        Self::new(RateLimit::default())
    }

    /// Wait until one request of `bytes` may go, then take its tokens.
    pub async fn acquire(&self, bytes: u64) {
        loop {
            let wait = {
                let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(s.last);
                s.last = now;
                let factor = s.factor;
                if let Some(b) = s.requests.as_mut() {
                    b.refill(elapsed, factor);
                }
                if let Some(b) = s.bytes.as_mut() {
                    b.refill(elapsed, factor);
                }
                let blocked = s
                    .blocked_until
                    .map_or(Duration::ZERO, |u| u.saturating_duration_since(now));
                let req = s
                    .requests
                    .as_ref()
                    .map_or(Duration::ZERO, |b| b.wait_for(1.0, factor));
                let byt = s
                    .bytes
                    .as_ref()
                    .map_or(Duration::ZERO, |b| b.wait_for(bytes as f64, factor));
                let wait = blocked.max(req).max(byt);
                if wait.is_zero() {
                    if let Some(b) = s.requests.as_mut() {
                        b.take(1.0);
                    }
                    if let Some(b) = s.bytes.as_mut() {
                        b.take(bytes as f64);
                    }
                    return;
                }
                wait
            };
            tokio::time::sleep(wait).await;
        }
    }

    /// The system said `Throttled { after }`: nobody on this instance sends
    /// before `after` has passed, and the rate halves (down to 1/16 of the
    /// configured rate).
    pub fn throttled(&self, after: Duration) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let until = Instant::now() + after;
        s.blocked_until = Some(s.blocked_until.map_or(until, |u| u.max(until)));
        s.factor = (s.factor * THROTTLE_FACTOR).max(MIN_FACTOR);
    }

    /// A request succeeded: the rate creeps back towards the configured one.
    pub fn succeeded(&self) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        s.factor = (s.factor * RECOVERY).min(1.0);
    }

    /// The current fraction of the configured rate.
    pub fn factor(&self) -> f64 {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).factor
    }
}
