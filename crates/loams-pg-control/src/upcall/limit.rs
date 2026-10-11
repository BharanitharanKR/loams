//! Per-peer token buckets for the upcall listener (PG2 Task 8): a bounded
//! map from the peer's IP to its bucket.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Instant;

/// A sustained rate and a burst.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rate {
    /// Tokens added per second.
    pub per_second: f64,
    /// The bucket's size: how many requests may come at once.
    pub burst: u32,
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Buckets of [`Rate`] per peer, at most `max_peers` of them. When full, the
/// peer seen longest ago is forgotten (its bucket starts full again).
#[derive(Debug)]
pub(crate) struct PeerLimiter {
    rate: Rate,
    max_peers: usize,
    peers: Mutex<HashMap<IpAddr, Bucket>>,
}

impl PeerLimiter {
    pub(crate) fn new(rate: Rate, max_peers: usize) -> Self {
        PeerLimiter {
            rate,
            max_peers: max_peers.max(1),
            peers: Mutex::new(HashMap::new()),
        }
    }

    /// Takes one token of `peer`'s bucket at `now`: `false` when it is
    /// empty.
    pub(crate) fn take(&self, peer: IpAddr, now: Instant) -> bool {
        let mut peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        let burst = f64::from(self.rate.burst);
        if !peers.contains_key(&peer)
            && peers.len() >= self.max_peers
            && let Some(oldest) = peers.iter().min_by_key(|(_, b)| b.last).map(|(ip, _)| *ip)
        {
            peers.remove(&oldest);
        }
        let bucket = peers.entry(peer).or_insert(Bucket {
            tokens: burst,
            last: now,
        });
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate.per_second).min(burst);
        bucket.last = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_bucket_empties_and_refills() {
        let l = PeerLimiter::new(
            Rate {
                per_second: 2.0,
                burst: 2,
            },
            8,
        );
        let ip: IpAddr = "10.0.0.1".parse().expect("an ip");
        let t0 = Instant::now();
        assert!(l.take(ip, t0));
        assert!(l.take(ip, t0));
        assert!(!l.take(ip, t0));
        assert!(l.take(ip, t0 + Duration::from_millis(500)));
        assert!(!l.take(ip, t0 + Duration::from_millis(500)));
        // Another peer has its own bucket.
        assert!(l.take("10.0.0.2".parse().expect("an ip"), t0));
    }

    #[test]
    fn the_map_is_bounded() {
        let l = PeerLimiter::new(
            Rate {
                per_second: 0.0,
                burst: 1,
            },
            2,
        );
        let t0 = Instant::now();
        let a: IpAddr = "10.0.0.1".parse().expect("an ip");
        assert!(l.take(a, t0));
        for i in 2..10u8 {
            assert!(l.take(
                IpAddr::from([10, 0, 0, i]),
                t0 + Duration::from_millis(u64::from(i))
            ));
        }
        assert!(l.peers.lock().expect("lock").len() <= 2);
    }
}
