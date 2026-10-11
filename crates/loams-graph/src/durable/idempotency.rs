//! Idempotency keys (design §48 §7.3): `key → (token, outcome summary)` per graph, for 24 hours.
//!
//! The index is rebuilt from the graph's stream on open: every record carries its write's key,
//! and the log record's timestamp says when it was written (R10.4). A keyed write that changed
//! nothing is still appended (Task 11 controller ruling), so every acknowledged key is durable
//! and a retry after a restart answers the first run.

use std::collections::{HashMap, VecDeque};

/// How long a key answers its first run: 24 hours (§48 §7.3).
pub const IDEMPOTENCY_WINDOW_MS: u64 = 24 * 3600 * 1000;

/// The most keys one graph remembers. Past it the oldest is forgotten first, so under a write
/// rate above about 1 160 keyed writes a second the window is shorter than 24 hours.
pub const MAX_KEYS: usize = 100_000;

/// What a retried key answers: the first run's token and commit epoch. The first run's rows are
/// not kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outcome {
    /// The consistency token's offset of the first run (the offset after its record).
    pub token_offset: u64,
    /// The first run's commit epoch (informational, R9.5).
    pub commit_epoch: u64,
}

/// One graph's idempotency index.
#[derive(Debug, Default)]
pub struct Idempotency {
    keys: HashMap<String, (u64, Outcome)>,
    /// Keys by the time they were recorded, oldest first.
    order: VecDeque<(u64, String)>,
}

impl Idempotency {
    /// The first run of `key`, if it was recorded within the window before `now_ms`.
    #[must_use]
    pub fn get(&self, key: &str, now_ms: u64) -> Option<Outcome> {
        self.keys
            .get(key)
            .filter(|(at, _)| now_ms.saturating_sub(*at) < IDEMPOTENCY_WINDOW_MS)
            .map(|(_, outcome)| *outcome)
    }

    /// Records `key`'s first run at `recorded_ms`, and forgets keys older than the window or
    /// past [`MAX_KEYS`]. A key already recorded keeps its first run.
    pub fn insert(&mut self, key: &str, outcome: Outcome, recorded_ms: u64, now_ms: u64) {
        if now_ms.saturating_sub(recorded_ms) >= IDEMPOTENCY_WINDOW_MS {
            return;
        }
        if self.get(key, now_ms).is_none() {
            self.keys.insert(key.to_string(), (recorded_ms, outcome));
            self.order.push_back((recorded_ms, key.to_string()));
        }
        self.prune(now_ms);
    }

    /// How many keys are remembered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether no key is remembered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn prune(&mut self, now_ms: u64) {
        while let Some((at, key)) = self.order.front() {
            let expired = now_ms.saturating_sub(*at) >= IDEMPOTENCY_WINDOW_MS;
            if !expired && self.order.len() <= MAX_KEYS {
                break;
            }
            // Only the entry this queue position recorded: a key recorded again later (after
            // it expired) has its own, newer position.
            if self
                .keys
                .get(key)
                .is_some_and(|(recorded, _)| recorded == at)
            {
                self.keys.remove(key);
            }
            self.order.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OUT: Outcome = Outcome {
        token_offset: 7,
        commit_epoch: 3,
    };

    #[test]
    fn a_key_answers_its_first_run_within_the_window() {
        let mut index = Idempotency::default();
        index.insert("k", OUT, 1_000, 1_000);
        index.insert(
            "k",
            Outcome {
                token_offset: 9,
                commit_epoch: 4,
            },
            2_000,
            2_000,
        );
        assert_eq!(index.get("k", 2_000), Some(OUT));
        assert_eq!(index.get("k", 1_000 + IDEMPOTENCY_WINDOW_MS), None);
    }

    #[test]
    fn expired_keys_are_forgotten_and_old_records_are_not_kept() {
        let mut index = Idempotency::default();
        index.insert("old", OUT, 0, 0);
        index.insert("new", OUT, IDEMPOTENCY_WINDOW_MS, IDEMPOTENCY_WINDOW_MS);
        assert_eq!(index.len(), 1);
        index.insert("ancient", OUT, 0, IDEMPOTENCY_WINDOW_MS);
        assert_eq!(index.get("ancient", IDEMPOTENCY_WINDOW_MS), None);
    }

    #[test]
    fn the_index_is_bounded() {
        let mut index = Idempotency::default();
        for i in 0..(MAX_KEYS + 10) {
            index.insert(&format!("k{i}"), OUT, 5, 5);
        }
        assert_eq!(index.len(), MAX_KEYS);
        assert_eq!(index.get("k0", 5), None);
        assert_eq!(index.get(&format!("k{}", MAX_KEYS + 9), 5), Some(OUT));
    }
}
