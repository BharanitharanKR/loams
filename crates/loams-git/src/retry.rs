//! Retries of transient store errors (plan GT1 Task 6: three retries,
//! 50, 200 and 800 ms, jittered).
//!
//! Only [`StoreError::Backend`] is transient here: a not-found, an
//! already-exists or a failed precondition is an answer the caller acts on,
//! not a failure to retry blindly.

use std::future::Future;
use std::time::Duration;

use loams_store::StoreError;
use rand::Rng;

/// The pauses before each retry; their count is the number of retries.
pub(crate) const BACKOFF: [Duration; 3] = [
    Duration::from_millis(50),
    Duration::from_millis(200),
    Duration::from_millis(800),
];

/// Whether `err` may go away if the same request is sent again.
pub(crate) fn is_transient(err: &StoreError) -> bool {
    matches!(err, StoreError::Backend(_))
}

/// Sleeps for the `attempt`th pause (0-based, the last one repeated), drawn
/// uniformly from half to one and a half times its nominal length.
pub(crate) async fn pause(attempt: usize) {
    let nominal = BACKOFF[attempt.min(BACKOFF.len() - 1)];
    let jittered = nominal.mul_f64(rand::rng().random_range(0.5..1.5));
    tokio::time::sleep(jittered).await;
}

/// Runs `op` and retries it after a transient error, up to [`BACKOFF`]'s
/// length times. The last error is returned.
pub(crate) async fn retry<T, F, Fut>(mut op: F) -> Result<T, StoreError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StoreError>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Err(err) if is_transient(&err) && attempt < BACKOFF.len() => {
                tracing::debug!(error = %err, attempt, "retrying a transient store error");
                pause(attempt).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}
