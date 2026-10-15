//! Live's keyspace (S5): scan its keys and change them in one transaction.
//! A migration writes new keys and the key that switches readers to them in
//! the same transaction; until it commits, readers see only the old keys.

use std::future::Future;
use std::pin::Pin;

use loams_kv::{Pair, Store, Txn, TxnError, TxnOptions};

use crate::migration::MigrationError;

/// See the module documentation.
#[derive(Clone, Copy, Debug)]
pub struct LiveCx<'a> {
    store: &'a Store,
}

fn kv_error(e: impl std::fmt::Display) -> MigrationError {
    MigrationError::new("live", e.to_string())
}

impl<'a> LiveCx<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        LiveCx { store }
    }

    pub fn store(&self) -> &Store {
        self.store
    }

    /// Up to `limit` pairs with keys in `start..end`, from one snapshot.
    pub async fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Pair>, MigrationError> {
        let at = self.store.now().await.map_err(kv_error)?;
        let mut snap = self.store.snapshot(at).await.map_err(kv_error)?;
        snap.scan(start, end, limit).await.map_err(kv_error)
    }

    /// Runs `body` in one transaction (with the store's retries): all of its
    /// writes commit together, or none do.
    pub async fn txn<F>(&self, op: &'static str, mut body: F) -> Result<(), MigrationError>
    where
        F: for<'t> FnMut(
                &'t mut Txn,
            )
                -> Pin<Box<dyn Future<Output = Result<(), TxnError>> + Send + 't>>
            + Send,
    {
        self.store
            .run(TxnOptions::new(op), move |txn| body(txn))
            .await
            .map(|_| ())
            .map_err(kv_error)
    }
}
