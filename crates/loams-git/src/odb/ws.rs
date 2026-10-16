//! A workspace's view of the objects (§36 §19.4.1, §19.5.1; plan GT1
//! Task 17): the repository's [`RangeOdb`] with the workspace's packs
//! (`ws/<ws_id>/packs/<checksum>.lpk`) searched after it.
//!
//! Workspace packs only add objects. An object the repository holds is
//! always found in the repository's packs (the workspace's are searched
//! last), so a workspace pack can never change what a repository object
//! reads as.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use gix_object::Kind;

use super::range::{EntryHeader, Layer, Location, OdbStats, RangeOdb};
use super::{ObjectDb, OdbError};
use crate::blob::BlobStore;
use crate::format::pb;
use crate::ids::ObjectId;

/// The objects a workspace reads: its repository's, then its own packs
/// (plan GT1 Task 17).
#[derive(Debug)]
pub struct WsOdb {
    odb: RangeOdb,
}

impl WsOdb {
    /// `repo_odb` (the repository at the workspace's base seq) with
    /// `ws_packs` (oldest first, as a snapshot lists them), stored in
    /// `ws_blobs` (`StoreBlobStore::workspace`), searched last. Nothing is
    /// read until the first lookup.
    pub fn open(
        repo_odb: &RangeOdb,
        ws_blobs: Arc<dyn BlobStore>,
        ws_packs: &[pb::PackRef],
    ) -> Result<Self, OdbError> {
        let layer = Layer::new(ws_blobs, ws_packs)?;
        Ok(Self {
            odb: repo_odb.with_top_layer(layer),
        })
    }

    /// The combined database; its last layer is the workspace's.
    pub fn odb(&self) -> &RangeOdb {
        &self.odb
    }

    /// The combined database, owned.
    pub fn into_odb(self) -> RangeOdb {
        self.odb
    }

    /// Whether `loc` is in one of the workspace's own packs.
    pub fn is_workspace(&self, loc: &Location) -> bool {
        loc.layer + 1 == self.odb.layers()
    }

    /// See [`RangeOdb::locate`].
    pub async fn locate(&self, oid: &ObjectId) -> Result<Option<Location>, OdbError> {
        self.odb.locate(oid).await
    }

    /// See [`RangeOdb::header`].
    pub async fn header(&self, loc: &Location) -> Result<EntryHeader, OdbError> {
        self.odb.header(loc).await
    }

    /// See [`RangeOdb::raw_entry`].
    pub async fn raw_entry(&self, loc: &Location) -> Result<Bytes, OdbError> {
        self.odb.raw_entry(loc).await
    }

    /// See [`RangeOdb::stats`].
    pub fn stats(&self) -> OdbStats {
        self.odb.stats()
    }
}

#[async_trait]
impl ObjectDb for WsOdb {
    async fn contains(&self, oid: &ObjectId) -> Result<bool, OdbError> {
        self.odb.contains(oid).await
    }

    async fn read(&self, oid: &ObjectId) -> Result<(Kind, Bytes), OdbError> {
        self.odb.read(oid).await
    }

    async fn missing_from_closure(
        &self,
        tips: &[ObjectId],
        known: &[ObjectId],
        extra: Option<&dyn ObjectDb>,
    ) -> Result<Vec<ObjectId>, OdbError> {
        self.odb.missing_from_closure(tips, known, extra).await
    }
}
