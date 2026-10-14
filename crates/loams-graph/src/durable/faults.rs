//! Kill points: the places on the write path and the checkpoint where the durability fault gate
//! stops a node (GR1 Task 16; design §48 §6.2's eight steps, the checkpoint upload, the manifest
//! CAS and the log trim; Review Focus 1).
//!
//! A test sets a [`KillHook`] on one node's [`GraphLog`](super::GraphLog) (feature `test-hooks`),
//! so only that node stops: the `fail` crate's failpoints are process-wide, and the fault gate
//! runs several nodes in one process. The hook is awaited at each point the node reaches. A hook
//! that never ends there is the node dying at that point: nothing after it runs, and the test
//! moves the graph to another node. Without `test-hooks` every point compiles to nothing.

/// Where the write path or a checkpoint can be stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KillPoint {
    /// Step 1: a write transaction's session is open, before it begins.
    Begin,
    /// Step 2: its statements ran, before it commits.
    Executed,
    /// Steps 3–4: Grafeo committed it (visible in the engine, not yet to clients), before its
    /// change set is captured.
    Committed,
    /// Step 5: its change set is captured, before it joins the group.
    Captured,
    /// Step 6: the group's records are encoded, before they are appended.
    Enqueued,
    /// Step 7: the log acknowledged the group's append, before the durable position moves.
    Appended,
    /// Step 8: the durable position moved, before the writers are answered.
    Published,
    /// A checkpoint took its snapshots, before it uploads them.
    CheckpointUpload,
    /// A checkpoint uploaded its snapshots, before it commits the manifest.
    ManifestCas,
    /// A checkpoint committed the manifest, before it trims the stream.
    LogTrim,
}

impl KillPoint {
    /// Every point, in the order a write and then a checkpoint reach them.
    pub const ALL: [Self; 10] = [
        Self::Begin,
        Self::Executed,
        Self::Committed,
        Self::Captured,
        Self::Enqueued,
        Self::Appended,
        Self::Published,
        Self::CheckpointUpload,
        Self::ManifestCas,
        Self::LogTrim,
    ];

    /// Whether a checkpoint reaches this point (a write reaches the others).
    #[must_use]
    pub fn in_checkpoint(self) -> bool {
        matches!(
            self,
            Self::CheckpointUpload | Self::ManifestCas | Self::LogTrim
        )
    }
}

/// **Tests only** (feature `test-hooks`): awaited at every [`KillPoint`] a node reaches.
#[cfg(feature = "test-hooks")]
pub type KillHook = std::sync::Arc<
    dyn Fn(KillPoint) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;
