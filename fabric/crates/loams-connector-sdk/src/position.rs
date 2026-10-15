//! Positions, split ids and instance keys (CN2 Shared contracts).

use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Where a reader is in a split or in the Fabric. `opaque` is the connector's
/// own encoding (a Kafka offset, a PG LSN, a GTID set, a continuation token);
/// `display` is for humans and **never holds a secret** (Review Focus 8). The
/// pipeline additionally scrubs `display` against the instance's resolved
/// secrets before it stores or reports it.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Position {
    pub opaque: Bytes,
    pub display: String,
}

impl Position {
    pub fn new(opaque: impl Into<Bytes>, display: impl Into<String>) -> Self {
        Self {
            opaque: opaque.into(),
            display: display.into(),
        }
    }

    /// A position that is a single monotone counter (an offset, a sequence
    /// number), encoded big-endian.
    pub fn from_u64(n: u64) -> Self {
        Self::new(Bytes::copy_from_slice(&n.to_be_bytes()), n.to_string())
    }

    /// The counter of a position made by [`Position::from_u64`].
    pub fn as_u64(&self) -> Option<u64> {
        let b: [u8; 8] = self.opaque.as_ref().try_into().ok()?;
        Some(u64::from_be_bytes(b))
    }
}

/// `Debug` shows `display` only: the opaque bytes are the connector's and are
/// not meant for logs.
impl fmt::Debug for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Position({})", self.display)
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

/// A split's id: stable across discoveries, so its checkpoint is found again
/// (`"orders/3"` for a Kafka partition, `"shardId-000000000001"`, a slot name).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SplitId(pub String);

impl SplitId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SplitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for SplitId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// A connector instance: its namespace and its name (CN1 Task 3's
/// `Instance { namespace, name }`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InstanceKey {
    pub namespace: u64,
    pub name: String,
}

impl InstanceKey {
    pub fn new(namespace: u64, name: impl Into<String>) -> Self {
        Self {
            namespace,
            name: name.into(),
        }
    }
}

impl fmt::Display for InstanceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}
