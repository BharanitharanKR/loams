//! Delivery modes (OR-7) and the dead-letter queue.

use async_trait::async_trait;

use crate::envelope::CloudEvent;
use crate::error::{ErrorClass, FabricError};
use crate::position::{InstanceKey, SplitId};

/// The delivery a direction declares in its manifest (`capabilities.delivery`).
///
/// - `AtLeastOnce` is the floor for every native connector.
/// - `EffectivelyOnce`: at-least-once into a target that deduplicates by
///   `ce_id` or a natural key (or, for a source, stable ids into `ingest`).
/// - `ExactlyOnce`: only through a [`crate::TransactionalSink`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Delivery {
    AtMostOnce,
    AtLeastOnce,
    EffectivelyOnce,
    ExactlyOnce,
}

impl Delivery {
    /// The manifest slug.
    pub fn as_str(self) -> &'static str {
        match self {
            Delivery::AtMostOnce => "at_most_once",
            Delivery::AtLeastOnce => "at_least_once",
            Delivery::EffectivelyOnce => "effectively_once",
            Delivery::ExactlyOnce => "exactly_once",
        }
    }

    pub fn from_slug(s: &str) -> Option<Self> {
        Some(match s {
            "at_most_once" => Delivery::AtMostOnce,
            "at_least_once" => Delivery::AtLeastOnce,
            "effectively_once" => Delivery::EffectivelyOnce,
            "exactly_once" => Delivery::ExactlyOnce,
            _ => return None,
        })
    }
}

/// One record of the instance's DLQ topic: the poison event (when there is
/// one: a source that cannot decode a record has no event to send) and the
/// error, scrubbed of secrets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    pub instance: InstanceKey,
    pub split: Option<SplitId>,
    pub event: Option<CloudEvent>,
    pub class: ErrorClass,
    pub error: String,
}

/// The instance's DLQ topic.
#[async_trait]
pub trait DeadLetterQueue: Send + Sync {
    async fn send(&self, dead: DeadLetter) -> Result<(), FabricError>;
}
