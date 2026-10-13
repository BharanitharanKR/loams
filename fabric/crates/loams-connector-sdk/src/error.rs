//! Error classes (CN2 Shared contracts). Every error a connector returns
//! carries an [`ErrorClass`], and the pipeline acts on the class alone: it
//! backs off, sends to the dead-letter queue, pauses or stops.

use std::fmt;
use std::time::Duration;

/// A boxed underlying error (a driver's or a protocol crate's).
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// What the pipeline does with an error (CN2 Task 4, "Retries").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorClass {
    /// Transient: back off per `RetryPolicy`, honouring `after` when the
    /// system said how long to wait.
    Retryable { after: Option<Duration> },
    /// The system asked Loams to slow down: wait `after`, and the instance's
    /// rate limiter halves its rate.
    Throttled { after: Duration },
    /// This one event cannot be processed: it goes to the instance's DLQ with
    /// the error, and the pipeline continues.
    Poison,
    /// Credentials refused: the instance stops with `last_error`.
    Auth,
    /// The data no longer fits the target's schema: the instance pauses
    /// (Task 5 decides evolution).
    SchemaIncompatible,
    /// The upstream position no longer exists (a purged binlog, a trimmed
    /// shard, a dropped slot): the instance stops with a re-snapshot hint.
    PositionLost,
    /// Anything else: the instance stops with `last_error`.
    Fatal,
}

impl ErrorClass {
    /// A stable, lowercase name for logs, metrics and DLQ records.
    pub fn name(&self) -> &'static str {
        match self {
            ErrorClass::Retryable { .. } => "retryable",
            ErrorClass::Throttled { .. } => "throttled",
            ErrorClass::Poison => "poison",
            ErrorClass::Auth => "auth",
            ErrorClass::SchemaIncompatible => "schema_incompatible",
            ErrorClass::PositionLost => "position_lost",
            ErrorClass::Fatal => "fatal",
        }
    }

    /// Whether the pipeline retries the operation after a delay.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            ErrorClass::Retryable { .. } | ErrorClass::Throttled { .. }
        )
    }
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// An error from a connector (CN2 Shared contracts).
///
/// `message` never holds a secret (Global Constraints, "No secret"). The
/// underlying `source` may (a driver's error can echo a connection string), so
/// neither `Display` nor `Debug` renders it, and [`std::error::Error::source`]
/// does not expose it either: an error chain printed by a caller can therefore
/// never reach it. Code that needs the driver error reads the field directly.
pub struct ConnectorError {
    pub class: ErrorClass,
    pub message: String,
    pub source: Option<BoxError>,
}

impl ConnectorError {
    pub fn new(class: ErrorClass, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
            source: None,
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Retryable { after: None }, message)
    }

    pub fn retry_after(after: Duration, message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Retryable { after: Some(after) }, message)
    }

    pub fn throttled(after: Duration, message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Throttled { after }, message)
    }

    pub fn poison(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Poison, message)
    }

    pub fn auth(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Auth, message)
    }

    pub fn schema_incompatible(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::SchemaIncompatible, message)
    }

    pub fn position_lost(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::PositionLost, message)
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Fatal, message)
    }

    /// Attach the underlying error. It is kept for programmatic inspection and
    /// is never rendered (see the type's documentation).
    pub fn with_source(mut self, source: impl Into<BoxError>) -> Self {
        self.source = Some(source.into());
        self
    }
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.class, self.message)
    }
}

impl fmt::Debug for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorError")
            .field("class", &self.class)
            .field("message", &self.message)
            .field("source", &self.source.as_ref().map(|_| "<not rendered>"))
            .finish()
    }
}

impl std::error::Error for ConnectorError {}

/// An error from the Fabric as a connector sees it (`FabricWriter`,
/// `FabricReader`, the DLQ). It carries an [`ErrorClass`] so the pipeline
/// retries a busy `ingest` exactly as it retries a busy target.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("fabric {class}: {message}")]
pub struct FabricError {
    pub class: ErrorClass,
    pub message: String,
}

impl FabricError {
    pub fn new(class: ErrorClass, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Retryable { after: None }, message)
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Fatal, message)
    }
}

impl From<FabricError> for ConnectorError {
    fn from(e: FabricError) -> Self {
        ConnectorError::new(e.class, format!("fabric: {}", e.message))
    }
}

/// An error from a [`crate::CheckpointStore`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CheckpointError {
    /// A newer lease holder has saved: this writer is stale and must stop
    /// (Review Focus 12).
    #[error("checkpoint fenced: epoch {attempted} is older than the stored epoch {stored}")]
    Fenced { stored: u64, attempted: u64 },
    /// The store could not be reached; the save is retried.
    #[error("checkpoint store unavailable: {0}")]
    Unavailable(String),
    #[error("checkpoint store: {0}")]
    Other(String),
}

impl From<CheckpointError> for ConnectorError {
    fn from(e: CheckpointError) -> Self {
        let class = match e {
            CheckpointError::Unavailable(_) => ErrorClass::Retryable { after: None },
            _ => ErrorClass::Fatal,
        };
        ConnectorError::new(class, e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Leaky;
    impl fmt::Display for Leaky {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("postgres://admin:hunter2@db")
        }
    }
    impl std::error::Error for Leaky {}

    #[test]
    fn source_is_never_rendered() {
        let e = ConnectorError::auth("login refused").with_source(Leaky);
        assert!(!format!("{e}").contains("hunter2"));
        assert!(!format!("{e:?}").contains("hunter2"));
        assert!(std::error::Error::source(&e).is_none());
        assert!(e.source.is_some());
    }
}
