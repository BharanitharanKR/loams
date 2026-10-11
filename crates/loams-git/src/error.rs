//! Errors shared across the crate's modules.

/// A string that is not a valid identifier of its kind. The message names
/// the kind, the value and the rule it breaks.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid {kind} {value:?}: {reason}")]
pub struct IdError {
    /// What was being parsed ("repository name", "ref name", …).
    pub kind: &'static str,
    /// The refused value.
    pub value: String,
    /// The rule it breaks.
    pub reason: &'static str,
}

impl IdError {
    pub(crate) fn new(kind: &'static str, value: &str, reason: &'static str) -> Self {
        Self {
            kind,
            value: value.to_string(),
            reason,
        }
    }
}
