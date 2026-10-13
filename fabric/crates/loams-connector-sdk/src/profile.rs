//! The profile a connector instance runs with (Shared contracts, "Profiles").
//! `ConnectorContext` carries one, so the type lives in the SDK, which
//! `loams-flow` depends on, rather than in `loams-flow/src/profile.rs`; Task 7
//! adds `load_profiles` and `effective_capabilities` in `loams-flow` over this
//! type and gives `HttpProfile` its typed fields (plan, "Rulings made during
//! execution").

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    pub id: String,
    #[serde(rename = "profile_kind")]
    pub kind: ProfileKind,
    pub module: String,
    pub system: SystemRef,
    #[serde(default)]
    pub defaults: serde_json::Value,
    #[serde(default)]
    pub narrows: Vec<String>,
    #[serde(default)]
    pub http: Option<HttpProfile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProfileKind {
    Target,
    HttpApi,
}

/// The real system a profile's suite runs against (OR-2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemRef {
    pub name: String,
    /// `{ compose: … }`, `{ fixture: … }` or `{ nightly: [...] }`.
    pub service: serde_json::Value,
}

/// An HTTP-API profile's `http` block, kept as its YAML value until Task 7
/// types it (`base_url`, `auth`, `resources`, `rate_limit`, `webhook`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HttpProfile(pub serde_json::Value);
