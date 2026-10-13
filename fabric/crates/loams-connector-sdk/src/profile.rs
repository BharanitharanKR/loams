//! The profile a connector instance runs with (Shared contracts, "Profiles").
//! `ConnectorContext` carries one, so the type lives in the SDK, which
//! `loams-flow` depends on, rather than in `loams-flow/src/profile.rs` (R4.4).
//! `loams-flow`'s `profile` module loads and validates profiles
//! (`load_profiles`, `effective_capabilities`) against
//! `connectors/schema/profile.schema.json`; `loams-connector-http`'s profile
//! engine runs an HTTP-API profile (CN2 Task 7).
//!
//! A profile never holds a secret (Review Focus 8): credentials are named by
//! the instance config (`auth.token`, `auth.client_secret`, …) and resolved
//! through the instance's `SecretResolver`. The `auth` block only says *how*
//! to authenticate.

use std::collections::BTreeMap;

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

impl Profile {
    /// The instance config with the profile's `defaults` merged *under* it:
    /// objects merge key by key, and anything the user set wins.
    pub fn config_with_defaults(&self, config: &serde_json::Value) -> serde_json::Value {
        let mut out = self.defaults.clone();
        if out.is_null() {
            return config.clone();
        }
        merge_over(&mut out, config);
        out
    }
}

fn merge_over(base: &mut serde_json::Value, over: &serde_json::Value) {
    match (base, over) {
        (serde_json::Value::Object(b), serde_json::Value::Object(o)) => {
            for (k, v) in o {
                match b.get_mut(k) {
                    Some(slot) => merge_over(slot, v),
                    None => {
                        b.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        (_, serde_json::Value::Null) => {}
        (b, o) => *b = o.clone(),
    }
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

/// An HTTP-API profile's `http` block (OR-11): a REST API described
/// declaratively and run by `loams-connector-http`'s profile engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HttpProfile {
    /// Scheme, host and optional path prefix every resource path is joined to.
    pub base_url: String,
    pub auth: HttpAuth,
    /// Fixed, non-secret request headers (an API version, `Accept`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// The API's resources by name; an instance picks one with `resource`.
    #[serde(default)]
    pub resources: BTreeMap<String, HttpResource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<HttpRateLimit>,
    /// Webhooks the API sends, handled by the webhooks source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<WebhookSpec>,
}

/// How requests authenticate. The credentials themselves are secret
/// references in the instance config, never here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HttpAuth {
    pub scheme: AuthScheme,
    /// The header that carries the credential (`bearer` and `header`;
    /// default `Authorization` for `bearer`, required for `header`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    /// A literal prefix before the credential in `header` (`"Token "`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// `oauth2`: the token endpoint (client credentials or refresh token).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url: Option<String>,
    /// `oauth2`: the scopes asked for.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    /// `aws-sigv4`: the signing service name (`execute-api`, `ses`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// `aws-sigv4`: the default region; an instance's `auth.region` wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthScheme {
    Oauth2,
    Bearer,
    Basic,
    Header,
    AwsSigv4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Delete => "DELETE",
        }
    }
}

/// One resource: what can be read from it (`list`, `async_job`) and written
/// to it (`upsert`, `create`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HttpResource {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list: Option<ListOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upsert: Option<WriteOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<WriteOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_job: Option<AsyncJob>,
}

/// A paginated read: a polling source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListOp {
    pub method: HttpMethod,
    /// Joined to `base_url`; may carry a query string.
    pub path: String,
    /// JSON pointer to the item array in the response; absent when the
    /// response body is the array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items: Option<String>,
    /// Cursor pagination; without it the `Link: <…>; rel="next"` header is
    /// followed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<CursorSpec>,
    /// JSON pointer to an item's key, which becomes the event's `subject`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Fixed query parameters (a page size).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub query: BTreeMap<String, String>,
}

/// Where the next page's cursor is and how it is sent back.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CursorSpec {
    /// JSON pointer to the next cursor in the response; absent or null means
    /// the last page.
    pub next: String,
    /// The query parameter the cursor goes back in.
    pub param: String,
}

/// A write: a sink.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteOp {
    pub method: HttpMethod,
    pub path: String,
    /// Items per request; absent means one item per request, sent as the
    /// body itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<usize>,
    /// JSON pointer to the item's natural key; the API upserts by it, so a
    /// retried request writes nothing twice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// JSON pointer, in the request body, where a batch's item array goes
    /// (`/inputs`); absent means the body is the array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// The API accepts `Idempotency-Key`; the engine sends the event id (or a
    /// digest of a batch's ids) in it.
    #[serde(default)]
    pub idempotency_key: bool,
}

/// A request with an optional fixed JSON body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestSpec {
    pub method: HttpMethod,
    /// May contain `{id}`, replaced by the job id.
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
}

/// A Bulk-style API: create a job, poll it until it is done, then page
/// through its results (Salesforce Bulk 2.0, Redshift-style statements).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AsyncJob {
    pub create: RequestSpec,
    /// JSON pointer to the job id in the create response.
    pub id: String,
    pub poll: JobPoll,
    /// The results listing; its `path` may contain `{id}`.
    pub results: ListOp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobPoll {
    pub method: HttpMethod,
    pub path: String,
    /// JSON pointer to the job's state in the poll response.
    pub state: String,
    /// States meaning the results are ready.
    pub done: Vec<String>,
    /// States meaning the job failed.
    #[serde(default)]
    pub failed: Vec<String>,
    /// Milliseconds between polls (default 1000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_ms: Option<u64>,
}

/// The API's published limit and the response headers that report it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HttpRateLimit {
    pub requests: u32,
    pub per_seconds: u32,
    /// `Retry-After`, and `*-Remaining` / `*-Reset` style headers.
    #[serde(default)]
    pub honour_headers: Vec<String>,
}

/// A webhook the API sends: the signature scheme and the header carrying it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookSpec {
    pub scheme: String,
    pub header: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_merge_under_config() {
        let p = Profile {
            id: "x".into(),
            kind: ProfileKind::Target,
            module: "kafka".into(),
            system: SystemRef {
                name: "X".into(),
                service: json!({"compose": "x"}),
            },
            defaults: json!({"a": 1, "nested": {"b": 2, "c": 3}}),
            narrows: vec![],
            http: None,
        };
        let merged = p.config_with_defaults(&json!({"nested": {"c": 4}, "d": 5}));
        assert_eq!(merged, json!({"a": 1, "nested": {"b": 2, "c": 4}, "d": 5}));
    }

    #[test]
    fn http_profile_parses_the_contract_example() {
        let http: HttpProfile = serde_json::from_value(json!({
            "base_url": "https://api.hubspot.com",
            "auth": {"scheme": "oauth2", "header": "Authorization", "token_url": "https://api.hubapi.com/oauth/v1/token"},
            "resources": {"contacts": {
                "list": {"method": "GET", "path": "/crm/v3/objects/contacts", "items": "/results",
                         "cursor": {"next": "/paging/next/after", "param": "after"}},
                "upsert": {"method": "POST", "path": "/crm/v3/objects/contacts/batch/upsert", "batch": 100, "key": "/id"},
                "async_job": null
            }},
            "rate_limit": {"requests": 100, "per_seconds": 10, "honour_headers": ["Retry-After"]},
            "webhook": {"scheme": "hmac-sha256", "header": "X-HubSpot-Signature-v3"}
        }))
        .expect("parses");
        assert_eq!(http.auth.scheme, AuthScheme::Oauth2);
        let contacts = &http.resources["contacts"];
        assert_eq!(contacts.upsert.as_ref().and_then(|u| u.batch), Some(100));
        assert!(contacts.async_job.is_none());
    }
}
