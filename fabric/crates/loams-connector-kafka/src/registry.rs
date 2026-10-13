//! A Confluent Schema Registry client behind the SDK's [`SchemaRegistry`]
//! trait (R5.4: "the Kafka family writes the HTTP client for a real registry
//! behind the same trait"). It speaks the registry's REST API over
//! `sdk::http::client`, so every request is egress-checked:
//!
//! * `by_id` → `GET /schemas/ids/{id}`, cached (a schema id never changes);
//! * `register` → `POST /subjects/{subject}/versions`, which returns the
//!   existing id when the schema is already registered.
//!
//! Basic credentials (Confluent Cloud's API key and secret) are optional; the
//! secret is resolved by the caller and held as a [`Secret`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use http::{Method, Request, StatusCode};
use loams_connector_sdk::http::{HttpClient, classify_status, client};
use loams_connector_sdk::schema::registry::{RegisteredSchema, SchemaFormat, SchemaRegistry};
use loams_connector_sdk::{ConnectorError, EgressCheck, ErrorClass, Secret};
use serde::{Deserialize, Serialize};

const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

/// A Confluent-compatible registry (Confluent, Redpanda's, Apicurio's
/// compatibility API).
pub struct ConfluentRegistry {
    base: String,
    http: HttpClient,
    auth: Option<(String, Secret)>,
    cache: Mutex<HashMap<u32, RegisteredSchema>>,
}

impl std::fmt::Debug for ConfluentRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfluentRegistry")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Deserialize)]
struct SchemaById {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
}

#[derive(Debug, Serialize)]
struct RegisterBody<'a> {
    schema: &'a str,
    #[serde(rename = "schemaType", skip_serializing_if = "Option::is_none")]
    schema_type: Option<&'static str>,
}

#[derive(Debug, Deserialize)]
struct Registered {
    id: u32,
}

fn format_of(t: Option<&str>) -> Result<SchemaFormat, ConnectorError> {
    Ok(match t.unwrap_or("AVRO") {
        "AVRO" => SchemaFormat::Avro,
        "PROTOBUF" => SchemaFormat::Protobuf,
        "JSON" => SchemaFormat::Json,
        other => {
            return Err(ConnectorError::fatal(format!(
                "the registry returned an unknown schemaType {other}"
            )));
        }
    })
}

fn type_name(f: SchemaFormat) -> Option<&'static str> {
    match f {
        // AVRO is the registry's default and is not sent.
        SchemaFormat::Avro => None,
        SchemaFormat::Protobuf => Some("PROTOBUF"),
        SchemaFormat::Json => Some("JSON"),
    }
}

impl ConfluentRegistry {
    /// A client for the registry at `url` (`http(s)://host[:port][/prefix]`).
    pub fn new(url: &str, egress: Arc<dyn EgressCheck>) -> Self {
        Self {
            base: url.trim_end_matches('/').to_owned(),
            http: client(egress),
            auth: None,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// HTTP Basic credentials (an API key and its secret).
    pub fn with_basic_auth(mut self, username: impl Into<String>, password: Secret) -> Self {
        self.auth = Some((username.into(), password));
        self
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<(StatusCode, Bytes), ConnectorError> {
        let mut req = Request::builder()
            .method(method)
            .uri(format!("{}{path}", self.base))
            .header(http::header::ACCEPT, CONTENT_TYPE);
        if body.is_some() {
            req = req.header(http::header::CONTENT_TYPE, CONTENT_TYPE);
        }
        if let Some((user, secret)) = &self.auth {
            let mut raw = user.as_bytes().to_vec();
            raw.push(b':');
            raw.extend_from_slice(secret.expose());
            let mut v = http::HeaderValue::from_str(&format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(&raw)
            ))
            .map_err(|e| ConnectorError::fatal(format!("registry credentials: {e}")))?;
            v.set_sensitive(true);
            req = req.header(http::header::AUTHORIZATION, v);
        }
        let req = req
            .body(Bytes::from(body.unwrap_or_default()))
            .map_err(|e| ConnectorError::fatal(format!("registry request: {e}")))?;
        let resp = self.http.send(req).await?;
        Ok((resp.status(), resp.body().clone()))
    }
}

fn error_of(what: &str, status: StatusCode, body: &Bytes) -> ConnectorError {
    // The registry's error body is `{error_code, message}`; the message names
    // the schema problem and never a credential.
    let detail = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("message").and_then(|m| m.as_str()).map(str::to_owned))
        .unwrap_or_default();
    let class = match status.as_u16() {
        // 409: incompatible with the subject's compatibility rule.
        409 => ErrorClass::SchemaIncompatible,
        // 422: an invalid schema.
        422 => ErrorClass::Fatal,
        _ => classify_status(status, None).unwrap_or(ErrorClass::Fatal),
    };
    ConnectorError::new(
        class,
        format!("schema registry {what}: HTTP {status} {detail}"),
    )
}

#[async_trait]
impl SchemaRegistry for ConfluentRegistry {
    async fn by_id(&self, id: u32) -> Result<RegisteredSchema, ConnectorError> {
        if let Some(s) = self
            .cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&id)
        {
            return Ok(s.clone());
        }
        let (status, body) = self
            .call(Method::GET, &format!("/schemas/ids/{id}"), None)
            .await?;
        if status == StatusCode::NOT_FOUND {
            // A record that names a schema the registry does not have.
            return Err(ConnectorError::poison(format!(
                "schema registry: no schema with id {id}"
            )));
        }
        if !status.is_success() {
            return Err(error_of(&format!("schema {id}"), status, &body));
        }
        let parsed: SchemaById = serde_json::from_slice(&body)
            .map_err(|e| ConnectorError::fatal(format!("schema registry: schema {id}: {e}")))?;
        let schema = RegisteredSchema {
            id,
            format: format_of(parsed.schema_type.as_deref())?,
            schema: Bytes::from(parsed.schema),
        };
        self.cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, schema.clone());
        Ok(schema)
    }

    async fn register(
        &self,
        subject: &str,
        format: SchemaFormat,
        schema: Bytes,
    ) -> Result<u32, ConnectorError> {
        if subject.is_empty()
            || !subject
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(ConnectorError::fatal(format!(
                "schema registry: subject {subject:?} is not a topic-name subject"
            )));
        }
        let text = std::str::from_utf8(&schema)
            .map_err(|_| ConnectorError::fatal("schema registry: a schema must be UTF-8 text"))?;
        let body = serde_json::to_vec(&RegisterBody {
            schema: text,
            schema_type: type_name(format),
        })
        .map_err(|e| ConnectorError::fatal(format!("schema registry: {e}")))?;
        let (status, resp) = self
            .call(
                Method::POST,
                &format!("/subjects/{subject}/versions"),
                Some(body),
            )
            .await?;
        if !status.is_success() {
            return Err(error_of(&format!("subject {subject}"), status, &resp));
        }
        let r: Registered = serde_json::from_slice(&resp)
            .map_err(|e| ConnectorError::fatal(format!("schema registry: {e}")))?;
        self.cache.lock().unwrap_or_else(|p| p.into_inner()).insert(
            r.id,
            RegisteredSchema {
                id: r.id,
                format,
                schema,
            },
        );
        Ok(r.id)
    }
}
