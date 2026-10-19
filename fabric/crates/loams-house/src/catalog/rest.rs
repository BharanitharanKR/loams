//! [`RestCatalog`]: an Iceberg REST catalog (Lakekeeper in production, §49 §5.3).
//!
//! A small client of the REST protocol's JSON (the Iceberg REST OpenAPI
//! specification), not iceberg-catalog-rest: that crate's `update_table` takes a
//! `TableCommit` only iceberg's own transactions can build, and the House commits
//! its own requirements and updates (DDL here, appends and compaction in HS1 Tasks
//! 12–13). The spec types (schemas, specs, sort orders, metadata, requirements,
//! updates) are iceberg-rust's, through `loams-house-lake`, and serialise to the
//! protocol's shapes.
//!
//! The catalog writes metadata files itself; the House sends it locations under
//! the House's store (`s3://<bucket>/<prefix>/ns/<id>/house/…`), so the warehouse's
//! storage profile must cover that bucket and prefix. Namespaces nest; a nested
//! namespace's levels are joined by U+001F in a path, as the protocol says.
//! `https` catalogs arrive with TLS (HS1 Task 20).

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use loams_house_lake::spec::TableMetadata;
use loams_house_lake::{NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate};
use reqwest::{Method, StatusCode, Url};
use serde_json::{Value, json};

use super::{CatalogError, HouseCatalog, LakeTable, namespace_text, table_text};

/// How long one catalog request may take.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How to reach a REST catalog.
#[derive(Clone)]
pub struct RestOptions {
    /// The catalog's base URL (`http://lakekeeper:8181/catalog`).
    pub uri: String,
    /// The warehouse name (`GET /v1/config?warehouse=`).
    pub warehouse: String,
    /// A bearer token, when the catalog wants one. Never printed.
    pub token: Option<String>,
}

impl fmt::Debug for RestOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestOptions")
            .field("uri", &self.uri)
            .field("warehouse", &self.warehouse)
            .field("token", &self.token.as_ref().map(|_| "[redacted]"))
            .finish()
    }
}

/// An Iceberg REST catalog.
pub struct RestCatalog {
    client: reqwest::Client,
    /// `<uri>/v1/<prefix>`, as path segments.
    base: Url,
    token: Option<String>,
}

impl fmt::Debug for RestCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestCatalog")
            .field("base", &self.base.as_str())
            .field("token", &self.token.as_ref().map(|_| "[redacted]"))
            .finish()
    }
}

fn unreachable_catalog(err: reqwest::Error) -> CatalogError {
    // reqwest's text names the URL, which is the operator's, not a secret.
    CatalogError::Unavailable(format!("the catalog could not be reached: {err}"))
}

/// The error a failed response carries: the protocol's `{"error": {"type",
/// "message"}}`, or the status alone.
fn error_of(status: StatusCode, body: &[u8]) -> (String, String) {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let error = parsed.as_ref().and_then(|v| v.get("error"));
    let kind = error
        .and_then(|e| e.get("type"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let message = error
        .and_then(|e| e.get("message"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("HTTP {status}"));
    (kind, message)
}

/// What a request is about, for its errors.
#[derive(Clone, Copy)]
enum Subject<'a> {
    Namespace(&'a NamespaceIdent),
    Table(&'a TableIdent),
}

impl Subject<'_> {
    fn text(self) -> String {
        match self {
            Self::Namespace(ns) => namespace_text(ns),
            Self::Table(table) => table_text(table),
        }
    }
}

fn table_of(value: &Value, ident: &TableIdent) -> Result<LakeTable, CatalogError> {
    let metadata_location = value
        .get("metadata-location")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            CatalogError::Unavailable(format!(
                "the catalog answered {} without a metadata location",
                table_text(ident)
            ))
        })?
        .to_string();
    let metadata: TableMetadata = value
        .get("metadata")
        .cloned()
        .ok_or_else(|| {
            CatalogError::Unavailable(format!(
                "the catalog answered {} without metadata",
                table_text(ident)
            ))
        })
        .and_then(|m| {
            serde_json::from_value(m).map_err(|err| {
                CatalogError::Unavailable(format!(
                    "the catalog's metadata of {} is unreadable: {err}",
                    table_text(ident)
                ))
            })
        })?;
    Ok(LakeTable {
        ident: ident.clone(),
        metadata_location,
        metadata,
    })
}

fn ident_json(table: &TableIdent) -> Value {
    json!({ "namespace": table.namespace().as_ref(), "name": table.name() })
}

impl RestCatalog {
    /// Connects: reads the warehouse's configuration (`GET /v1/config`) for the
    /// path prefix its routes take.
    pub async fn connect(options: RestOptions) -> Result<Self, CatalogError> {
        let uri = Url::parse(options.uri.trim_end_matches('/')).map_err(|err| {
            CatalogError::Invalid(format!("catalog URI {:?}: {err}", options.uri))
        })?;
        if uri.scheme() != "http" {
            return Err(CatalogError::Invalid(format!(
                "catalog URI {:?}: only http:// until TLS (HS1 Task 20)",
                options.uri
            )));
        }
        // A bearer token over plaintext would hand it to anyone on the path: a
        // token waits for `https` (HS1 Task 20; PR #448 review).
        if options.token.is_some() {
            return Err(CatalogError::Invalid(format!(
                "catalog URI {:?}: a token is sent only over https, which arrives with TLS \
                 (HS1 Task 20)",
                options.uri
            )));
        }
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(unreachable_catalog)?;
        let mut config = uri.clone();
        segments(&mut config, &["v1", "config"]);
        config
            .query_pairs_mut()
            .append_pair("warehouse", &options.warehouse);
        let mut catalog = Self {
            client,
            base: uri,
            token: options.token,
        };
        let answer = catalog.send(Method::GET, config, None).await?;
        let (status, body) = answer;
        if !status.is_success() {
            let (_, message) = error_of(status, &body);
            return Err(CatalogError::Unavailable(format!(
                "the catalog's configuration for warehouse {:?}: {message}",
                options.warehouse
            )));
        }
        let config: Value = serde_json::from_slice(&body).map_err(|err| {
            CatalogError::Unavailable(format!("the catalog's configuration is unreadable: {err}"))
        })?;
        let prefix = ["overrides", "defaults"]
            .iter()
            .find_map(|part| config.get(part)?.get("prefix")?.as_str())
            .map(str::to_string);
        segments(&mut catalog.base, &["v1"]);
        if let Some(prefix) = prefix.filter(|p| !p.is_empty()) {
            segments(&mut catalog.base, &[prefix.as_str()]);
        }
        Ok(catalog)
    }

    fn url(&self, parts: &[&str]) -> Url {
        let mut url = self.base.clone();
        segments(&mut url, parts);
        url
    }

    fn namespace_url(&self, namespace: &NamespaceIdent, rest: &[&str]) -> Url {
        let joined = namespace.to_url_string();
        let mut parts = vec!["namespaces", joined.as_str()];
        parts.extend_from_slice(rest);
        self.url(&parts)
    }

    fn table_url(&self, table: &TableIdent) -> Url {
        self.namespace_url(table.namespace(), &["tables", table.name()])
    }

    async fn send(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
    ) -> Result<(StatusCode, Vec<u8>), CatalogError> {
        let mut request = self.client.request(method, url);
        if let Some(token) = &self.token {
            request = request.header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"));
        }
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(
                    serde_json::to_vec(&body)
                        .map_err(|err| CatalogError::Invalid(err.to_string()))?,
                );
        }
        let response = request.send().await.map_err(unreachable_catalog)?;
        let status = response.status();
        let body = response.bytes().await.map_err(unreachable_catalog)?;
        Ok((status, body.to_vec()))
    }

    /// Sends a request and reads a JSON answer, mapping the protocol's errors.
    async fn call(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
        subject: Subject<'_>,
    ) -> Result<Value, CatalogError> {
        let (status, bytes) = self.send(method, url, body).await?;
        if status.is_success() {
            if bytes.is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_slice(&bytes).map_err(|err| {
                CatalogError::Unavailable(format!("the catalog's answer is unreadable: {err}"))
            });
        }
        let (kind, message) = error_of(status, &bytes);
        let text = subject.text();
        Err(match (status, kind.as_str()) {
            (StatusCode::NOT_FOUND, k) if k.contains("Namespace") => {
                CatalogError::NoSuchNamespace(match subject {
                    Subject::Namespace(_) => text,
                    Subject::Table(table) => namespace_text(table.namespace()),
                })
            }
            (StatusCode::NOT_FOUND, _) => match subject {
                Subject::Table(_) => CatalogError::NoSuchTable(text),
                Subject::Namespace(_) => CatalogError::NoSuchNamespace(text),
            },
            (StatusCode::CONFLICT, k) if k.contains("NotEmpty") => CatalogError::NotEmpty(text),
            (StatusCode::CONFLICT, k) if k.contains("CommitFailed") => CatalogError::Conflict {
                table: text,
                message,
            },
            (StatusCode::CONFLICT, _) => match subject {
                Subject::Namespace(_) => CatalogError::NamespaceExists(text),
                Subject::Table(_) => CatalogError::TableExists(text),
            },
            (StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY, _) => {
                CatalogError::Invalid(format!("the catalog refused {text}: {message}"))
            }
            _ => CatalogError::Unavailable(format!(
                "the catalog failed on {text} ({status}): {message}"
            )),
        })
    }
}

/// Appends path segments, percent-encoding each.
fn segments(url: &mut Url, parts: &[&str]) {
    if let Ok(mut path) = url.path_segments_mut() {
        path.pop_if_empty();
        path.extend(parts);
    }
}

impl RestCatalog {
    /// The namespaces directly under `parent`, or the top-level ones, every
    /// page of them.
    async fn namespaces_under(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>, CatalogError> {
        // Only named in errors about the top level.
        let root = NamespaceIdent::new(String::new());
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut url = self.url(&["namespaces"]);
            if let Some(parent) = parent {
                url.query_pairs_mut()
                    .append_pair("parent", &parent.to_url_string());
            }
            if let Some(t) = &token {
                url.query_pairs_mut().append_pair("pageToken", t);
            }
            let answer = self
                .call(
                    Method::GET,
                    url,
                    None,
                    Subject::Namespace(parent.unwrap_or(&root)),
                )
                .await?;
            for ns in answer
                .get("namespaces")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let levels: Vec<String> = ns
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|l| l.as_str().map(str::to_string))
                    .collect();
                if let Ok(ident) = NamespaceIdent::from_vec(levels) {
                    out.push(ident);
                }
            }
            token = answer
                .get("next-page-token")
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty())
                .map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl HouseCatalog for RestCatalog {
    async fn list_root_databases(&self) -> Result<Vec<NamespaceIdent>, CatalogError> {
        self.namespaces_under(None).await
    }

    async fn list_databases(
        &self,
        parent: &NamespaceIdent,
    ) -> Result<Vec<NamespaceIdent>, CatalogError> {
        let mut out = self.namespaces_under(Some(parent)).await?;
        // Some catalogs answer an unknown parent with an empty list: the House
        // asks for the parent itself so a missing namespace is one.
        if out.is_empty() {
            self.database_properties(parent).await?;
        }
        out.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        Ok(out)
    }

    async fn create_database(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<(), CatalogError> {
        self.call(
            Method::POST,
            self.url(&["namespaces"]),
            Some(json!({ "namespace": namespace.as_ref(), "properties": properties })),
            Subject::Namespace(namespace),
        )
        .await
        .map(|_| ())
    }

    async fn database_properties(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<HashMap<String, String>, CatalogError> {
        let answer = self
            .call(
                Method::GET,
                self.namespace_url(namespace, &[]),
                None,
                Subject::Namespace(namespace),
            )
            .await?;
        Ok(answer
            .get("properties")
            .and_then(Value::as_object)
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn drop_database(&self, namespace: &NamespaceIdent) -> Result<(), CatalogError> {
        self.call(
            Method::DELETE,
            self.namespace_url(namespace, &[]),
            None,
            Subject::Namespace(namespace),
        )
        .await
        .map(|_| ())
    }

    async fn list_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>, CatalogError> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut url = self.namespace_url(namespace, &["tables"]);
            if let Some(t) = &token {
                url.query_pairs_mut().append_pair("pageToken", t);
            }
            let answer = self
                .call(Method::GET, url, None, Subject::Namespace(namespace))
                .await?;
            for ident in answer
                .get("identifiers")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(name) = ident.get("name").and_then(Value::as_str) {
                    out.push(TableIdent::new(namespace.clone(), name.to_string()));
                }
            }
            token = answer
                .get("next-page-token")
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty())
                .map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        out.sort_by(|a, b| a.name().cmp(b.name()));
        Ok(out)
    }

    async fn load_table(&self, table: &TableIdent) -> Result<LakeTable, CatalogError> {
        let answer = self
            .call(
                Method::GET,
                self.table_url(table),
                None,
                Subject::Table(table),
            )
            .await?;
        table_of(&answer, table)
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<LakeTable, CatalogError> {
        let ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let mut properties = creation.properties.clone();
        if creation.format_version != loams_house_lake::spec::FormatVersion::V2 {
            properties.insert(
                "format-version".to_string(),
                (creation.format_version as u8).to_string(),
            );
        }
        let mut body = json!({
            "name": creation.name,
            "location": creation.location,
            "schema": creation.schema,
            "properties": properties,
            "stage-create": false,
        });
        if let Some(spec) = &creation.partition_spec {
            body["partition-spec"] =
                serde_json::to_value(spec).map_err(|err| CatalogError::Invalid(err.to_string()))?;
        }
        if let Some(order) = &creation.sort_order {
            body["write-order"] = serde_json::to_value(order)
                .map_err(|err| CatalogError::Invalid(err.to_string()))?;
        }
        let answer = self
            .call(
                Method::POST,
                self.namespace_url(namespace, &["tables"]),
                Some(body),
                Subject::Table(&ident),
            )
            .await?;
        table_of(&answer, &ident)
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<(), CatalogError> {
        let mut url = self.table_url(table);
        url.query_pairs_mut().append_pair("purgeRequested", "false");
        self.call(Method::DELETE, url, None, Subject::Table(table))
            .await
            .map(|_| ())
    }

    async fn rename_table(&self, from: &TableIdent, to: &TableIdent) -> Result<(), CatalogError> {
        let (status, bytes) = self
            .send(
                Method::POST,
                self.url(&["tables", "rename"]),
                Some(json!({ "source": ident_json(from), "destination": ident_json(to) })),
            )
            .await?;
        if status.is_success() {
            return Ok(());
        }
        let (kind, message) = error_of(status, &bytes);
        Err(match status {
            StatusCode::NOT_FOUND if kind.contains("Namespace") => {
                CatalogError::NoSuchNamespace(namespace_text(to.namespace()))
            }
            StatusCode::NOT_FOUND => CatalogError::NoSuchTable(table_text(from)),
            StatusCode::CONFLICT => CatalogError::TableExists(table_text(to)),
            _ => CatalogError::Unavailable(format!(
                "the catalog failed to rename {} ({status}): {message}",
                table_text(from)
            )),
        })
    }

    async fn commit(
        &self,
        table: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<LakeTable, CatalogError> {
        let answer = self
            .call(
                Method::POST,
                self.table_url(table),
                Some(json!({
                    "identifier": ident_json(table),
                    "requirements": requirements,
                    "updates": updates,
                })),
                Subject::Table(table),
            )
            .await
            // Every 409 of a commit is a failed requirement, whatever type the
            // catalog names (Lakekeeper 0.13.6 does not say `CommitFailed`).
            .map_err(|err| match err {
                CatalogError::TableExists(table) | CatalogError::NamespaceExists(table) => {
                    CatalogError::Conflict {
                        table,
                        message: "a requirement of the commit failed".to_string(),
                    }
                }
                other => other,
            })?;
        table_of(&answer, table)
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: &str,
    ) -> Result<LakeTable, CatalogError> {
        let answer = self
            .call(
                Method::POST,
                self.namespace_url(table.namespace(), &["register"]),
                Some(json!({ "name": table.name(), "metadata-location": metadata_location })),
                Subject::Table(table),
            )
            .await?;
        table_of(&answer, table)
    }
}
