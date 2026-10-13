//! `loams.admin.v1.KeyService` (API2 Task 7): API keys and token revocation,
//! from the console seed.
//!
//! `CreateKey` answers a secret once per `idempotency_key`. The mock keeps
//! only the keys whose secret it answered, never the secret, which is the
//! rule the server's idempotency ledger keeps too (`secret_already_issued`,
//! `docs/api/reasons.md`).

use std::sync::Arc;

use connectrpc::{ErrorCode, RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::Value;

use super::journal::event;
use super::{ConsoleApi, fix, from_json, invalid, not_found, page};
use crate::proto::loams::admin::v1::{
    ApiKey, CreateKeyRequest, CreateKeyResponse, KeyService, ListKeysRequest, ListKeysResponse,
    RevokeKeyRequest, RevokeKeyResponse, RevokeTokenRequest, RevokeTokenResponse,
};
use crate::refuse;

/// `loams.admin.v1.KeyService`.
pub(crate) struct Keys(pub(crate) Arc<ConsoleApi>);

impl Keys {
    /// Every seed record under `key` (an object of arrays), with the entry
    /// name it was filed under.
    fn all(&self, key: &str) -> Result<Vec<(String, Value)>, connectrpc::ConnectError> {
        Ok(self
            .0
            .get(key)?
            .as_object()
            .into_iter()
            .flatten()
            .flat_map(|(entry, items)| {
                items
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(move |item| (entry.clone(), item.clone()))
            })
            .collect())
    }
}

impl KeyService for Keys {
    async fn list_keys(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListKeysRequest>,
    ) -> ServiceResult<ListKeysResponse> {
        self.0.environment(request.project, request.environment)?;
        let keys = self
            .0
            .entry_items(
                "keys",
                &format!("{}/{}", request.project, request.environment),
            )?
            .into_iter()
            .map(|mut k| {
                fix::api_key(&mut k);
                from_json::<ApiKey>(k)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (keys, next_page_token) = page(keys, request.page_size, request.page_token)?;
        Response::ok(ListKeysResponse {
            keys,
            next_page_token,
            ..Default::default()
        })
    }

    async fn create_key(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateKeyRequest>,
    ) -> ServiceResult<CreateKeyResponse> {
        let request = request.to_owned_message();
        self.0.environment(&request.project, &request.environment)?;
        if request.idempotency_key.is_empty() {
            return Err(invalid(
                "idempotency_key",
                "required: a key's secret is answered once, so a retry must be recognisable",
            ));
        }
        if request.name.is_empty() {
            return Err(invalid("name", "required"));
        }
        if !self.0.issued().insert(request.idempotency_key.clone()) {
            return Err(refuse(
                ErrorCode::FailedPrecondition,
                "secret_already_issued",
                "this idempotency_key already created a key, and its secret is answered once; \
                 create a new key with a new idempotency_key",
            ));
        }
        let mut created = self.0.example("api_key_created")?;
        if let Some(key) = created.get_mut("key") {
            fix::api_key(key);
        }
        let created: CreateKeyResponse = from_json(created)?;
        let key = created.key.as_option().cloned().unwrap_or_default();
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "api_key.create",
            ("api_key", &key.id, &request.name),
            &request.project,
            &request.environment,
        ));
        Response::ok(created)
    }

    async fn revoke_key(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RevokeKeyRequest>,
    ) -> ServiceResult<RevokeKeyResponse> {
        let id = request.key.to_owned();
        let (entry, key) = self
            .all("keys")?
            .into_iter()
            .find(|(_, k)| k["id"] == id.as_str())
            .ok_or_else(|| not_found("key", &id))?;
        let (project, environment) = entry.split_once('/').unwrap_or((&entry, ""));
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "api_key.revoke",
            ("api_key", &id, key["name"].as_str().unwrap_or_default()),
            project,
            environment,
        ));
        Response::ok(RevokeKeyResponse::default())
    }

    async fn revoke_token(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RevokeTokenRequest>,
    ) -> ServiceResult<RevokeTokenResponse> {
        let jti = request.jti.to_owned();
        let (_, token) = self
            .all("tokens")?
            .into_iter()
            .find(|(_, t)| t["jti"] == jti.as_str())
            .ok_or_else(|| not_found("token", &jti))?;
        let environment = token["environment"].as_str().unwrap_or_default();
        self.0.journal.record(event(
            &self.0,
            &ctx,
            "token.revoke",
            ("token", &jti, &jti),
            "",
            environment,
        ));
        Response::ok(RevokeTokenResponse::default())
    }
}
