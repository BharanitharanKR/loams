//! [`DurableGateway`]: the durable API's protocol endpoint (`POST /` of one
//! request envelope) as a function, so a host serves it on a port of its own
//! choosing behind its own authentication (B12: `loams` serves it under
//! `/durable/` on its main port, beside the loopback listener).
//!
//! It answers exactly what Resonate's HTTP gateway (`resonate-gateway-http`,
//! `handle_api`) answers for the same bytes: an envelope that does not parse
//! or validate is a `400` envelope naming what could be salvaged of its
//! `kind` and `corrId`, the console's `ui.*` kinds are refused with `404`
//! (they are not part of the worker protocol), a server that cannot answer is
//! a `503` envelope, and anything else is the server's own envelope with its
//! `head.status` as the HTTP status.
//!
//! Two things differ, on purpose:
//!
//! - The caller's `Authorization` header is **not** copied into `head.auth`.
//!   It carries the host's credential (a Loams key or session), which the
//!   host has already checked and which is no Resonate token.
//! - There are no `resonate_request_*` metrics: those are the listener's.

use std::sync::Arc;

use resonate_plugin::ResonateServer;
use resonate_plugin::types::{self, ResponseEnvelope};

/// The durable API's protocol endpoint over a running server (module docs).
#[derive(Clone)]
pub struct DurableGateway {
    server: Arc<dyn ResonateServer>,
}

impl std::fmt::Debug for DurableGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableGateway").finish_non_exhaustive()
    }
}

impl DurableGateway {
    /// The endpoint over `server`.
    pub fn new(server: Arc<dyn ResonateServer>) -> Self {
        Self { server }
    }

    /// The HTTP status and JSON body Resonate's gateway answers a `POST /`
    /// of `body` with.
    pub async fn handle(&self, body: &[u8]) -> (u16, Vec<u8>) {
        let response = match types::parse_and_validate(body) {
            Err(invalid) => {
                let (kind, corr_id) = types::salvage_context(body);
                tracing::warn!(kind = %kind, corr_id = %corr_id, reason = %invalid, "an invalid durable request");
                invalid.to_response(kind, corr_id)
            }
            // The console's read model is served on Resonate's own console
            // route, never on the worker endpoint (`resonate_core::ui`).
            Ok(req) if req.kind.starts_with("ui.") => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                404,
                "Console requests ('ui.*') are served on the console's own endpoint, not here",
            ),
            Ok(req) => match self.server.process(&req).await {
                Ok(response) => response,
                Err(err) => {
                    tracing::error!(kind = %req.kind, error = %err, "the durable server is unavailable");
                    ResponseEnvelope::error(
                        req.kind.clone(),
                        req.head.corr_id.clone(),
                        503,
                        &err.to_string(),
                    )
                }
            },
        };
        let status = u16::try_from(response.head.status)
            .ok()
            .filter(|s| (100..600).contains(s))
            .unwrap_or(500);
        let body = serde_json::to_vec(&response).unwrap_or_else(|err| {
            format!(r#"{{"code":"internal","message":"an unwritable durable answer: {err}"}}"#)
                .into_bytes()
        });
        (status, body)
    }
}
