//! `POST /v1/namespaces/{ns}/query` (plan M1.2 Task 11 rule 1): a
//! `SearchRequest`, or the §05 §4 hybrid body (Task 1 rule 8).

use axum::extract::rejection::{BytesRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use bytes::Bytes;
use loams_query::json::hybrid::parse_query_body;
use serde_json::Value;

use super::{ApiResult, AppState, parse_json, read_consistency, with_token};

pub(super) async fn search(
    State(state): State<AppState>,
    ns: Result<Path<String>, PathRejection>,
    principal: Option<axum::Extension<crate::auth::Principal>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path(ns), body) = (ns?, body?);
    let body: Value = parse_json(&body)?;
    // Date math (`now-30d`) is relative to the proposer's clock.
    let mut request = parse_query_body(body, state.meta.now_ms())?;
    request.consistency = read_consistency(&headers, Some(request.consistency))?;
    // An `expand` reads its graph as the caller (GR1 R24.12).
    let caller = super::graph_caller(&state.principals, principal.as_ref().map(|p| &p.0));
    let response = state.collections.search_as(&ns, request, &caller).await?;
    let token = response.read_token.clone();
    Ok(with_token(axum::Json(response).into_response(), &token))
}
