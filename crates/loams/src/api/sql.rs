//! `POST /v1/namespaces/{ns}/sql` (plan M1.2 Task 11 rule 1): one read-only
//! statement, answered as `{"columns", "rows", "truncated"}` (Task 10
//! rule 7).

use axum::extract::rejection::{BytesRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use bytes::Bytes;
use loams_query::ReadConsistency;
use loams_query::sql::{rows_to_json, run_read_only};
use serde::Deserialize;

use super::{ApiResult, AppState, parse_json, read_consistency};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Sql {
    query: String,
    consistency: Option<ReadConsistency>,
}

pub(super) async fn sql(
    State(state): State<AppState>,
    ns: Result<Path<String>, PathRejection>,
    principal: Option<axum::Extension<crate::auth::Principal>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path(ns), body) = (ns?, body?);
    let request: Sql = parse_json(&body)?;
    let consistency = read_consistency(&headers, request.consistency)?;
    // Made inside the request, so its scans see this request's hot scope.
    // Its graph and algorithm functions read as the caller (GR1 R24.12).
    let caller = super::graph_caller(&state.principals, principal.as_ref().map(|p| &p.0));
    let ctx = state.collections.sql_context_with(&ns, consistency, caller);
    let result = run_read_only(&ctx, &request.query, &state.collections.config().sql).await?;
    Ok(axum::Json(rows_to_json(&result)).into_response())
}
