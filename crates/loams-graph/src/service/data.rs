//! `GraphService`: statements against one graph (GR1 Task 3).
//!
//! Every statement is gated ([`crate::classify::gate`]) before it runs and then runs, unchanged,
//! on a Grafeo session whose role matches what it needs. Parameters are bound by the engine on
//! every path; a value is never interpolated into a statement.

use std::collections::HashMap;

use connectrpc::{ConnectError, ErrorCode};
use loams_proto::loams::graph::v1 as pb;

use super::errors::{code_of, engine_error_info, map_engine, refuse};
use super::find_serving;
use crate::durable::{Isolation, WriteOptions};
use crate::engine::{BatchStatement, Engine, GraphError, GraphResult, PlanNode};
use crate::limits::StatementLimits;
use crate::value::{from_proto, to_proto};

/// Refuses a query language this build does not have.
///
/// This build compiles Grafeo with `gql` only (root `Cargo.toml`, R0.2), so asking for another is
/// `Unimplemented` rather than a parse error deep in the engine. A wire number that names no
/// variant is refused rather than read as GQL: running a statement in a language the caller did
/// not ask for is worse than refusing it.
pub(crate) fn check_language(
    language: Option<pb::QueryLanguage>,
    wire_value: impl std::fmt::Display,
) -> Result<(), ConnectError> {
    use pb::QueryLanguage as L;
    match language {
        Some(L::Unspecified) | Some(L::Gql) => Ok(()),
        _ => Err(refuse(
            ErrorCode::Unimplemented,
            "graph_language_disabled",
            format!(
                "query language {wire_value} is not available in this build; Loams Graph serves \
                 GQL, ISO/IEC 39075 (D634 (a))"
            ),
        )),
    }
}

/// One engine row on the wire.
pub(crate) fn to_pb_row(row: &crate::engine::GraphRow) -> pb::Row {
    pb::Row {
        values: row.values.iter().map(to_proto).collect(),
        ..Default::default()
    }
}

/// What a row adds to its message: its encoded length, plus at most 6 bytes of field tag and
/// length prefix.
pub(crate) fn row_bytes(row: &pb::Row) -> u64 {
    use buffa::Message as _;
    u64::from(row.try_encoded_len().unwrap_or(u32::MAX)) + 6
}

/// The largest row any answer carries: a Connect message's limit (connectrpc's default, 4 MiB,
/// which clients share), less room for the message around the row (review fix 1, M7).
pub(crate) fn max_row_bytes() -> u64 {
    (connectrpc::Limits::default().max_message_size() as u64).saturating_sub(4096)
}

/// Refuses a row no message can carry (review fix 1, M7).
pub(crate) fn check_row(bytes: u64) -> Result<(), GraphError> {
    let limit = max_row_bytes();
    if bytes > limit {
        return Err(GraphError::RowTooLarge { bytes, limit });
    }
    Ok(())
}

/// The bytes a unary answer may still carry (§48 §13.1's `max_result_bytes`), across every
/// result of a batch.
pub(crate) struct ByteBudget {
    limit: u64,
    used: u64,
}

impl ByteBudget {
    pub(crate) fn new(limit: u64) -> Self {
        Self { limit, used: 0 }
    }

    fn spend(&mut self, bytes: u64) -> Result<(), GraphError> {
        self.used = self.used.saturating_add(bytes);
        if self.used > self.limit {
            return Err(GraphError::ResultTooLarge { limit: self.limit });
        }
        Ok(())
    }
}

/// Converts an engine result into its protobuf row set, within `budget`: the conversion stops
/// with `graph_result_too_large` at the first row past it.
fn to_pb_rows(result: &GraphResult, budget: &mut ByteBudget) -> Result<pb::RowSet, GraphError> {
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let row = to_pb_row(row);
        let bytes = row_bytes(&row);
        check_row(bytes)?;
        budget.spend(bytes)?;
        rows.push(row);
    }
    Ok(pb::RowSet {
        columns: result.columns.clone(),
        column_types: result.column_types.clone(),
        rows,
        ..Default::default()
    })
}

/// Fills one `ExecuteResponse` from an engine result, with the token, commit epoch and counters
/// of a durable write (GR1 Task 11). An answer replayed from the idempotency index has no rows and
/// says so in a `01000` notification.
///
/// An answer past the byte budget is `graph_result_too_large`, unless `committed`: a statement
/// that committed is never reported as a failure (review fix 1, I3), so its rows are dropped,
/// `truncated` is set, and a `01000` notification says why.
fn to_pb_response(
    result: &GraphResult,
    budget: &mut ByteBudget,
    committed: bool,
) -> Result<pb::ExecuteResponse, GraphError> {
    let (rows, truncated, notifications) = match to_pb_rows(result, budget) {
        Ok(rows) => (rows, result.truncated, Vec::new()),
        Err(err) if committed => (
            pb::RowSet {
                columns: result.columns.clone(),
                column_types: result.column_types.clone(),
                ..Default::default()
            },
            true,
            vec![pb::Notification {
                gqlstatus: "01000".to_string(),
                message: format!("the statement committed; its rows were dropped: {err}"),
                ..Default::default()
            }],
        ),
        Err(err) => return Err(err),
    };
    let mut notifications = notifications;
    let mut response = pb::ExecuteResponse {
        truncated,
        elapsed_nanos: result.elapsed_nanos.unwrap_or_default(),
        ..Default::default()
    };
    if let Some(commit) = &result.commit {
        response.consistency_token = commit.token.to_string();
        response.commit_epoch = commit.commit_epoch;
        response.counters = to_pb_counters(&commit.counters).into();
        if commit.replayed {
            notifications.push(pb::Notification {
                gqlstatus: "01000".to_string(),
                message: "already applied under this idempotency_key: the statement did not run \
                          again, and the first run's rows are not kept"
                    .to_string(),
                ..Default::default()
            });
        }
    }
    response.rows = rows.into();
    response.notifications = notifications;
    Ok(response)
}

/// A write's counters on the wire.
pub(crate) fn to_pb_counters(counters: &crate::changeset::Counters) -> pb::Counters {
    pb::Counters {
        nodes_created: counters.nodes_created,
        nodes_deleted: counters.nodes_deleted,
        relationships_created: counters.relationships_created,
        relationships_deleted: counters.relationships_deleted,
        properties_set: counters.properties_set,
        labels_added: counters.labels_added,
        labels_removed: counters.labels_removed,
        ..Default::default()
    }
}

/// A request's idempotency key: empty is none, and at most
/// [`super::admin::MAX_IDEMPOTENCY_KEY`] bytes.
fn idempotency_key(key: &str) -> Result<Option<String>, ConnectError> {
    if key.len() > super::admin::MAX_IDEMPOTENCY_KEY {
        return Err(refuse(
            ErrorCode::InvalidArgument,
            "invalid_argument",
            format!(
                "idempotency_key must be at most {} bytes, got {}",
                super::admin::MAX_IDEMPOTENCY_KEY,
                key.len()
            ),
        ));
    }
    Ok((!key.is_empty()).then(|| key.to_string()))
}

/// Decodes a parameter map into the engine's values, refusing more than the limit allows before
/// any is decoded.
pub(crate) fn to_parameters(
    parameters: &::buffa::__private::HashMap<String, pb::Value>,
    limits: &StatementLimits,
) -> Result<HashMap<String, grafeo::Value>, ConnectError> {
    limits
        .check_parameters(parameters.len())
        .map_err(map_engine)?;
    parameters
        .iter()
        .map(|(name, value)| {
            from_proto(value)
                .map(|value| (name.clone(), value))
                .map_err(|err| {
                    refuse(
                        ErrorCode::InvalidArgument,
                        "invalid_argument",
                        format!("parameter ${name}: {err}"),
                    )
                })
        })
        .collect()
}

/// `Execute`: runs one statement. Parameters are bound by the engine, never interpolated.
pub fn execute(
    engine: &Engine,
    req: pb::ExecuteRequest,
) -> Result<pb::ExecuteResponse, ConnectError> {
    let graph = find_serving(engine, &req.namespace, &req.graph)?;
    execute_on(&graph, req, &StatementLimits::DEFAULT)
}

/// `Execute` on a graph the caller has already found and validated (review M1: the catalog path
/// hands over the graph it checked, rather than having it looked up again by name), within the
/// graph's limits: at most `max_rows` rows (`truncated` past them) and `max_result_bytes`
/// (`graph_result_too_large`).
pub fn execute_on(
    graph: &crate::engine::Graph,
    req: pb::ExecuteRequest,
    limits: &StatementLimits,
) -> Result<pb::ExecuteResponse, ConnectError> {
    let max_rows = limits.rows_for(req.max_rows) as usize;
    let result = run_statement(graph, &req, limits, Some(max_rows))?;
    let mut response = to_pb_response(
        &result,
        &mut ByteBudget::new(limits.max_result_bytes),
        result.wrote,
    )
    .map_err(map_engine)?;
    state_token(graph, &mut response.consistency_token);
    Ok(response)
}

/// A read's token (GR1 Task 15): the state of the replica that served it, read after the
/// statement, so it is never older than what the statement saw. Left alone when a write already
/// set its own; a graph that is not durable has none.
pub(crate) fn state_token(graph: &crate::engine::Graph, token: &mut String) {
    if token.is_empty()
        && let Some(state) = graph.consistency_token()
    {
        *token = state.to_string();
    }
}

/// Checks and runs one statement of an `ExecuteRequest`, keeping at most `max_rows` rows: what
/// `Execute` and `ExecuteStream` share.
pub(crate) fn run_statement(
    graph: &crate::engine::Graph,
    req: &pb::ExecuteRequest,
    limits: &StatementLimits,
    max_rows: Option<usize>,
) -> Result<GraphResult, ConnectError> {
    check_language(req.language.as_known(), req.language)?;
    limits.check_statement(&req.statement).map_err(map_engine)?;
    let parameters = to_parameters(&req.parameters, limits)?;
    let options = WriteOptions {
        idempotency_key: idempotency_key(&req.idempotency_key)?,
        ..WriteOptions::default()
    };
    graph
        .execute_with(
            &req.statement,
            parameters,
            req.read_only,
            limits,
            max_rows,
            &options,
        )
        .map_err(map_engine)
}

/// `ExecuteBatch`: runs statements as one transaction (`atomic`) or one transaction each.
///
/// The contract is `ExecuteBatchResponse`'s: an atomic batch fails as a whole; a non-atomic one
/// stops at the first failing statement and answers the committed results, `committed_through`
/// and a `StatementError` for the failed one.
pub fn execute_batch(
    engine: &Engine,
    req: pb::ExecuteBatchRequest,
) -> Result<pb::ExecuteBatchResponse, ConnectError> {
    let graph = find_serving(engine, &req.namespace, &req.graph)?;
    execute_batch_on(&graph, req, &StatementLimits::DEFAULT)
}

/// `ExecuteBatch` on a graph the caller has already found and validated (review M1), within the
/// graph's limits: at most `max_batch_statements` statements, each checked before any runs;
/// each result cut at `max_rows`; and `max_result_bytes` across the whole answer.
pub fn execute_batch_on(
    graph: &crate::engine::Graph,
    req: pb::ExecuteBatchRequest,
    limits: &StatementLimits,
) -> Result<pb::ExecuteBatchResponse, ConnectError> {
    limits
        .check_batch(req.statements.len())
        .map_err(map_engine)?;
    for statement in &req.statements {
        limits
            .check_statement(&statement.statement)
            .map_err(map_engine)?;
    }
    // A statement's own language wins over the batch's (`Statement.language`); the batch's
    // applies to a statement that names none, and is checked when no statement names one (an
    // empty batch included) so a batch in a language this build lacks is still refused.
    let mut batch_language_used = req.statements.is_empty();
    for statement in &req.statements {
        if statement.language.as_known() == Some(pb::QueryLanguage::Unspecified) {
            batch_language_used = true;
        } else {
            check_language(statement.language.as_known(), statement.language)?;
        }
    }
    if batch_language_used {
        check_language(req.language.as_known(), req.language)?;
    }
    let statements = req
        .statements
        .iter()
        .map(|statement| {
            Ok(BatchStatement {
                text: statement.statement.clone(),
                parameters: to_parameters(&statement.parameters, limits)?,
            })
        })
        .collect::<Result<Vec<_>, ConnectError>>()?;
    let max_rows = limits.max_rows as usize;
    let key = idempotency_key(&req.idempotency_key)?;
    let isolation = match req.isolation.as_known() {
        Some(pb::Isolation::Serializable) => Isolation::Serializable,
        _ => Isolation::Snapshot,
    };
    if req.atomic {
        // Grafeo's own transaction, not a Loams-side emulation, run in the write lane on a
        // durable graph: one record, one token, one commit epoch (GR1 Task 11, R0.5).
        let options = WriteOptions {
            idempotency_key: key,
            isolation,
            if_version: req.if_version,
        };
        let results = graph
            .execute_batch_with(&statements, limits, Some(max_rows), &options)
            .map_err(map_engine)?;
        let commit = results.first().and_then(|result| result.commit);
        let mut budget = ByteBudget::new(limits.max_result_bytes);
        // A batch that wrote has committed: none of its answers may fail it now (I3).
        let committed = results.iter().any(|result| result.wrote);
        let mut response = pb::ExecuteBatchResponse {
            results: results
                .iter()
                .map(|result| to_pb_response(result, &mut budget, committed))
                .collect::<Result<_, _>>()
                .map_err(map_engine)?,
            committed: true,
            committed_through: u32::try_from(results.len()).unwrap_or(u32::MAX),
            consistency_token: commit.map(|c| c.token.to_string()).unwrap_or_default(),
            commit_epoch: commit.map_or(0, |c| c.commit_epoch),
            ..Default::default()
        };
        state_token(graph, &mut response.consistency_token);
        Ok(response)
    } else {
        // Each statement is its own transaction, with its parameters bound; a failure stops the
        // batch and leaves its predecessors committed. On a durable graph each one that writes
        // is its own record, keyed `<key>#<index>` so a retry of the batch skips the statements
        // that committed; `if_version` guards the first.
        let mut results = Vec::with_capacity(statements.len());
        let mut error = None;
        let mut token = String::new();
        let mut budget = ByteBudget::new(limits.max_result_bytes);
        for (index, statement) in statements.iter().enumerate() {
            let options = WriteOptions {
                idempotency_key: key.as_ref().map(|key| format!("{key}#{index}")),
                isolation,
                if_version: if index == 0 { req.if_version } else { None },
            };
            match graph.execute_with(
                &statement.text,
                statement.parameters.clone(),
                false,
                limits,
                Some(max_rows),
                &options,
            ) {
                // A statement that wrote committed: past the byte budget it is answered without
                // rows (`truncated`) and the batch goes on. A read past it changed nothing: it
                // fails and stops the batch, and `committed_through` does not count it (I3).
                Ok(result) => match to_pb_response(&result, &mut budget, result.wrote) {
                    Ok(response) => {
                        if !response.consistency_token.is_empty() {
                            token.clone_from(&response.consistency_token);
                        }
                        results.push(response);
                    }
                    Err(err) => {
                        error = Some(statement_error(index, &err));
                        break;
                    }
                },
                Err(err) => {
                    error = Some(statement_error(index, &err));
                    break;
                }
            }
        }
        state_token(graph, &mut token);
        Ok(pb::ExecuteBatchResponse {
            committed_through: u32::try_from(results.len()).unwrap_or(u32::MAX),
            results,
            committed: false,
            consistency_token: token,
            error: error.into(),
            ..Default::default()
        })
    }
}

/// A non-atomic batch's failed statement.
fn statement_error(index: usize, err: &GraphError) -> pb::StatementError {
    pb::StatementError {
        index: u32::try_from(index).unwrap_or(u32::MAX),
        code: code_of(err).as_str().to_string(),
        message: err.to_string(),
        info: engine_error_info(err).into(),
        ..Default::default()
    }
}

/// `Explain`: the plan of a statement, or its profile.
///
/// The statement is gated like any other (its limits, file access, graph management and
/// unbounded paths are refused). An EXPLAIN plans without running, so a write's plan is
/// answered. A PROFILE runs the statement, which must itself begin with `PROFILE` (Loams never
/// rewrites a statement, D634); one that writes is refused with `graph_read_only` before anything
/// runs.
pub fn explain(engine: &Engine, req: pb::ExplainRequest) -> Result<pb::Plan, ConnectError> {
    let graph = find_serving(engine, &req.namespace, &req.graph)?;
    explain_on(&graph, req, &StatementLimits::DEFAULT)
}

/// `Explain` on a graph the caller has already found and validated (review M1).
pub fn explain_on(
    graph: &crate::engine::Graph,
    req: pb::ExplainRequest,
    limits: &StatementLimits,
) -> Result<pb::Plan, ConnectError> {
    check_language(req.language.as_known(), req.language)?;
    limits.check_statement(&req.statement).map_err(map_engine)?;
    let plan = if req.profile {
        let parameters = to_parameters(&req.parameters, limits)?;
        graph.profile_within(&req.statement, parameters, limits)
    } else {
        graph.explain_within(&req.statement, limits)
    }
    .map_err(|err| match err {
        // A PROFILE that writes: the refusal Task 2 named (`profile_refuses_a_write`).
        GraphError::ReadOnly => refuse(
            ErrorCode::PermissionDenied,
            "graph_read_only",
            "PROFILE runs the statement, and this statement writes; use EXPLAIN for its plan",
        ),
        err => map_engine(err),
    })?;
    Ok(pb::Plan {
        root: to_pb_plan(&plan.root).into(),
        text: plan.text,
        ..Default::default()
    })
}

fn to_pb_plan(node: &PlanNode) -> pb::PlanOperator {
    let mut details = ::buffa::__private::HashMap::default();
    if !node.label.is_empty() {
        details.insert("label".to_string(), node.label.clone());
    }
    if !node.line.is_empty() {
        details.insert("operator".to_string(), node.line.clone());
    }
    pb::PlanOperator {
        name: node.name.clone(),
        details,
        children: node.children.iter().map(to_pb_plan).collect(),
        rows: node.rows.unwrap_or_default(),
        elapsed_nanos: node.elapsed_nanos.unwrap_or_default(),
        ..Default::default()
    }
}
