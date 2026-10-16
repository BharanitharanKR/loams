//! The graph table functions (GR1 Task 21; design §07 §5.1, §48 §10.3; D743): `graph_expand`,
//! `graph_neighbors`, `graph_degree` and `graph_shortest_path`, with §07 §5.1's output columns.
//!
//! **Positional arguments (R21.1).** DataFusion 54 plans a table function's arguments before it
//! runs and does not take named arguments (`graph => 'kg'` is "Unsupported function argument
//! type"), so the arguments are §07 §5.1's, in its order, by position; `NULL` takes an
//! argument's default:
//!
//! ```sql
//! graph_expand(graph, seeds, seed_label, hops, direction, edge_types, edge_filter, vertex_filter, limit_per_seed)
//! graph_neighbors(graph, keys, label, direction, edge_types, edge_filter, limit_per_key)
//! graph_degree(graph, keys, label, direction, edge_types)
//! graph_shortest_path(graph, from, to, label, direction, edge_types, max_hops)
//! ```
//!
//! * `seeds` and `keys` are a key (an unsigned integer or a string), an `ARRAY` of keys, or a
//!   query string (`'SELECT entity_id FROM collections.mentions WHERE …'`) whose first column
//!   holds them: the `seeds => 'SELECT …'` form, because a table function cannot see a `LATERAL`
//!   outer column (R0.x, Task 0), which is refused with that advice.
//! * `edge_filter` and `vertex_filter` are filter JSON, as `vector_search`'s `filter_json` is
//!   (the IR's structured `Query`): a graph's edges and vertices come from several collections,
//!   so a SQL predicate has no one schema to plan over.
//! * Keys come back as text (an integer key as its decimal digits), so one column holds the
//!   keys of every label.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Builder, RecordBatch, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DfResult, plan_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use loams_collection::PrimaryKey;

use crate::error::ServiceError;
use crate::exec::{df_error, plan_properties};
use crate::expand::{Expand, ExpandCall, ExpandDirection, ExpandOutput, MAX_EXPAND_LIMIT};
use crate::graph::{
    DEFAULT_LIMIT_PER_KEY, DEFAULT_PATH_HOPS, DegreeCall, MAX_PATH_HOPS, NeighborsCall, PathCall,
};
use crate::ir::Query;
use crate::sql::SqlScope;

/// The four functions' names.
pub const GRAPH_FUNCTIONS: [&str; 4] = [
    "graph_expand",
    "graph_neighbors",
    "graph_degree",
    "graph_shortest_path",
];

/// Which function a provider runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GraphFunction {
    Expand,
    Neighbors,
    Degree,
    ShortestPath,
}

impl GraphFunction {
    /// The function's name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Expand => "graph_expand",
            Self::Neighbors => "graph_neighbors",
            Self::Degree => "graph_degree",
            Self::ShortestPath => "graph_shortest_path",
        }
    }

    /// The arguments, in order (§07 §5.1).
    #[must_use]
    pub fn arguments(self) -> &'static [&'static str] {
        match self {
            Self::Expand => &[
                "graph",
                "seeds",
                "seed_label",
                "hops",
                "direction",
                "edge_types",
                "edge_filter",
                "vertex_filter",
                "limit_per_seed",
            ],
            Self::Neighbors => &[
                "graph",
                "keys",
                "label",
                "direction",
                "edge_types",
                "edge_filter",
                "limit_per_key",
            ],
            Self::Degree => &["graph", "keys", "label", "direction", "edge_types"],
            Self::ShortestPath => &[
                "graph",
                "from",
                "to",
                "label",
                "direction",
                "edge_types",
                "max_hops",
            ],
        }
    }

    /// The output columns (§07 §5.1).
    #[must_use]
    pub fn schema(self) -> SchemaRef {
        let text = |name: &str, nullable: bool| Field::new(name, DataType::Utf8, nullable);
        let fields = match self {
            Self::Expand => vec![
                text("seed", false),
                text("label", false),
                text("key", false),
                Field::new("hop", DataType::UInt32, false),
                text("edge_type", false),
                text("edge", false),
                text("via", false),
            ],
            Self::Neighbors => vec![
                text("key", false),
                text("direction", false),
                text("edge_type", false),
                text("edge", false),
                text("other_label", false),
                text("other_key", false),
            ],
            Self::Degree => vec![
                text("key", false),
                Field::new("degree", DataType::Int64, false),
            ],
            Self::ShortestPath => vec![
                Field::new("step", DataType::UInt32, false),
                text("label", false),
                text("key", false),
                text("edge_type", true),
                text("edge", true),
            ],
        };
        Arc::new(Schema::new(fields))
    }
}

/// A key's text.
pub(super) fn key_text(pk: &PrimaryKey) -> String {
    match pk {
        PrimaryKey::U64(n) => n.to_string(),
        PrimaryKey::Str(s) => s.clone(),
        PrimaryKey::Uuid(bytes) => crate::json::pk::format_uuid(bytes),
    }
}

/// Keys, or a query string whose first column holds them.
#[derive(Clone, Debug, PartialEq)]
enum Keys {
    Listed(Vec<PrimaryKey>),
    Query(String),
}

/// One parsed call.
#[derive(Clone, Debug)]
struct Call {
    function: GraphFunction,
    graph: String,
    keys: Keys,
    to: Option<PrimaryKey>,
    label: Option<String>,
    hops: u8,
    direction: ExpandDirection,
    edge_types: Vec<String>,
    edge_filter: Option<Query>,
    vertex_filter: Option<Query>,
    limit: usize,
}

fn key_of(function: &str, what: &str, value: &ScalarValue) -> DfResult<PrimaryKey> {
    match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Ok(PrimaryKey::Str(s.clone())),
        ScalarValue::UInt64(Some(n)) => Ok(PrimaryKey::U64(*n)),
        ScalarValue::UInt32(Some(n)) => Ok(PrimaryKey::U64(u64::from(*n))),
        ScalarValue::Int64(Some(n)) if *n >= 0 => Ok(PrimaryKey::U64(*n as u64)),
        ScalarValue::Int32(Some(n)) if *n >= 0 => Ok(PrimaryKey::U64(*n as u64)),
        other => {
            plan_err!("{function}: {what} must be an unsigned integer or a string key, got {other}")
        }
    }
}

/// Whether a string argument is a query rather than a key.
fn is_query(text: &str) -> bool {
    let text = text.trim_start();
    let word = |w: &str| {
        text.len() > w.len()
            && text[..w.len()].eq_ignore_ascii_case(w)
            && text[w.len()..].starts_with(|c: char| c.is_whitespace() || c == '(')
    };
    word("SELECT") || word("WITH")
}

fn keys_of(function: &str, what: &str, value: &ScalarValue) -> DfResult<Keys> {
    match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s))
            if is_query(s) =>
        {
            Ok(Keys::Query(s.clone()))
        }
        ScalarValue::List(list) => {
            let mut keys = Vec::new();
            for array in list.iter().flatten() {
                for i in 0..array.len() {
                    let scalar = ScalarValue::try_from_array(&array, i)?;
                    if !scalar.is_null() {
                        keys.push(key_of(function, what, &scalar)?);
                    }
                }
            }
            Ok(Keys::Listed(keys))
        }
        other => Ok(Keys::Listed(vec![key_of(function, what, other)?])),
    }
}

pub(super) fn text_of(function: &str, what: &str, value: &ScalarValue) -> DfResult<String> {
    match value.try_as_str() {
        Some(Some(text)) => Ok(text.to_string()),
        _ => plan_err!("{function}: {what} must be a string"),
    }
}

pub(super) fn whole_of(function: &str, what: &str, value: &ScalarValue) -> DfResult<u64> {
    let n = match value {
        ScalarValue::Int64(Some(n)) if *n >= 0 => Some(*n as u64),
        ScalarValue::Int32(Some(n)) if *n >= 0 => Some(*n as u64),
        ScalarValue::UInt64(Some(n)) => Some(*n),
        ScalarValue::UInt32(Some(n)) => Some(u64::from(*n)),
        _ => None,
    };
    match n {
        Some(n) => Ok(n),
        None => plan_err!("{function}: {what} must be a non-negative whole number"),
    }
}

fn direction_of(function: &str, value: &ScalarValue) -> DfResult<ExpandDirection> {
    match text_of(function, "direction", value)?
        .to_ascii_lowercase()
        .as_str()
    {
        "out" => Ok(ExpandDirection::Out),
        "in" => Ok(ExpandDirection::In),
        "both" => Ok(ExpandDirection::Both),
        other => plan_err!("{function}: direction is 'out', 'in' or 'both', got '{other}'"),
    }
}

pub(super) fn types_of(function: &str, value: &ScalarValue) -> DfResult<Vec<String>> {
    match value {
        ScalarValue::List(list) => {
            let mut out = Vec::new();
            for array in list.iter().flatten() {
                for i in 0..array.len() {
                    let scalar = ScalarValue::try_from_array(&array, i)?;
                    if !scalar.is_null() {
                        out.push(text_of(function, "edge_types", &scalar)?);
                    }
                }
            }
            Ok(out)
        }
        other => Ok(vec![text_of(function, "edge_types", other)?]),
    }
}

fn filter_of(function: &str, what: &str, value: &ScalarValue) -> DfResult<Query> {
    let text = text_of(function, what, value)?;
    serde_json::from_str(&text).map_err(|err| {
        DataFusionError::Plan(format!("{function}: {what} is not filter JSON: {err}"))
    })
}

/// A call's arguments as constants (`None` for `NULL`), refusing a column with the `LATERAL`
/// advice (R21.2) and anything else that is not a constant.
pub(super) fn constants(
    name: &str,
    names: &[&str],
    exprs: &[Expr],
) -> DfResult<Vec<Option<ScalarValue>>> {
    let simplifier = ExprSimplifier::new(SimplifyContext::default());
    let mut values: Vec<Option<ScalarValue>> = Vec::with_capacity(exprs.len());
    for (i, expr) in exprs.iter().enumerate() {
        match simplifier.simplify(expr.clone())? {
            Expr::Literal(value, _) => values.push((!value.is_null()).then_some(value)),
            Expr::Column(_) => {
                return plan_err!(
                    "{name}: {} is a column; a table function is planned before the rows of a \
                     LATERAL join, so pass the keys as a query string instead: '{}'",
                    names[i],
                    "SELECT key FROM …"
                );
            }
            _ => return plan_err!("{name}: {} must be a constant", names[i]),
        }
    }
    Ok(values)
}

impl Call {
    fn parse(function: GraphFunction, exprs: &[Expr]) -> DfResult<Self> {
        let name = function.name();
        let names = function.arguments();
        if exprs.len() < 2 || exprs.len() > names.len() {
            return plan_err!(
                "{name} takes 2 to {} arguments ({}), got {}",
                names.len(),
                names.join(", "),
                exprs.len()
            );
        }
        let values = constants(name, names, exprs)?;
        let at = |arg: &str| -> Option<&ScalarValue> {
            names
                .iter()
                .position(|n| *n == arg)
                .and_then(|i| values.get(i))
                .and_then(Option::as_ref)
        };
        let graph = match at("graph") {
            Some(value) => text_of(name, "graph", value)?,
            None => return plan_err!("{name}: graph must name a graph"),
        };
        let keys_arg = if function == GraphFunction::ShortestPath {
            "from"
        } else if function == GraphFunction::Expand {
            "seeds"
        } else {
            "keys"
        };
        let keys = match at(keys_arg) {
            Some(value) => keys_of(name, keys_arg, value)?,
            None => return plan_err!("{name}: {keys_arg} must not be NULL"),
        };
        let to = match (function, at("to")) {
            (GraphFunction::ShortestPath, Some(value)) => Some(key_of(name, "to", value)?),
            (GraphFunction::ShortestPath, None) => return plan_err!("{name}: to must not be NULL"),
            _ => None,
        };
        if function == GraphFunction::ShortestPath && matches!(keys, Keys::Query(_)) {
            return plan_err!("{name}: from is one key");
        }
        let label = match at(if function == GraphFunction::Expand {
            "seed_label"
        } else {
            "label"
        }) {
            Some(value) => Some(text_of(name, "label", value)?),
            None => None,
        };
        let direction = match at("direction") {
            Some(value) => direction_of(name, value)?,
            None if function == GraphFunction::Expand => ExpandDirection::Out,
            None => ExpandDirection::Both,
        };
        let edge_types = match at("edge_types") {
            Some(value) => types_of(name, value)?,
            None => Vec::new(),
        };
        let edge_filter = at("edge_filter")
            .map(|v| filter_of(name, "edge_filter", v))
            .transpose()?;
        let vertex_filter = at("vertex_filter")
            .map(|v| filter_of(name, "vertex_filter", v))
            .transpose()?;
        let hops = match at("hops") {
            Some(value) => match whole_of(name, "hops", value)? {
                n @ 1..=2 => n as u8,
                _ => return plan_err!("{name}: hops is 1 or 2"),
            },
            None => 1,
        };
        let limit = match function {
            GraphFunction::Expand => at("limit_per_seed")
                .map(|v| whole_of(name, "limit_per_seed", v))
                .transpose()?
                .unwrap_or(50) as usize,
            GraphFunction::Neighbors => at("limit_per_key")
                .map(|v| whole_of(name, "limit_per_key", v))
                .transpose()?
                .unwrap_or(DEFAULT_LIMIT_PER_KEY as u64)
                as usize,
            GraphFunction::ShortestPath => {
                let hops = at("max_hops")
                    .map(|v| whole_of(name, "max_hops", v))
                    .transpose()?
                    .unwrap_or(u64::from(DEFAULT_PATH_HOPS));
                if hops == 0 || hops > u64::from(MAX_PATH_HOPS) {
                    return plan_err!("{name}: max_hops is 1 to {MAX_PATH_HOPS}");
                }
                hops as usize
            }
            GraphFunction::Degree => 0,
        };
        if matches!(function, GraphFunction::Expand | GraphFunction::Neighbors) && limit == 0 {
            return plan_err!("{name}: the limit must be at least 1");
        }
        Ok(Self {
            function,
            graph,
            keys,
            to,
            label,
            hops,
            direction,
            edge_types,
            edge_filter,
            vertex_filter,
            limit,
        })
    }
}

/// One of the four functions, bound to a namespace's scope.
#[derive(Debug)]
pub struct GraphTableFunction {
    scope: SqlScope,
    function: GraphFunction,
}

impl GraphTableFunction {
    pub(crate) fn new(scope: SqlScope, function: GraphFunction) -> Self {
        Self { scope, function }
    }
}

impl TableFunctionImpl for GraphTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let call = Call::parse(self.function, args.exprs())?;
        Ok(Arc::new(GraphProvider {
            inner: Arc::new(Inner {
                scope: self.scope.clone(),
                call,
            }),
        }))
    }
}

#[derive(Debug)]
struct Inner {
    scope: SqlScope,
    call: Call,
}

/// One graph table function's call, run when its plan executes: the four functions here and the
/// algorithms ([`super::graph_algos`]).
#[async_trait]
pub(crate) trait GraphBatch: fmt::Debug + Send + Sync {
    /// The function's name.
    fn function(&self) -> &'static str;
    /// The graph it reads.
    fn graph(&self) -> &str;
    /// Its output columns.
    fn schema(&self) -> SchemaRef;
    /// Runs it.
    async fn run(&self) -> Result<RecordBatch, ServiceError>;
}

#[async_trait]
impl GraphBatch for Inner {
    fn function(&self) -> &'static str {
        self.call.function.name()
    }

    fn graph(&self) -> &str {
        &self.call.graph
    }

    fn schema(&self) -> SchemaRef {
        self.call.function.schema()
    }

    async fn run(&self) -> Result<RecordBatch, ServiceError> {
        self.answer().await
    }
}

pub(crate) fn unavailable() -> ServiceError {
    ServiceError::InvalidArgument(
        "the graph table functions run on Loams Graph, which this server does not run".to_string(),
    )
}

impl Inner {
    /// The listed keys, or the first column of the key query.
    async fn keys(&self) -> Result<Vec<PrimaryKey>, ServiceError> {
        match &self.call.keys {
            Keys::Listed(keys) => Ok(keys.clone()),
            Keys::Query(sql) => {
                let scope = &self.scope;
                let ctx = crate::sql::context(
                    scope.service.clone(),
                    &scope.ns,
                    scope.consistency.clone(),
                );
                let result =
                    crate::sql::run_read_only(&ctx, sql, &scope.service.config().sql).await?;
                let mut keys = Vec::new();
                for batch in &result.batches {
                    let Some(column) = batch.columns().first() else {
                        continue;
                    };
                    for i in 0..column.len() {
                        let scalar = ScalarValue::try_from_array(column, i)
                            .map_err(|err| ServiceError::InvalidArgument(err.to_string()))?;
                        if scalar.is_null() {
                            continue;
                        }
                        keys.push(
                            key_of(self.call.function.name(), "a key query's column", &scalar)
                                .map_err(|err| ServiceError::InvalidArgument(err.to_string()))?,
                        );
                    }
                }
                Ok(keys)
            }
        }
    }

    async fn answer(&self) -> Result<RecordBatch, ServiceError> {
        let call = &self.call;
        let service = &self.scope.service;
        let ns = self.scope.ns.as_str();
        let keys = self.keys().await?;
        let schema = call.function.schema();
        let text = |values: Vec<String>| -> ArrayRef {
            let mut b = StringBuilder::new();
            for v in values {
                b.append_value(v);
            }
            Arc::new(b.finish())
        };
        let columns: Vec<ArrayRef> = match call.function {
            GraphFunction::Expand => {
                let expander = service.graph_expander().ok_or_else(unavailable)?;
                let expand = Expand {
                    graph: call.graph.clone(),
                    from_field: None,
                    seed_label: call.label.clone(),
                    seeds: keys.len().max(1),
                    hops: call.hops,
                    direction: call.direction,
                    edge_types: call.edge_types.clone(),
                    edge_filter: call.edge_filter.clone(),
                    vertex_filter: call.vertex_filter.clone(),
                    limit_per_seed: call.limit,
                    limit: MAX_EXPAND_LIMIT,
                    select: None,
                    output: ExpandOutput::Nested,
                    consistency_token: None,
                };
                crate::expand::validate(Some(&expand), None, false, false, false)?;
                let seeds: Vec<Option<PrimaryKey>> = keys.iter().cloned().map(Some).collect();
                let expanded = expander
                    .expand(ExpandCall {
                        namespace: ns,
                        expand: &expand,
                        seeds,
                    })
                    .await?;
                let mut hops = UInt32Builder::new();
                for r in &expanded.reached {
                    hops.append_value(u32::from(r.hop));
                }
                let reached = &expanded.reached;
                vec![
                    text(reached.iter().map(|r| key_text(&keys[r.seed])).collect()),
                    text(reached.iter().map(|r| r.label.clone()).collect()),
                    text(reached.iter().map(|r| key_text(&r.key)).collect()),
                    Arc::new(hops.finish()),
                    text(reached.iter().map(|r| r.edge_type.clone()).collect()),
                    text(reached.iter().map(|r| key_text(&r.edge)).collect()),
                    text(reached.iter().map(|r| key_text(&r.via)).collect()),
                ]
            }
            GraphFunction::Neighbors => {
                let tables = service.graph_tables().ok_or_else(unavailable)?;
                let edges = tables
                    .neighbors(NeighborsCall {
                        namespace: ns,
                        graph: &call.graph,
                        keys,
                        label: call.label.clone(),
                        direction: call.direction,
                        edge_types: call.edge_types.clone(),
                        edge_filter: call.edge_filter.clone(),
                        limit_per_key: call.limit,
                    })
                    .await?;
                vec![
                    text(edges.iter().map(|e| key_text(&e.key)).collect()),
                    text(
                        edges
                            .iter()
                            .map(|e| match e.direction {
                                ExpandDirection::In => "in".to_string(),
                                _ => "out".to_string(),
                            })
                            .collect(),
                    ),
                    text(edges.iter().map(|e| e.edge_type.clone()).collect()),
                    text(edges.iter().map(|e| key_text(&e.edge)).collect()),
                    text(edges.iter().map(|e| e.other_label.clone()).collect()),
                    text(edges.iter().map(|e| key_text(&e.other_key)).collect()),
                ]
            }
            GraphFunction::Degree => {
                let tables = service.graph_tables().ok_or_else(unavailable)?;
                let degrees = tables
                    .degree(DegreeCall {
                        namespace: ns,
                        graph: &call.graph,
                        keys,
                        label: call.label.clone(),
                        direction: call.direction,
                        edge_types: call.edge_types.clone(),
                    })
                    .await?;
                let mut degree = Int64Builder::new();
                for (_, d) in &degrees {
                    degree.append_value(i64::try_from(*d).unwrap_or(i64::MAX));
                }
                vec![
                    text(degrees.iter().map(|(k, _)| key_text(k)).collect()),
                    Arc::new(degree.finish()),
                ]
            }
            GraphFunction::ShortestPath => {
                let tables = service.graph_tables().ok_or_else(unavailable)?;
                let from = keys.into_iter().next().ok_or_else(|| {
                    ServiceError::InvalidArgument(
                        "graph_shortest_path: from is one key".to_string(),
                    )
                })?;
                let steps = tables
                    .shortest_path(PathCall {
                        namespace: ns,
                        graph: &call.graph,
                        from,
                        to: call.to.clone().unwrap_or(PrimaryKey::U64(0)),
                        label: call.label.clone(),
                        direction: call.direction,
                        edge_types: call.edge_types.clone(),
                        max_hops: u8::try_from(call.limit).unwrap_or(MAX_PATH_HOPS),
                    })
                    .await?;
                let mut step = UInt32Builder::new();
                let mut edge_type = StringBuilder::new();
                let mut edge = StringBuilder::new();
                for s in &steps {
                    step.append_value(s.step);
                    edge_type.append_option(s.edge_type.as_deref());
                    edge.append_option(s.edge.as_ref().map(key_text));
                }
                vec![
                    Arc::new(step.finish()),
                    text(steps.iter().map(|s| s.label.clone()).collect()),
                    text(steps.iter().map(|s| key_text(&s.key)).collect()),
                    Arc::new(edge_type.finish()),
                    Arc::new(edge.finish()),
                ]
            }
        };
        RecordBatch::try_new(schema, columns).map_err(|err| ServiceError::Internal(err.to_string()))
    }
}

#[derive(Debug)]
pub(crate) struct GraphProvider {
    pub(crate) inner: Arc<dyn GraphBatch>,
}

#[async_trait]
impl TableProvider for GraphProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let full = self.inner.schema();
        let projection = projection.cloned();
        let schema = match &projection {
            Some(columns) => Arc::new(full.project(columns)?),
            None => full,
        };
        Ok(Arc::new(GraphExec {
            inner: self.inner.clone(),
            projection,
            properties: plan_properties(schema.clone()),
            schema,
        }))
    }
}

struct GraphExec {
    inner: Arc<dyn GraphBatch>,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for GraphExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphExec")
            .field("function", &self.inner.function())
            .field("graph", &self.inner.graph())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for GraphExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GraphExec: function={}, graph={}",
            self.inner.function(),
            self.inner.graph()
        )
    }
}

impl ExecutionPlan for GraphExec {
    fn name(&self) -> &str {
        "GraphExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "GraphExec has no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GraphExec has one partition, not {partition}"
            )));
        }
        let (inner, projection) = (self.inner.clone(), self.projection.clone());
        let stream = futures::stream::once(async move {
            let batch = inner.run().await.map_err(df_error)?;
            match projection {
                Some(columns) => batch.project(&columns).map_err(DataFusionError::from),
                None => Ok(batch),
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udtf_signatures_match_section_07() {
        let columns = |f: GraphFunction| -> Vec<String> {
            f.schema()
                .fields()
                .iter()
                .map(|c| c.name().clone())
                .collect()
        };
        assert_eq!(
            columns(GraphFunction::Expand),
            ["seed", "label", "key", "hop", "edge_type", "edge", "via"]
        );
        assert_eq!(
            columns(GraphFunction::Neighbors),
            [
                "key",
                "direction",
                "edge_type",
                "edge",
                "other_label",
                "other_key"
            ]
        );
        assert_eq!(columns(GraphFunction::Degree), ["key", "degree"]);
        assert_eq!(
            columns(GraphFunction::ShortestPath),
            ["step", "label", "key", "edge_type", "edge"]
        );
        assert!(is_query(" select 1") && is_query("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(!is_query("selective"));
    }
}
