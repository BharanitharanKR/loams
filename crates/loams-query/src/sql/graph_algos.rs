//! The graph algorithm table functions (GR1 Task 22; design §07 §5.3): `pagerank`, `wcc` and
//! `leiden`, answered by Loams Graph on its adjacency index (§48 §26.3).
//!
//! Arguments are §07 §5.3's, in its order, by position (R21.1: DataFusion takes no named
//! table-function arguments); `NULL` takes an argument's default:
//!
//! ```sql
//! pagerank(graph, label, edge_types, damping, max_iterations, tolerance)   -> (label, key, rank)
//! wcc(graph, edge_types)                                                    -> (label, key, component)
//! leiden(graph, edge_types, weight, resolution, max_levels, max_cluster_size, seed)
//!                                         -> (label, key, level, community, parent_community)
//! ```
//!
//! Keys are text, as the other graph functions answer them (R21.4). A vertex without a `_key`
//! takes part in the computation and has no row.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Float64Builder, RecordBatch, StringBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DfResult, plan_err};
use datafusion::scalar::ScalarValue;

use super::graph_udtf::{
    GraphBatch, GraphProvider, constants, key_text, text_of, types_of, unavailable, whole_of,
};
use crate::error::ServiceError;
use crate::graph::{AlgorithmCall, AlgorithmSpec, AlgorithmValue};
use crate::sql::SqlScope;

/// Which algorithm a function runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlgorithmFunction {
    PageRank,
    Wcc,
    Leiden,
}

impl AlgorithmFunction {
    /// Every algorithm function.
    pub const ALL: [Self; 3] = [Self::PageRank, Self::Wcc, Self::Leiden];

    /// The function's name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::PageRank => "pagerank",
            Self::Wcc => "wcc",
            Self::Leiden => "leiden",
        }
    }

    /// The arguments, in order (§07 §5.3).
    #[must_use]
    pub fn arguments(self) -> &'static [&'static str] {
        match self {
            Self::PageRank => &[
                "graph",
                "label",
                "edge_types",
                "damping",
                "max_iterations",
                "tolerance",
            ],
            Self::Wcc => &["graph", "edge_types"],
            Self::Leiden => &[
                "graph",
                "edge_types",
                "weight",
                "resolution",
                "max_levels",
                "max_cluster_size",
                "seed",
            ],
        }
    }

    /// The output columns (§07 §5.3).
    #[must_use]
    pub fn schema(self) -> SchemaRef {
        let text = |name: &str| Field::new(name, DataType::Utf8, false);
        let mut fields = vec![text("label"), text("key")];
        match self {
            Self::PageRank => fields.push(Field::new("rank", DataType::Float64, false)),
            Self::Wcc => fields.push(Field::new("component", DataType::UInt64, false)),
            Self::Leiden => fields.extend([
                Field::new("level", DataType::UInt32, false),
                Field::new("community", DataType::UInt64, false),
                Field::new("parent_community", DataType::UInt64, true),
            ]),
        }
        Arc::new(Schema::new(fields))
    }
}

fn number_of(function: &str, what: &str, value: &ScalarValue) -> DfResult<f64> {
    let n = match value {
        ScalarValue::Float64(Some(x)) => Some(*x),
        ScalarValue::Float32(Some(x)) => Some(f64::from(*x)),
        ScalarValue::Int64(Some(n)) => Some(*n as f64),
        ScalarValue::Int32(Some(n)) => Some(f64::from(*n)),
        ScalarValue::UInt64(Some(n)) => Some(*n as f64),
        ScalarValue::UInt32(Some(n)) => Some(f64::from(*n)),
        ScalarValue::Decimal128(Some(v), _, scale) => {
            Some(*v as f64 / 10f64.powi(i32::from(*scale)))
        }
        _ => None,
    };
    match n {
        Some(n) if n.is_finite() => Ok(n),
        _ => plan_err!("{function}: {what} must be a number"),
    }
}

/// One parsed call.
#[derive(Clone, Debug)]
struct Call {
    function: AlgorithmFunction,
    graph: String,
    edge_types: Vec<String>,
    spec: AlgorithmSpec,
}

impl Call {
    fn parse(
        function: AlgorithmFunction,
        exprs: &[datafusion::logical_expr::Expr],
    ) -> DfResult<Self> {
        let name = function.name();
        let names = function.arguments();
        if exprs.is_empty() || exprs.len() > names.len() {
            return plan_err!(
                "{name} takes 1 to {} arguments ({}), got {}",
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
        let edge_types = at("edge_types")
            .map(|v| types_of(name, v))
            .transpose()?
            .unwrap_or_default();
        let number = |arg: &str, default: f64| -> DfResult<f64> {
            at(arg).map_or(Ok(default), |v| number_of(name, arg, v))
        };
        let whole = |arg: &str, default: u64| -> DfResult<u64> {
            at(arg).map_or(Ok(default), |v| whole_of(name, arg, v))
        };
        let spec = match function {
            AlgorithmFunction::PageRank => AlgorithmSpec::PageRank {
                label: at("label").map(|v| text_of(name, "label", v)).transpose()?,
                damping: number("damping", 0.85)?,
                max_iterations: u32::try_from(whole("max_iterations", 20)?).unwrap_or(u32::MAX),
                tolerance: number("tolerance", 1e-6)?,
            },
            AlgorithmFunction::Wcc => AlgorithmSpec::Wcc,
            AlgorithmFunction::Leiden => AlgorithmSpec::Leiden {
                weight: at("weight")
                    .map(|v| text_of(name, "weight", v))
                    .transpose()?,
                resolution: number("resolution", 1.0)?,
                max_levels: u32::try_from(whole("max_levels", 4)?).unwrap_or(u32::MAX),
                max_cluster_size: u32::try_from(whole("max_cluster_size", 1000)?)
                    .unwrap_or(u32::MAX),
                seed: whole("seed", 42)?,
            },
        };
        Ok(Self {
            function,
            graph,
            edge_types,
            spec,
        })
    }
}

/// One of the three functions, bound to a namespace's scope.
#[derive(Debug)]
pub struct AlgorithmTableFunction {
    scope: SqlScope,
    function: AlgorithmFunction,
}

impl AlgorithmTableFunction {
    pub(crate) fn new(scope: SqlScope, function: AlgorithmFunction) -> Self {
        Self { scope, function }
    }
}

impl TableFunctionImpl for AlgorithmTableFunction {
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
        let tables = self.scope.service.graph_tables().ok_or_else(unavailable)?;
        let rows = tables
            .algorithm(AlgorithmCall {
                namespace: self.scope.ns.as_str(),
                graph: &self.call.graph,
                edge_types: self.call.edge_types.clone(),
                spec: self.call.spec.clone(),
            })
            .await?;
        let mut label = StringBuilder::new();
        let mut key = StringBuilder::new();
        for row in &rows {
            label.append_value(&row.label);
            key.append_value(key_text(&row.key));
        }
        let mut columns: Vec<ArrayRef> = vec![Arc::new(label.finish()), Arc::new(key.finish())];
        match self.call.function {
            AlgorithmFunction::PageRank => {
                let mut rank = Float64Builder::new();
                for row in &rows {
                    if let AlgorithmValue::Rank(r) = row.value {
                        rank.append_value(r);
                    } else {
                        rank.append_value(f64::NAN);
                    }
                }
                columns.push(Arc::new(rank.finish()));
            }
            AlgorithmFunction::Wcc => {
                let mut component = UInt64Builder::new();
                for row in &rows {
                    component.append_value(match row.value {
                        AlgorithmValue::Component(c) => c,
                        _ => 0,
                    });
                }
                columns.push(Arc::new(component.finish()));
            }
            AlgorithmFunction::Leiden => {
                let mut level = UInt32Builder::new();
                let mut community = UInt64Builder::new();
                let mut parent = UInt64Builder::new();
                for row in &rows {
                    let (l, c, p) = match row.value {
                        AlgorithmValue::Community {
                            level,
                            community,
                            parent_community,
                        } => (level, community, parent_community),
                        _ => (0, 0, None),
                    };
                    level.append_value(l);
                    community.append_value(c);
                    parent.append_option(p);
                }
                columns.push(Arc::new(level.finish()));
                columns.push(Arc::new(community.finish()));
                columns.push(Arc::new(parent.finish()));
            }
        }
        RecordBatch::try_new(self.call.function.schema(), columns)
            .map_err(|err| ServiceError::Internal(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn algorithm_signatures_match_section_07() {
        let columns = |f: AlgorithmFunction| -> Vec<String> {
            f.schema()
                .fields()
                .iter()
                .map(|c| c.name().clone())
                .collect()
        };
        assert_eq!(
            columns(AlgorithmFunction::PageRank),
            ["label", "key", "rank"]
        );
        assert_eq!(
            columns(AlgorithmFunction::Wcc),
            ["label", "key", "component"]
        );
        assert_eq!(
            columns(AlgorithmFunction::Leiden),
            ["label", "key", "level", "community", "parent_community"]
        );
        assert_eq!(
            AlgorithmFunction::Leiden.arguments(),
            [
                "graph",
                "edge_types",
                "weight",
                "resolution",
                "max_levels",
                "max_cluster_size",
                "seed"
            ]
        );
    }
}
