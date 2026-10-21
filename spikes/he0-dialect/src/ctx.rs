//! The planning context: stub tables with ClickHouse-shaped schemas, stub
//! ClickHouse functions (planning only, never executed), a ClickHouse type
//! planner, and a relation planner that turns `FROM t FINAL` into an
//! extension node.

use datafusion_common::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit,
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DFSchemaRef, Result, TableReference, not_impl_err, plan_err};
use datafusion_expr::logical_plan::builder::LogicalTableSource;
use datafusion_expr::planner::{
    ContextProvider, ExprPlanner, PlannedRelation, RelationPlanner, RelationPlannerContext,
    RelationPlanning, TypePlanner,
};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Expr, Extension, HigherOrderUDF,
    LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TableSource,
    TypeSignature, UserDefinedLogicalNodeCore, Volatility, WindowUDF,
    function::AccumulatorArgs,
};
use sqlparser::ast::{self as sql, TableFactor};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

// ------------------------------------------------------------------ types

fn ts_ms() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}
fn lc_str() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}
fn list(t: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", t, true)))
}
fn map(k: DataType, v: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", k, false),
                Field::new("value", v, true),
            ])),
            false,
        )),
        false,
    )
}

/// ReplacingMergeTree metadata the FINAL node needs.
#[derive(Debug, Clone)]
pub struct Replacing {
    pub key: Vec<String>,
    pub version: String,
    pub is_deleted: Option<String>,
}

pub struct Table {
    pub schema: SchemaRef,
    pub replacing: Option<Replacing>,
}

fn tables() -> HashMap<String, Table> {
    let mut t = HashMap::new();
    let f = |n: &str, d: DataType, null: bool| Field::new(n, d, null);
    t.insert(
        "events".into(),
        Table {
            schema: Arc::new(Schema::new(vec![
                f("id", DataType::Utf8, false),
                f("name", DataType::Utf8, false),
                f("project_id", DataType::Utf8, false),
                f("profile_id", DataType::Utf8, false),
                f("session_id", DataType::Utf8, false),
                f("ts", ts_ms(), false),
                f("v", DataType::Float64, false),
                f("duration", DataType::UInt64, false),
                f("country", lc_str(), false),
                f("path", DataType::Utf8, false),
                f("properties", map(DataType::Utf8, DataType::Utf8), false),
                f("model", DataType::Utf8, true),
            ])),
            replacing: None,
        },
    );
    t.insert(
        "traces".into(),
        Table {
            schema: Arc::new(Schema::new(vec![
                f("id", DataType::Utf8, false),
                f("project_id", DataType::Utf8, false),
                f("timestamp", ts_ms(), false),
                f("name", DataType::Utf8, false),
                f("user_id", DataType::Utf8, true),
                f("tags", list(DataType::Utf8), false),
                f("metadata", map(DataType::Utf8, DataType::Utf8), false),
                f("event_ts", ts_ms(), false),
                f("is_deleted", DataType::UInt8, false),
            ])),
            replacing: Some(Replacing {
                key: vec!["project_id".into(), "id".into()],
                version: "event_ts".into(),
                is_deleted: Some("is_deleted".into()),
            }),
        },
    );
    t.insert(
        "observations".into(),
        Table {
            schema: Arc::new(Schema::new(vec![
                f("id", DataType::Utf8, false),
                f("trace_id", DataType::Utf8, false),
                f("project_id", DataType::Utf8, false),
                f("type", lc_str(), false),
                f("start_time", ts_ms(), false),
                f("end_time", ts_ms(), true),
                f("model", DataType::Utf8, true),
                f("usage_details", map(DataType::Utf8, DataType::UInt64), false),
                f("cost_details", map(DataType::Utf8, DataType::Decimal128(18, 12)), false),
                f("tags", list(DataType::Utf8), false),
                f("name", DataType::Utf8, false),
                f("v", DataType::Float64, false),
                f("metadata", map(DataType::Utf8, DataType::Utf8), false),
                f("event_ts", ts_ms(), false),
                f("is_deleted", DataType::UInt8, false),
            ])),
            replacing: Some(Replacing {
                key: vec!["project_id".into(), "trace_id".into(), "id".into()],
                version: "event_ts".into(),
                is_deleted: Some("is_deleted".into()),
            }),
        },
    );
    t.insert(
        "scores".into(),
        Table {
            schema: Arc::new(Schema::new(vec![
                f("id", DataType::Utf8, false),
                f("trace_id", DataType::Utf8, false),
                f("project_id", DataType::Utf8, false),
                f("name", DataType::Utf8, false),
                f("value", DataType::Float64, false),
                f("event_ts", ts_ms(), false),
                f("is_deleted", DataType::UInt8, false),
            ])),
            replacing: Some(Replacing {
                key: vec!["project_id".into(), "trace_id".into(), "id".into()],
                version: "event_ts".into(),
                is_deleted: Some("is_deleted".into()),
            }),
        },
    );
    // Tables the broader corpus (tier0.sql's SELECTs, HS1's select corpus)
    // reads; `t` follows HS1's differential corpus.
    let simple = |cols: Vec<Field>| Table { schema: Arc::new(Schema::new(cols)), replacing: None };
    t.insert(
        "sessions".into(),
        simple(vec![
            f("id", DataType::Utf8, false),
            f("profile_id", DataType::Utf8, false),
            f("created_at", DataType::Timestamp(TimeUnit::Second, None), false),
            f("ended_at", DataType::Timestamp(TimeUnit::Second, None), false),
        ]),
    );
    t.insert("raw_events".into(), simple(vec![f("id", DataType::Utf8, false), f("payload", DataType::Utf8, false)]));
    t.insert(
        "t".into(),
        simple(vec![
            f("k", DataType::UInt64, false),
            f("g", DataType::Utf8, false),
            f("v", DataType::Float64, false),
            f("d", DataType::Date32, false),
            f("flag", DataType::UInt8, false),
            f("vals", list(DataType::Float64), false),
        ]),
    );
    t.insert(
        "tables".into(),
        simple(vec![
            f("name", DataType::Utf8, false),
            f("engine", DataType::Utf8, false),
            f("database", DataType::Utf8, false),
        ]),
    );
    t.insert(
        "parts".into(),
        simple(vec![
            f("partition", DataType::Utf8, false),
            f("rows", DataType::UInt64, false),
            f("bytes_on_disk", DataType::UInt64, false),
            f("table", DataType::Utf8, false),
            // ClickHouse's booleans are UInt8.
            f("active", DataType::UInt8, false),
        ]),
    );
    t.insert(
        "daily".into(),
        Table {
            schema: Arc::new(Schema::new(vec![
                f("day", DataType::Date32, false),
                f("project_id", DataType::Utf8, false),
                // AggregateFunction(uniq, String): an opaque state.
                f("users", DataType::Binary, false),
                f("events", DataType::UInt64, false),
            ])),
            replacing: None,
        },
    );
    t
}

// ------------------------------------------------------------------ stubs

/// How a stub's return type follows from its arguments.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Ret {
    Fixed(DataType),
    Arg(usize),
    LastArg,
    ListOfLast,
    ElemOfArg(usize),
    MapKeysOf(usize),
    MapValuesOf(usize),
}

impl Ret {
    fn resolve(&self, name: &str, args: &[DataType]) -> Result<DataType> {
        let get = |i: usize| {
            args.get(i).cloned().ok_or_else(|| {
                datafusion_common::DataFusionError::Plan(format!("{name}: missing argument {i}"))
            })
        };
        Ok(match self {
            Ret::Fixed(t) => t.clone(),
            Ret::Arg(i) => get(*i)?,
            Ret::LastArg => get(args.len().saturating_sub(1))?,
            Ret::ListOfLast => list(get(args.len().saturating_sub(1))?),
            Ret::ElemOfArg(i) => match get(*i)? {
                DataType::List(f) => f.data_type().clone(),
                other => return plan_err!("{name}: expected an array, got {other}"),
            },
            Ret::MapKeysOf(i) | Ret::MapValuesOf(i) => match get(*i)? {
                DataType::Map(entries, _) => match entries.data_type() {
                    DataType::Struct(fs) => {
                        let idx = if matches!(self, Ret::MapKeysOf(_)) { 0 } else { 1 };
                        list(fs[idx].data_type().clone())
                    }
                    _ => return plan_err!("{name}: bad map"),
                },
                other => return plan_err!("{name}: expected a map, got {other}"),
            },
        })
    }
}

fn any_sig() -> Signature {
    Signature::one_of(vec![TypeSignature::Nullary, TypeSignature::VariadicAny], Volatility::Immutable)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StubScalar {
    name: String,
    ret: Ret,
    sig: Signature,
}

impl ScalarUDFImpl for StubScalar {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.ret.resolve(&self.name, args)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("stub {} is planning-only", self.name)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StubAgg {
    name: String,
    ret: Ret,
    sig: Signature,
}

impl AggregateUDFImpl for StubAgg {
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.ret.resolve(&self.name, args)
    }
    fn accumulator(&self, _a: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        not_impl_err!("stub {} is planning-only", self.name)
    }
}

fn scalar_stubs() -> Vec<(&'static str, Ret)> {
    use DataType::*;
    let b = || Ret::Fixed(Boolean);
    vec![
        ("toStartOfDay", Ret::Arg(0)),
        ("toStartOfHour", Ret::Arg(0)),
        ("toStartOfMinute", Ret::Arg(0)),
        ("toStartOfMonth", Ret::Fixed(Date32)),
        ("toStartOfWeek", Ret::Fixed(Date32)),
        ("toStartOfInterval", Ret::Arg(0)),
        ("toDate", Ret::Fixed(Date32)),
        ("toDateTime", Ret::Fixed(Timestamp(TimeUnit::Second, None))),
        ("toDateTime64", Ret::Fixed(ts_ms())),
        ("now", Ret::Fixed(Timestamp(TimeUnit::Second, None))),
        ("now64", Ret::Fixed(ts_ms())),
        ("today", Ret::Fixed(Date32)),
        ("toYYYYMM", Ret::Fixed(UInt32)),
        ("toYear", Ret::Fixed(UInt16)),
        ("toMonth", Ret::Fixed(UInt8)),
        ("dateDiff", Ret::Fixed(Int64)),
        ("toTimeZone", Ret::Arg(0)),
        ("formatDateTime", Ret::Fixed(Utf8)),
        ("parseDateTimeBestEffort", Ret::Fixed(Timestamp(TimeUnit::Second, None))),
        ("toIntervalDay", Ret::Fixed(Interval(IntervalUnit::MonthDayNano))),
        ("toIntervalMinute", Ret::Fixed(Interval(IntervalUnit::MonthDayNano))),
        ("toIntervalHour", Ret::Fixed(Interval(IntervalUnit::MonthDayNano))),
        ("if", Ret::Arg(1)),
        ("multiIf", Ret::Arg(1)),
        ("ifNull", Ret::Arg(0)),
        ("toString", Ret::Fixed(Utf8)),
        ("toUInt32", Ret::Fixed(UInt32)),
        ("toUInt64", Ret::Fixed(UInt64)),
        ("toFloat64", Ret::Fixed(Float64)),
        ("JSONExtractString", Ret::Fixed(Utf8)),
        ("JSONExtractRaw", Ret::Fixed(Utf8)),
        ("JSONExtract", Ret::Fixed(Utf8)),
        ("JSONHas", b()),
        ("mapKeys", Ret::MapKeysOf(0)),
        ("mapValues", Ret::MapValuesOf(0)),
        ("mapContains", b()),
        ("has", b()),
        ("hasAny", b()),
        ("empty", b()),
        ("notEmpty", b()),
        // ClickHouse's length works on strings, arrays and maps.
        ("length", Ret::Fixed(UInt64)),
        ("positionCaseInsensitive", Ret::Fixed(UInt64)),
        ("match", b()),
        ("extract", Ret::Fixed(Utf8)),
        ("cityHash64", Ret::Fixed(UInt64)),
        ("sipHash64", Ret::Fixed(UInt64)),
        ("intDiv", Ret::Arg(0)),
        ("generateUUIDv4", Ret::Fixed(Utf8)),
        ("toUUID", Ret::Fixed(Utf8)),
        ("arrayEnumerate", Ret::Fixed(list(UInt32))),
        ("arrayElement", Ret::ElemOfArg(0)),
        ("arraySlice", Ret::Arg(0)),
        ("arraySort", Ret::Arg(0)),
        ("version", Ret::Fixed(Utf8)),
        ("currentDatabase", Ret::Fixed(Utf8)),
        ("upper", Ret::Arg(0)),
        ("JSONExtractKeysAndValues", Ret::Fixed(list(Utf8))),
        // ClickHouse's array(...) is DataFusion's make_array(...).
        ("array", Ret::Fixed(list(Utf8))),
    ]
}

fn agg_stubs() -> Vec<(&'static str, Ret)> {
    use DataType::*;
    vec![
        ("uniq", Ret::Fixed(UInt64)),
        ("uniqExact", Ret::Fixed(UInt64)),
        ("uniqCombined", Ret::Fixed(UInt64)),
        ("quantile", Ret::Fixed(Float64)),
        ("quantileExact", Ret::Fixed(Float64)),
        ("quantileTDigest", Ret::Fixed(Float64)),
        ("quantiles", Ret::Fixed(list(Float64))),
        ("argMax", Ret::Arg(0)),
        ("argMin", Ret::Arg(0)),
        ("any", Ret::Arg(0)),
        ("anyLast", Ret::Arg(0)),
        ("groupArray", Ret::ListOfLast),
        ("groupUniqArray", Ret::ListOfLast),
        ("windowFunnel", Ret::Fixed(UInt8)),
        ("retention", Ret::Fixed(list(UInt8))),
        ("sequenceMatch", Ret::Fixed(UInt8)),
        ("topK", Ret::ListOfLast),
        ("uniqState", Ret::Fixed(Binary)),
        ("uniqMerge", Ret::Fixed(UInt64)),
        ("uniqMergeState", Ret::Fixed(Binary)),
        ("lagInFrame", Ret::Arg(0)),
        ("histogram", Ret::Fixed(list(Utf8))),
    ]
}

// ------------------------------------------------------------------ context

pub struct Ctx {
    tables: HashMap<String, Table>,
    scalars: HashMap<String, Arc<ScalarUDF>>,
    aggs: HashMap<String, Arc<AggregateUDF>>,
    windows: HashMap<String, Arc<WindowUDF>>,
    hofs: HashMap<String, Arc<HigherOrderUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
    relation_planners: Vec<Arc<dyn RelationPlanner>>,
    type_planner: Option<Arc<dyn TypePlanner>>,
    options: ConfigOptions,
    /// DataFusion lowercases unquoted function names whatever
    /// `enable_ident_normalization` says. Without the frontend (which quotes
    /// camelCase names), lookups fold case so the raw column shows structural
    /// gaps rather than name misses.
    folded: bool,
}

impl Ctx {
    /// `frontend` adds the spike's ClickHouse type planner and FINAL relation
    /// planner; without it the context is DataFusion as shipped plus stubs.
    pub fn new(frontend: bool) -> Self {
        let mut scalars = HashMap::new();
        for f in datafusion_functions::all_default_functions()
            .into_iter()
            .chain(datafusion_functions_nested::all_default_nested_functions())
        {
            scalars.insert(f.name().to_string(), f);
        }
        let mut aggs = HashMap::new();
        for f in datafusion_functions_aggregate::all_default_aggregate_functions() {
            aggs.insert(f.name().to_string(), f);
        }
        let mut windows = HashMap::new();
        for f in datafusion_functions_window::all_default_window_functions() {
            windows.insert(f.name().to_string(), f);
        }
        let mut hofs = HashMap::new();
        for f in datafusion_functions_nested::all_default_higher_order_functions() {
            hofs.insert(f.name().to_string(), f);
        }
        for (n, r) in scalar_stubs() {
            scalars.insert(
                n.to_string(),
                Arc::new(ScalarUDF::new_from_impl(StubScalar { name: n.into(), ret: r, sig: any_sig() })),
            );
        }
        for (n, r) in agg_stubs() {
            aggs.insert(
                n.to_string(),
                Arc::new(AggregateUDF::new_from_impl(StubAgg { name: n.into(), ret: r, sig: any_sig() })),
            );
        }
        let expr_planners: Vec<Arc<dyn ExprPlanner>> = vec![
            Arc::new(datafusion_functions::core::planner::CoreFunctionPlanner::default()),
            Arc::new(datafusion_functions_nested::planner::NestedFunctionPlanner),
            Arc::new(datafusion_functions_nested::planner::FieldAccessPlanner),
            Arc::new(datafusion_functions::datetime::planner::DatetimeFunctionPlanner),
            Arc::new(datafusion_functions::unicode::planner::UnicodeFunctionPlanner),
            Arc::new(datafusion_functions_aggregate::planner::AggregateFunctionPlanner),
            Arc::new(datafusion_functions_window::planner::WindowFunctionPlanner),
        ];
        let tables = tables();
        let mut options = ConfigOptions::default();
        options.sql_parser.enable_ident_normalization = false;
        let (relation_planners, type_planner): (Vec<Arc<dyn RelationPlanner>>, Option<Arc<dyn TypePlanner>>) =
            if frontend {
                let meta = tables
                    .iter()
                    .filter_map(|(k, v)| v.replacing.clone().map(|r| (k.clone(), r)))
                    .collect();
                (vec![Arc::new(FinalPlanner { meta })], Some(Arc::new(ChTypes)))
            } else {
                (vec![], None)
            };
        Ctx {
            tables,
            scalars,
            aggs,
            windows,
            hofs,
            expr_planners,
            relation_planners,
            type_planner,
            options,
            folded: !frontend,
        }
    }
}

impl ContextProvider for Ctx {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(t) => Ok(Arc::new(LogicalTableSource::new(t.schema.clone()))),
            None => plan_err!("table '{name}' not found"),
        }
    }
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
    fn get_relation_planners(&self) -> &[Arc<dyn RelationPlanner>] {
        &self.relation_planners
    }
    fn get_type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        self.type_planner.clone()
    }
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.scalars.get(name).or_else(|| self.scalars.get(&name.to_lowercase())).cloned().or_else(|| {
            self.folded
                .then(|| self.scalars.iter().find(|(k, _)| k.to_lowercase() == name).map(|(_, v)| v.clone()))
                .flatten()
        })
    }
    fn get_higher_order_meta(&self, name: &str) -> Option<Arc<HigherOrderUDF>> {
        self.hofs.get(name).cloned()
    }
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggs.get(name).or_else(|| self.aggs.get(&name.to_lowercase())).cloned().or_else(|| {
            self.folded
                .then(|| self.aggs.iter().find(|(k, _)| k.to_lowercase() == name).map(|(_, v)| v.clone()))
                .flatten()
        })
    }
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.windows.get(name).or_else(|| self.windows.get(&name.to_lowercase())).cloned()
    }
    fn get_variable_type(&self, _v: &[String]) -> Option<DataType> {
        None
    }
    fn options(&self) -> &ConfigOptions {
        &self.options
    }
    fn udf_names(&self) -> Vec<String> {
        self.scalars.keys().cloned().collect()
    }
    fn higher_order_function_names(&self) -> Vec<String> {
        self.hofs.keys().cloned().collect()
    }
    fn udaf_names(&self) -> Vec<String> {
        self.aggs.keys().cloned().collect()
    }
    fn udwf_names(&self) -> Vec<String> {
        self.windows.keys().cloned().collect()
    }
}

// ------------------------------------------------------------------ ClickHouse types

#[derive(Debug)]
pub struct ChTypes;

pub fn ch_type(t: &sql::DataType) -> Option<(DataType, bool)> {
    use sql::DataType as S;
    Some(match t {
        S::UInt8 => (DataType::UInt8, false),
        S::UInt16 => (DataType::UInt16, false),
        S::UInt32 => (DataType::UInt32, false),
        S::UInt64 => (DataType::UInt64, false),
        // sqlparser reads ClickHouse's Int8 as Postgres's int8 (64 bits).
        S::Int8(None) => (DataType::Int8, false),
        S::Int16 => (DataType::Int16, false),
        S::Int32 => (DataType::Int32, false),
        S::Int64 => (DataType::Int64, false),
        S::Float32 => (DataType::Float32, false),
        S::Float64 => (DataType::Float64, false),
        S::String(_) => (DataType::Utf8, false),
        S::FixedString(n) => (DataType::FixedSizeBinary(*n as i32), false),
        S::Uuid => (DataType::Utf8, false),
        S::Bool | S::Boolean => (DataType::Boolean, false),
        S::Date | S::Date32 => (DataType::Date32, false),
        S::Datetime(_) => (DataType::Timestamp(TimeUnit::Second, None), false),
        S::Datetime64(p, tz) => {
            let unit = match p {
                0 => TimeUnit::Second,
                1..=3 => TimeUnit::Millisecond,
                4..=6 => TimeUnit::Microsecond,
                _ => TimeUnit::Nanosecond,
            };
            (DataType::Timestamp(unit, tz.clone().map(Into::into)), false)
        }
        S::Decimal(sql::ExactNumberInfo::PrecisionAndScale(p, s)) => {
            (DataType::Decimal128(*p as u8, *s as i8), false)
        }
        S::Nullable(inner) => (ch_type(inner)?.0, true),
        S::LowCardinality(inner) => {
            let (i, n) = ch_type(inner)?;
            (DataType::Dictionary(Box::new(DataType::Int32), Box::new(i)), n)
        }
        S::Array(sql::ArrayElemTypeDef::Parenthesis(inner))
        | S::Array(sql::ArrayElemTypeDef::AngleBracket(inner)) => (list(ch_type(inner)?.0), false),
        S::Map(k, v) => (map(ch_type(k)?.0, ch_type(v)?.0), false),
        _ => return None,
    })
}

impl TypePlanner for ChTypes {
    fn plan_type_field(&self, t: &sql::DataType) -> Result<Option<FieldRef>> {
        Ok(ch_type(t).map(|(d, n)| Arc::new(Field::new("", d, n))))
    }
}

// ------------------------------------------------------------------ FINAL

/// `FROM t FINAL` on a ReplacingMergeTree table: keep, per sorting key, the
/// row with the highest version, and drop it if `is_deleted`. An extension
/// node, so HE1's physical planner can choose a merge of sorted runs (the
/// table's sort order) rather than a hash or a window.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct ReplacingFinal {
    pub input: LogicalPlan,
    pub key: Vec<String>,
    pub version: String,
    pub is_deleted: Option<String>,
}

impl UserDefinedLogicalNodeCore for ReplacingFinal {
    fn name(&self) -> &str {
        "ReplacingFinal"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ReplacingFinal: key=[{}], version={}, is_deleted={}",
            self.key.join(", "),
            self.version,
            self.is_deleted.as_deref().unwrap_or("-")
        )
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self { input: inputs.swap_remove(0), ..self.clone() })
    }
}

#[derive(Debug)]
pub struct FinalPlanner {
    meta: HashMap<String, Replacing>,
}

impl RelationPlanner for FinalPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        ctx: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        // sqlparser 0.62 reads `FROM t FINAL` as the alias FINAL.
        let TableFactor::Table { name, alias: Some(alias), args: None, .. } = &relation else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        if !alias.name.value.eq_ignore_ascii_case("final") {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }
        let table = name.0.last().map(|p| p.to_string()).unwrap_or_default();
        let Some(meta) = self.meta.get(&table) else {
            return plan_err!("FINAL on '{table}', which is not a ReplacingMergeTree");
        };
        let mut plain = relation.clone();
        if let TableFactor::Table { alias, .. } = &mut plain {
            *alias = None;
        }
        let input = ctx.plan(plain)?;
        let node = ReplacingFinal {
            input,
            key: meta.key.clone(),
            version: meta.version.clone(),
            is_deleted: meta.is_deleted.clone(),
        };
        let plan = LogicalPlan::Extension(Extension { node: Arc::new(node) });
        let alias = sql::TableAlias { explicit: false, name: sql::Ident::new(table), columns: vec![], at: None };
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(plan, Some(alias)))))
    }
}

// ------------------------------------------------------------------ registry

impl datafusion_expr::registry::FunctionRegistry for Ctx {
    fn udfs(&self) -> std::collections::HashSet<String> {
        self.scalars.keys().cloned().collect()
    }
    fn higher_order_function_names(&self) -> std::collections::HashSet<String> {
        self.hofs.keys().cloned().collect()
    }
    fn udafs(&self) -> std::collections::HashSet<String> {
        self.aggs.keys().cloned().collect()
    }
    fn udwfs(&self) -> std::collections::HashSet<String> {
        self.windows.keys().cloned().collect()
    }
    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        self.scalars.get(name).cloned().ok_or_else(|| datafusion_common::DataFusionError::Plan(format!("no udf {name}")))
    }
    fn higher_order_function(&self, name: &str) -> Result<Arc<HigherOrderUDF>> {
        self.hofs.get(name).cloned().ok_or_else(|| datafusion_common::DataFusionError::Plan(format!("no hof {name}")))
    }
    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        self.aggs.get(name).cloned().ok_or_else(|| datafusion_common::DataFusionError::Plan(format!("no udaf {name}")))
    }
    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
        self.windows.get(name).cloned().ok_or_else(|| datafusion_common::DataFusionError::Plan(format!("no udwf {name}")))
    }
    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        self.expr_planners.clone()
    }
}

/// The optimizer's config with a function registry (rules such as
/// `ReplaceDistinctWithAggregate` look up `first_value`); a SessionState
/// would provide it.
pub struct OptCfg<'a> {
    pub inner: datafusion_optimizer::OptimizerContext,
    pub reg: &'a Ctx,
}

impl datafusion_optimizer::OptimizerConfig for OptCfg<'_> {
    fn query_execution_start_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.inner.query_execution_start_time()
    }
    fn alias_generator(&self) -> &Arc<datafusion_common::alias::AliasGenerator> {
        self.inner.alias_generator()
    }
    fn options(&self) -> Arc<ConfigOptions> {
        self.inner.options()
    }
    fn function_registry(&self) -> Option<&dyn datafusion_expr::registry::FunctionRegistry> {
        Some(self.reg)
    }
}
