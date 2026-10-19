//! SDKP's `rust_builders_match_corpus`.
//!
//! Every case in `sdks/fixtures/builders/` is driven through the public
//! builders from its neutral spec (the notation is in that directory's
//! README), and the result is compared with `expect` after both are decoded
//! into the named message, so key order and number spelling do not matter.

use std::path::{Path, PathBuf};

use loams::builders::search::{ExpandOptions, HighlightOptions, SelectOptions, VectorOptions};
use loams::builders::{DocId, Filter, Fusion, Schema, Search, Sort, TextRetriever};
use loams::collection_v1::{Query, SearchRequest};
use serde_json::Value;

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/builders")
}

fn cases() -> Vec<Value> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(corpus_dir())
        .expect("the builder corpus")
        .map(|entry| entry.expect("an entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    files
        .iter()
        .map(|path| {
            serde_json::from_str(&std::fs::read_to_string(path).expect("readable")).expect("JSON")
        })
        .collect()
}

fn is_call(value: &Value) -> bool {
    value.get("op").is_some()
}

fn args(call: &Value) -> Vec<Value> {
    call.get("args")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn s(value: &Value) -> &str {
    value.as_str().expect("a string argument")
}

fn u(value: Option<&Value>) -> Option<u32> {
    value
        .and_then(Value::as_u64)
        .map(|n| u32::try_from(n).expect("a u32"))
}

fn doc_id(value: &Value) -> DocId {
    match value {
        Value::Number(n) => DocId::Uint(n.as_u64().expect("a uint id")),
        Value::String(id) => DocId::Str(id.clone()),
        other => DocId::Uuid(other["uuid"].as_str().expect("a uuid id").to_owned()),
    }
}

fn filter(call: &Value) -> Filter {
    let a = args(call);
    match call["op"].as_str().expect("an op") {
        "all" => Filter::all(),
        "none" => Filter::none(),
        "eq" => Filter::eq(s(&a[0]), a[1].clone()),
        "in" => Filter::is_in(s(&a[0]), a[1].as_array().expect("values").clone()),
        "range" => {
            let mut range = Filter::range(s(&a[0]));
            let bounds = a[1].as_object().expect("bounds");
            for (key, value) in bounds {
                range = match key.as_str() {
                    "gt" => range.gt(value.clone()),
                    "gte" => range.gte(value.clone()),
                    "lt" => range.lt(value.clone()),
                    "lte" => range.lte(value.clone()),
                    other => panic!("no bound {other}"),
                };
            }
            range.build()
        }
        "exists" => Filter::exists(s(&a[0])),
        "is_null" => Filter::is_null(s(&a[0])),
        "is_empty" => Filter::is_empty(s(&a[0])),
        "prefix" => Filter::prefix(s(&a[0]), s(&a[1])),
        "wildcard" => Filter::wildcard(s(&a[0]), s(&a[1])),
        "match" => Filter::match_text(s(&a[0]), s(&a[1])),
        "phrase" => Filter::phrase(s(&a[0]), s(&a[1]), u(a.get(2))),
        "ids" => Filter::ids(a.iter().map(doc_id)),
        "and" => Filter::and(a.iter().map(filter)),
        "or" => Filter::or(a.iter().map(filter)),
        "not" => Filter::not(a.iter().map(filter)),
        "filter" => Filter::filter(a.iter().map(filter)),
        other => panic!("no filter verb {other}"),
    }
}

fn fusion(call: &Value) -> Fusion {
    let a = args(call);
    match call["op"].as_str().expect("an op") {
        "rrf" => Fusion::rrf(u(a.first())),
        "dbsf" => Fusion::dbsf(),
        "weighted" => Fusion::weighted(a.iter().map(|w| w.as_f64().expect("a weight"))),
        other => panic!("no fusion {other}"),
    }
}

fn sort(call: &Value) -> Sort {
    match call["op"].as_str().expect("an op") {
        "score" => Sort::score(None),
        "pk" => Sort::pk(None),
        other => panic!("no sort {other}"),
    }
}

fn strings(value: Option<&Value>) -> Option<Vec<String>> {
    value
        .and_then(Value::as_array)
        .map(|list| list.iter().map(|v| s(v).to_owned()).collect())
}

fn schema(calls: &[Value]) -> Value {
    let mut builder = Schema::new();
    for call in calls {
        let a = args(call);
        builder = match call["op"].as_str().expect("an op") {
            "text" => builder.text(s(&a[0])),
            "keyword" => builder.keyword(s(&a[0])),
            "i64" => builder.i64(s(&a[0])),
            "f64" => builder.f64(s(&a[0])),
            "bool" => builder.bool(s(&a[0])),
            "timestamp" => builder.timestamp(s(&a[0])),
            "vector" => builder.vector(s(&a[0]), u(a.get(1)).expect("a dim")),
            other => panic!("no schema verb {other}"),
        };
    }
    builder.to_json()
}

fn search(spec: &Value) -> Search {
    let mut builder = Search::on(s(&spec["on"]));
    for call in spec["calls"].as_array().expect("calls") {
        let a = args(call);
        let opts = a
            .last()
            .filter(|last| last.is_object() && !is_call(last))
            .cloned()
            .unwrap_or(Value::Null);
        builder = match call["op"].as_str().expect("an op") {
            "text" => {
                let fields = strings(opts.get("fields")).unwrap_or_default();
                builder.text(TextRetriever::fields(s(&a[0]), fields))
            }
            "vector" => {
                let values: Vec<f32> = a[1]
                    .as_array()
                    .expect("values")
                    .iter()
                    .map(|v| v.as_f64().expect("a float") as f32)
                    .collect();
                builder.vector(
                    s(&a[0]),
                    &values,
                    VectorOptions {
                        k: u(opts.get("k")),
                        nprobes: u(opts.get("nprobes")),
                        refine_factor: u(opts.get("refine_factor")),
                        ef: u(opts.get("ef")),
                        ..VectorOptions::default()
                    },
                )
            }
            "fuse" => builder.fuse(fusion(&a[0])),
            "filter" => builder.filter(filter(&a[0])),
            "highlight" => builder.highlight(s(&a[0]), HighlightOptions::default()),
            "select" => builder.select(SelectOptions {
                include: strings(opts.get("include")),
                exclude: strings(opts.get("exclude")),
                ..SelectOptions::default()
            }),
            "limit" => builder.limit(u(a.first()).expect("a limit")),
            "sort" => builder.sort(a.iter().map(sort)),
            "after" => builder.after(a[0].as_array().expect("sort values").clone()),
            "expand" => builder.expand(
                s(&a[0]),
                ExpandOptions {
                    from_field: opts.get("from_field").map(|v| s(v).to_owned()),
                    hops: u(opts.get("hops")),
                    ..ExpandOptions::default()
                },
            ),
            other => panic!("no search verb {other}"),
        };
    }
    builder
}

/// `rust_builders_match_corpus`: every builder case, through the public
/// builders.
#[test]
fn rust_builders_match_corpus() {
    let cases = cases();
    let names: Vec<&str> = cases
        .iter()
        .map(|c| c["name"].as_str().expect("a name"))
        .collect();
    for expected in [
        "schema_notes",
        "filter_term",
        "filter_bool_nested",
        "filter_range_open",
        "filter_ids",
        "search_text_filter_highlight_select",
        "search_vector_ann_params",
        "search_hybrid_rrf",
        "search_hybrid_dbsf",
        "search_hybrid_weighted",
        "search_after_page",
        "search_expand",
    ] {
        assert!(names.contains(&expected), "the corpus has {expected}");
    }
    for case in &cases {
        let name = case["name"].as_str().expect("a name");
        match case["builder"].as_str().expect("a builder") {
            "schema" => {
                assert_eq!(
                    schema(case["spec"].as_array().expect("calls")),
                    case["expect"],
                    "{name}"
                );
            }
            "filter" => {
                let built: Query = filter(&case["spec"]).to_query().expect("decodes");
                let expected: Query =
                    serde_json::from_value(case["expect"].clone()).expect("expect decodes");
                assert_eq!(built, expected, "{name}");
            }
            "search" => {
                let built: SearchRequest = search(&case["spec"]).build().expect("builds");
                let expected: SearchRequest =
                    serde_json::from_value(case["expect"].clone()).expect("expect decodes");
                assert_eq!(built, expected, "{name}");
            }
            other => panic!("no builder {other}"),
        }
    }
}

#[test]
fn a_weighted_fusion_with_the_wrong_number_of_weights_is_refused_before_any_call() {
    let error = Search::on("notes")
        .text(TextRetriever::fields("refund", ["body"]))
        .vector("embedding", &[1.0, 0.0, 0.0, 0.0], VectorOptions::default())
        .fuse(Fusion::weighted([1.0]))
        .build()
        .expect_err("refused");
    assert_eq!(error.code, loams::Code::InvalidArgument);
    assert_eq!(
        error.metadata().get("field").map(String::as_str),
        Some("fusion.weighted_sum.weights")
    );
}
