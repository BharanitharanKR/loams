//! The search IR's [`Query`] evaluated over a vertex's or an edge's properties (GR1 Task 20): an
//! expansion's `edge_filter` and `vertex_filter`.
//!
//! The structured subset is served: `match_all`, `match_none`, `term`, `terms`, `range`,
//! `exists`, `is_null`, `prefix`, `ids` (over `_key`) and `bool` (with `minimum_should_match`).
//! A full-text query needs a collection's analyzers and index, which a graph element does not
//! have: it is refused.

use std::cmp::Ordering;

use loams_query::ServiceError;
use loams_query::ir::{FieldValue, Query};
use serde_json::{Map, Value};

/// Checks that `query` is in the served subset.
///
/// # Errors
///
/// `InvalidArgument` naming the first query that is not.
pub fn check(query: &Query, what: &str) -> Result<(), ServiceError> {
    match query {
        Query::MatchAll
        | Query::MatchNone
        | Query::Term { .. }
        | Query::Terms { .. }
        | Query::Range { .. }
        | Query::Exists { .. }
        | Query::IsNull { .. }
        | Query::Prefix { .. }
        | Query::Ids(_) => Ok(()),
        Query::Bool {
            must,
            should,
            must_not,
            filter,
            ..
        } => must
            .iter()
            .chain(should)
            .chain(must_not)
            .chain(filter)
            .try_for_each(|q| check(q, what)),
        Query::Boost { query, .. } | Query::ConstantScore { query, .. } => check(query, what),
        _ => Err(ServiceError::InvalidArgument(format!(
            "expand.{what} takes match_all, match_none, term, terms, range, exists, is_null, \
             prefix, ids and bool; a full-text query needs a collection's index"
        ))),
    }
}

fn at<'a>(props: &'a Map<String, Value>, field: &str) -> Option<&'a Value> {
    let mut segments = field.split('.');
    let mut value = props.get(segments.next()?)?;
    for segment in segments {
        value = value.as_object()?.get(segment)?;
    }
    Some(value)
}

/// `value` and its elements, when it is an array.
fn each(value: &Value) -> Vec<&Value> {
    match value {
        Value::Array(items) => items.iter().collect(),
        other => vec![other],
    }
}

fn compare(value: &Value, wanted: &FieldValue) -> Option<Ordering> {
    match (value, wanted) {
        (Value::String(s), FieldValue::Str(w)) => Some(s.as_str().cmp(w.as_str())),
        (Value::Bool(b), FieldValue::Bool(w)) => Some(b.cmp(w)),
        (Value::Number(n), FieldValue::I64(w)) => n.as_f64()?.partial_cmp(&(*w as f64)),
        (Value::Number(n), FieldValue::U64(w)) => n.as_f64()?.partial_cmp(&(*w as f64)),
        (Value::Number(n), FieldValue::F64(w)) => n.as_f64()?.partial_cmp(w),
        (Value::Number(n), FieldValue::Date(w)) => n.as_f64()?.partial_cmp(&(*w as f64)),
        _ => None,
    }
}

/// Whether `props` matches `query`, which [`check`] accepted.
#[must_use]
pub fn matches(query: &Query, props: &Map<String, Value>) -> bool {
    let any = |field: &str, test: &dyn Fn(&Value) -> bool| {
        at(props, field).is_some_and(|v| each(v).into_iter().any(test))
    };
    match query {
        Query::MatchAll => true,
        Query::MatchNone => false,
        Query::Term { field, value } => any(field, &|v| compare(v, value) == Some(Ordering::Equal)),
        Query::Terms { field, values } => any(field, &|v| {
            values
                .iter()
                .any(|w| compare(v, w) == Some(Ordering::Equal))
        }),
        Query::Range {
            field,
            gt,
            gte,
            lt,
            lte,
        } => any(field, &|v| {
            let ok = |bound: &Option<FieldValue>, accept: &[Ordering]| {
                bound
                    .as_ref()
                    .is_none_or(|b| compare(v, b).is_some_and(|o| accept.contains(&o)))
            };
            ok(gt, &[Ordering::Greater])
                && ok(gte, &[Ordering::Greater, Ordering::Equal])
                && ok(lt, &[Ordering::Less])
                && ok(lte, &[Ordering::Less, Ordering::Equal])
        }),
        Query::Exists { field } => at(props, field).is_some_and(|v| !v.is_null()),
        Query::IsNull { field } => at(props, field).is_none_or(Value::is_null),
        Query::Prefix { field, value } => any(field, &|v| {
            v.as_str().is_some_and(|s| s.starts_with(value.as_str()))
        }),
        Query::Ids(keys) => props.get("_key").is_some_and(|key| {
            keys.iter()
                .any(|pk| &loams_query::json::pk::to_json(pk) == key)
        }),
        Query::Bool {
            must,
            should,
            must_not,
            filter,
            minimum_should_match,
        } => {
            if !must.iter().chain(filter).all(|q| matches(q, props)) {
                return false;
            }
            if must_not.iter().any(|q| matches(q, props)) {
                return false;
            }
            if should.is_empty() {
                return true;
            }
            let wanted = minimum_should_match
                .as_deref()
                .and_then(|m| m.parse::<usize>().ok())
                .unwrap_or(usize::from(must.is_empty() && filter.is_empty()));
            should.iter().filter(|q| matches(q, props)).count() >= wanted
        }
        Query::Boost { query, .. } | Query::ConstantScore { query, .. } => matches(query, props),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn q(v: Value) -> Query {
        serde_json::from_value(v).expect("a query")
    }

    #[test]
    fn the_structured_subset_matches() {
        let props = json!({"kind": "PERSON", "w": 0.7, "tags": ["a", "b"], "n": {"x": 3}})
            .as_object()
            .cloned()
            .expect("object");
        assert!(matches(
            &q(json!({"term": {"field": "kind", "value": "PERSON"}})),
            &props
        ));
        assert!(matches(
            &q(json!({"term": {"field": "tags", "value": "b"}})),
            &props
        ));
        assert!(matches(
            &q(json!({"range": {"field": "w", "gte": 0.5, "lt": 1}})),
            &props
        ));
        assert!(!matches(
            &q(json!({"range": {"field": "n.x", "gt": 3}})),
            &props
        ));
        assert!(matches(
            &q(json!({"is_null": {"field": "valid_to"}})),
            &props
        ));
        assert!(matches(
            &q(json!({"bool": {"must_not": [{"term": {"field": "kind", "value": "ORG"}}]}})),
            &props
        ));
        assert!(
            check(
                &q(json!({"match": {"field": "kind", "text": "person"}})),
                "edge_filter"
            )
            .is_err()
        );
    }
}
