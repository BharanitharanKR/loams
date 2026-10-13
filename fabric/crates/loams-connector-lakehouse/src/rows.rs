//! Fabric batches as Arrow rows. A sink takes an `EventBatch::Arrow` as is
//! (the bulk path, D356); an `EventBatch::Events` batch is read as one JSON
//! object per event `data`, against the table's schema when it has one.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use loams_connector_sdk::{ConnectorError, EventBatch};

/// The rows of `batch`, decoded against `schema` when given (columns the
/// table lacks are refused by the writer, not here), else against the schema
/// inferred from the events.
pub fn to_record_batch(
    batch: &EventBatch,
    schema: Option<&SchemaRef>,
) -> Result<RecordBatch, ConnectorError> {
    match batch {
        EventBatch::Arrow { batch, .. } => Ok(batch.clone()),
        EventBatch::Events(events) => {
            let mut values = Vec::with_capacity(events.len());
            for ev in events {
                let v: serde_json::Value = serde_json::from_slice(&ev.data).map_err(|e| {
                    ConnectorError::poison(format!("event {} is not JSON: {e}", ev.id))
                })?;
                if !v.is_object() {
                    return Err(ConnectorError::poison(format!(
                        "event {} is not a JSON object",
                        ev.id
                    )));
                }
                values.push(v);
            }
            let schema = match schema {
                Some(s) => s.clone(),
                None => Arc::new(infer(&values)?),
            };
            let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
                .build_decoder()
                .map_err(|e| ConnectorError::fatal(format!("JSON decoder: {e}")))?;
            decoder
                .serialize(&values)
                .map_err(|e| ConnectorError::poison(format!("rows do not fit the table: {e}")))?;
            Ok(decoder
                .flush()
                .map_err(|e| ConnectorError::poison(format!("rows do not fit the table: {e}")))?
                .unwrap_or_else(|| RecordBatch::new_empty(schema)))
        }
    }
}

fn infer(values: &[serde_json::Value]) -> Result<Schema, ConnectorError> {
    arrow_json::reader::infer_json_schema_from_iterator(values.iter().map(Ok))
        .map_err(|e| ConnectorError::poison(format!("cannot infer a schema from the events: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::CloudEvent;

    fn ev(i: u32, data: &str) -> CloudEvent {
        CloudEvent::new(format!("e{i}"), "/t", "t").with_data("application/json", data.to_owned())
    }

    #[test]
    fn events_decode_and_refuse_non_objects() {
        let b = EventBatch::Events(vec![ev(1, r#"{"a":1}"#), ev(2, r#"{"a":2,"b":"x"}"#)]);
        let rb = to_record_batch(&b, None).expect("decodes");
        assert_eq!(rb.num_rows(), 2);
        assert_eq!(rb.num_columns(), 2);
        let bad = EventBatch::Events(vec![ev(3, "[1]")]);
        assert_eq!(
            to_record_batch(&bad, None).expect_err("refused").class,
            loams_connector_sdk::ErrorClass::Poison
        );
    }
}
