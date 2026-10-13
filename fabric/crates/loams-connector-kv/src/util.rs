//! Small helpers every module of the family shares: content digests for
//! stable ids, RFC 3339 times without a date crate, and the events a sink
//! receives flattened to one event per row.

use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use loams_connector_sdk::codecs::Codec;
use loams_connector_sdk::codecs::ndjson::NdjsonCodec;
use loams_connector_sdk::{CloudEvent, ConnectorError, EventBatch};
use sha2::{Digest, Sha256};

/// The first 16 hex digits of SHA-256 over length-prefixed parts, so
/// `("ab", "c")` and `("a", "bc")` differ. Used for ids derived from
/// content: an unchanged record re-read has the same id (D334 dedups it),
/// a changed one gets a new id.
pub fn digest16(parts: &[&[u8]]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update((p.len() as u64).to_be_bytes());
        h.update(p);
    }
    h.finalize()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Milliseconds since the Unix epoch, now.
pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// `ms` since the epoch as an RFC 3339 UTC time with milliseconds.
pub fn rfc3339_millis(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let milli = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{milli:03}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant's days-to-civil algorithm (proleptic Gregorian).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// A sink's input as one event per row: events as they are, and each row of
/// an Arrow batch (D356's bulk path) as a JSON event whose id is the
/// envelope's id plus the row number, so a replayed batch maps to the same
/// ids.
pub fn rows_of(batch: EventBatch) -> Result<Vec<CloudEvent>, ConnectorError> {
    match batch {
        EventBatch::Events(v) => Ok(v),
        EventBatch::Arrow { batch, envelope } => arrow_rows(&batch, &envelope),
    }
}

fn arrow_rows(
    batch: &RecordBatch,
    envelope: &CloudEvent,
) -> Result<Vec<CloudEvent>, ConnectorError> {
    let codec = NdjsonCodec::new(batch.schema());
    let bytes = codec.encode(std::slice::from_ref(batch)).map_err(|e| {
        ConnectorError::poison(format!("an Arrow batch cannot be rendered as rows: {e}"))
    })?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for (i, line) in bytes
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .enumerate()
    {
        let mut ev = envelope.clone();
        ev.id = format!("{}/{i}", envelope.id);
        ev.datacontenttype = Some("application/json".to_owned());
        ev.data = bytes::Bytes::copy_from_slice(line);
        out.push(ev);
    }
    Ok(out)
}

/// The event's data as a JSON object, or `Poison` (this one event cannot be
/// written; the pipeline isolates it and sends it to the DLQ).
pub fn json_object(
    ev: &CloudEvent,
) -> Result<serde_json::Map<String, serde_json::Value>, ConnectorError> {
    match serde_json::from_slice::<serde_json::Value>(&ev.data) {
        Ok(serde_json::Value::Object(m)) => Ok(m),
        Ok(_) => Err(ConnectorError::poison(format!(
            "event {}: data is JSON but not an object",
            ev.id
        ))),
        Err(e) => Err(ConnectorError::poison(format!(
            "event {}: data is not JSON: {e}",
            ev.id
        ))),
    }
}

/// Whether the event is a CDC delete (`loamsop = d`).
pub fn is_delete(ev: &CloudEvent) -> bool {
    ev.extensions.get("loamsop").is_some_and(|op| op == "d")
}

/// Fill a `{ce_id}`, `{subject}`, `{type}`, `{source}` or `{<extension>}`
/// template from an event. An unknown name, or `{subject}` on an event
/// without one, is `Poison`.
pub fn render_template(template: &str, ev: &CloudEvent) -> Result<String, ConnectorError> {
    let mut out = String::with_capacity(template.len() + 16);
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            return Err(ConnectorError::fatal(format!(
                "template {template:?} has an unclosed `{{`"
            )));
        };
        let name = &after[..close];
        let value = match name {
            "ce_id" | "id" => Some(ev.id.as_str()),
            "subject" => ev.subject.as_deref(),
            "type" => Some(ev.ty.as_str()),
            "source" => Some(ev.source.as_str()),
            other => ev.extensions.get(other).map(String::as_str),
        };
        match value {
            Some(v) => out.push_str(v),
            None => {
                return Err(ConnectorError::poison(format!(
                    "event {}: template field {{{name}}} has no value",
                    ev.id
                )));
            }
        }
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn rfc3339_known_instants() {
        assert_eq!(rfc3339_millis(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(rfc3339_millis(951_782_400_123), "2000-02-29T00:00:00.123Z");
        assert_eq!(
            rfc3339_millis(1_791_936_000_000),
            "2026-10-14T00:00:00.000Z"
        );
    }

    #[test]
    fn digest_is_length_prefixed() {
        assert_ne!(digest16(&[b"ab", b"c"]), digest16(&[b"a", b"bc"]));
        assert_eq!(digest16(&[b"x"]).len(), 16);
    }

    #[test]
    fn arrow_batches_become_row_events_with_stable_ids() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .expect("batch");
        let env = CloudEvent::new(
            "run/0/0-1",
            "/connectors/i/t",
            "io.loams.dev.flow.x.rows.v1",
        );
        let rows = rows_of(EventBatch::Arrow {
            batch,
            envelope: env,
        })
        .expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, "run/0/0-1/0");
        assert_eq!(rows[1].id, "run/0/0-1/1");
        assert_eq!(json_object(&rows[0]).expect("object")["name"], "a");
    }

    #[test]
    fn templates_fill_from_the_event() {
        let ev = CloudEvent::new("e1", "/s", "t")
            .with_subject("user:7")
            .with_extension("partitionkey", "p");
        assert_eq!(
            render_template("k:{subject}:{ce_id}", &ev).expect("t"),
            "k:user:7:e1"
        );
        assert_eq!(render_template("{partitionkey}", &ev).expect("t"), "p");
        let e = render_template("{nope}", &ev).expect_err("unknown");
        assert_eq!(e.class, loams_connector_sdk::ErrorClass::Poison);
    }
}
