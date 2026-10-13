//! The Redis and Valkey sink: each batch is one pipeline of `XADD`, `SET`,
//! `HSET`/`HDEL`, `DEL` or `PUBLISH` commands. Every event is checked before
//! anything is sent, so an event that cannot be written fails as `Poison`
//! (the pipeline isolates it and dead-letters it) and the rest of its batch
//! is retried without it.
//!
//! * `xadd` — the event in binary content mode (`ce_id`, `ce_source`,
//!   `ce_type`, `ce_specversion`, optional `ce_subject`/`ce_time`/
//!   `content-type`, `ce_<extension>`, the payload under `data`), so the
//!   Streams source passes it through with its identity (§33 §6). A replayed
//!   batch appends again: at least once.
//! * `set` / `hset` — keyed by a template (`{subject}` by default); a
//!   `loamsop = d` event deletes. Writing the same event twice leaves one
//!   value, so the sink is idempotent (effectively once when the key is
//!   derived from the event).
//! * `publish` — the event as structured JSON on a channel (at most once:
//!   pub/sub keeps nothing for a subscriber that is not connected).

use async_trait::async_trait;
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, EventBatch, Sink, SinkAck, SinkFactory,
    SinkHandle,
};
use redis::aio::MultiplexedConnection;

use super::{classify, connect};
use crate::config::Config;
use crate::util::{is_delete, json_object, render_template, rows_of};

/// What the sink does with each event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteMode {
    Xadd {
        stream: String,
        max_len: Option<u64>,
    },
    Set {
        key_template: String,
        ttl_seconds: Option<u64>,
    },
    Hset {
        key_template: String,
        field_template: Option<String>,
    },
    Publish {
        channel: String,
    },
}

#[derive(Debug)]
pub struct RedisSinkFactory {
    ctx: ConnectorContext,
    cfg: Config,
    mode: WriteMode,
}

impl RedisSinkFactory {
    pub fn new(ctx: ConnectorContext, cfg: Config, mode: WriteMode) -> Self {
        Self { ctx, cfg, mode }
    }
}

impl SinkFactory for RedisSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(RedisSink {
            ctx: self.ctx.clone(),
            cfg: self.cfg.clone(),
            mode: self.mode.clone(),
            conn: None,
        })))
    }
}

#[derive(Debug)]
struct RedisSink {
    ctx: ConnectorContext,
    cfg: Config,
    mode: WriteMode,
    conn: Option<MultiplexedConnection>,
}

/// The `XADD` field/value pairs of an event (binary content mode).
pub fn binary_fields(ev: &CloudEvent) -> Vec<(String, Vec<u8>)> {
    let mut f = vec![
        ("ce_id".to_owned(), ev.id.clone().into_bytes()),
        ("ce_source".to_owned(), ev.source.clone().into_bytes()),
        ("ce_type".to_owned(), ev.ty.clone().into_bytes()),
        (
            "ce_specversion".to_owned(),
            ev.specversion.clone().into_bytes(),
        ),
    ];
    if let Some(s) = &ev.subject {
        f.push(("ce_subject".to_owned(), s.clone().into_bytes()));
    }
    if let Some(t) = &ev.time {
        f.push(("ce_time".to_owned(), t.clone().into_bytes()));
    }
    if let Some(ct) = &ev.datacontenttype {
        f.push(("content-type".to_owned(), ct.clone().into_bytes()));
    }
    for (k, v) in &ev.extensions {
        f.push((format!("ce_{k}"), v.clone().into_bytes()));
    }
    f.push(("data".to_owned(), ev.data.to_vec()));
    f
}

/// The event as a structured-mode CloudEvent (`application/cloudevents+json`).
pub fn structured_json(ev: &CloudEvent) -> Vec<u8> {
    let mut m = serde_json::Map::new();
    m.insert("specversion".into(), ev.specversion.clone().into());
    m.insert("id".into(), ev.id.clone().into());
    m.insert("source".into(), ev.source.clone().into());
    m.insert("type".into(), ev.ty.clone().into());
    if let Some(s) = &ev.subject {
        m.insert("subject".into(), s.clone().into());
    }
    if let Some(t) = &ev.time {
        m.insert("time".into(), t.clone().into());
    }
    if let Some(ct) = &ev.datacontenttype {
        m.insert("datacontenttype".into(), ct.clone().into());
    }
    for (k, v) in &ev.extensions {
        m.insert(k.clone(), v.clone().into());
    }
    match serde_json::from_slice::<serde_json::Value>(&ev.data) {
        Ok(v)
            if ev
                .datacontenttype
                .as_deref()
                .is_none_or(|c| c.contains("json")) =>
        {
            m.insert("data".into(), v);
        }
        _ => {
            use base64::Engine as _;
            m.insert(
                "data_base64".into(),
                base64::engine::general_purpose::STANDARD
                    .encode(&ev.data)
                    .into(),
            );
        }
    }
    serde_json::to_vec(&serde_json::Value::Object(m)).unwrap_or_default()
}

/// One hash field's value: a JSON string as its text, anything else as JSON.
fn field_value(v: &serde_json::Value) -> Vec<u8> {
    match v {
        serde_json::Value::String(s) => s.clone().into_bytes(),
        other => other.to_string().into_bytes(),
    }
}

/// Add the commands for `ev` to `pipe`. Fails, before anything is sent, when
/// the event cannot be written.
pub fn add_commands(
    mode: &WriteMode,
    pipe: &mut redis::Pipeline,
    ev: &CloudEvent,
) -> Result<(), ConnectorError> {
    match mode {
        WriteMode::Xadd { stream, max_len } => {
            let cmd = pipe.cmd("XADD").arg(stream);
            if let Some(n) = max_len {
                cmd.arg("MAXLEN").arg("~").arg(*n);
            }
            cmd.arg("*");
            for (k, v) in binary_fields(ev) {
                cmd.arg(k).arg(v);
            }
        }
        WriteMode::Set {
            key_template,
            ttl_seconds,
        } => {
            let key = render_template(key_template, ev)?;
            if is_delete(ev) {
                pipe.cmd("DEL").arg(key);
            } else {
                let cmd = pipe.cmd("SET").arg(key).arg(ev.data.as_ref());
                if let Some(ttl) = ttl_seconds {
                    cmd.arg("EX").arg(*ttl);
                }
            }
        }
        WriteMode::Hset {
            key_template,
            field_template,
        } => {
            let key = render_template(key_template, ev)?;
            match (field_template, is_delete(ev)) {
                (Some(ft), true) => {
                    pipe.cmd("HDEL").arg(key).arg(render_template(ft, ev)?);
                }
                (None, true) => {
                    pipe.cmd("DEL").arg(key);
                }
                (Some(ft), false) => {
                    pipe.cmd("HSET")
                        .arg(key)
                        .arg(render_template(ft, ev)?)
                        .arg(ev.data.as_ref());
                }
                (None, false) => {
                    let obj = json_object(ev)?;
                    if obj.is_empty() {
                        return Err(ConnectorError::poison(format!(
                            "event {}: an empty object has no hash field to set",
                            ev.id
                        )));
                    }
                    let cmd = pipe.cmd("HSET").arg(key);
                    for (k, v) in &obj {
                        cmd.arg(k).arg(field_value(v));
                    }
                }
            }
        }
        WriteMode::Publish { channel } => {
            pipe.cmd("PUBLISH").arg(channel).arg(structured_json(ev));
        }
    }
    Ok(())
}

impl RedisSink {
    async fn conn(&mut self) -> Result<MultiplexedConnection, ConnectorError> {
        if let Some(c) = &self.conn {
            return Ok(c.clone());
        }
        let c = connect(&self.ctx, &self.cfg).await?;
        self.conn = Some(c.clone());
        Ok(c)
    }
}

#[async_trait]
impl Sink for RedisSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let events = rows_of(batch)?;
        let mut pipe = redis::pipe();
        let mut bytes = 0u64;
        for ev in &events {
            add_commands(&self.mode, &mut pipe, ev)?;
            bytes += ev.size_bytes() as u64;
        }
        if events.is_empty() {
            return Ok(SinkAck::default());
        }
        let mut c = self.conn().await?;
        let r: Result<Vec<redis::Value>, _> = pipe.query_async(&mut c).await;
        if let Err(e) = r {
            let err = classify(&e, "writing a batch");
            if err.class.is_transient() {
                self.conn = None;
            }
            return Err(match e.code() {
                // A key of another type, or a value the server refuses: the
                // data, not the server, is at fault.
                Some("WRONGTYPE") | Some("ERR") => ConnectorError::poison(err.message),
                _ => err,
            });
        }
        Ok(SinkAck {
            rows: events.len() as u64,
            bytes,
        })
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        // Every write was acknowledged by the server when `write` returned.
        Ok(SinkAck::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packed(pipe: &redis::Pipeline) -> String {
        String::from_utf8_lossy(&pipe.get_packed_pipeline()).replace("\r\n", " ")
    }

    #[test]
    fn xadd_writes_binary_mode_fields() {
        let ev = CloudEvent::new("e1", "/s", "t.v1")
            .with_extension("loamsinstance", "i")
            .with_data("application/json", "{}");
        let mut pipe = redis::pipe();
        add_commands(
            &WriteMode::Xadd {
                stream: "out".into(),
                max_len: Some(1000),
            },
            &mut pipe,
            &ev,
        )
        .expect("xadd");
        let p = packed(&pipe);
        for part in [
            "XADD",
            "out",
            "MAXLEN",
            "~",
            "1000",
            "ce_id",
            "e1",
            "ce_loamsinstance",
            "content-type",
            "data",
        ] {
            assert!(p.contains(part), "{part} missing in {p}");
        }
    }

    #[test]
    fn keyed_modes_set_hset_and_delete() {
        let ev = CloudEvent::new("e1", "/s", "t")
            .with_subject("user:1")
            .with_data("application/json", r#"{"name":"ada","age":36}"#);
        let mut pipe = redis::pipe();
        let set = WriteMode::Set {
            key_template: "u:{subject}".into(),
            ttl_seconds: Some(60),
        };
        add_commands(&set, &mut pipe, &ev).expect("set");
        let hset = WriteMode::Hset {
            key_template: "h:{subject}".into(),
            field_template: None,
        };
        add_commands(&hset, &mut pipe, &ev).expect("hset");
        let del = ev.clone().with_extension("loamsop", "d");
        add_commands(&set, &mut pipe, &del).expect("del");
        let p = packed(&pipe);
        assert!(p.contains("SET") && p.contains("u:user:1") && p.contains("EX"));
        assert!(
            p.contains("HSET") && p.contains("h:user:1") && p.contains("age") && p.contains("36")
        );
        assert!(p.contains("DEL"));
        let no_subject = CloudEvent::new("e2", "/s", "t");
        let e = add_commands(&set, &mut redis::pipe(), &no_subject).expect_err("no subject");
        assert_eq!(e.class, loams_connector_sdk::ErrorClass::Poison);
    }

    #[test]
    fn publish_sends_structured_json() {
        let ev = CloudEvent::new("e1", "/s", "t").with_data("text/plain", "hi");
        let v: serde_json::Value = serde_json::from_slice(&structured_json(&ev)).expect("json");
        assert_eq!(v["id"], "e1");
        assert_eq!(v["data_base64"], "aGk=");
    }
}
