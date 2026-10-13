//! A fake SQS queue speaking the SQS JSON protocol (AWS JSON 1.0,
//! `X-Amz-Target: AmazonSQS.<Operation>`), with visibility timeouts and
//! receipt handles, for the unit halves of the SQS and EventBridge tests.
#![allow(dead_code, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::{FakeApi, Reply, Seen, json_reply};

#[derive(Debug, Clone)]
pub struct Msg {
    pub id: String,
    pub body: String,
    pub attributes: Value,
    pub visible_at: Instant,
    pub handle: Option<String>,
    pub receives: u32,
    pub dedup_id: Option<String>,
    pub group_id: Option<String>,
}

#[derive(Debug, Default)]
pub struct State {
    pub messages: Vec<Msg>,
    pub next: u64,
    pub deleted: Vec<String>,
    /// Fail the next N `DeleteMessageBatch` calls with a 500.
    pub fail_deletes: u32,
}

#[derive(Clone)]
pub struct FakeSqs {
    pub api: FakeApi,
    pub state: Arc<Mutex<State>>,
}

impl FakeSqs {
    pub async fn start() -> Self {
        let state: Arc<Mutex<State>> = Arc::default();
        let s = state.clone();
        let api = FakeApi::start(Arc::new(move |seen: &Seen| handle(&s, seen))).await;
        Self { api, state }
    }

    pub fn queue_url(&self, name: &str) -> String {
        format!("{}/000000000000/{name}", self.api.base)
    }

    pub fn send(&self, body: &str, attributes: Value) -> String {
        let mut s = self.state.lock().unwrap();
        s.next += 1;
        let id = format!("msg-{}", s.next);
        s.messages.push(Msg {
            id: id.clone(),
            body: body.to_owned(),
            attributes,
            visible_at: Instant::now(),
            handle: None,
            receives: 0,
            dedup_id: None,
            group_id: None,
        });
        id
    }

    pub fn remaining(&self) -> usize {
        self.state.lock().unwrap().messages.len()
    }

    pub fn messages(&self) -> Vec<Msg> {
        self.state.lock().unwrap().messages.clone()
    }

    pub fn deleted(&self) -> Vec<String> {
        self.state.lock().unwrap().deleted.clone()
    }
}

fn handle(state: &Arc<Mutex<State>>, seen: &Seen) -> Reply {
    let req = seen.json();
    let mut s = state.lock().unwrap();
    let now = Instant::now();
    match seen.op().as_str() {
        "ReceiveMessage" => {
            let max = req["MaxNumberOfMessages"].as_u64().unwrap_or(1) as usize;
            let vis = req["VisibilityTimeout"].as_u64().unwrap_or(30);
            let mut out = Vec::new();
            let mut counter = s.next;
            for m in s.messages.iter_mut() {
                if out.len() >= max {
                    break;
                }
                if m.visible_at > now {
                    continue;
                }
                counter += 1;
                let h = format!("rh-{}-{counter}", m.id);
                m.handle = Some(h.clone());
                m.receives += 1;
                m.visible_at = now + Duration::from_secs(vis);
                let mut o = json!({
                    "MessageId": m.id,
                    "ReceiptHandle": h,
                    "Body": m.body,
                    "MD5OfBody": "",
                    "Attributes": {"SentTimestamp": "1760313600000", "ApproximateReceiveCount": m.receives.to_string()},
                });
                if m.attributes.is_object() {
                    o["MessageAttributes"] = m.attributes.clone();
                }
                out.push(o);
            }
            s.next = counter;
            json_reply(200, json!({"Messages": out}))
        }
        "DeleteMessageBatch" => {
            if s.fail_deletes > 0 {
                s.fail_deletes -= 1;
                return json_reply(500, json!({"__type": "InternalFailure", "message": "boom"}));
            }
            let mut ok = Vec::new();
            let mut failed = Vec::new();
            for e in req["Entries"].as_array().cloned().unwrap_or_default() {
                let h = e["ReceiptHandle"].as_str().unwrap_or("").to_owned();
                let before = s.messages.len();
                let mut gone = None;
                s.messages.retain(|m| {
                    let hit = m.handle.as_deref() == Some(h.as_str());
                    if hit {
                        gone = Some(m.id.clone());
                    }
                    !hit
                });
                if s.messages.len() < before {
                    s.deleted.extend(gone);
                    ok.push(json!({"Id": e["Id"]}));
                } else {
                    failed.push(json!({"Id": e["Id"], "Code": "ReceiptHandleIsInvalid", "SenderFault": true}));
                }
            }
            json_reply(200, json!({"Successful": ok, "Failed": failed}))
        }
        "ChangeMessageVisibilityBatch" => {
            let mut ok = Vec::new();
            for e in req["Entries"].as_array().cloned().unwrap_or_default() {
                let h = e["ReceiptHandle"].as_str().unwrap_or("");
                let secs = e["VisibilityTimeout"].as_u64().unwrap_or(0);
                for m in s.messages.iter_mut() {
                    if m.handle.as_deref() == Some(h) {
                        m.visible_at = now + Duration::from_secs(secs);
                    }
                }
                ok.push(json!({"Id": e["Id"]}));
            }
            json_reply(200, json!({"Successful": ok, "Failed": []}))
        }
        "SendMessageBatch" => {
            let mut ok = Vec::new();
            for e in req["Entries"].as_array().cloned().unwrap_or_default() {
                let dedup = e["MessageDeduplicationId"].as_str().map(str::to_owned);
                if dedup.is_some() && s.messages.iter().any(|m| m.dedup_id == dedup) {
                    // FIFO deduplication: accepted, not enqueued again.
                    ok.push(json!({"Id": e["Id"], "MessageId": "dup"}));
                    continue;
                }
                s.next += 1;
                let id = format!("msg-{}", s.next);
                s.messages.push(Msg {
                    id: id.clone(),
                    body: e["MessageBody"].as_str().unwrap_or("").to_owned(),
                    attributes: e["MessageAttributes"].clone(),
                    visible_at: now,
                    handle: None,
                    receives: 0,
                    dedup_id: dedup,
                    group_id: e["MessageGroupId"].as_str().map(str::to_owned),
                });
                ok.push(json!({"Id": e["Id"], "MessageId": id}));
            }
            json_reply(200, json!({"Successful": ok, "Failed": []}))
        }
        other => json_reply(
            400,
            json!({"__type": "InvalidAction", "message": format!("unknown {other}")}),
        ),
    }
}

/// Every message attribute map as (name → string value).
pub fn string_attributes(v: &Value) -> BTreeMap<String, String> {
    v.as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, a)| Some((k.clone(), a["StringValue"].as_str()?.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}
