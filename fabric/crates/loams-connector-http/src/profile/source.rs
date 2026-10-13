//! `list` and `async_job` resources as polling sources: one split per
//! resource, positioned at a page (its cursor or `Link` URL) and the items
//! already taken from it.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    AsyncJob, Budget, CloudEvent, ConnectorError, CursorFrom, EventBatch, ListOp, Position,
    ResponseFormat, Source, SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use super::{Engine, ITEM_EVENT, SendOpts};

/// The default `poll.interval_ms` of an async job.
const DEFAULT_JOB_POLL: Duration = Duration::from_secs(1);

/// Empty pages one `next` call walks past before it reports nothing.
const MAX_EMPTY_PAGES: usize = 16;

#[derive(Debug, Clone)]
pub(crate) enum Mode {
    List(Box<ListOp>),
    Job(Box<AsyncJob>),
}

/// One split per resource, named for it.
#[derive(Debug)]
pub(crate) struct ResourceEnumerator {
    resource: String,
}

impl ResourceEnumerator {
    pub(crate) fn new(resource: &str) -> Self {
        Self {
            resource: resource.to_string(),
        }
    }
}

#[async_trait]
impl SplitEnumerator for ResourceEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(
            self.resource.clone(),
            serde_json::json!({ "resource": self.resource }),
        )])
    }
}

#[derive(Debug)]
pub(crate) struct HttpApiSourceFactory {
    engine: Arc<Engine>,
    mode: Mode,
    resource: String,
    poll_interval: Option<Duration>,
}

impl HttpApiSourceFactory {
    pub(crate) fn new(
        engine: Arc<Engine>,
        mode: Mode,
        resource: String,
        poll_interval: Option<Duration>,
    ) -> Self {
        Self {
            engine,
            mode,
            resource,
            poll_interval,
        }
    }
}

impl SourceFactory for HttpApiSourceFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(HttpApiSource {
            engine: self.engine.clone(),
            mode: self.mode.clone(),
            resource: self.resource.clone(),
            poll_interval: self.poll_interval,
            pos: Pos::default(),
            page: None,
            exhausted_at: None,
            job_ready: false,
            last_poll: None,
        }))
    }
}

/// The page to read and how many of its items are already taken. Serialised
/// as the position's opaque bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Pos {
    /// The cursor the page is asked for with (`None`: the first page); for
    /// offset pagination, the offset or page number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
    /// The `Link: rel="next"` URL the page is fetched from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    /// Items of the page already emitted.
    #[serde(default)]
    skip: usize,
    /// The listing is over.
    #[serde(default)]
    done: bool,
    /// The async job whose results are being read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    job: Option<String>,
}

impl Pos {
    fn position(&self, resource: &str) -> Result<Position, ConnectorError> {
        let opaque = serde_json::to_vec(self)
            .map_err(|e| ConnectorError::fatal(format!("encoding a position: {e}")))?;
        let page = match (&self.cursor, &self.url) {
            _ if self.done => "done".to_string(),
            (Some(c), _) => format!("cursor {c}"),
            (None, Some(_)) => "next link".to_string(),
            (None, None) => "first page".to_string(),
        };
        let job = self
            .job
            .as_deref()
            .map_or(String::new(), |j| format!(" job {j}"));
        Ok(Position::new(
            Bytes::from(opaque),
            format!("{resource}{job}: {page} +{}", self.skip),
        ))
    }
}

/// What follows a page.
#[derive(Debug, Clone)]
enum Next {
    Cursor(String),
    Url(String),
    End,
}

#[derive(Debug)]
struct Page {
    items: Vec<Value>,
    next: Next,
}

#[derive(Debug)]
struct HttpApiSource {
    engine: Arc<Engine>,
    mode: Mode,
    resource: String,
    poll_interval: Option<Duration>,
    pos: Pos,
    page: Option<Page>,
    exhausted_at: Option<Instant>,
    job_ready: bool,
    last_poll: Option<Instant>,
}

impl HttpApiSource {
    fn listing(&self) -> &ListOp {
        match &self.mode {
            Mode::List(l) => l,
            Mode::Job(j) => &j.results,
        }
    }

    /// Start the next cycle when the listing is over and `poll_interval` has
    /// passed; `false` while there is nothing to do.
    fn ready(&mut self) -> bool {
        if !self.pos.done {
            return true;
        }
        match (self.poll_interval, self.exhausted_at) {
            (Some(every), Some(at)) if at.elapsed() >= every => {
                self.pos = Pos::default();
                self.page = None;
                self.exhausted_at = None;
                self.job_ready = false;
                self.last_poll = None;
                true
            }
            _ => false,
        }
    }

    /// For an async job: create it, then poll it; `true` once its results can
    /// be read.
    async fn job_ready(&mut self) -> Result<bool, ConnectorError> {
        let Mode::Job(job) = &self.mode else {
            return Ok(true);
        };
        if self.job_ready {
            return Ok(true);
        }
        let engine = self.engine.clone();
        let id = match &self.pos.job {
            Some(id) => id.clone(),
            None => {
                let body = job.create.body.as_ref().map(|b| Bytes::from(b.to_string()));
                let path = engine.with_secrets(&job.create.path, true).await?;
                let url = engine.url(&path, None, &[]);
                let opts = SendOpts {
                    label: Some(&job.create.path),
                    ..SendOpts::default()
                };
                let resp = engine
                    .send(job.create.method, &url, body, &[], opts)
                    .await?;
                let doc = json_body(resp.body())?;
                let id = match doc.pointer(&job.id) {
                    Some(Value::String(s)) => s.clone(),
                    Some(Value::Number(n)) => n.to_string(),
                    _ => {
                        return Err(ConnectorError::fatal(format!(
                            "the job create response has no id at {}",
                            job.id
                        )));
                    }
                };
                self.pos.job = Some(id.clone());
                id
            }
        };
        let every = job
            .poll
            .interval_ms
            .map_or(DEFAULT_JOB_POLL, Duration::from_millis);
        if self.last_poll.is_some_and(|t| t.elapsed() < every) {
            return Ok(false);
        }
        self.last_poll = Some(Instant::now());
        let path = engine.with_secrets(&job.poll.path, true).await?;
        let url = engine.url(&path, Some(&id), &[]);
        let opts = SendOpts {
            label: Some(&job.poll.path),
            ..SendOpts::default()
        };
        let resp = engine.send(job.poll.method, &url, None, &[], opts).await?;
        let doc = json_body(resp.body())?;
        let state = doc
            .pointer(&job.poll.state)
            .and_then(Value::as_str)
            .unwrap_or_default();
        if job.poll.done.iter().any(|d| d == state) {
            self.job_ready = true;
            Ok(true)
        } else if job.poll.failed.iter().any(|f| f == state) {
            Err(ConnectorError::fatal(format!(
                "the async job {id} of {} ended in state {state}",
                self.resource
            )))
        } else {
            Ok(false)
        }
    }

    /// Fetch the page `pos` names.
    async fn fetch(&self) -> Result<Page, ConnectorError> {
        let list = self.listing();
        let engine = &self.engine;
        let offset_start = |o: &loams_connector_sdk::OffsetSpec| if o.pages { 1 } else { 0 };
        let url = match (&self.pos.url, &self.pos.cursor) {
            (Some(url), _) => url.clone(),
            (None, cursor) => {
                let mut query: Vec<(String, String)> = list
                    .query
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if let Some(o) = &list.offset {
                    let at = cursor
                        .clone()
                        .unwrap_or_else(|| offset_start(o).to_string());
                    query.push((o.param.clone(), at));
                    if let Some(lp) = &o.limit_param {
                        query.push((lp.clone(), o.limit.to_string()));
                    }
                } else if let (Some(c), Some(spec)) = (cursor, &list.cursor) {
                    query.push((spec.param.clone(), c.clone()));
                }
                let query: Vec<(&str, &str)> = query
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                let path = engine.with_secrets(&list.path, true).await?;
                engine.url(&path, self.pos.job.as_deref(), &query)
            }
        };
        let opts = SendOpts {
            label: Some(&list.path),
            ..SendOpts::default()
        };
        let resp = engine.send(list.method, &url, None, &[], opts).await?;
        let (doc, items) = match list.format {
            ResponseFormat::Json => {
                let doc = json_body(resp.body())?;
                let items = match &list.items {
                    Some(p) => doc.pointer(p),
                    None => Some(&doc),
                };
                let items = match items {
                    Some(Value::Array(a)) => a.clone(),
                    None | Some(Value::Null) => Vec::new(),
                    Some(_) => {
                        return Err(ConnectorError::fatal(format!(
                            "the {} listing's items are not an array",
                            self.resource
                        )));
                    }
                };
                (doc, items)
            }
            ResponseFormat::Csv => (Value::Null, csv_rows(resp.body())?),
            ResponseFormat::Ndjson => (
                Value::Null,
                ndjson_items(resp.body(), list.items.as_deref())?,
            ),
        };
        let text = |v: Option<&Value>| match v {
            Some(Value::String(s)) if !s.is_empty() && s != "null" => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            _ => None,
        };
        let more = list
            .cursor
            .as_ref()
            .and_then(|c| c.more.as_ref())
            .is_none_or(|m| doc.pointer(m) == Some(&Value::Bool(true)));
        let next = if let Some(ptr) = &list.next_url {
            match text(doc.pointer(ptr)) {
                Some(u) => Next::Url(engine.next_page(&u).ok_or_else(|| {
                    ConnectorError::fatal(
                        "a next-page URL points off the API's origin; not followed",
                    )
                })?),
                None => Next::End,
            }
        } else if let Some(spec) = &list.cursor {
            let cursor = match spec.from {
                CursorFrom::Body => text(doc.pointer(&spec.next)),
                CursorFrom::LastItem => items.last().and_then(|i| text(i.pointer(&spec.next))),
                CursorFrom::Header => resp
                    .headers()
                    .get(spec.next.as_str())
                    .and_then(|v| v.to_str().ok())
                    .map(|v| Value::String(v.trim().to_string()))
                    .and_then(|v| text(Some(&v))),
            };
            match cursor {
                Some(c) if more => Next::Cursor(c),
                _ => Next::End,
            }
        } else if let Some(o) = &list.offset {
            let at: u64 = self
                .pos
                .cursor
                .as_deref()
                .and_then(|c| c.parse().ok())
                .unwrap_or(offset_start(o));
            if items.len() >= o.limit as usize {
                let step = if o.pages { 1 } else { items.len() as u64 };
                Next::Cursor((at + step).to_string())
            } else {
                Next::End
            }
        } else {
            match link_next(resp.headers()) {
                Some(url) => Next::Url(engine.next_page(&url).ok_or_else(|| {
                    ConnectorError::fatal("a Link header points off the API's origin; not followed")
                })?),
                None => Next::End,
            }
        };
        Ok(Page { items, next })
    }

    fn event(&self, item: &Value) -> Result<CloudEvent, ConnectorError> {
        let raw = serde_json::to_vec(item)
            .map_err(|e| ConnectorError::fatal(format!("encoding an item: {e}")))?;
        // The id is the raw item's, so a projection (an import route) keeps
        // the identity of what was read.
        let mut digest = Sha256::new();
        digest.update(self.resource.as_bytes());
        digest.update(b"\n");
        digest.update(&raw);
        let data = match self.engine.project() {
            Some(p) => serde_json::to_vec(&p.apply(item))
                .map_err(|e| ConnectorError::fatal(format!("encoding an item: {e}")))?,
            None => raw,
        };
        let id: String = digest
            .finalize()
            .iter()
            .take(16)
            .map(|b| format!("{b:02x}"))
            .collect();
        let mut ev = self
            .engine
            .envelope
            .event(
                self.listing().event.as_deref().unwrap_or(ITEM_EVENT),
                &self.resource,
                id,
            )
            .with_data("application/json", data);
        if let Some(key) = &self.listing().key {
            match item.pointer(key) {
                Some(Value::String(s)) => ev = ev.with_subject(s.clone()),
                Some(Value::Number(n)) => ev = ev.with_subject(n.to_string()),
                _ => {}
            }
        }
        Ok(ev)
    }
}

#[async_trait]
impl Source for HttpApiSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.pos = match pos {
            Some(p) => serde_json::from_slice(&p.opaque).map_err(|_| {
                ConnectorError::fatal(format!("not a position of the {} listing", self.resource))
            })?,
            None => Pos::default(),
        };
        self.page = None;
        self.job_ready = false;
        self.last_poll = None;
        self.exhausted_at = self.pos.done.then(Instant::now);
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        for _ in 0..MAX_EMPTY_PAGES {
            if !self.ready() || !self.job_ready().await? {
                return Ok(None);
            }
            if self.page.is_none() {
                self.page = Some(self.fetch().await?);
            }
            let Some(page) = self.page.as_ref() else {
                return Ok(None);
            };
            let mut events = Vec::new();
            let mut bytes = 0usize;
            let mut taken = 0usize;
            for item in page.items.iter().skip(self.pos.skip) {
                if events.len() >= budget.max_rows {
                    break;
                }
                let ev = self.event(item)?;
                let size = ev.size_bytes();
                if bytes + size > budget.max_bytes {
                    if events.is_empty() {
                        // R4.7: an item larger than any batch can never be read;
                        // it is skipped to the DLQ and the listing carries on.
                        self.pos.skip += 1;
                        return Err(ConnectorError::poison(format!(
                            "an item of {} is {size} bytes, more than a batch may hold",
                            self.resource
                        )));
                    }
                    break;
                }
                bytes += size;
                taken += 1;
                events.push(ev);
            }
            self.pos.skip += taken;
            if self.pos.skip >= page.items.len() {
                let next = page.next.clone();
                let job = self.pos.job.clone();
                self.pos = match next {
                    Next::Cursor(c) => Pos {
                        cursor: Some(c),
                        job,
                        ..Pos::default()
                    },
                    Next::Url(u) => Pos {
                        url: Some(u),
                        job,
                        ..Pos::default()
                    },
                    Next::End => {
                        self.exhausted_at = Some(Instant::now());
                        Pos {
                            done: true,
                            job,
                            ..Pos::default()
                        }
                    }
                };
                self.page = None;
            }
            if !events.is_empty() {
                return Ok(Some(SourceBatch {
                    events: EventBatch::Events(events),
                    position: self.pos.position(&self.resource)?,
                }));
            }
        }
        Ok(None)
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        // An HTTP listing has nothing to acknowledge upstream.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A CSV body (RFC 4180: quoted fields, doubled quotes, CRLF or LF) as one
/// object of strings per row, keyed by the header row.
fn csv_rows(body: &[u8]) -> Result<Vec<Value>, ConnectorError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| ConnectorError::retryable("the API's CSV response is not UTF-8"))?;
    let mut records: Vec<Vec<String>> = Vec::new();
    let mut record = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = text.chars().peekable();
    let mut any = false;
    while let Some(c) = chars.next() {
        any = true;
        match (quoted, c) {
            (true, '"') if chars.peek() == Some(&'"') => {
                chars.next();
                field.push('"');
            }
            (true, '"') => quoted = false,
            (true, c) => field.push(c),
            (false, '"') => quoted = true,
            (false, ',') => record.push(std::mem::take(&mut field)),
            (false, '\r') => {}
            (false, '\n') => {
                record.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut record));
                any = false;
            }
            (false, c) => field.push(c),
        }
    }
    if quoted {
        return Err(ConnectorError::retryable(
            "the API's CSV response ends inside a quote",
        ));
    }
    if any {
        record.push(field);
        records.push(record);
    }
    let mut rows = records.into_iter();
    let Some(header) = rows.next() else {
        return Ok(Vec::new());
    };
    rows.filter(|r| !(r.len() == 1 && r[0].is_empty()))
        .map(|r| {
            if r.len() != header.len() {
                return Err(ConnectorError::retryable(
                    "a CSV row has a different number of fields from the header",
                ));
            }
            Ok(Value::Object(
                header
                    .iter()
                    .cloned()
                    .zip(r.into_iter().map(Value::String))
                    .collect(),
            ))
        })
        .collect()
}

/// One JSON document per line; `items` points at the item in each (lines
/// without one, such as a stream's trailer, are skipped).
fn ndjson_items(body: &[u8], items: Option<&str>) -> Result<Vec<Value>, ConnectorError> {
    let mut out = Vec::new();
    for line in body.split(|b| *b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let doc = json_body(line)?;
        match items {
            Some(p) => {
                if let Some(v) = doc.pointer(p).filter(|v| !v.is_null()) {
                    out.push(v.clone());
                }
            }
            None => out.push(doc),
        }
    }
    Ok(out)
}

fn json_body(body: &[u8]) -> Result<Value, ConnectorError> {
    serde_json::from_slice(body)
        .map_err(|_| ConnectorError::retryable("the API's response is not JSON"))
}

/// The `rel="next"` target of a `Link` header (RFC 8288).
fn link_next(headers: &http::HeaderMap) -> Option<String> {
    headers
        .get_all(http::header::LINK)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .find_map(|link| {
            let (target, params) = link.split_once(';').unwrap_or((link, ""));
            let target = target.trim().strip_prefix('<')?.strip_suffix('>')?;
            let next = params.split(';').any(|p| {
                p.trim()
                    .strip_prefix("rel=")
                    .is_some_and(|r| r.trim_matches('"').split_whitespace().any(|r| r == "next"))
            });
            next.then(|| target.to_string())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_header_next() {
        let mut h = http::HeaderMap::new();
        h.insert(
            http::header::LINK,
            "<https://api.example/x?page=1>; rel=\"prev\", <https://api.example/x?page=3>; rel=\"next\""
                .parse()
                .expect("header"),
        );
        assert_eq!(
            link_next(&h).as_deref(),
            Some("https://api.example/x?page=3")
        );
    }

    #[test]
    fn csv_and_ndjson_bodies() {
        let rows =
            csv_rows(b"Id,Name\r\n001,\"Acme, Inc.\"\r\n002,\"Say \"\"hi\"\"\"\r\n").expect("csv");
        assert_eq!(
            rows,
            vec![
                serde_json::json!({"Id": "001", "Name": "Acme, Inc."}),
                serde_json::json!({"Id": "002", "Name": "Say \"hi\""}),
            ]
        );
        assert!(csv_rows(b"").expect("empty").is_empty());
        assert!(csv_rows(b"a,b\n1\n").is_err());
        let items = ndjson_items(
            b"{\"result\":{\"a\":1}}\n\n{\"result\":{\"a\":2}}\n{\"lastrow\":true}\n",
            Some("/result"),
        )
        .expect("ndjson");
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn positions_round_trip() {
        let p = Pos {
            cursor: Some("p2".into()),
            skip: 1,
            ..Pos::default()
        };
        let pos = p.position("contacts").expect("encodes");
        assert_eq!(pos.display, "contacts: cursor p2 +1");
        let back: Pos = serde_json::from_slice(&pos.opaque).expect("decodes");
        assert_eq!(back, p);
    }
}
