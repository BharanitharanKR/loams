//! `list` and `async_job` resources as polling sources: one split per
//! resource, positioned at a page (its cursor or `Link` URL) and the items
//! already taken from it.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    AsyncJob, Budget, CloudEvent, ConnectorError, EventBatch, ListOp, Position, Source,
    SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use super::{Engine, RECORD_EVENT};

/// The default `poll.interval_ms` of an async job.
const DEFAULT_JOB_POLL: Duration = Duration::from_secs(1);

/// Empty pages one `next` call walks past before it reports nothing.
const MAX_EMPTY_PAGES: usize = 16;

#[derive(Debug, Clone)]
pub(crate) enum Mode {
    List(ListOp),
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
    /// The cursor the page is asked for with (`None`: the first page).
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
                let url = engine.url(&job.create.path, None, &[]);
                let resp = engine.send(job.create.method, &url, body, &[]).await?;
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
        let url = engine.url(&job.poll.path, Some(&id), &[]);
        let resp = engine.send(job.poll.method, &url, None, &[]).await?;
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
        let url = match (&self.pos.url, &self.pos.cursor) {
            (Some(url), _) => url.clone(),
            (None, cursor) => {
                let mut query: Vec<(&str, &str)> = list
                    .query
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                if let (Some(c), Some(spec)) = (cursor, &list.cursor) {
                    query.push((spec.param.as_str(), c.as_str()));
                }
                engine.url(&list.path, self.pos.job.as_deref(), &query)
            }
        };
        let resp = engine.send(list.method, &url, None, &[]).await?;
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
        let next = match &list.cursor {
            Some(spec) => match doc.pointer(&spec.next) {
                Some(Value::String(s)) if !s.is_empty() => Next::Cursor(s.clone()),
                Some(Value::Number(n)) => Next::Cursor(n.to_string()),
                _ => Next::End,
            },
            None => match link_next(resp.headers()) {
                Some(url) => {
                    let url = if url.starts_with('/') {
                        let origin = engine
                            .url("", None, &[])
                            .split('/')
                            .take(3)
                            .collect::<Vec<_>>()
                            .join("/");
                        format!("{origin}{url}")
                    } else {
                        url
                    };
                    if !engine.same_origin(&url) {
                        return Err(ConnectorError::fatal(
                            "a Link header points off the API's origin; not followed",
                        ));
                    }
                    Next::Url(url)
                }
                None => Next::End,
            },
        };
        Ok(Page { items, next })
    }

    fn event(&self, item: &Value) -> Result<CloudEvent, ConnectorError> {
        let data = serde_json::to_vec(item)
            .map_err(|e| ConnectorError::fatal(format!("encoding an item: {e}")))?;
        let mut digest = Sha256::new();
        digest.update(self.resource.as_bytes());
        digest.update(b"\n");
        digest.update(&data);
        let id: String = digest
            .finalize()
            .iter()
            .take(16)
            .map(|b| format!("{b:02x}"))
            .collect();
        let mut ev = self
            .engine
            .envelope
            .event(RECORD_EVENT, &self.resource, id)
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
