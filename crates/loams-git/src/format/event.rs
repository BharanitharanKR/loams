//! A transaction as a CloudEvent (§36 §4.3's attribute table; D364, D415).
//!
//! | Attribute | Value |
//! |---|---|
//! | `specversion` | `1.0` |
//! | `id` | the transaction's idempotency key |
//! | `source` | `/ns/<ns>/repos/<repo_id>` |
//! | `type` | `io.loams.dev.git.{reftxn,packset,config}.v1` |
//! | `time` | the sequencer's clock at commit, RFC 3339 in UTC with milliseconds |
//! | `datacontenttype` | `application/protobuf` |
//! | `dataschema` | `urn:loams:proto:loams.git.v1.<Message>` |
//! | `tenantid` | `<org>/<ns>`, from the credential |
//! | `traceparent` | the request's W3C trace context, if any |
//! | `loamsseq` | the segment's seq, in decimal |
//!
//! No other attribute is written or accepted. They are written in the order
//! the protobuf format reads them back (the required four, then by name).
//! An event's position is its segment's seq plus its index in the batch.

use bytes::Bytes;
use loams_cloudevents::{Attr, CloudEvent, format_time_ms};
use prost::Message;

use super::{FormatError, pb};
use crate::ids::{IdemKey, NamespaceId, RepoId, Seq, TraceParent};

/// `type` of a ref transaction.
pub const TYPE_REF_TXN: &str = "io.loams.dev.git.reftxn.v1";
/// `type` of a pack-set change (compaction, D396).
pub const TYPE_PACK_SET: &str = "io.loams.dev.git.packset.v1";
/// `type` of a configuration change (`HEAD`, protections).
pub const TYPE_CONFIG: &str = "io.loams.dev.git.config.v1";
/// `datacontenttype` of every event.
pub const DATA_CONTENT_TYPE: &str = "application/protobuf";

/// The attributes a transaction event may carry.
const ATTRIBUTES: [&str; 10] = [
    "specversion",
    "id",
    "source",
    "type",
    "time",
    "datacontenttype",
    "dataschema",
    "tenantid",
    "traceparent",
    "loamsseq",
];

/// One committed (or to-be-committed) transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct TxnEvent {
    /// The repository's namespace.
    pub ns: NamespaceId,
    /// The repository.
    pub repo: RepoId,
    /// `<org>/<ns>`, from the credential (D182).
    pub tenant: String,
    /// The idempotency key: the client's, the derived push key, or a ULID
    /// the sequencer generated.
    pub key: IdemKey,
    /// The request's trace context.
    pub trace: Option<TraceParent>,
    /// The sequencer's clock at commit.
    pub time_unix_ms: i64,
    /// What the transaction does.
    pub body: TxnBody,
}

/// A transaction's data message.
#[derive(Clone, Debug, PartialEq)]
pub enum TxnBody {
    /// `io.loams.dev.git.reftxn.v1`.
    RefTxn(pb::RefTransaction),
    /// `io.loams.dev.git.packset.v1`.
    PackSet(pb::PackSetChange),
    /// `io.loams.dev.git.config.v1`.
    Config(pb::ConfigChange),
}

impl TxnBody {
    /// The CloudEvents `type`.
    pub fn event_type(&self) -> &'static str {
        match self {
            TxnBody::RefTxn(_) => TYPE_REF_TXN,
            TxnBody::PackSet(_) => TYPE_PACK_SET,
            TxnBody::Config(_) => TYPE_CONFIG,
        }
    }

    /// The `dataschema` URN.
    pub fn dataschema(&self) -> &'static str {
        dataschema_of(self.event_type()).unwrap_or_default()
    }

    fn encode(&self) -> Vec<u8> {
        match self {
            TxnBody::RefTxn(m) => m.encode_to_vec(),
            TxnBody::PackSet(m) => m.encode_to_vec(),
            TxnBody::Config(m) => m.encode_to_vec(),
        }
    }
}

fn dataschema_of(event_type: &str) -> Option<&'static str> {
    Some(match event_type {
        TYPE_REF_TXN => "urn:loams:proto:loams.git.v1.RefTransaction",
        TYPE_PACK_SET => "urn:loams:proto:loams.git.v1.PackSetChange",
        TYPE_CONFIG => "urn:loams:proto:loams.git.v1.ConfigChange",
        _ => return None,
    })
}

/// `/ns/<ns>/repos/<repo_id>`.
pub fn source_of(ns: NamespaceId, repo: &RepoId) -> String {
    format!("/ns/{ns}/repos/{repo}")
}

/// The event of transaction `e` in segment `seq`.
pub fn to_cloudevent(e: &TxnEvent, seq: Seq) -> CloudEvent {
    // The protobuf format reads the required four back first and the rest
    // by name, so writing them in that order makes a decoded segment equal
    // to the batch that was written.
    let mut attrs = vec![
        Attr::string("specversion", "1.0"),
        Attr::string("id", e.key.as_str()),
        Attr::string("source", source_of(e.ns, &e.repo)),
        Attr::string("type", e.body.event_type()),
        Attr::string("datacontenttype", DATA_CONTENT_TYPE),
        Attr::string("dataschema", e.body.dataschema()),
        Attr::string("loamsseq", seq.0.to_string()),
        Attr::string("tenantid", e.tenant.clone()),
        Attr::string("time", format_time_ms(e.time_unix_ms)),
    ];
    if let Some(trace) = &e.trace {
        attrs.push(Attr::string("traceparent", trace.as_str()));
    }
    // Every value above is valid for its attribute: the four required ones
    // are present and non-empty, `time` is RFC 3339, and names are fixed.
    CloudEvent::new(attrs, Some(Bytes::from(e.body.encode())))
        .expect("a transaction event is a valid CloudEvent")
}

fn parse_source(source: &str) -> Option<(NamespaceId, RepoId)> {
    let rest = source.strip_prefix("/ns/")?;
    let (ns, repo) = rest.split_once("/repos/")?;
    Some((ns.parse().ok()?, RepoId::parse(repo).ok()?))
}

/// Reads a transaction event. Refuses an unknown `type`, a `dataschema` that
/// is not the type's, an `id` that is not an idempotency key, a missing
/// `time`, `tenantid` or `loamsseq`, any other attribute, and data that is
/// not the type's message (D415).
pub fn from_cloudevent(ce: &CloudEvent) -> Result<TxnEvent, FormatError> {
    let bad = |reason: String| FormatError::event(reason);
    if let Some(attr) = ce
        .attrs()
        .iter()
        .find(|attr| !ATTRIBUTES.contains(&attr.name.as_str()))
    {
        return Err(bad(format!("unexpected attribute {:?}", attr.name)));
    }
    let required = |name: &'static str| {
        ce.attr(name)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| bad(format!("missing {name}")))
    };
    let event_type = ce.ty();
    let schema =
        dataschema_of(event_type).ok_or_else(|| bad(format!("unknown type {event_type:?}")))?;
    let dataschema = required("dataschema")?;
    if dataschema != schema {
        return Err(bad(format!(
            "dataschema {dataschema:?} does not match type {event_type:?}"
        )));
    }
    let content_type = required("datacontenttype")?;
    if content_type != DATA_CONTENT_TYPE {
        return Err(bad(format!(
            "datacontenttype {content_type:?} is not {DATA_CONTENT_TYPE}"
        )));
    }
    let key = IdemKey::parse(ce.id()).map_err(|err| bad(format!("id: {err}")))?;
    let (ns, repo) =
        parse_source(ce.source()).ok_or_else(|| bad(format!("source {:?}", ce.source())))?;
    let tenant = required("tenantid")?.to_string();
    let time_unix_ms = ce
        .time_ms()
        .ok_or_else(|| bad("missing or invalid time".to_string()))?;
    let trace = ce
        .attr("traceparent")
        .map(TraceParent::parse)
        .transpose()
        .map_err(|err| bad(format!("traceparent: {err}")))?;
    let seq = required("loamsseq")?;
    if seq.parse::<u64>().map_or(true, |n| n.to_string() != seq) {
        return Err(bad(format!("loamsseq {seq:?} is not a decimal seq")));
    }
    let data: &[u8] = ce.data().map_or(&[], |data| data);
    let decode_err = |err: prost::DecodeError| bad(format!("data: {err}"));
    let body = match event_type {
        TYPE_REF_TXN => TxnBody::RefTxn(pb::RefTransaction::decode(data).map_err(decode_err)?),
        TYPE_PACK_SET => TxnBody::PackSet(pb::PackSetChange::decode(data).map_err(decode_err)?),
        _ => TxnBody::Config(pb::ConfigChange::decode(data).map_err(decode_err)?),
    };
    Ok(TxnEvent {
        ns,
        repo,
        tenant,
        key,
        trace,
        time_unix_ms,
        body,
    })
}
