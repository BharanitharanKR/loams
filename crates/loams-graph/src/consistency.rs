//! How fresh a read must be (GR1 Task 15; design §48 §7.2; D745, D76).
//!
//! | `consistency` | Served by | Meaning |
//! |---|---|---|
//! | `strong` (the default) | the owner | its durable state, which holds every acknowledged write |
//! | `at_least { token }` | any replica | waits until the replica has applied the token's offset, up to `consistency_wait` (5 s), then reads; otherwise `UNAVAILABLE`/`consistency_wait_timeout` |
//! | `eventual` | any replica | whatever the replica has applied |
//!
//! A replica is the owner or a follower ([`crate::replica`]). A token is D76's shape over the
//! graph's stream ([`ConsistencyToken`], `v1:s<stream>/p0@<offset>`): every answer carries the
//! token of the state it read or wrote, in its message and in the `loams-consistency-token`
//! header, and a request may send one in the header. The header follows `loams.collection.v1`'s
//! rule: it turns a `strong`, `eventual` or absent consistency into `at_least(header)`, and merges
//! into an `at_least` one (the later of the two).
//!
//! A write is always the owner's, whatever the request's consistency says.

use std::time::Duration;

use loams_proto::loams::graph::v1 as pb;
use pb::__buffa::oneof::consistency::Level;

use crate::durable::ConsistencyToken;
use crate::engine::{Graph, GraphError};

/// How long an `at_least` read waits for its replica by default (§48 §7.2's
/// `consistency_wait_ms`).
pub const DEFAULT_CONSISTENCY_WAIT: Duration = Duration::from_secs(5);

/// The header a token travels in, on requests and answers (`loams.collection.v1`'s).
pub const CONSISTENCY_TOKEN_HEADER: &str = "loams-consistency-token";

/// How fresh a read must be (shared contract `Consistency`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Consistency {
    /// The owner's durable state.
    #[default]
    Strong,
    /// Whatever the replica that serves it has applied.
    Eventual,
    /// At least the state of this token.
    AtLeast(ConsistencyToken),
}

impl Consistency {
    /// A request's `consistency`; absent is `strong`.
    ///
    /// # Errors
    ///
    /// [`GraphError::InvalidValue`] for an `at_least` token that does not parse.
    pub fn from_proto(consistency: Option<&pb::Consistency>) -> Result<Self, GraphError> {
        Ok(match consistency.and_then(|c| c.level.as_ref()) {
            None | Some(Level::Strong(_)) => Self::Strong,
            Some(Level::Eventual(_)) => Self::Eventual,
            Some(Level::AtLeast(token)) => {
                Self::AtLeast(parse_token(token, "consistency.at_least")?)
            }
        })
    }

    /// This consistency on the wire.
    #[must_use]
    pub fn to_proto(self) -> pb::Consistency {
        pb::Consistency {
            level: Some(match self {
                Self::Strong => Level::from(pb::Strong::default()),
                Self::Eventual => Level::from(pb::Eventual::default()),
                Self::AtLeast(token) => Level::AtLeast(token.to_string()),
            }),
            ..Default::default()
        }
    }

    /// This consistency with a `loams-consistency-token` request header applied: `strong` and
    /// `eventual` become `at_least(header)`, and an `at_least` takes the later of its token and
    /// the header's.
    ///
    /// # Errors
    ///
    /// [`GraphError::InvalidValue`] for a header that does not parse, or one naming another
    /// stream than the body's token.
    pub fn with_header(self, header: &str) -> Result<Self, GraphError> {
        let header = parse_token(header, CONSISTENCY_TOKEN_HEADER)?;
        Ok(match self {
            Self::Strong | Self::Eventual => Self::AtLeast(header),
            Self::AtLeast(body) if body.stream != header.stream => {
                return Err(GraphError::InvalidValue(format!(
                    "the {CONSISTENCY_TOKEN_HEADER} header names stream {}, and \
                     consistency.at_least names stream {}",
                    header.stream, body.stream
                )));
            }
            Self::AtLeast(body) => Self::AtLeast(if header.offset > body.offset {
                header
            } else {
                body
            }),
        })
    }

    /// Whether a replica other than the owner may serve the read.
    #[must_use]
    pub fn any_replica(self) -> bool {
        !matches!(self, Self::Strong)
    }

    /// The token the read must wait for, if any.
    #[must_use]
    pub fn token(self) -> Option<ConsistencyToken> {
        match self {
            Self::AtLeast(token) => Some(token),
            Self::Strong | Self::Eventual => None,
        }
    }
}

/// Applies a `loams-consistency-token` request header (its raw bytes) to a request's
/// `consistency` field, in place: what the transport does before it hands the request over
/// ([`Consistency::with_header`]).
///
/// # Errors
///
/// `INVALID_ARGUMENT`/`invalid_argument` for a header that is not text or not a token, a body
/// token that is not one, or two tokens of different streams.
pub fn apply_header(
    consistency: &mut ::buffa::MessageField<pb::Consistency, ::buffa::Inline<pb::Consistency>>,
    header: &[u8],
) -> Result<(), connectrpc::ConnectError> {
    let merged = std::str::from_utf8(header)
        .map_err(|_| {
            GraphError::InvalidValue(format!("the {CONSISTENCY_TOKEN_HEADER} header is not text"))
        })
        .and_then(|header| Consistency::from_proto(consistency.as_option())?.with_header(header))
        .map_err(crate::service::errors::map_engine)?;
    *consistency = merged.to_proto().into();
    Ok(())
}

fn parse_token(text: &str, field: &str) -> Result<ConsistencyToken, GraphError> {
    text.parse().map_err(|_| {
        GraphError::InvalidValue(format!(
            "{field} is not a graph consistency token (v1:s<stream>/p0@<offset>)"
        ))
    })
}

/// Waits until `graph` (the owner's engine or a follower) has applied every record below
/// `token`'s offset, at most `wait`.
///
/// # Errors
///
/// * [`GraphError::InvalidValue`] for a token of another stream, or a graph that has no tokens
///   (not durable);
/// * [`GraphError::ConsistencyWaitTimeout`] when the wait runs out;
/// * [`GraphError::Reloading`] when the graph closes while the read waits.
pub async fn wait_for(
    graph: &Graph,
    token: ConsistencyToken,
    wait: Duration,
) -> Result<(), GraphError> {
    let (Some(stream), Some(mut applied)) = (graph.stream(), graph.durable_epoch()) else {
        return Err(GraphError::InvalidValue(
            "this graph is not durable, so it has no consistency tokens".to_string(),
        ));
    };
    if token.stream != stream {
        return Err(GraphError::InvalidValue(format!(
            "the consistency token names stream {}, not this graph's stream {stream}",
            token.stream
        )));
    }
    let started = tokio::time::Instant::now();
    let waited =
        tokio::time::timeout(wait, applied.wait_for(|offset| *offset >= token.offset)).await;
    match waited.map(|reached| reached.map(|_| ())) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(GraphError::Reloading),
        Err(_) => Err(GraphError::ConsistencyWaitTimeout {
            wanted: token.offset,
            applied: *applied.borrow(),
            waited_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        }),
    }
}

#[cfg(test)]
mod tests {
    use loams_common::StreamId;

    use super::*;

    fn token(stream: u64, offset: u64) -> ConsistencyToken {
        ConsistencyToken {
            stream: StreamId(stream),
            offset,
        }
    }

    #[test]
    fn absent_is_strong_and_levels_round_trip() {
        assert_eq!(
            Consistency::from_proto(None).ok(),
            Some(Consistency::Strong)
        );
        assert_eq!(
            Consistency::from_proto(Some(&pb::Consistency::default())).ok(),
            Some(Consistency::Strong)
        );
        for level in [
            Consistency::Strong,
            Consistency::Eventual,
            Consistency::AtLeast(token(7, 12)),
        ] {
            assert_eq!(
                Consistency::from_proto(Some(&level.to_proto())).ok(),
                Some(level)
            );
        }
        let bad = pb::Consistency {
            level: Some(Level::AtLeast("v1:s7/p0@x".to_string())),
            ..Default::default()
        };
        assert!(matches!(
            Consistency::from_proto(Some(&bad)),
            Err(GraphError::InvalidValue(_))
        ));
    }

    #[test]
    fn a_header_follows_the_collection_rule() {
        let header = "v1:s7/p0@20";
        for body in [Consistency::Strong, Consistency::Eventual] {
            assert_eq!(
                body.with_header(header).ok(),
                Some(Consistency::AtLeast(token(7, 20)))
            );
        }
        // An at_least takes the later of the two.
        assert_eq!(
            Consistency::AtLeast(token(7, 30)).with_header(header).ok(),
            Some(Consistency::AtLeast(token(7, 30)))
        );
        assert_eq!(
            Consistency::AtLeast(token(7, 3)).with_header(header).ok(),
            Some(Consistency::AtLeast(token(7, 20)))
        );
        // Tokens of two streams, or a header that is not a token, are refused.
        assert!(
            Consistency::AtLeast(token(8, 3))
                .with_header(header)
                .is_err()
        );
        assert!(Consistency::Strong.with_header("nope").is_err());

        let mut field = pb::ExecuteRequest::default().consistency;
        apply_header(&mut field, header.as_bytes()).expect("applied");
        let err = apply_header(&mut field, b"\xff").expect_err("not text");
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument);
        assert_eq!(
            Consistency::from_proto(field.as_option()).ok(),
            Some(Consistency::AtLeast(token(7, 20)))
        );
    }

    #[test]
    fn only_strong_needs_the_owner() {
        assert!(!Consistency::Strong.any_replica());
        assert!(Consistency::Eventual.any_replica());
        assert!(Consistency::AtLeast(token(1, 1)).any_replica());
        assert_eq!(Consistency::Eventual.token(), None);
        assert_eq!(Consistency::AtLeast(token(1, 1)).token(), Some(token(1, 1)));
    }
}
