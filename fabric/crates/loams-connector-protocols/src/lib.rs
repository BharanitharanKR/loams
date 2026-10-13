//! `loams-connector-protocols`: the protocols, graph, observability,
//! functions and AI connector family (CN2 Task 21, OR-6), on
//! `loams-connector-sdk`.
//!
//! | Manifest | `runtime.ref` | Source | Sink |
//! |---|---|---|---|
//! | `grpc` | [`grpc`] | server-streaming calls | unary calls, with replies |
//! | `graphql` | [`graphql`] | Relay-cursor queries; `graphql-ws` subscriptions | mutations |
//! | `neo4j` | [`neo4j`] | cursor-property `MATCH` over the HTTP Query API | parameterised `MERGE` |
//! | `grafeo` | [`grafeo`] | `ExecuteStream` with a cursor property | `ExecuteBatch` of parameterised GQL |
//! | `prometheus` | [`prometheus`] | a remote-write receiver (`ingest`'s route) | remote write |
//! | `lambda-cloud-run-azure-functions` | [`functions`] | — | `Invoke` / HTTP POST, with replies |
//! | `openai` | [`openai`] | — | Chat Completions, with replies |
//! | `anthropic` | [`anthropic`] | — | Messages, with replies |
//!
//! Every connection goes through the SDK: HTTP/1.1 through `sdk::http`, and
//! gRPC's HTTP/2 and the GraphQL WebSocket over `sdk::net`, so each is
//! egress-checked after DNS (rulings R21.1, R21.2). No Bolt client (D634
//! (c)); no `tonic`, `reqwest` or vendor SDK.

pub mod client;
pub mod functions;
pub mod graph;
pub mod graphql;
pub mod grpc;
pub mod json_proto;
pub mod llm;
pub mod neo4j;
pub mod prometheus;
pub mod reply;
pub mod template;
pub mod ws;

/// `runtime.ref: loams_connector_protocols::grafeo` is the [`graph`] module.
pub use self::graph as grafeo;
pub use llm::{anthropic, openai};
pub use reply::{Caller, OneTopic, Reply, ReplySink, ReplyTopics};
