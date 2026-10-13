//! Calls into the daemon's engine with an ndjson method's own types.
//!
//! The Connect handlers reach the engine through the same `RpcService` the
//! ndjson dispatcher serves (plan SD1 Ruling 1 as amended by ruling T5-1):
//! the request is built as the method's `Params` type and the reply parsed
//! as its `Result` type, both from the method table
//! (`loams_agentd_proto::rpc::Method`), so a Connect call and an ndjson call
//! run the same dispatch arm with the same values. The only extra step is
//! the in-process `serde_json::Value` the arm already takes.

use std::sync::Arc;

use connectrpc::ConnectError;
use futures::StreamExt;
use futures::stream::BoxStream;
use loams_agentd_proto::rpc::Method;
use loams_agentd_rpc::{RpcReply, RpcService};

use crate::error;

/// The engine the handlers call: the daemon's `RpcService` (without its
/// lifecycle methods; `Shutdown` is never on the Connect surface).
pub type Engine = Arc<dyn RpcService>;

fn params_value<M: Method>(params: &M::Params) -> Result<serde_json::Value, ConnectError> {
    serde_json::to_value(params)
        .map_err(|e| error::internal(format!("{}: params do not serialize: {e}", M::NAME)))
}

fn parse<M: Method>(value: serde_json::Value) -> Result<M::Result, ConnectError> {
    serde_json::from_value(value).map_err(|e| {
        error::internal(format!(
            "{}: the engine's reply does not parse: {e}",
            M::NAME
        ))
    })
}

/// A unary call of method `M`.
///
/// # Errors
///
/// The engine's error as a Connect error ([`error::from_rpc`]), or
/// `internal` when `M` answers a stream or its reply does not parse.
pub async fn call<M: Method>(
    engine: &dyn RpcService,
    params: &M::Params,
) -> Result<M::Result, ConnectError> {
    match engine
        .handle(M::NAME, params_value::<M>(params)?)
        .await
        .map_err(error::from_rpc)?
    {
        RpcReply::Value(value) => parse::<M>(value),
        RpcReply::Stream(_) => Err(error::internal(format!("{} answered a stream", M::NAME))),
    }
}

/// A stream call of method `M`: each item as `M::Result`. An item that does
/// not parse ends the stream with `internal`.
///
/// # Errors
///
/// As [`call`], for the call itself.
pub async fn watch<M: Method>(
    engine: &dyn RpcService,
    params: &M::Params,
) -> Result<BoxStream<'static, Result<M::Result, ConnectError>>, ConnectError> {
    match engine
        .handle(M::NAME, params_value::<M>(params)?)
        .await
        .map_err(error::from_rpc)?
    {
        RpcReply::Stream(items) => Ok(items.map(parse::<M>).boxed()),
        RpcReply::Value(_) => Err(error::internal(format!(
            "{} answered once, not a stream",
            M::NAME
        ))),
    }
}
