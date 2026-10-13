//! gRPC against a loopback HTTP/2 peer that speaks the gRPC wire protocol
//! and the v1 reflection service, with a descriptor built here. (A recorded
//! fixture cannot carry HTTP/2 trailers through the kit's HTTP/1.1
//! `FixtureProxy`; ruling R21.2.)
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::VecDeque;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use base64::Engine;
use bytes::{Buf, Bytes};
use common::*;
use http_body::Frame;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use loams_connector_protocols::grpc::{
    FileDescriptorResponse, Grpc, ReflectionRequest, ReflectionResponse, frame, pool_from,
};
use loams_connector_protocols::{OneTopic, ReplyTopics};
use loams_connector_sdk::testing::memory::MemoryFabric;
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorFactory, ErrorClass, EventBatch, Position, SinkHandle,
    Source,
};
use prost::Message as _;
use prost_reflect::prost_types::field_descriptor_proto::{Label, Type};
use prost_reflect::prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    MethodDescriptorProto, ServiceDescriptorProto,
};
use prost_reflect::{DescriptorPool, DynamicMessage, Value as PValue};
use serde_json::{Value, json};

fn field(
    name: &str,
    number: i32,
    ty: Type,
    repeated: bool,
    type_name: Option<&str>,
) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        r#type: Some(ty as i32),
        label: Some(if repeated {
            Label::Repeated
        } else {
            Label::Optional
        } as i32),
        type_name: type_name.map(str::to_owned),
        json_name: Some(name.into()),
        ..Default::default()
    }
}

fn message(name: &str, fields: Vec<FieldDescriptorProto>) -> DescriptorProto {
    DescriptorProto {
        name: Some(name.into()),
        field: fields,
        ..Default::default()
    }
}

/// `loams/test/v1/echo.proto`.
fn file() -> FileDescriptorProto {
    FileDescriptorProto {
        name: Some("loams/test/v1/echo.proto".into()),
        package: Some("loams.test.v1".into()),
        syntax: Some("proto3".into()),
        message_type: vec![
            message(
                "EchoRequest",
                vec![
                    field("text", 1, Type::String, false, None),
                    field("n", 2, Type::Int64, false, None),
                ],
            ),
            message(
                "EchoReply",
                vec![
                    field("text", 1, Type::String, false, None),
                    field("n", 2, Type::Int64, false, None),
                    field("tags", 3, Type::String, true, None),
                ],
            ),
            message(
                "TailRequest",
                vec![
                    field("after", 1, Type::String, false, None),
                    field("count", 2, Type::Int32, false, None),
                ],
            ),
            message(
                "Item",
                vec![
                    field("id", 1, Type::String, false, None),
                    field("body", 2, Type::String, false, None),
                ],
            ),
        ],
        service: vec![ServiceDescriptorProto {
            name: Some("Echo".into()),
            method: vec![
                MethodDescriptorProto {
                    name: Some("Say".into()),
                    input_type: Some(".loams.test.v1.EchoRequest".into()),
                    output_type: Some(".loams.test.v1.EchoReply".into()),
                    ..Default::default()
                },
                MethodDescriptorProto {
                    name: Some("Tail".into()),
                    input_type: Some(".loams.test.v1.TailRequest".into()),
                    output_type: Some(".loams.test.v1.Item".into()),
                    server_streaming: Some(true),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A body of fixed frames (data, then trailers).
struct Frames(VecDeque<Frame<Bytes>>);

impl http_body::Body for Frames {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.pop_front().map(Ok))
    }
}

fn answer(messages: Vec<Vec<u8>>, status: u32, msg: &str) -> Response<Frames> {
    let mut frames: VecDeque<Frame<Bytes>> =
        messages.iter().map(|m| Frame::data(frame(m))).collect();
    let mut t = http::HeaderMap::new();
    t.insert("grpc-status", status.to_string().parse().unwrap());
    if !msg.is_empty() {
        t.insert("grpc-message", msg.parse().unwrap());
    }
    frames.push_back(Frame::trailers(t));
    let mut r = Response::new(Frames(frames));
    r.headers_mut()
        .insert("content-type", "application/grpc".parse().unwrap());
    r
}

fn unframe(mut b: Bytes) -> Vec<Bytes> {
    let mut out = Vec::new();
    while b.len() >= 5 {
        b.advance(1);
        let len = b.get_u32() as usize;
        out.push(b.split_to(len));
    }
    out
}

#[derive(Default)]
struct Seen {
    paths: Vec<String>,
    auth: Vec<Option<String>>,
}

async fn peer() -> (String, Arc<Mutex<Seen>>) {
    let pool =
        DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file()] }).unwrap();
    let seen = Arc::new(Mutex::new(Seen::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let st = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = listener.accept().await else {
                return;
            };
            let (pool, st) = (pool.clone(), st.clone());
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
                    let (pool, st) = (pool.clone(), st.clone());
                    async move {
                        let path = req.uri().path().to_owned();
                        let auth = req
                            .headers()
                            .get("authorization")
                            .map(|v| v.to_str().unwrap().to_owned());
                        assert_eq!(req.headers().get("te").unwrap(), "trailers");
                        {
                            let mut g = st.lock().unwrap();
                            g.paths.push(path.clone());
                            g.auth.push(auth);
                        }
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        let msgs = unframe(body);
                        Ok::<_, Infallible>(match path.as_str() {
                            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo" => {
                                let r = ReflectionRequest::decode(msgs[0].clone()).unwrap();
                                assert_eq!(
                                    r.file_containing_symbol.as_deref(),
                                    Some("loams.test.v1.Echo")
                                );
                                let resp = ReflectionResponse {
                                    file_descriptor_response: Some(FileDescriptorResponse {
                                        file_descriptor_proto: vec![file().encode_to_vec()],
                                    }),
                                    ..Default::default()
                                };
                                answer(vec![resp.encode_to_vec()], 0, "")
                            }
                            "/loams.test.v1.Echo/Say" => {
                                let d = pool
                                    .get_message_by_name("loams.test.v1.EchoRequest")
                                    .unwrap();
                                let m = DynamicMessage::decode(d, msgs[0].clone()).unwrap();
                                let text = m
                                    .get_field_by_name("text")
                                    .unwrap()
                                    .as_str()
                                    .unwrap()
                                    .to_owned();
                                if text == "bad" {
                                    return Ok(answer(vec![], 3, "bad%20text"));
                                }
                                let n = m.get_field_by_name("n").unwrap().as_i64().unwrap();
                                let mut out = DynamicMessage::new(
                                    pool.get_message_by_name("loams.test.v1.EchoReply").unwrap(),
                                );
                                out.set_field_by_name("text", PValue::String(text.to_uppercase()));
                                out.set_field_by_name("n", PValue::I64(n + 1));
                                out.set_field_by_name(
                                    "tags",
                                    PValue::List(vec![
                                        PValue::String("a".into()),
                                        PValue::String("b".into()),
                                    ]),
                                );
                                answer(vec![out.encode_to_vec()], 0, "")
                            }
                            "/loams.test.v1.Echo/Tail" => {
                                let d = pool
                                    .get_message_by_name("loams.test.v1.TailRequest")
                                    .unwrap();
                                let m = DynamicMessage::decode(d, msgs[0].clone()).unwrap();
                                let after = m
                                    .get_field_by_name("after")
                                    .unwrap()
                                    .as_str()
                                    .unwrap()
                                    .to_owned();
                                let item = pool.get_message_by_name("loams.test.v1.Item").unwrap();
                                let out = (1..=5)
                                    .map(|i| format!("i{i}"))
                                    .filter(|id| *id > after)
                                    .map(|id| {
                                        let mut x = DynamicMessage::new(item.clone());
                                        x.set_field_by_name(
                                            "body",
                                            PValue::String(format!("body of {id}")),
                                        );
                                        x.set_field_by_name("id", PValue::String(id));
                                        x.encode_to_vec()
                                    })
                                    .collect();
                                answer(out, 0, "")
                            }
                            _ => answer(vec![], 12, "unknown method"),
                        })
                    }
                });
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(s), svc)
                    .await;
            });
        }
    });
    (format!("http://{addr}"), seen)
}

#[tokio::test]
async fn grpc_dynamic_unary_reply() {
    let (url, seen) = peer().await;
    let fabric = MemoryFabric::new();
    let topics: Arc<dyn ReplyTopics> = Arc::new(OneTopic(Arc::new(fabric.clone())));
    let ctx = ctx(
        json!({"url": url, "reflection": true, "jwt": "jwt", "metadata": {"x-tenant": "acme"},
               "sink": {"method": "loams.test.v1.Echo/Say"}, "reply_topic": "echo.replies"}),
        &json!({"jwt": "synthetic.jwt.token"}),
    );
    let SinkHandle::Plain(mut sink) = Grpc::new()
        .with_replies(topics)
        .sink(&ctx)
        .unwrap()
        .unwrap()
        .create()
        .unwrap()
    else {
        panic!("plain")
    };
    sink.write(EventBatch::Events(vec![event(
        "e1",
        Some("greeting-1"),
        &json!({"text": "hi", "n": "5"}),
    )]))
    .await
    .unwrap();
    let r = &fabric.events()[0];
    assert_eq!(r.ty, "io.loams.dev.flow.grpc.reply.v1");
    assert_eq!(r.source, "/connectors/test/loams.test.v1.Echo/Say");
    assert_eq!(r.subject.as_deref(), Some("greeting-1"));
    assert_eq!(data(r), json!({"text": "HI", "n": "6", "tags": ["a", "b"]}));
    // A status the server gives a bad request is the event's (DLQ).
    let err = sink
        .write(EventBatch::Events(vec![event(
            "e2",
            None,
            &json!({"text": "bad"}),
        )]))
        .await
        .unwrap_err();
    assert_eq!(err.class, ErrorClass::Poison);
    assert!(err.message.contains("bad text"), "{}", err.message);
    // A field the message lacks is refused before any call.
    let err = sink
        .write(EventBatch::Events(vec![event(
            "e3",
            None,
            &json!({"txt": "x"}),
        )]))
        .await
        .unwrap_err();
    assert_eq!(err.class, ErrorClass::Poison);
    let s = seen.lock().unwrap();
    assert_eq!(
        s.paths[0],
        "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
    );
    assert_eq!(s.paths.iter().filter(|p| p.ends_with("/Say")).count(), 2);
    assert_eq!(s.auth[1].as_deref(), Some("Bearer synthetic.jwt.token"));
}

async fn source(ctx: &ConnectorContext, pos: Option<Position>) -> Box<dyn Source> {
    let (mut en, f) = Grpc::new().source(ctx).unwrap().unwrap();
    let split = en.discover(&[]).await.unwrap().remove(0);
    let mut s = f.create(&split).unwrap();
    s.open(&split, pos).await.unwrap();
    s
}

#[tokio::test]
async fn grpc_server_stream_source() {
    let (url, seen) = peer().await;
    // A FileDescriptorSet instead of reflection.
    let set = base64::engine::general_purpose::STANDARD
        .encode(FileDescriptorSet { file: vec![file()] }.encode_to_vec());
    let ctx = ctx(
        json!({"url": url, "descriptor_set": set,
               "source": {"method": "loams.test.v1.Echo/Tail", "request": {"count": 5},
                          "cursor": {"request_field": "after", "response_pointer": "/id"}}}),
        &json!({}),
    );
    let mut src = source(&ctx, None).await;
    let first = src
        .next(Budget {
            max_rows: 2,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap()
        .unwrap();
    let evs = events(&first);
    assert_eq!(
        evs.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
        [
            "loams.test.v1.Echo/Tail@\"i1\"",
            "loams.test.v1.Echo/Tail@\"i2\""
        ]
    );
    assert_eq!(evs[0].ty, "io.loams.dev.flow.grpc.message.v1");
    assert_eq!(data(&evs[1]), json!({"id": "i2", "body": "body of i2"}));
    assert_eq!(first.position.display, "cursor \"i2\"");
    drop(src);
    // A restart re-opens the call from the cursor: no gap, no repeat.
    let mut resumed = source(&ctx, Some(first.position)).await;
    let rest: Vec<Value> = drain(&mut *resumed)
        .await
        .iter()
        .map(|e| data(e)["id"].clone())
        .collect();
    assert_eq!(rest, vec![json!("i3"), json!("i4"), json!("i5")]);
    assert!(
        seen.lock()
            .unwrap()
            .paths
            .iter()
            .all(|p| p.ends_with("/Tail")),
        "no reflection with a descriptor set"
    );
}

#[test]
fn descriptors_build_in_any_order() {
    let mut dep = file();
    dep.name = Some("a.proto".into());
    dep.package = Some("a".into());
    dep.service.clear();
    let mut main = file();
    main.dependency = vec!["a.proto".into()];
    assert!(pool_from(vec![main, dep]).is_ok());
    let _: Value = json!(null);
}
