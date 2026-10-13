# gRPC

gRPC runs on the native protocols family, `loams-connector-protocols`
(`runtime.ref: loams_connector_protocols::grpc`; CN2 Task 21, OR-12). It is a dynamic client:
messages are built at run time with `prost-reflect` from a `FileDescriptorSet` you supply, or
from the server's reflection service. No code is generated per service.

| | |
|---|---|
| Tested against | a loopback HTTP/2 peer speaking the gRPC wire protocol and reflection v1 (the crate's own tests). The real-system suite is a follow-up (ruling R21.2) |
| Sink | unary calls, one per event, each answer a reply event (`sink.reply`) |
| Source | server-streaming calls; resumable only with a `cursor` |
| Auth | `mtls` (`tls.client_cert`/`tls.client_key`), `jwt` (`Authorization: Bearer`) |
| Fallback | none |

## Transport

The client speaks [gRPC over HTTP/2](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
on hyper's HTTP/2 connection, over a socket the SDK opened after the egress check
(ruling R21.2). `http://` is h2c, `https://` is TLS with ALPN `h2`. Every call sends
`te: trailers` and, on unary calls, `grpc-timeout` from `request_timeout_ms`. Compression is
not negotiated.

## Descriptors

- `descriptor_set`: a base64 `FileDescriptorSet` (`protoc --include_imports
  --descriptor_set_out=…`).
- `reflection: true`: ask `grpc.reflection.v1.ServerReflection` (falling back to `v1alpha`)
  for the services the config names and every file they import.

Messages map to JSON with the proto3 JSON mapping (ruling R21.3): fields by JSON or proto
name, 64-bit integers as decimal strings out (numbers or strings in), `bytes` as base64, enums
by name (an unknown number stays a number). Well-known types are plain messages.

## Sink: unary calls with replies

Each event's JSON data, or the object at `sink.request_pointer`, is the request. The answer is
the reply event's data, on the instance's `reply_topic` (see [functions](lambda-cloud-run-azure-functions.md)
for the reply envelope). Status mapping: `INVALID_ARGUMENT`, `NOT_FOUND`, `ALREADY_EXISTS`,
`FAILED_PRECONDITION`, `OUT_OF_RANGE` are `Poison` (the event goes to the DLQ);
`UNAUTHENTICATED`/`PERMISSION_DENIED` are `Auth`; `RESOURCE_EXHAUSTED` is `Throttled`;
`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `ABORTED`, `INTERNAL`, `UNKNOWN`, `CANCELLED` are
`Retryable`; `UNIMPLEMENTED` and `DATA_LOSS` are `Fatal`.

## Source: server streaming

One split per method; the call stays open across reads, and each streamed message becomes an
`io.loams.dev.flow.grpc.message.v1` event. With `source.cursor`, the source is resumable: the
position is the last message's value at `response_pointer`, a restart re-opens the call with
`request_field` set to it, and event ids are `<method>@<cursor>`. Without a cursor the manifest
says `resumable: false`: a restart opens a new call. When the server ends the stream, the
source re-opens it after `reconnect_ms`, or ends.

## Config

```yaml
url: https://orders.internal:443
reflection: true                 # or descriptor_set: <base64>
jwt: orders-jwt                  # a secret's name
tls: { client_cert: orders-cert, client_key: orders-key, ca: orders-ca }   # secret names, PEM
metadata: { x-tenant: acme }     # fixed, non-secret
sink: { method: acme.orders.v1.Orders/Price }
reply_topic: orders.prices
source:
  method: acme.orders.v1.Orders/Watch
  request: { status: OPEN }
  cursor: { request_field: after, response_pointer: /id }
  reconnect_ms: 5000
```
