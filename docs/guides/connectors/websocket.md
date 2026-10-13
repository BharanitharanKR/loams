# WebSocket

The `websocket` connector reads frames from a WebSocket endpoint and writes frames to one (RFC 6455). It is Loams' own connector, `loams_connector_brokers::websocket` (CN2 Task 18), built on `tokio-tungstenite` 0.30.0. The socket is the SDK's, which checks egress after DNS and adds TLS for `wss://`.

**Tested against** a fixture of frames recorded from a public RFC 6455 server (`conformance/connectors/fixtures/websocket`), replayed by this family's own replayer, because the kit's FixtureProxy handles HTTP only. That recording does not exist yet (R18.11). Until it does, the suites are skipped, and the unit tests against an in-process server are the only coverage. Status: `planned`.

## Config

```json
{
  "url": "wss://feed.example/stream",
  "auth": { "bearer": "feed-token" },
  "headers": { "X-Client": "loams" },
  "source": { "subscribe": "{\"op\":\"subscribe\",\"channel\":\"trades\"}" },
  "sink": { "frame": "auto" }
}
```

`auth.key` (sent in the header named by `auth.key_header`, the manifest's `key`), `auth.bearer` (`Authorization: Bearer …`, the manifest's `jwt`) and `tls.key_pem` are secret names. A credential header given in `headers` is refused.

## Source

There is one split: the URL. Each text or binary frame is an event.

- A text frame that holds a structured CloudEvent passes through.
- Any other text frame becomes a `message` event: `application/json` when it parses as JSON, `text/plain` otherwise.
- A binary frame becomes `application/octet-stream`.

WebSocket has no acknowledgement and no replay. The source is therefore declared **not resumable** and at most once (`websocket_declares_not_resumable`). After a reconnect it reads whatever the server sends next. Backpressure is TCP's: the source reads only when the pipeline has budget.

## Sink

The sink writes one frame per event. With `frame: auto`, textual data goes as a text frame and anything else as a binary frame. `frame` can also be `text`, `binary`, or `structured` (the whole CloudEvent as JSON). A written frame is not a frame the peer has processed, so the sink is **at most once** as well.
