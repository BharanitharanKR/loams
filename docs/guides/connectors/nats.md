# NATS and JetStream

The `nats` connector reads and writes NATS. It is Loams' own connector, `loams_connector_brokers::nats` (CN2 Task 18), built on `async-nats` 0.50.0 with ring, JetStream and nkeys only.

**Tested against** NATS 2.15.0 with JetStream (`nats:2.15.0 -js`, Apache-2.0), the `nats` service of `deploy/fabric/compose.yaml`, profile `connectors-p2`. Status: `planned` until its suites are green in CI.

## Config

```json
{
  "servers": ["tls://nats.example:4222"],
  "auth": { "credentials": "nats-user-creds" },
  "source": { "stream": "ORDERS", "durable": "loams-orders", "filter_subject": "orders.>" },
  "sink": { "subject": "events.loams" }
}
```

`auth.nkey_seed` (`key`), `auth.credentials` (`jwt`: a `.creds` file's contents) and `tls.key_pem` (`mtls`) are secret names. Every configured server is checked against the egress policy before the client is built. The client ignores servers the cluster advertises, so it never dials an unchecked address.

## JetStream (the default)

- **Source.** The source is a durable pull consumer with `ack_policy: explicit`. Each fetch is bounded by the budget (`max_messages`, `max_bytes`), and `max_ack_pending` is the pipeline's in-flight budget. Messages are acknowledged after the Fabric acknowledges them, and the last one in a batch is double-acked. The consumer keeps its ack floor on the server, so a restart resumes there (`resumable: true`, **at least once**). Messages that are not CloudEvents get the id `<stream>/<stream sequence>`, which is stable across redeliveries.
- **Sink.** Each publish carries `Nats-Msg-Id` set to the event id, and the sink waits for every publish ack. The stream drops a repeated id within its duplicate window, so the sink is **effectively once** within that window (`sink.idempotent`).

## Core NATS (`mode: core`)

The source subscribes to `source.subjects`, one split each, optionally in a `queue_group`. Core NATS keeps nothing for a subscriber that is not connected, so this mode is **not resumable** and is at most once. A core sink publishes and flushes, also at most once. The manifest declares the JetStream values; `nats::declared(config)` reports the weaker ones for a core config (R18.6).

## CloudEvents

The event's attributes travel as `ce_` headers, with a `Content-Type` header. Upstream CloudEvents pass through.
