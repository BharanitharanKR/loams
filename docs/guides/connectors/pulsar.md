# Apache Pulsar

The `pulsar` connector consumes Pulsar topics and produces to them. It is Loams' own connector, `loams_connector_brokers::pulsar` (CN2 Task 18), built on `pulsar` 6.9.0. That crate is built without default features: tokio with rustls on ring, no async-std (RUSTSEC-2025-0052), no native TLS, and no compression C libraries (R0.17).

**Tested against** Apache Pulsar 4.0.13 standalone (`apachepulsar/pulsar`, Apache-2.0), the `pulsar` service of `deploy/fabric/compose.yaml`, profile `connectors-p2`. Status: `planned` until its suites are green in CI.

## Config

```json
{
  "url": "pulsar+ssl://pulsar.example:6651",
  "token": "pulsar-jwt",
  "source": { "topics": ["persistent://acme/billing/invoices"], "subscription_type": "failover" },
  "sink": { "topic": "persistent://acme/billing/loams" }
}
```

`token` is a secret name. The service URL's host is checked against the egress policy before the client is built. The brokers that a topic lookup returns are dialled by the crate itself (R18.4).

## Source

There is one split per topic. Each is a consumer on a durable subscription: `failover` by default, or `shared` or `key_shared`. A new subscription starts at `earliest` unless set otherwise. Messages are acknowledged individually after the Fabric acknowledges them. The subscription's cursor is the position, so a restart resumes there. Messages that are not CloudEvents get the id `<topic>/<ledger>:<entry>:<partition>:<batch>`, which is stable across redeliveries. The manifest therefore declares `stable_ids`, and with `ingest`'s dedup the source is **effectively once**. The receiver queue is the pipeline's in-flight budget.

## Sink

The sink uses one producer per instance. A batch is acknowledged once every send receipt has arrived. The sink is **at least once**, not effectively once by sequence id (R18.7): `pulsar` 6.9.0 numbers sequence ids from 0 for every producer and renumbers a retried send. With broker-side deduplication on, a restarted producer would have its new messages dropped as repeats. The factory refuses a deduplicated claim (`pulsar_producer_dedup`).

## CloudEvents

Pulsar has no headers, only string properties. The event's attributes travel as `ce_` properties, with `content-type` for `datacontenttype` and the subject as the partition key. Upstream CloudEvent properties pass through (`pulsar_ce_properties_pass_through`).

## Auth

The connector supports `jwt` only. The crate offers no client certificates, and its OAuth 2 support pulls in a second HTTP stack, so `mtls` and `oauth2` are narrowed (R18.7).
