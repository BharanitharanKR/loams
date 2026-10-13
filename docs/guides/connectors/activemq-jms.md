# ActiveMQ (AMQP 1.0)

The `activemq-jms` connector reads and writes ActiveMQ Artemis and ActiveMQ Classic over AMQP 1.0 (OR-22). It is Loams' own connector, `loams_connector_brokers::amqp10` (CN2 Task 18), built on `fe2o3-amqp` 0.18.2. JMS providers that do not speak AMQP 1.0 use the Camel `jms` fallback, which each instance opts into (OR-4; CN2 Task 23 keeps it while Java is deferred).

**Tested against** ActiveMQ Artemis 2.57.0 (`apache/artemis`) and ActiveMQ Classic 6.3.2 (`apache/activemq`), both Apache-2.0, as the `artemis` and `activemq` services of `deploy/fabric/compose.yaml`, profile `connectors-p2`. The old `apache/activemq-artemis` and `apache/activemq-classic` repositories are archived (R0.27). Status: `planned` until its suites are green in CI.

## Config

```json
{
  "url": "amqps://artemis.example:5671",
  "username": "loams",
  "password": "artemis-password",
  "tls": { "ca_pem": "…" },
  "source": { "addresses": ["orders"], "credit": 200 },
  "sink": { "address": "events" }
}
```

`password` and `tls.key_pem` are secret names. Without a `username`, the connection uses SASL ANONYMOUS. The connection is opened over the SDK's socket, which checks egress after DNS and adds TLS for `amqps://`. Links carry the `queue` capability, so a broker that creates an address on first attach makes it anycast.

## Source

There is one split per address. The receiver link keeps its deliveries **unsettled**. A delivery is settled with `accepted` only after the Fabric acknowledges it. Both brokers force `rcv-settle-mode` first (spike §5.4), and the connector does not rely on `second`. A dropped link leaves its deliveries unsettled, and the broker redelivers them, so the source is **at least once**.

**Backpressure.** The link credit is the pipeline's in-flight budget unless `source.credit` is set. fe2o3 refills credit only as deliveries are settled, so a slow Fabric stops the broker from sending.

## Sink

The sender link is unsettled. A batch is acknowledged when every message's outcome is `accepted`. A `rejected` outcome is poison. `released` and `modified` are retried. The sink is **at least once**.

## CloudEvents

The event's attributes go into application properties with the AMQP binding's `cloudEvents:` prefix. The content type and subject go into the message properties, `message-id` is the event id, and the data is one data section. On the way in, any recognised prefix (`cloudEvents:`, `cloudEvents_`, `ce_`, `ce-`) passes through.

## Auth

The connector supports `basic` (SASL PLAIN) and `mtls`. With `mtls`, the client certificate is presented on the TLS socket together with SASL PLAIN or ANONYMOUS. SASL EXTERNAL is not offered (R18.8).
