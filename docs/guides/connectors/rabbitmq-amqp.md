# RabbitMQ (AMQP 0-9-1)

The `rabbitmq-amqp` connector reads RabbitMQ queues and publishes to RabbitMQ exchanges over AMQP 0-9-1. It is Loams' own connector, `loams_connector_brokers::amqp091` (CN2 Task 18), built on `lapin` 4.12.0 with rustls on ring.

**Tested against** RabbitMQ 4.3.5 (`rabbitmq:4.3.5-management`, MPL-2.0), the `rabbitmq` service of `deploy/fabric/compose.yaml`, profile `connectors-p2`. Status: `planned` until its suites are green in CI (OR-23).

## Config

```json
{
  "url": "amqps://rabbit.example:5671/%2f",
  "username": "loams",
  "password": "rabbit-password",
  "tls": { "ca_pem": "-----BEGIN CERTIFICATE-----…", "cert_pem": "…", "key_pem": "rabbit-client-key" },
  "source": { "queues": ["orders"], "prefetch": 200 },
  "sink": { "exchange": "events", "routing_key": "orders.created" }
}
```

`password` and `tls.key_pem` are secret names, resolved through the namespace's secret store. The URL never carries credentials; a URL with user information is refused.

## Source

There is one split per queue. Deliveries are consumed with manual acknowledgement. A delivery is acknowledged (`basic.ack`, `multiple`) only after the Fabric has acknowledged it. A crash leaves the deliveries unacknowledged, and RabbitMQ redelivers them, so the source is **at least once**. The position is held by the broker, so a restart resumes without loss.

**Backpressure.** `basic.qos` sets the prefetch to the pipeline's in-flight budget: `max_in_flight_batches` × `min(max_batch_rows, source.max_batch)`, capped at 65,535, unless `source.prefetch` is set. Acknowledgements wait for the Fabric, so once that many deliveries are outstanding the broker stops sending. The source does not buffer.

A message larger than the batch budget is rejected without requeue, so a dead-letter exchange takes it if one is configured, and it is reported as poison.

## Sink

The sink enables publisher confirms (`confirm.select`). A batch is acknowledged only after the broker has confirmed every message in it. If the broker nacks a message, the batch is retried. A `mandatory` message that no queue takes comes back unroutable and is poison. Messages are persistent (`delivery_mode` 2) by default. The routing key is `sink.routing_key`, or the event's `subject` when that is not set. The sink is **at least once**.

## CloudEvents

Both directions use binary mode: `ce_` headers in the message's header table, `datacontenttype` as the message's `content_type`, and the event id as its `message_id`. A message that already carries `ce_`, `ce-`, `cloudEvents:` or `cloudEvents_` headers, or an `application/cloudevents+json` body, passes through and keeps its `type`, `source` and `id`. Any other message becomes `io.loams.dev.flow.rabbitmq-amqp.message.v1`. Its id is `<queue>/<message_id>` when the message has a message id. Otherwise the id is per connection, and ids are not stable across redeliveries.

## Auth

The connector supports `basic` (user name and password) and `mtls` (a client certificate on `amqps://`).
