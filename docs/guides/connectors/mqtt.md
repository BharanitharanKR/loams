# MQTT 5

The `mqtt` connector subscribes to and publishes on MQTT 5 brokers. It is Loams' own connector, `loams_connector_brokers::mqtt` (CN2 Task 18), and it uses the family's own MQTT 5 client rather than `rumqttc` (R18.3).

**Tested against** Eclipse Mosquitto 2.0.22 (`eclipse-mosquitto`, EPL-2.0 OR BSD-3-Clause), the `mosquitto` service of `deploy/fabric/compose.yaml`, profile `connectors-p2`. The unit tests also run the client against an in-process broker. That checks the protocol handling, but it never counts toward the suites (OR-2). Status: `planned` until its suites are green in CI.

## Config

```json
{
  "url": "mqtts://mqtt.example:8883",
  "username": "loams",
  "password": "mqtt-password",
  "source": { "topics": ["devices/+/telemetry"] },
  "sink": { "topic": "loams/out" }
}
```

`password` and `tls.key_pem` are secret names. The socket is the SDK's, which checks egress after DNS and adds TLS for `mqtts://`.

## Source

There is one split, and one session, per topic filter. Each session is persistent: `clean_start = false`, a session expiry (`session_expiry_s`, one hour by default), and a stable client id `loams-<instance>-<n>`. The subscription is QoS 1. A message's PUBACK is sent only after the Fabric acknowledges it, in the order the messages arrived (MQTT 5 §4.6). A crash leaves the unacknowledged messages in the session, and the broker resends them (DUP) on reconnect, so the source is **at least once** (`mqtt5_qos1_no_loss_on_restart`).

**Backpressure.** The CONNECT's Receive Maximum is the pipeline's in-flight budget, capped at 65,535, unless `source.receive_maximum` is set. The broker stops sending once that many QoS 1 messages are unacknowledged. This is MQTT 5's own flow control.

## Sink

The sink publishes at QoS 1. A batch is acknowledged when every PUBACK has arrived with a success code (below 0x80; 0x10, "no matching subscribers", counts as a success). The sink is **at least once**. QoS 2 is never used.

## CloudEvents

The event's attributes travel as MQTT 5 user properties with the `ce_` prefix, and `datacontenttype` as the Content Type property. Textual data sets the payload format indicator. Upstream CloudEvents pass through. Other messages become `io.loams.dev.flow.mqtt.message.v1` events with the topic as `subject`.

## Auth

The connector supports `basic` (user name and password) and `mtls`.
