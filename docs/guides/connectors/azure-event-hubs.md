# Azure Event Hubs

Event Hubs runs on the native [Kafka connector](kafka.md) through its Kafka
endpoint (CN2 OR-22), using the target profile
`connectors/profiles/azure-event-hubs.yaml`. No Azure SDK is involved.

## Tested against

| System | Version | Where | Suites |
|---|---|---|---|
| Event Hubs emulator | 2.2.1 (`mcr.microsoft.com/azure-messaging/eventhubs-emulator`, with Azurite 3.37.0) | `deploy/fabric/compose.yaml`, profile `connectors-ci-only` (Microsoft EULA, so CI only, OR-3); CI job `connectors-ci-only` | contract, roundtrip, kill-restart, dup-check |
| A real Event Hubs namespace | the service | CI job `connectors-nightly`, secrets `LOAMS_EVENTHUBS_NAMESPACE`, `LOAMS_EVENTHUBS_CONNECTION_STRING` | the same |

## Connecting

The profile sets `security_protocol: sasl_ssl`, SASL `plain` and the
username `$ConnectionString`. An instance gives:

```yaml
brokers: <namespace>.servicebus.windows.net:9093
topics: [<event hub>]
group_id: <consumer group>      # an Event Hubs consumer group, e.g. $Default
sasl:
  password: <name of the secret holding the namespace connection string>
```

The emulator listens without TLS on 9092, so a CI instance sets
`security_protocol: sasl_plaintext`.

## Narrowings

* `sink.transactional`: Event Hubs has no Kafka transactions on the emulator,
  and offers them only in preview on Premium and Dedicated tiers (R0.19). The
  sink is therefore `effectively_once` through the idempotent producer, and
  `delivery: exactly_once` is refused when the instance is created. The
  compacted `__loams_checkpoints` topic, which only the transactional sink
  uses, goes with it.
* It is not yet verified whether Event Hubs accepts `enable.idempotence=true`.
  `eventhubs_kafka_endpoint_suite` is the proof. If Event Hubs refuses it, the
  sink narrows to `at_least_once`.
* Topics (event hubs) and consumer groups are managed in Azure, not through
  the Kafka admin API.

## Fallback

None (native, OR-22).
