# Azure Service Bus

The `azure-service-bus` connector reads Service Bus queues and topic subscriptions, and sends to queues and topics, over AMQP 1.0 (OR-22). It is Loams' own connector, `loams_connector_brokers::amqp10` (CN2 Task 18), built on `fe2o3-amqp` 0.18.2 and `fe2o3-amqp-cbs` 0.18.0. It needs no Azure-specific SDK.

**Tested against** the Service Bus emulator 2.0.1 (`mcr.microsoft.com/azure-messaging/servicebus-emulator`, Microsoft emulator EULA) with its SQL Server sidecar. Both are `ci-service` images in the `connectors-ci-only` profile: CI only, never shipped (OR-3). The emulator's entities are in `deploy/fabric/emulators/servicebus.config.json`. A real namespace runs in the nightly job (`LOAMS_SERVICEBUS_CONNECTION_STRING`). That covers what the emulator lacks: partitioned entities, messages over 256 KB and AMQP over WebSockets (spike §5.3). Status: `planned` until its suites are green in CI.

## Config

With a SAS connection string (`cs`):

```json
{
  "connection_string": "servicebus-connection-string",
  "source": { "addresses": ["orders", "events/Subscriptions/loams"] },
  "sink": { "address": "out" }
}
```

With Entra ID (`aad`):

```json
{
  "namespace": "contoso.servicebus.windows.net",
  "entra": { "tenant_id": "…", "client_id": "…", "client_secret": "servicebus-client-secret" }
}
```

`connection_string` and `entra.client_secret` are secret names.

## Auth

Authentication is claims-based security on `$cbs`.

- **SAS.** With a connection string, the connector signs a SAS token for `sb://<host>/` (HMAC-SHA256 of `<encoded resource>\n<expiry>`) and puts it on `$cbs`. It renews the token at four fifths of `token_ttl_s` (default one hour).
- **Entra ID.** With Entra, it fetches a token through the client-credentials grant (scope `https://servicebus.azure.net/.default`) over the SDK's egress-checked HTTP client, and renews it the same way.
- **Emulator.** A connection string with `UseDevelopmentEmulator=true` connects without TLS to the given host and port.

## Source and sink

The source uses peek-lock. Deliveries stay unsettled, and a message is completed only after the Fabric acknowledges it. A lost lock (a crash, or a Fabric acknowledgement slower than the entity's `LockDuration`) makes Service Bus redeliver the message, so the source is **at least once**.

The sink sets `message-id` to the event id. An entity with duplicate detection drops repeats within its window, but the manifest claims only **at least once**. CloudEvents mapping, credit and outcomes work as for [ActiveMQ](activemq-jms.md).
