# MongoDB connector

The native MongoDB connector (`fabric/crates/loams-connector-mongodb`, CN2 Task 16) serves the `mongodb` manifest. It is first-party Rust on the connector SDK. It has its own MongoDB wire-protocol client (`OP_MSG`, BSON, SCRAM-SHA-256), and every connection goes through the SDK's egress check after DNS. It does not use the `mongodb` driver crate (plan ruling R16.1).

**Tested against:** MongoDB 8.0 (`mongo:8.0.30`, pinned by digest), as two compose services in the `connectors-ci-only` profile:

- `mongodb`, a one-member replica set;
- `mongodb-standalone`, a standalone server.

MongoDB is SSPL, so it is a CI-only `ci-service` image that Loams never ships (OR-3). The suites run in PR CI. Until they are green there, the manifest keeps its current status (OR-23).

## Directions

| Mode | What it does | Events |
|---|---|---|
| `mode: batch` (default) | Reads each collection once, in `_id` order, at one cluster time | `document`, as Arrow (`application/vnd.apache.arrow.stream`) |
| `mode: cdc` | Takes a snapshot, then follows a change stream | `change` (JSON) and `schema-change` |
| `collection: …` | Writes events into a collection: exactly once on replica sets | — |

## Configuration

| Key | Meaning | Default |
|---|---|---|
| `hosts` (or `host` and `port`) | The seed list, as `host` or `host:port`. A URI is refused, because it could carry credentials | port 27017 |
| `replica_set` | Fail unless the server is a member of this set | not checked |
| `database` | The database to read or write | — |
| `username`, `password` | SCRAM-SHA-256 (`basic`). `password` is the **name** of a secret | no authentication |
| `auth_source` | The database that holds the user | `admin` |
| `auth_mechanism` | `SCRAM-SHA-256`, or `MONGODB-X509` (`mtls`) | `SCRAM-SHA-256` when `username` is set |
| `tls` | `true`, or `{ca_pem, cert, key, server_name}`. `cert` and `key` are secret names (PEM) | off |
| `connect_timeout_ms`, `request_timeout_ms` | Timeouts | 10000, 60000 |
| `collections` | Sources only: the collections to read | every collection in `database` |
| `max_splits`, `split_rows` | Batch only: up to `max_splits` `_id` ranges per collection, one per `split_rows` documents | 8, 1000000 |
| `snapshot` | CDC only: `initial` or `never` | `initial` |
| `full_document` | CDC only: `update_lookup`, `when_available`, `required` or `off` | `update_lookup` |
| `before_change` | CDC only: pre-images, `off`, `when_available` or `required` (6.0+) | `off` |
| `heartbeat_ms`, `await_ms`, `window_ms` | CDC only: how often an idle stream checkpoints its token, how long a `getMore` waits, and how often the oplog window is sampled | 10000, 1000, 30000 |
| `collection` | Sinks only: the target collection | — |
| `write` | Sinks only: `upsert` (replace by key, delete on `loamsop = d`) or `insert` | `upsert` |
| `key` | Sinks only: the fields a document is matched on (dotted paths allowed) | `["_id"]` |
| `apply_deletes` | Sinks only: apply `loamsop = d` | `true` |
| `transactional` | Sinks only: exactly once, with a transaction and `_loams_checkpoints` | `true` |
| `checkpoint_collection` | Sinks only: where the checkpoint lives, in `database` | `_loams_checkpoints` |
| `evolution`, `lossy` | The schema policy (see [schema mapping](schema-mapping.md)) | `additive`, none |

All reads and writes go to the primary. If a seed is a secondary, the connector connects to the primary that the seed's `hello` names, and that connection is egress-checked too. After an election, a write fails as retryable, and the next attempt finds the new primary.

The task leaves the config schema (`connectors/schemas/mongodb.config.json`) and the manifest to Task 2 (plan ruling R16.12).

## Batch reads

Splits are planned once per run:

- A collection of up to `split_rows` documents (by its metadata count) is one split, named after the collection.
- A larger collection is cut into up to `max_splits` `_id` ranges, taken from a `$sample` of its ids. This happens only when its smallest and largest `_id` share a BSON type bracket. Otherwise it stays one split.

Every split reads with `readConcern: snapshot` at the one cluster time the planner recorded, so together the splits are one consistent snapshot.

A split resumes after the last `_id` it delivered:

- If the snapshot has aged out of `minSnapshotHistoryWindowInSeconds` (5 minutes by default), the rest of the split is read at a new cluster time. Event ids keep the run's time.
- A range split's id includes a hash of the plan. A restart with a new plan therefore reads those collections again instead of mixing two plans. This is at least once.
- A standalone server has no snapshot reads, so it is read with `readConcern: local`.

Documents become Arrow batches (see Types). A field seen for the first time is an additive change and comes with a `schema-change` event.

## CDC

**Requirements.** Change streams need a replica set or a sharded cluster. A standalone server is refused with a clear error. The user needs `changeStream` and `find` on the database. To read the oplog window metric, it also needs `find` on `local.oplog.rs`; without that, the metric is turned off with one warning.

**Snapshot handoff.** With `snapshot: initial`:

- The connector records a cluster time T and reads every collection at T. Each document becomes a `change` event with `loamsop = r`.
- The change stream then starts at T and drops events at or before T.

So a write is in the snapshot or in the stream, never both and never neither. If the snapshot runs longer than the server's snapshot history, the rest of it is read at a newer time. Its documents may then also arrive as changes (at least once), but none is missed (plan ruling R16.8).

**Ids and order.**

- A change's id is `<db>.<collection>/<resume token>`, so a re-read yields the same id and `ingest` drops it.
- `loamslsn` is the token's `_data`, which sorts in stream order.
- The position is the resume token. On a quiet database it still advances through the server's `postBatchResumeToken`, and is checkpointed at most every `heartbeat_ms`.

**Data.** Each change carries `{"ns", "op", "key", "before", "after", "update"}` in relaxed Extended JSON:

- `after` is the full document (`update_lookup`: the document as it is when the event is read).
- `update` is the update description (`updated`, `removed`, `truncated`).
- `before` is the pre-image, when it is configured and the collection has `changeStreamPreAndPostImages` enabled.

**DDL.** Collection DDL (`drop`, `rename`, `create`, `modify`, index changes, and on 6.0+ the expanded events) becomes a `schema-change` event. The connector also tracks each collection's document fields:

- A new field is an additive `schema-change`.
- A value that no existing column can hold pauses the instance (see Types).

**Invalidate.** Dropping or renaming the database invalidates the stream, and the instance pauses with an error that says so. When you resume it, the stream starts after the invalidate.

**Stop and delete.** A change stream holds nothing on the server, so stopping and deleting free nothing. The oplog is the server's: if the instance stays stopped longer than the oplog window, its token is lost. See the [runbook](../../runbooks/connectors/mongodb-cdc.md).

## Sink

An event can be one of:

- a JSON document (Extended JSON is read, so `{"$oid": …}` is an ObjectId);
- a CDC `change`: `after` replaces the document, `update` alone patches it with `$set`/`$unset`, and `loamsop = d` deletes by `key`;
- an Arrow batch, where each row is a document.

**Writes.** With `write: upsert`:

- Each document replaces the one that matches `key`, or is inserted when there is none.
- A document with no `_id` gets the event id as its `_id` when the key is `_id`.
- The writes go as ordered `update`, `delete` and `insert` commands, so the order per key holds.

A document the server refuses (a validation error, a duplicate key on another index) goes to the DLQ with its error.

**Exactly once** (`transactional: true` on a replica set):

- Each batch is one multi-document transaction. It also writes the instance's document in `_loams_checkpoints`, and that write is fenced by the lease epoch.
- The transaction commits with `w: "majority"`. After a crash, the sink resumes from the checkpoint the database committed.
- The sink uses one logical session per instance, with transaction numbers that only grow. A restarted sink therefore aborts whatever its crashed predecessor left open instead of waiting for it to time out (R16.9).

**Standalone servers** have no transactions. There, the sink writes each batch and then the checkpoint, which is effectively once for `upsert` (a replayed batch writes the same documents) and at least once for `insert`. `MongoFactory::sink_delivery` reports which one a deployment gives.

## Types

| BSON | Arrow |
|---|---|
| `double` | `Float64` |
| `int`, `long` | `Int64` |
| `int` and `double` in one field | `Float64` |
| `string` | `Utf8` |
| `bool` | `Boolean` |
| `date` | `Timestamp(ms, UTC)` |
| `objectId` | `Utf8`, 24 hex digits |
| `decimal` | `Utf8`, the exact decimal |
| `binData` subtype 0 | `Binary` |
| documents, arrays, other binary subtypes, timestamps, regexes and fields whose types no one column holds | `LargeUtf8`, relaxed Extended JSON |

Each column records its BSON type in the field metadata `loams.bson.type`, so the sink writes an ObjectId back as an ObjectId.

The schema of a collection is the union of what the connector has seen:

- A field missing from a batch stays as a null column.
- A null-only field waits until it has a typed value.
- An `int` goes into an existing `long` or `double` column.

A value that no existing column holds changes the column's type, and the instance's `evolution` policy decides what happens. Under `additive`, the default, a `string` field that gets a number pauses the instance. Under `widen`, a `Utf8` column may become `LargeUtf8` (Extended JSON). From Arrow, `Timestamp(µs/ns)` and decimals over 34 digits are lossy and refused unless they are listed in `lossy`. Nested Arrow types (lists, structs) are not mapped.

## Fallbacks

Debezium's MongoDB connector (`debezium-mongodb`) and Iggy's `mongodb_sink` remain opt-in fallbacks (OR-4). Task 2 records them in the manifests (R16.12).
