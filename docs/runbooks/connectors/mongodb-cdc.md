# Runbook: MongoDB CDC (change streams)

This runbook is for `mongodb` instances with `mode: cdc` (CN2 Task 16).

## What the connector holds on the server

Nothing. A change stream is a cursor, and the connector keeps its position, the resume token, in the instance's checkpoint. Deleting an instance frees nothing on the server, and nothing is left behind.

The connector can only resume from a token while the oplog still holds it. The oplog is a capped collection that the server trims by size, or by `oplogMinRetentionHours`. An instance that is stopped or paused for longer than the oplog window loses its position.

## Watching it

Every `window_ms` (30 s by default), the source reads the first and last entries of `local.oplog.rs`. It reports two values through the connector observer, which Task 24 exports as metrics:

- **lag**: how far the token's cluster time is behind the newest oplog entry.
- **position risk**: how much of the oplog window the token has used, from 0 to 1. Past 1, the token is gone.

You can check the same thing by hand:

```js
rs.printReplicationInfo()          // the oplog's size and time window
db.getSiblingDB("local").oplog.rs.find().sort({$natural: 1}).limit(1)   // oldest entry
```

If the connector's user cannot read `local.oplog.rs`, the metric is off, and the instance logs one warning. To turn it on, grant `read` on `local` (or `clusterMonitor`).

## Symptoms and actions

| Symptom | Cause | Action |
|---|---|---|
| Position risk climbs toward 1 | The instance is slow, stopped or paused, or the oplog is small for the write rate | Resume the instance or find out why it is slow; raise the oplog size or `oplogMinRetentionHours` |
| The instance fails with `position_lost` (`ChangeStreamHistoryLost`, or the resume token was not found) | The token fell out of the oplog | Delete and recreate the instance. It takes a new snapshot, and the new snapshot's documents are new events downstream (at least once) |
| The instance is paused: "the change stream … was invalidated" | The database was dropped or renamed | Check that this was intended, then resume: the stream starts after the invalidate. If the database was renamed, point a new instance at the new name |
| The instance is paused with a schema error | A field got a value that no existing column holds (for example, a number in a string field) | Fix the data, or restart the instance under `evolution: widen` (it accepts lossless widenings such as `Utf8` → `LargeUtf8`) |
| The start fails: "change streams need a replica set" | The server is standalone | Run it as a replica set (one member is enough), or use `mode: batch` |
| `update` events have no `after` | `full_document: off`, or the document was deleted before the lookup | Use `update_lookup`. For an exact post-image, use `required` with `changeStreamPreAndPostImages` on the collection (6.0+) |
| The snapshot logs "aged out; continuing at a new cluster time" | A collection took longer to read than `minSnapshotHistoryWindowInSeconds` | Nothing is lost, but some documents may arrive twice. Raise the window on the server for very large collections |

## Deleting an instance

`DeleteInstance` and `StopInstance` both free nothing on the server. Delete an instance that will not come back, so that its position-risk alerts stop.
