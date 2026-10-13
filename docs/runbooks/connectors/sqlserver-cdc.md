# SQL Server CDC

How to prepare a database for the native SQL Server CDC source (`mode: cdc`, see the [guide](../../guides/connectors/sql-server.md)), follow a schema change, and recover a lost position.

## Enable CDC

1. On SQL Server, make sure SQL Server Agent is running: the capture and cleanup jobs are Agent jobs. Azure SQL runs its own scheduler.
2. Enable CDC on the database (this needs `sysadmin`, or `db_owner` on Azure SQL):

   ```sql
   EXEC sys.sp_cdc_enable_db;
   ```

3. Enable snapshot isolation, which the initial snapshot reads under (or set `isolation: read_committed`):

   ```sql
   ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;
   ```

4. Enable each table the instance reads. The capture instance must capture the table's key columns:

   ```sql
   EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo', @source_name = N'orders',
     @role_name = N'loams_cdc',          -- the gating role the instance's login is in
     @supports_net_changes = 0;
   ```

5. Grant the instance's login `SELECT` on the table (for the snapshot) and membership of the gating role. Reading the cleanup job's retention for the metric also needs `SELECT` on `msdb.dbo.cdc_jobs`; without it, set `retention_minutes`.

## After a schema change

A capture instance keeps the columns it was created with. After an `ALTER TABLE`:

1. Create a second capture instance (a table may have two):

   ```sql
   EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders',
     @role_name = N'loams_cdc', @capture_instance = N'dbo_orders_v2';
   ```

2. The source reads the old instance up to the new one's start LSN, emits a `schema-change` event, and follows the new one. Check that the instance's position names the new capture instance.
3. Drop the old one: `EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'orders', @capture_instance = N'dbo_orders';`

If the instance's `evolution` refuses the change (a dropped column under `additive`, or anything under `pause`), the instance pauses with `SchemaIncompatible`. Change the policy or recreate the instance.

## Retention and a lost position

The cleanup job deletes changes older than its retention (4320 minutes, three days, by default):

```sql
SELECT retention FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'cleanup';
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080;   -- seven days
```

Watch the `position_risk` metric (the position's age over the retention). It nears 1 when a stopped or slow instance is about to lose changes, and past 1 the instance stops with `PositionLost`. To recover, re-snapshot: delete the instance's checkpoint (or delete the instance and create it again) so that it starts with a new snapshot.

## Disable

Run `EXEC sys.sp_cdc_disable_table … @capture_instance = N'all'` for each table, then `EXEC sys.sp_cdc_disable_db`. Deleting a Loams instance leaves CDC as it was: the database's capture instances belong to the operator.
