-- Interop baseline: today's House v2 tables (nested namespace ns_1.base) read by Spark.
SHOW NAMESPACES IN house;
SHOW NAMESPACES IN house.ns_1;
SHOW TABLES IN house.ns_1.base;
DESCRIBE TABLE house.ns_1.base.events;
SELECT 'events', count(*), sum(CAST(user_id AS BIGINT)), sum(CASE WHEN maybe IS NULL THEN 1 ELSE 0 END), min(ts), max(ts), sum(u32), sum(small), sum(big) FROM house.ns_1.base.events;
SELECT 'events_types', ts, day, user_id, name, value, amount, tags, attrs, maybe, flag, uid, small, big, u8, u32, f32, dt FROM house.ns_1.base.events ORDER BY user_id, ts LIMIT 2;
SELECT 'events_pruned', count(*) FROM house.ns_1.base.events WHERE ts >= TIMESTAMP '2026-10-01 00:00:00';
SELECT 'latest', count(*), sum(CAST(id AS BIGINT)), sum(is_deleted) FROM house.ns_1.base.latest;
SELECT 'mutated', count(*) FROM house.ns_1.base.mutated;
SELECT 'events_files', count(*), sum(record_count) FROM house.ns_1.base.events.files;
SELECT 'events_partitions', partition, record_count FROM house.ns_1.base.events.partitions ORDER BY partition;
