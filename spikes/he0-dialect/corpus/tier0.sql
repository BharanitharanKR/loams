-- HE0 Task 10 spike corpus: representative ClickHouse statements for the
-- Tier 0 list of design 75 section 7.3. Written by hand for this spike, in
-- the shapes ClickHouse applications send; no application source and no
-- ClickBench text. Each statement starts with "-- @id <name>" and an
-- optional "-- @features a,b,c" line.

-- ============================================================ DDL
-- @id ddl_mergetree_basic
-- @features engine:MergeTree,order_by
CREATE TABLE events (id UUID, name String, ts DateTime64(3), v Float64) ENGINE = MergeTree ORDER BY (name, ts);

-- @id ddl_mergetree_parens
-- @features engine:MergeTree,partition_by,order_by
CREATE TABLE IF NOT EXISTS events (id UUID, name String, ts DateTime64(3)) ENGINE = MergeTree() PARTITION BY toYYYYMM(ts) ORDER BY (name, toStartOfHour(ts), id);

-- @id ddl_replacing_version_deleted
-- @features engine:ReplacingMergeTree,partition_by,order_by,primary_key
CREATE TABLE traces (id String, project_id String, timestamp DateTime64(3), name String, event_ts DateTime64(3), is_deleted UInt8) ENGINE = ReplacingMergeTree(event_ts, is_deleted) PARTITION BY toYYYYMM(timestamp) PRIMARY KEY (project_id, toDate(timestamp)) ORDER BY (project_id, toDate(timestamp), id);

-- @id ddl_replacing_settings
-- @features engine:ReplacingMergeTree,settings
CREATE TABLE scores (id String, trace_id String, name String, value Float64, event_ts DateTime64(3), is_deleted UInt8) ENGINE = ReplacingMergeTree(event_ts, is_deleted) ORDER BY (trace_id, id) SETTINGS index_granularity = 8192;

-- @id ddl_on_cluster_replicated
-- @features on_cluster,engine:ReplicatedReplacingMergeTree
CREATE TABLE traces ON CLUSTER default (id String, project_id String, event_ts DateTime64(3), is_deleted UInt8) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/traces', '{replica}', event_ts, is_deleted) ORDER BY (project_id, id);

-- @id ddl_replicated_macros_short
-- @features on_cluster,engine:ReplicatedMergeTree
CREATE TABLE logs ON CLUSTER '{cluster}' (ts DateTime, msg String) ENGINE = ReplicatedMergeTree ORDER BY ts;

-- @id ddl_distributed
-- @features engine:Distributed,on_cluster
CREATE TABLE traces_dist ON CLUSTER default AS traces ENGINE = Distributed(default, currentDatabase(), traces, rand());

-- @id ddl_ttl_delete
-- @features ttl,engine:MergeTree
CREATE TABLE sessions (id String, created_at DateTime, ended_at DateTime) ENGINE = MergeTree ORDER BY id TTL created_at + INTERVAL 90 DAY;

-- @id ddl_ttl_delete_where
-- @features ttl
CREATE TABLE logs (ts DateTime, level LowCardinality(String), msg String) ENGINE = MergeTree ORDER BY ts TTL toDateTime(ts) + INTERVAL 30 DAY DELETE WHERE level = 'debug';

-- @id ddl_column_ttl
-- @features ttl
CREATE TABLE payloads (id String, ts DateTime, body String TTL ts + INTERVAL 7 DAY) ENGINE = MergeTree ORDER BY id;

-- @id ddl_types_core
-- @features type:Nullable,type:LowCardinality,type:Map,type:Array,type:Decimal,type:Enum8,type:UUID
CREATE TABLE observations (id String, trace_id String, type LowCardinality(String), start_time DateTime64(3), end_time Nullable(DateTime64(3)), model Nullable(String), usage_details Map(LowCardinality(String), UInt64), cost_details Map(LowCardinality(String), Decimal(18, 12)), tags Array(String), level Enum8('DEBUG' = 1, 'DEFAULT' = 2, 'WARNING' = 3, 'ERROR' = 4), uid UUID) ENGINE = MergeTree ORDER BY (trace_id, id);

-- @id ddl_datetime64_tz
-- @features type:DateTime64
CREATE TABLE ticks (ts DateTime64(3, 'UTC'), d Date, d32 Date32, t DateTime('UTC')) ENGINE = MergeTree ORDER BY ts;

-- @id ddl_defaults_materialized_alias
-- @features column:default,column:materialized,column:alias
CREATE TABLE events (ts DateTime64(3), day Date MATERIALIZED toDate(ts), name String DEFAULT '', path String ALIAS concat('/', name)) ENGINE = MergeTree ORDER BY ts;

-- @id ddl_codecs
-- @features column:codec
CREATE TABLE metrics (ts DateTime CODEC(Delta, ZSTD(1)), value Float64 CODEC(Gorilla), name LowCardinality(String) CODEC(ZSTD(3))) ENGINE = MergeTree ORDER BY (name, ts);

-- @id ddl_skip_index_bloom
-- @features index:bloom_filter
CREATE TABLE traces (id String, user_id String, INDEX idx_user user_id TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY id;

-- @id ddl_skip_index_minmax_tokenbf
-- @features index:minmax,index:tokenbf_v1
CREATE TABLE logs (ts DateTime, msg String, INDEX ts_mm ts TYPE minmax GRANULARITY 4, INDEX msg_tok msg TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY ts;

-- @id ddl_comment_and_settings
-- @features settings
CREATE TABLE t (k UInt64 COMMENT 'key', v String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1 COMMENT 'a table';

-- @id ddl_json_type
-- @features type:JSON
CREATE TABLE docs (id UInt64, body JSON) ENGINE = MergeTree ORDER BY id;

-- @id ddl_aggregating
-- @features engine:AggregatingMergeTree,type:AggregateFunction
CREATE TABLE daily (day Date, project_id String, users AggregateFunction(uniq, String), events SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (project_id, day);

-- @id ddl_summing
-- @features engine:SummingMergeTree
CREATE TABLE totals (day Date, k String, n UInt64) ENGINE = SummingMergeTree(n) ORDER BY (day, k);

-- @id ddl_mv_to
-- @features mv
CREATE MATERIALIZED VIEW daily_mv TO daily AS SELECT toDate(ts) AS day, project_id, uniqState(user_id) AS users, count() AS events FROM events GROUP BY day, project_id;

-- @id ddl_mv_on_cluster
-- @features mv,on_cluster
CREATE MATERIALIZED VIEW IF NOT EXISTS events_core_mv ON CLUSTER default TO events_core AS SELECT id, project_id, start_time, name FROM events_full WHERE is_deleted = 0;

-- @id ddl_view
-- @features view
CREATE VIEW recent AS SELECT * FROM events WHERE ts > now() - INTERVAL 1 DAY;

-- @id ddl_database
-- @features on_cluster
CREATE DATABASE IF NOT EXISTS analytics ON CLUSTER default;

-- @id ddl_create_as_select
-- @features ctas
CREATE TABLE top_names ENGINE = MergeTree ORDER BY name AS SELECT name, count() AS c FROM events GROUP BY name;

-- @id ddl_drop_sync
-- @features drop
DROP TABLE IF EXISTS events ON CLUSTER default SYNC;

-- @id ddl_rename
-- @features rename
RENAME TABLE events_tmp TO events;

-- @id ddl_truncate
-- @features truncate
TRUNCATE TABLE IF EXISTS events;

-- ============================================================ ALTER
-- @id alter_add_column
-- @features alter:add_column
ALTER TABLE traces ADD COLUMN IF NOT EXISTS environment LowCardinality(String) DEFAULT 'default' AFTER name;

-- @id alter_add_column_on_cluster
-- @features alter:add_column,on_cluster
ALTER TABLE traces ON CLUSTER default ADD COLUMN IF NOT EXISTS version Nullable(String);

-- @id alter_drop_column
-- @features alter:drop_column
ALTER TABLE traces DROP COLUMN IF EXISTS legacy;

-- @id alter_modify_column
-- @features alter:modify_column
ALTER TABLE traces MODIFY COLUMN name LowCardinality(String);

-- @id alter_delete
-- @features mutation:delete
ALTER TABLE traces DELETE WHERE project_id = 'p1' AND id IN ('a', 'b');

-- @id alter_delete_on_cluster
-- @features mutation:delete,on_cluster
ALTER TABLE scores ON CLUSTER default DELETE WHERE trace_id = 't1';

-- @id alter_update
-- @features mutation:update
ALTER TABLE traces UPDATE is_deleted = 1, event_ts = now64(3) WHERE id = 'x';

-- @id alter_delete_settings
-- @features mutation:delete,settings
ALTER TABLE traces DELETE WHERE project_id = 'p1' SETTINGS mutations_sync = 2;

-- @id alter_drop_partition
-- @features partition:drop
ALTER TABLE events DROP PARTITION 202401;

-- @id alter_drop_partition_tuple
-- @features partition:drop
ALTER TABLE events DROP PARTITION tuple(toYYYYMM(toDate('2024-01-01')));

-- @id alter_modify_ttl
-- @features ttl,alter:modify_ttl
ALTER TABLE sessions MODIFY TTL created_at + INTERVAL 30 DAY;

-- @id alter_add_index
-- @features index:bloom_filter,alter:add_index
ALTER TABLE traces ADD INDEX IF NOT EXISTS idx_name name TYPE bloom_filter() GRANULARITY 1;

-- @id alter_materialize_index
-- @features alter:materialize_index
ALTER TABLE traces MATERIALIZE INDEX idx_name;

-- @id alter_modify_setting
-- @features alter:modify_setting
ALTER TABLE traces MODIFY SETTING ttl_only_drop_parts = 1;

-- @id alter_lightweight_delete
-- @features mutation:lightweight_delete
DELETE FROM traces WHERE id = 'x';

-- @id alter_optimize_final
-- @features optimize
OPTIMIZE TABLE traces FINAL;

-- @id alter_optimize_partition_dedup
-- @features optimize
OPTIMIZE TABLE traces PARTITION 202401 FINAL DEDUPLICATE;

-- ============================================================ INSERT
-- @id ins_values
-- @features insert:values
INSERT INTO events (id, name, ts) VALUES ('00000000-0000-0000-0000-000000000001', 'a', '2024-01-01 00:00:00.000');

-- @id ins_format_jsoneachrow
-- @features insert:format
INSERT INTO events FORMAT JSONEachRow;

-- @id ins_format_jsoneachrow_cols
-- @features insert:format
INSERT INTO events (id, name, ts) FORMAT JSONEachRow;

-- @id ins_settings_async
-- @features insert:format,settings,async_insert
INSERT INTO events SETTINGS async_insert = 1, wait_for_async_insert = 0 FORMAT JSONEachRow;

-- @id ins_select
-- @features insert:select
INSERT INTO daily SELECT toDate(ts), project_id, uniqState(user_id), count() FROM events GROUP BY 1, 2;

-- @id ins_format_rowbinary
-- @features insert:format
INSERT INTO events FORMAT RowBinary;

-- @id ins_format_csv_with_names
-- @features insert:format
INSERT INTO events FORMAT CSVWithNames;

-- @id ins_function_table
-- @features insert:function
INSERT INTO FUNCTION null('x UInt8') SELECT 1;

-- ============================================================ SELECT: basics
-- @id sel_count_empty_args
-- @features function:count
SELECT count() FROM events;

-- @id sel_group_by_alias
-- @features alias_in_group_by,function:toStartOfDay
SELECT toStartOfDay(ts) AS day, count() AS c FROM events WHERE ts >= now() - INTERVAL 7 DAY GROUP BY day ORDER BY day;

-- @id sel_group_by_position
-- @features group_by_position
SELECT name, count() FROM events GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- @id sel_alias_in_where
-- @features alias_in_where
SELECT toDate(ts) AS d, count() FROM events WHERE d >= today() - 7 GROUP BY d;

-- @id sel_alias_reuse_select
-- @features alias_reuse
SELECT v * 2 AS doubled, doubled + 1 AS plus_one FROM events;

-- @id sel_limit_offset
-- @features limit
SELECT * FROM events ORDER BY ts DESC LIMIT 50 OFFSET 100;

-- @id sel_limit_comma
-- @features limit_comma
SELECT * FROM events ORDER BY ts DESC LIMIT 100, 50;

-- @id sel_limit_by
-- @features limit_by
SELECT trace_id, id, start_time FROM observations ORDER BY trace_id, start_time DESC LIMIT 3 BY trace_id;

-- @id sel_limit_by_with_limit
-- @features limit_by,limit
SELECT project_id, name, count() AS c FROM events GROUP BY project_id, name ORDER BY project_id, c DESC LIMIT 5 BY project_id LIMIT 100;

-- @id sel_limit_by_offset
-- @features limit_by
SELECT k, v FROM t ORDER BY k, v LIMIT 1, 2 BY k;

-- @id sel_with_totals
-- @features with_totals
SELECT country, count() FROM events GROUP BY country WITH TOTALS ORDER BY country;

-- @id sel_with_rollup
-- @features with_rollup
SELECT project_id, name, count() FROM events GROUP BY project_id, name WITH ROLLUP;

-- @id sel_rollup_function
-- @features rollup
SELECT project_id, name, count() FROM events GROUP BY ROLLUP(project_id, name);

-- @id sel_final
-- @features final
SELECT id, name FROM traces FINAL WHERE project_id = 'p1' ORDER BY timestamp DESC LIMIT 50;

-- @id sel_final_alias
-- @features final
SELECT t.id FROM traces AS t FINAL WHERE t.project_id = 'p1';

-- @id sel_final_join
-- @features final,join
SELECT t.id, s.value FROM traces AS t FINAL LEFT JOIN scores AS s FINAL ON s.trace_id = t.id WHERE t.project_id = 'p1';

-- @id sel_final_subquery
-- @features final
SELECT count() FROM (SELECT id FROM traces FINAL WHERE is_deleted = 0);

-- @id sel_prewhere
-- @features prewhere
SELECT id, name FROM events PREWHERE project_id = 'p1' WHERE name = 'click';

-- @id sel_final_prewhere
-- @features final,prewhere
SELECT id FROM traces FINAL PREWHERE project_id = 'p1' WHERE name != '';

-- @id sel_sample
-- @features sample
SELECT count() FROM events SAMPLE 0.1;

-- @id sel_settings_tail
-- @features settings
SELECT count() FROM events SETTINGS max_threads = 4, max_execution_time = 30;

-- @id sel_format_tail
-- @features format
SELECT name, count() FROM events GROUP BY name FORMAT JSONEachRow;

-- @id sel_settings_format_tail
-- @features settings,format
SELECT name FROM events LIMIT 1 SETTINGS max_threads = 1 FORMAT JSON;

-- @id sel_query_params
-- @features query_params
SELECT * FROM events WHERE project_id = {projectId: String} AND ts >= {from: DateTime64(3)} LIMIT {limit: UInt32};

-- @id sel_query_params_array
-- @features query_params
SELECT * FROM traces WHERE id IN {ids: Array(String)};

-- @id sel_query_params_identifier
-- @features query_params
SELECT count() FROM {table: Identifier};

-- @id sel_distinct_on
-- @features distinct
SELECT DISTINCT ON (profile_id) profile_id, ts FROM events ORDER BY profile_id, ts DESC;

-- ============================================================ SELECT: joins and CTEs
-- @id sel_cte
-- @features cte
WITH recent AS (SELECT * FROM events WHERE ts > now() - INTERVAL 1 HOUR) SELECT name, count() FROM recent GROUP BY name;

-- @id sel_with_scalar
-- @features with_scalar
WITH 10 AS n SELECT * FROM events LIMIT n;

-- @id sel_with_scalar_subquery
-- @features with_scalar
WITH (SELECT max(ts) FROM events) AS latest SELECT count() FROM events WHERE ts > latest - INTERVAL 1 DAY;

-- @id sel_join_using
-- @features join
SELECT * FROM traces AS t INNER JOIN observations AS o USING (project_id);

-- @id sel_left_join
-- @features join
SELECT t.id, count(o.id) FROM traces AS t LEFT JOIN observations AS o ON o.trace_id = t.id GROUP BY t.id;

-- @id sel_any_left_join
-- @features join:any
SELECT t.id, o.model FROM traces AS t ANY LEFT JOIN observations AS o ON o.trace_id = t.id;

-- @id sel_asof_join
-- @features join:asof
SELECT e.id, p.price FROM events AS e ASOF LEFT JOIN prices AS p ON e.sym = p.sym AND e.ts >= p.ts;

-- @id sel_global_in
-- @features global_in
SELECT count() FROM events WHERE profile_id GLOBAL IN (SELECT profile_id FROM sessions);

-- @id sel_global_join
-- @features global_join
SELECT * FROM events AS e GLOBAL LEFT JOIN sessions AS s ON e.session_id = s.id;

-- @id sel_tuple_in_subquery
-- @features tuple_in
SELECT * FROM traces WHERE (project_id, id) IN (SELECT project_id, trace_id FROM scores);

-- @id sel_semi_join
-- @features join:semi
SELECT t.id FROM traces AS t LEFT SEMI JOIN scores AS s ON s.trace_id = t.id;

-- @id sel_union_all
-- @features union
SELECT 'a' AS k, count() FROM events UNION ALL SELECT 'b', count() FROM traces;

-- @id sel_cross_join_numbers
-- @features table_function:numbers
SELECT number FROM numbers(10);

-- @id sel_numbers_range
-- @features table_function:numbers
SELECT toDate('2024-01-01') + number AS d FROM numbers(0, 30);

-- ============================================================ SELECT: ARRAY JOIN
-- @id sel_array_join
-- @features array_join
SELECT tag, count() FROM traces ARRAY JOIN tags AS tag GROUP BY tag;

-- @id sel_left_array_join
-- @features array_join:left
SELECT id, tag FROM traces LEFT ARRAY JOIN tags AS tag;

-- @id sel_array_join_two_arrays
-- @features array_join
SELECT id, k, v FROM traces ARRAY JOIN mapKeys(metadata) AS k, mapValues(metadata) AS v;

-- @id sel_array_join_function
-- @features function:arrayJoin
SELECT arrayJoin(tags) AS tag, count() FROM traces GROUP BY tag;

-- @id sel_array_join_enumerate
-- @features array_join,function:arrayEnumerate
SELECT id, tag, idx FROM traces ARRAY JOIN tags AS tag, arrayEnumerate(tags) AS idx;

-- ============================================================ SELECT: aggregates and combinators
-- @id sel_if_combinators
-- @features combinator:If
SELECT countIf(name = 'click') AS clicks, sumIf(v, name = 'buy') AS revenue, uniqIf(profile_id, name = 'view') AS viewers FROM events;

-- @id sel_uniq_family
-- @features function:uniq,function:uniqExact,function:uniqCombined
SELECT uniq(profile_id), uniqExact(session_id), uniqCombined(id) FROM events;

-- @id sel_quantile_parametric
-- @features parametric,function:quantile
SELECT quantile(0.95)(duration) FROM events;

-- @id sel_quantiles_multi
-- @features parametric,function:quantiles
SELECT quantiles(0.5, 0.9, 0.99)(duration) FROM events;

-- @id sel_quantile_tdigest
-- @features parametric,function:quantileTDigest
SELECT name, quantileTDigest(0.5)(duration), quantileExact(0.99)(duration) FROM events GROUP BY name;

-- @id sel_quantile_if
-- @features parametric,combinator:If
SELECT quantileIf(0.9)(duration, name = 'load') FROM events;

-- @id sel_argmax
-- @features function:argMax
SELECT id, argMax(name, event_ts) AS name, argMax(is_deleted, event_ts) AS deleted FROM traces GROUP BY id HAVING deleted = 0;

-- @id sel_any_anylast
-- @features function:any,function:anyLast
SELECT session_id, any(country), anyLast(path), min(ts), max(ts) FROM events GROUP BY session_id;

-- @id sel_group_array
-- @features function:groupArray,function:groupUniqArray
SELECT trace_id, groupArray(id), groupUniqArray(name), groupArray(10)(name) FROM observations GROUP BY trace_id;

-- @id sel_state_merge
-- @features combinator:State,combinator:Merge
SELECT day, uniqMerge(users) AS users, sum(events) FROM daily GROUP BY day ORDER BY day;

-- @id sel_state
-- @features combinator:State
SELECT toDate(ts) AS day, uniqState(profile_id) FROM events GROUP BY day;

-- @id sel_merge_state_nested
-- @features combinator:Merge,combinator:State
SELECT uniqMergeState(users) FROM daily;

-- @id sel_array_combinators
-- @features combinator:Array,combinator:ForEach
SELECT sumArray(vals), avgForEach(vals) FROM t;

-- @id sel_window_funnel
-- @features parametric,function:windowFunnel
SELECT level, count() FROM (SELECT profile_id, windowFunnel(86400)(toDateTime(ts), name = 'view', name = 'cart', name = 'buy') AS level FROM events WHERE ts >= now() - INTERVAL 30 DAY GROUP BY profile_id) GROUP BY level ORDER BY level;

-- @id sel_window_funnel_mode
-- @features parametric,function:windowFunnel
SELECT profile_id, windowFunnel(3600, 'strict_order')(toDateTime(ts), name = 'a', name = 'b') FROM events GROUP BY profile_id;

-- @id sel_retention
-- @features function:retention
SELECT profile_id, retention(toDate(ts) = '2024-01-01', toDate(ts) = '2024-01-02') AS r FROM events GROUP BY profile_id;

-- @id sel_sequence_match
-- @features parametric,function:sequenceMatch
SELECT profile_id, sequenceMatch('(?1)(?2)')(toDateTime(ts), name = 'a', name = 'b') FROM events GROUP BY profile_id;

-- @id sel_top_k
-- @features parametric,function:topK
SELECT topK(10)(path) FROM events;

-- @id sel_histogram
-- @features parametric,function:histogram
SELECT histogram(5)(duration) FROM events;

-- ============================================================ SELECT: windows
-- @id sel_window_row_number
-- @features window
SELECT id, row_number() OVER (PARTITION BY trace_id ORDER BY start_time) AS rn FROM observations;

-- @id sel_window_frame
-- @features window
SELECT ts, sum(v) OVER (ORDER BY ts ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) FROM events;

-- @id sel_window_lag_in_frame
-- @features window,function:lagInFrame
SELECT ts, lagInFrame(ts) OVER (PARTITION BY session_id ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prev FROM events;

-- @id sel_window_named
-- @features window
SELECT ts, avg(v) OVER w FROM events WINDOW w AS (PARTITION BY name ORDER BY ts);

-- ============================================================ SELECT: functions
-- @id sel_date_funcs
-- @features function:toStartOfInterval,function:dateDiff,function:toYYYYMM
SELECT toStartOfInterval(ts, INTERVAL 15 minute) AS b, dateDiff('second', min(ts), max(ts)), toYYYYMM(ts) FROM events GROUP BY b, toYYYYMM(ts);

-- @id sel_date_trunc_tz
-- @features function:toStartOfDay,function:toTimeZone
SELECT toStartOfDay(toTimeZone(ts, 'Europe/Berlin')) AS d, count() FROM events GROUP BY d;

-- @id sel_date_conversions
-- @features function:toDateTime64,function:parseDateTimeBestEffort,function:formatDateTime
SELECT toDateTime64('2024-01-01 00:00:00', 3), parseDateTimeBestEffort('2024-01-01T00:00:00Z'), formatDateTime(ts, '%Y-%m-%d') FROM events;

-- @id sel_interval_arith
-- @features interval
SELECT now() - INTERVAL 1 HOUR, today() - toIntervalDay(7), ts + INTERVAL '5' MINUTE FROM events;

-- @id sel_conditionals
-- @features function:if,function:multiIf
SELECT if(v > 0, 'pos', 'neg'), multiIf(v < 10, 'low', v < 100, 'mid', 'high'), coalesce(model, 'none'), ifNull(model, '') FROM observations;

-- @id sel_ternary
-- @features ternary
SELECT v > 0 ? 'pos' : 'neg' FROM events;

-- @id sel_string_funcs
-- @features function:lower,function:position,function:match,function:extract
SELECT lower(name), positionCaseInsensitive(path, 'api'), match(path, '^/api/v[0-9]+'), extract(path, '/([a-z]+)') FROM events;

-- @id sel_like_ilike
-- @features like
SELECT * FROM events WHERE name ILIKE '%click%' AND path NOT LIKE '/static/%';

-- @id sel_json_extract
-- @features function:JSONExtractString,function:JSONExtract
SELECT JSONExtractString(payload, 'user', 'id'), JSONExtract(payload, 'tags', 'Array(String)'), JSONHas(payload, 'x') FROM raw_events;

-- @id sel_json_extract_raw
-- @features function:JSONExtractRaw,function:JSONExtractKeysAndValues
SELECT JSONExtractRaw(payload, 'meta'), JSONExtractKeysAndValues(payload, 'attrs', 'String') FROM raw_events;

-- @id sel_map_access
-- @features map_subscript
SELECT metadata['environment'], usage_details['input'] FROM observations WHERE metadata['env'] = 'prod';

-- @id sel_map_functions
-- @features function:mapKeys,function:mapContains
SELECT mapKeys(metadata), mapValues(metadata), mapContains(metadata, 'x') FROM traces;

-- @id sel_array_funcs
-- @features function:has,function:arrayFilter,function:arrayMap,lambda
SELECT has(tags, 'prod'), length(tags), arrayFilter(x -> x != '', tags), arrayMap(x -> upper(x), tags) FROM traces;

-- @id sel_array_lambda_two_args
-- @features lambda,function:arrayExists
SELECT arrayExists((k, v) -> k = 'env' AND v = 'prod', mapKeys(metadata), mapValues(metadata)) FROM traces;

-- @id sel_array_index
-- @features array_subscript
SELECT tags[1], arrayElement(tags, -1) FROM traces;

-- @id sel_array_literal
-- @features array_literal
SELECT [1, 2, 3] AS a, array('x', 'y') AS b, hasAny(tags, ['a', 'b']) FROM traces;

-- @id sel_tuple_access
-- @features tuple
SELECT tuple(1, 'a').1, (1, 'a') AS t, t.2;

-- @id sel_cast_ch_types
-- @features cast:ch_types
SELECT CAST(v AS UInt64), CAST(name AS LowCardinality(String)), CAST(ts AS DateTime64(3)), CAST(model AS Nullable(String)) FROM events;

-- @id sel_cast_function_form
-- @features cast:string_type
SELECT CAST(v, 'Float32'), toUInt32(v), toString(id) FROM events;

-- @id sel_cast_double_colon
-- @features cast:double_colon
SELECT v::UInt32, ts::Date FROM events;

-- @id sel_uuid_funcs
-- @features function:generateUUIDv4,function:toUUID
SELECT generateUUIDv4(), toUUID('00000000-0000-0000-0000-000000000000');

-- @id sel_hash_funcs
-- @features function:cityHash64,function:sipHash64
SELECT cityHash64(id) % 10, sipHash64(name) FROM events;

-- @id sel_rounding
-- @features function:round,function:intDiv
SELECT round(v, 2), intDiv(duration, 1000), floor(v), v % 7 FROM events;

-- @id sel_in_list_and_between
-- @features in,between
SELECT * FROM events WHERE name IN ('a', 'b') AND ts BETWEEN '2024-01-01' AND '2024-02-01';

-- @id sel_not_in_subquery
-- @features in_subquery
SELECT * FROM traces WHERE id NOT IN (SELECT trace_id FROM scores WHERE value < 0);

-- @id sel_exists_subquery
-- @features exists
SELECT * FROM traces AS t WHERE EXISTS (SELECT 1 FROM scores AS s WHERE s.trace_id = t.id);

-- @id sel_order_with_fill
-- @features with_fill
SELECT toStartOfDay(ts) AS d, count() FROM events GROUP BY d ORDER BY d WITH FILL STEP toIntervalDay(1);

-- @id sel_order_with_fill_from_to
-- @features with_fill
SELECT toStartOfHour(ts) AS h, count() AS c FROM events GROUP BY h ORDER BY h WITH FILL FROM toStartOfHour(now() - INTERVAL 24 HOUR) TO now() STEP INTERVAL 1 HOUR;

-- @id sel_order_nulls
-- @features order_by
SELECT * FROM observations ORDER BY end_time DESC NULLS LAST, id;

-- @id sel_order_collate
-- @features order_by
SELECT name FROM events ORDER BY name COLLATE 'en';

-- @id sel_except_columns
-- @features select_except
SELECT * EXCEPT (payload) FROM raw_events;

-- @id sel_columns_matcher
-- @features columns_matcher
SELECT COLUMNS('^cost_') FROM observations;

-- @id sel_qualify
-- @features qualify
SELECT id, trace_id FROM observations QUALIFY row_number() OVER (PARTITION BY trace_id ORDER BY start_time) = 1;

-- @id sel_system_tables
-- @features system_tables
SELECT name, engine FROM system.tables WHERE database = currentDatabase();

-- @id sel_system_parts
-- @features system_tables
SELECT partition, sum(rows), sum(bytes_on_disk) FROM system.parts WHERE table = 'events' AND active GROUP BY partition;

-- @id sel_heredoc_string
-- @features string_literal
SELECT 'it''s', 'a\tb', $$raw$$;

-- @id sel_escaped_identifiers
-- @features identifiers
SELECT `name`, "path" FROM `events`;

-- @id sel_nested_subqueries
-- @features subquery
SELECT * FROM (SELECT * FROM (SELECT id, ts FROM events) WHERE ts > now() - INTERVAL 1 DAY) ORDER BY ts LIMIT 5;

-- @id sel_scalar_subquery
-- @features scalar_subquery
SELECT id, (SELECT count() FROM scores WHERE trace_id = traces.id) FROM traces;

-- @id sel_having_alias
-- @features having
SELECT name, count() AS c FROM events GROUP BY name HAVING c > 10;

-- @id sel_interval_toStart_tz_param
-- @features function:toStartOfInterval
SELECT toStartOfInterval(ts, toIntervalMinute(5), 'UTC') FROM events;

-- @id sel_date_literal
-- @features literal
SELECT DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00';

-- ============================================================ misc
-- @id misc_set
-- @features set
SET max_threads = 4;

-- @id misc_use
-- @features use
USE analytics;

-- @id misc_show_tables
-- @features show
SHOW TABLES FROM analytics;

-- @id misc_show_create
-- @features show
SHOW CREATE TABLE traces;

-- @id misc_describe
-- @features describe
DESCRIBE TABLE traces;

-- @id misc_exists
-- @features exists_table
EXISTS TABLE traces;

-- @id misc_explain
-- @features explain
EXPLAIN SELECT count() FROM events;

-- @id misc_explain_ast
-- @features explain
EXPLAIN AST SELECT count() FROM events;

-- @id misc_system_flush
-- @features system
SYSTEM FLUSH LOGS;

-- @id misc_kill_query
-- @features kill
KILL QUERY WHERE query_id = 'abc';

-- @id misc_select_version
-- @features function:version
SELECT version();
