## Spike Tier 0 corpus (tier0.sql + plan20.sql)

| | statements | parse 0.62 | parse 0.63 | faithful 0.62 | faithful 0.63 |
|---|---|---|---|---|---|
| alter | 17 | 5 | 5 | 5 | 5 |
| ddl | 28 | 13 | 13 | 13 | 13 |
| ins | 8 | 8 | 8 | 8 | 8 |
| misc | 11 | 7 | 7 | 7 | 7 |
| sel | 131 | 118 | 119 | 113 | 114 |
| **all** | **195** | **151 (77.4 %)** | **152 (77.9 %)** | **146 (74.9 %)** | **147 (75.4 %)** |

### Failures and lossy parses (0.63; 0.62 where it differs)

| id | 0.63 | 0.62 |
|---|---|---|
| `ddl_mergetree_parens` | fails: sql parser error: Expected: end of statement, found: ORDER at Line: 1, Column: 123 | same |
| `ddl_replacing_version_deleted` | fails: sql parser error: Expected: end of statement, found: PRIMARY at Line: 1, Column: 215 | same |
| `ddl_replacing_settings` | fails: sql parser error: Expected: end of statement, found: SETTINGS at Line: 1, Column: 194 | same |
| `ddl_distributed` | fails: sql parser error: Expected: SELECT, VALUES, or a subquery in the query body, found: traces at Line: 1, Column: | same |
| `ddl_ttl_delete` | fails: sql parser error: Expected: end of statement, found: TTL at Line: 1, Column: 106 | same |
| `ddl_ttl_delete_where` | fails: sql parser error: Expected: end of statement, found: TTL at Line: 1, Column: 106 | same |
| `ddl_column_ttl` | fails: sql parser error: Expected: ',' or ')' after column definition, found: TTL at Line: 1, Column: 60 | same |
| `ddl_datetime64_tz` | fails: sql parser error: Expected: literal int, found: 'UTC' at Line: 1, Column: 77 | same |
| `ddl_codecs` | fails: sql parser error: Expected: ',' or ')' after column definition, found: CODEC at Line: 1, Column: 35 | same |
| `ddl_skip_index_bloom` | fails: sql parser error: Expected: ',' or ')' after column definition, found: user_id at Line: 1, Column: 64 | same |
| `ddl_skip_index_minmax_tokenbf` | fails: sql parser error: Expected: ',' or ')' after column definition, found: ts at Line: 1, Column: 57 | same |
| `ddl_comment_and_settings` | fails: sql parser error: Expected: end of statement, found: SETTINGS at Line: 1, Column: 81 | same |
| `ddl_mv_on_cluster` | fails: sql parser error: Expected: AS, found: ON at Line: 1, Column: 55 | same |
| `ddl_database` | fails: sql parser error: Expected: end of statement, found: ON at Line: 1, Column: 41 | same |
| `ddl_drop_sync` | fails: sql parser error: Expected: end of statement, found: default at Line: 1, Column: 40 | same |
| `alter_add_column` | fails: sql parser error: Expected: end of statement, found: EXISTS at Line: 1, Column: 38 | same |
| `alter_add_column_on_cluster` | fails: sql parser error: Expected: end of statement, found: EXISTS at Line: 1, Column: 57 | same |
| `alter_delete` | fails: sql parser error: Expected: ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, SET, or SET TBLPROPERTIES af | same |
| `alter_delete_on_cluster` | fails: sql parser error: Expected: ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, SET, or SET TBLPROPERTIES af | same |
| `alter_update` | fails: sql parser error: Expected: ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, SET, or SET TBLPROPERTIES af | same |
| `alter_delete_settings` | fails: sql parser error: Expected: ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, SET, or SET TBLPROPERTIES af | same |
| `alter_drop_partition` | fails: sql parser error: Expected: (, found: 202401 at Line: 1, Column: 35 | same |
| `alter_drop_partition_tuple` | fails: sql parser error: Expected: (, found: tuple at Line: 1, Column: 35 | same |
| `alter_modify_ttl` | fails: sql parser error: Expected: end of statement, found: + at Line: 1, Column: 44 | same |
| `alter_add_index` | fails: sql parser error: Expected: end of statement, found: NOT at Line: 1, Column: 33 | same |
| `alter_materialize_index` | fails: sql parser error: Expected: ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, SET, or SET TBLPROPERTIES af | same |
| `alter_modify_setting` | fails: sql parser error: Expected: end of statement, found: = at Line: 1, Column: 55 | same |
| `sel_limit_by_with_limit` | fails: sql parser error: Expected: end of statement, found: LIMIT at Line: 1, Column: 127 | same |
| `sel_limit_by_offset` | fails: sql parser error: Expected: end of statement, found: BY at Line: 1, Column: 45 | same |
| `sel_final` | lossy: final_parsed_as_alias | same |
| `sel_final_alias` | fails: sql parser error: Expected: end of statement, found: FINAL at Line: 1, Column: 30 | same |
| `sel_final_join` | fails: sql parser error: Expected: end of statement, found: FINAL at Line: 1, Column: 39 | same |
| `sel_final_prewhere` | lossy: final_parsed_as_alias | same |
| `sel_query_params_array` | ok | fails: sql parser error: Expected: (, found: { at Line: 1, Column: 34 |
| `sel_query_params_identifier` | fails: sql parser error: Expected: identifier, found: { at Line: 1, Column: 21 | same |
| `sel_with_scalar` | fails: sql parser error: Expected: identifier, found: 10 at Line: 1, Column: 6 | same |
| `sel_with_scalar_subquery` | fails: sql parser error: Expected: identifier, found: ( at Line: 1, Column: 6 | same |
| `sel_any_left_join` | fails: sql parser error: Expected: end of statement, found: ANY at Line: 1, Column: 39 | same |
| `sel_asof_join` | fails: sql parser error: Expected: JOIN, found: LEFT at Line: 1, Column: 44 | same |
| `sel_global_in` | fails: sql parser error: Expected: end of statement, found: GLOBAL at Line: 1, Column: 45 | same |
| `sel_array_join_two_arrays` | lossy: array_join_list_parsed_as_cross_join | same |
| `sel_array_join_enumerate` | lossy: array_join_list_parsed_as_cross_join | same |
| `sel_ternary` | fails: sql parser error: Expected: end of statement, found: ? at Line: 1, Column: 14 | same |
| `sel_tuple_access` | fails: sql parser error: Expected: end of statement, found: .1 at Line: 1, Column: 21 | same |
| `misc_exists` | fails: sql parser error: Expected: an SQL statement, found: EXISTS at Line: 1, Column: 1 | same |
| `misc_explain_ast` | fails: sql parser error: Expected: end of statement, found: SELECT at Line: 1, Column: 13 | same |
| `misc_system_flush` | fails: sql parser error: Expected: an SQL statement, found: SYSTEM at Line: 1, Column: 1 | same |
| `misc_kill_query` | fails: sql parser error: Expected: literal int, found: WHERE at Line: 1, Column: 12 | same |
| `q04_final` | lossy: final_parsed_as_alias | same |

### Features with a failing or lossy statement (0.63)

| feature | statements | parse 0.63 | faithful 0.63 |
|---|---|---|---|
| `alter:add_column` | 2 | 0 | 0 |
| `alter:add_index` | 1 | 0 | 0 |
| `alter:materialize_index` | 1 | 0 | 0 |
| `alter:modify_setting` | 1 | 0 | 0 |
| `alter:modify_ttl` | 1 | 0 | 0 |
| `array_join` | 4 | 4 | 2 |
| `column:codec` | 1 | 0 | 0 |
| `drop` | 1 | 0 | 0 |
| `engine:Distributed` | 1 | 0 | 0 |
| `engine:MergeTree` | 3 | 1 | 1 |
| `engine:ReplacingMergeTree` | 2 | 0 | 0 |
| `exists_table` | 1 | 0 | 0 |
| `explain` | 2 | 1 | 1 |
| `final` | 6 | 4 | 1 |
| `function:arrayEnumerate` | 1 | 1 | 0 |
| `global_in` | 1 | 0 | 0 |
| `index:bloom_filter` | 2 | 0 | 0 |
| `index:minmax` | 1 | 0 | 0 |
| `index:tokenbf_v1` | 1 | 0 | 0 |
| `join` | 4 | 3 | 3 |
| `join:any` | 1 | 0 | 0 |
| `join:asof` | 1 | 0 | 0 |
| `kill` | 1 | 0 | 0 |
| `limit` | 2 | 1 | 1 |
| `limit_by` | 4 | 2 | 2 |
| `mutation:delete` | 3 | 0 | 0 |
| `mutation:update` | 1 | 0 | 0 |
| `mv` | 2 | 1 | 1 |
| `on_cluster` | 7 | 2 | 2 |
| `order_by` | 5 | 3 | 3 |
| `partition:drop` | 2 | 0 | 0 |
| `partition_by` | 2 | 0 | 0 |
| `prewhere` | 3 | 3 | 2 |
| `primary_key` | 1 | 0 | 0 |
| `query_params` | 4 | 3 | 3 |
| `settings` | 7 | 4 | 4 |
| `system` | 1 | 0 | 0 |
| `ternary` | 1 | 0 | 0 |
| `ttl` | 4 | 0 | 0 |
| `tuple` | 1 | 0 | 0 |
| `type:DateTime64` | 1 | 0 | 0 |
| `with_scalar` | 2 | 0 | 0 |

