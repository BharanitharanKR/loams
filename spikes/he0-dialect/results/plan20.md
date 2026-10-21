| id | DataFusion 54.1 as shipped | with the proto-frontend | rewrites applied |
|---|---|---|---|
| `q01_daily_counts` | plans | plans | camelCase function names quoted |
| `q02_if_combinators` | fails: Error during planning: Invalid function 'countif'. | plans | -If → FILTER (WHERE …) |
| `q03_parametric_quantiles` | **silently drops parameters of quantile(...)(...), parameters of quantiles(...)(...)** | plans | parameters → leading arguments |
| `q04_final` | **silently drops FINAL (read as a table alias)** | plans | FINAL → ReplacingFinal extension node (RelationPlanner) |
| `q05_limit_by` | fails: Schema error: No field named trace_id. | plans | LIMIT n BY k → row_number() window + filter |
| `q06_with_totals` | **silently drops GROUP BY WITH TOTALS** | plans | WITH TOTALS → GROUPING SETS ((keys), ()). Notes: WITH TOTALS: totals row must bypass ORDER BY/LIMIT and go to the format's totals slot |
| `q07_array_join` | fails: Error during planning: table 'tags' not found | plans | ARRAY JOIN → derived table with unnest() |
| `q08_left_array_join_map` | fails: This feature is not implemented: Table Functions are not supported | plans | ARRAY JOIN → derived table with unnest(); camelCase function names quoted. Notes: LEFT ARRAY JOIN planned as ARRAY JOIN: unnest() drops empty arrays; needs an Unnest option |
| `q09_array_join_function` | fails: Error during planning: Invalid function 'arrayjoin'. | plans | arrayJoin() → unnest(); camelCase function names quoted |
| `q10_map_and_json` | plans | plans |  |
| `q11_window_funnel` | **silently drops parameters of windowFunnel(...)(...)** | plans | camelCase function names quoted; parameters → leading arguments |
| `q12_argmax_dedup` | plans | plans | camelCase function names quoted |
| `q13_cte_left_join` | plans | plans |  |
| `q14_window_functions` | plans | plans | camelCase function names quoted |
| `q15_prewhere` | **silently drops PREWHERE** | plans | PREWHERE → WHERE |
| `q16_state_merge` | plans | plans | camelCase function names quoted |
| `q17_alias_scoping` | fails: Schema error: No field named d. Valid fields are events.id, events.name, events.project_id, events.profile_id, events.session_id, events.ts, | plans | camelCase function names quoted; select aliases substituted in WHERE / SELECT |
| `q18_lambdas` | fails: Error during planning: Invalid function 'arrayfilter'. | plans | arrayFilter(λ, a) → array_filter(a, λ); arrayMap(λ, a) → array_transform(a, λ) |
| `q19_params_settings_format` | fails: Error during planning: Invalid function 'datetime64'. | plans | query parameters substituted (the front's job); FORMAT, SETTINGS left to the front |
| `q20_casts_buckets_conditionals` | fails: This feature is not implemented: Unsupported SQL type LowCardinality(STRING) | plans | camelCase function names quoted |

Raw: 6/20 plan correctly, 5/20 plan but silently drop a construct. Proto-frontend: 20/20 plan.
