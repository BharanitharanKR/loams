| id | DataFusion 54.1 as shipped | with the proto-frontend | rewrites applied |
|---|---|---|---|
| `sel_limit_by_with_limit` | parse fails: sql parser error: Expected: end of statement, found: LIMIT at Line: 1, Column: 127 | parse fails: sql parser error: Expected: end of statement, found: LIMIT at Line: 1, Column: 127 |  |
| `sel_limit_by_offset` | parse fails: sql parser error: Expected: end of statement, found: BY at Line: 1, Column: 45 | parse fails: sql parser error: Expected: end of statement, found: BY at Line: 1, Column: 45 |  |
| `sel_final_alias` | parse fails: sql parser error: Expected: end of statement, found: FINAL at Line: 1, Column: 30 | parse fails: sql parser error: Expected: end of statement, found: FINAL at Line: 1, Column: 30 |  |
| `sel_final_join` | parse fails: sql parser error: Expected: end of statement, found: FINAL at Line: 1, Column: 39 | parse fails: sql parser error: Expected: end of statement, found: FINAL at Line: 1, Column: 39 |  |
| `sel_sample` | **silently drops SAMPLE** | **silently drops SAMPLE** |  |
| `sel_with_scalar` | parse fails: sql parser error: Expected: identifier, found: 10 at Line: 1, Column: 6 | parse fails: sql parser error: Expected: identifier, found: 10 at Line: 1, Column: 6 |  |
| `sel_with_scalar_subquery` | parse fails: sql parser error: Expected: identifier, found: ( at Line: 1, Column: 6 | parse fails: sql parser error: Expected: identifier, found: ( at Line: 1, Column: 6 |  |
| `sel_any_left_join` | parse fails: sql parser error: Expected: end of statement, found: ANY at Line: 1, Column: 39 | parse fails: sql parser error: Expected: end of statement, found: ANY at Line: 1, Column: 39 |  |
| `sel_asof_join` | parse fails: sql parser error: Expected: JOIN, found: LEFT at Line: 1, Column: 44 | parse fails: sql parser error: Expected: JOIN, found: LEFT at Line: 1, Column: 44 |  |
| `sel_global_in` | parse fails: sql parser error: Expected: end of statement, found: GLOBAL at Line: 1, Column: 45 | parse fails: sql parser error: Expected: end of statement, found: GLOBAL at Line: 1, Column: 45 |  |
| `sel_tuple_in_subquery` | fails: Error during planning: Too many columns! The subquery should only return one column: scores.project_id, scores.trace_id | fails: Error during planning: Too many columns! The subquery should only return one column: scores.project_id, scores.trace_id |  |
| `sel_cross_join_numbers` | fails: This feature is not implemented: Table Functions are not supported | fails: This feature is not implemented: Table Functions are not supported |  |
| `sel_numbers_range` | fails: This feature is not implemented: Table Functions are not supported | fails: This feature is not implemented: Table Functions are not supported | camelCase function names quoted |
| `sel_array_combinators` | fails: Error during planning: Invalid function 'sumarray'. | fails: Error during planning: Invalid function 'sumArray'. | camelCase function names quoted |
| `sel_ternary` | parse fails: sql parser error: Expected: end of statement, found: ? at Line: 1, Column: 14 | parse fails: sql parser error: Expected: end of statement, found: ? at Line: 1, Column: 14 |  |
| `sel_array_lambda_two_args` | fails: Error during planning: Invalid function 'arrayexists'. | fails: Error during planning: Invalid function 'arrayexists'. | camelCase function names quoted. Notes: arrayExists over 2 arrays: no DataFusion equivalent |
| `sel_tuple_access` | parse fails: sql parser error: Expected: end of statement, found: .1 at Line: 1, Column: 21 | parse fails: sql parser error: Expected: end of statement, found: .1 at Line: 1, Column: 21 |  |
| `sel_order_with_fill` | fails: This feature is not implemented: ORDER BY WITH FILL is not supported: WITH FILL STEP toIntervalDay(1) | fails: This feature is not implemented: ORDER BY WITH FILL is not supported: WITH FILL STEP "toIntervalDay"(1) | camelCase function names quoted |
| `sel_order_with_fill_from_to` | fails: This feature is not implemented: ORDER BY WITH FILL is not supported: WITH FILL FROM toStartOfHour(now() - INTERVAL 24 HOUR) TO now() STEP I | fails: This feature is not implemented: ORDER BY WITH FILL is not supported: WITH FILL FROM "toStartOfHour"(now() - INTERVAL 24 HOUR) TO now() STEP | camelCase function names quoted |
| `sel_order_collate` | fails: This feature is not implemented: Unsupported ast node in sqltorel: Collate { expr: Identifier(Ident { value: "name", quote_style: None, span | fails: This feature is not implemented: Unsupported ast node in sqltorel: Collate { expr: Identifier(Ident { value: "name", quote_style: None, span |  |
| `sel_columns_matcher` | fails: Error during planning: Invalid function 'columns'. | fails: Error during planning: Invalid function 'COLUMNS'. | camelCase function names quoted |
| `sel_system_parts` | fails: analyzer: type_coercion | fails: analyzer: type_coercion |  |

Raw: 58/112 plan correctly, 16/112 plan but silently drop a construct. Proto-frontend: 90/112 plan.
