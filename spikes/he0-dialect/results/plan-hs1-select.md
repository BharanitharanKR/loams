| id | DataFusion 54.1 as shipped | with the proto-frontend | rewrites applied |
|---|---|---|---|
| `select/types#1` | fails: Error during planning: Invalid function 'totypename'. | fails: Error during planning: Invalid function 'toTypeName'. | camelCase function names quoted |

Raw: 17/20 plan correctly, 1/20 plan but silently drop a construct. Proto-frontend: 19/20 plan.
