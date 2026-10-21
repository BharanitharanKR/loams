## HS1 classifier and deny corpus (adversarial)

| | statements | parse 0.62 | parse 0.63 | faithful 0.62 | faithful 0.63 |
|---|---|---|---|---|---|
| hs1:classify | 57 | 38 | 38 | 38 | 38 |
| hs1:deny | 78 | 54 | 55 | 54 | 55 |
| **all** | **135** | **92 (68.1 %)** | **93 (68.9 %)** | **92 (68.1 %)** | **93 (68.9 %)** |

### Failures and lossy parses (0.63; 0.62 where it differs)

| id | 0.63 | 0.62 |
|---|---|---|
| `classify/adversarial_describe_into_outfile#1` | fails: sql parser error: Expected: end of statement, found: INTO at Line: 1, Column: 27 | same |
| `classify/adversarial_drop_temporary_multi#1` | fails: sql parser error: Expected: COLLATION, CONNECTOR, DATABASE, EXTENSION, FUNCTION, INDEX, OPERATOR, POLICY, PROC | same |
| `classify/adversarial_explain_into_outfile#1` | fails: sql parser error: Expected: end of statement, found: '/abs/path/e.tsv' at Line: 1, Column: 31 | same |
| `classify/adversarial_from_infile_compression#1` | fails: sql parser error: Expected: end of statement, found: COMPRESSION at Line: 1, Column: 41 | same |
| `classify/adversarial_heredoc_into_outfile#1` | fails: sql parser error: Expected: end of statement, found: '/abs/path/h.tsv' at Line: 1, Column: 27 | same |
| `classify/adversarial_into_outfile#1` | fails: sql parser error: Expected: end of statement, found: '/abs/path/out.tsv' at Line: 1, Column: 23 | same |
| `classify/adversarial_parallel_with#1` | fails: sql parser error: Expected: end of statement, found: WITH at Line: 1, Column: 19 | same |
| `classify/adversarial_show_into_outfile#1` | fails: sql parser error: Expected: end of statement, found: INTO at Line: 1, Column: 13 | same |
| `classify/adversarial_unicode_space_outfile#1` | fails: sql parser error: Expected: end of statement, found: 'x' at Line: 1, Column: 23 | same |
| `classify/attach#1` | fails: sql parser error: Expected: AS, found: a at Line: 1, Column: 14 | same |
| `classify/backup#1` | fails: sql parser error: Expected: an SQL statement, found: BACKUP at Line: 1, Column: 1 | same |
| `classify/create_dictionary#1` | fails: sql parser error: Expected: an object type after CREATE, found: DICTIONARY at Line: 1, Column: 8 | same |
| `classify/create_function#1` | fails: sql parser error: Expected: an object type after CREATE, found: FUNCTION at Line: 1, Column: 19 | same |
| `classify/drop_temporary#1` | fails: sql parser error: Expected: COLLATION, CONNECTOR, DATABASE, EXTENSION, FUNCTION, INDEX, OPERATOR, POLICY, PROC | same |
| `classify/exists#1` | fails: sql parser error: Expected: an SQL statement, found: EXISTS at Line: 1, Column: 1 | same |
| `classify/kill_query#1` | fails: sql parser error: Expected: literal int, found: WHERE at Line: 1, Column: 12 | same |
| `classify/system#1` | fails: sql parser error: Expected: an SQL statement, found: SYSTEM at Line: 1, Column: 1 | same |
| `classify/undrop#1` | fails: sql parser error: Expected: an SQL statement, found: UNDROP at Line: 1, Column: 1 | same |
| `classify/with#1` | fails: sql parser error: Expected: identifier, found: 1 at Line: 1, Column: 6 | same |
| `deny/attach#1` | fails: sql parser error: Expected: AS, found: a at Line: 1, Column: 14 | same |
| `deny/backup#1` | fails: sql parser error: Expected: an SQL statement, found: BACKUP at Line: 1, Column: 1 | same |
| `deny/create_dictionary_http#1` | fails: sql parser error: Expected: an object type after CREATE, found: DICTIONARY at Line: 1, Column: 8 | same |
| `deny/create_function#1` | fails: sql parser error: Expected: an object type after CREATE, found: FUNCTION at Line: 1, Column: 8 | same |
| `deny/create_or_replace_function#1` | fails: sql parser error: Expected: an object type after CREATE, found: FUNCTION at Line: 1, Column: 19 | same |
| `deny/format_before_settings#1` | fails: sql parser error: Expected: end of statement, found: SETTINGS at Line: 1, Column: 47 | same |
| `deny/identifier_param_describe#1` | fails: sql parser error: Expected: identifier, found: { at Line: 1, Column: 16 | same |
| `deny/identifier_param_in#1` | ok | fails: sql parser error: Expected: (, found: { at Line: 1, Column: 21 |
| `deny/identifier_param_in_create_as#1` | fails: sql parser error: Expected: identifier, found: { at Line: 1, Column: 43 | same |
| `deny/identifier_param_in_insert_select#1` | fails: sql parser error: Expected: identifier, found: { at Line: 1, Column: 29 | same |
| `deny/identifier_param_show_create#1` | fails: sql parser error: Expected: identifier, found: { at Line: 1, Column: 19 | same |
| `deny/identifier_param_system_suffix#1` | fails: sql parser error: Expected: identifier, found: { at Line: 1, Column: 22 | same |
| `deny/identifier_param_system_table#1` | fails: sql parser error: Expected: identifier, found: { at Line: 1, Column: 15 | same |
| `deny/into_outfile#1` | fails: sql parser error: Expected: end of statement, found: '/tmp/loams-deny-out' at Line: 1, Column: 23 | same |
| `deny/ok_identifier_param#1` | fails: sql parser error: Expected: identifier, found: { at Line: 1, Column: 26 | same |
| `deny/ok_view_generate#1` | fails: sql parser error: Expected: ), found: generateRandom at Line: 1, Column: 40 | same |
| `deny/settings_format_schema#1` | fails: sql parser error: Expected: end of statement, found: format_schema at Line: 1, Column: 19 | same |
| `deny/settings_values_interpret#1` | fails: sql parser error: Expected: end of statement, found: input_format_values_interpret_expressions at Line: 1, Col | same |
| `deny/system_statement#1` | fails: sql parser error: Expected: an SQL statement, found: SYSTEM at Line: 1, Column: 1 | same |
| `deny/url_create_as_table_function#1` | fails: sql parser error: Expected: SELECT, VALUES, or a subquery in the query body, found: url at Line: 1, Column: 29 | same |
| `deny/url_describe#1` | fails: sql parser error: Expected: end of statement, found: ( at Line: 1, Column: 19 | same |
| `deny/url_explain_plan#1` | fails: sql parser error: Expected: end of statement, found: SELECT at Line: 1, Column: 14 | same |
| `deny/url_in_scalar_with#1` | fails: sql parser error: Expected: identifier, found: ( at Line: 1, Column: 6 | same |
| `deny/url_in_view#1` | fails: sql parser error: Expected: ), found: url at Line: 1, Column: 34 | same |
