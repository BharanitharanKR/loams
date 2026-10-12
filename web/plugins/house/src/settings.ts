// Settings a run may carry: the House's allowlist (fabric/crates/loams-house/src/
// settings.rs, HS1 Task 4; §32 §8.5) and the deny list it subtracts (deny.rs,
// DENIED_SETTINGS). The page only offers what the server accepts, and checks the caps
// the server enforces with `164 READONLY`, so a refusal is caught before the run. The
// server still decides: these lists are a mirror, kept in step by
// `the_allowlist_matches_the_house` in the tests.

export const ALLOWED_SETTINGS = [
  'max_execution_time',
  'max_result_rows',
  'max_result_bytes',
  'result_overflow_mode',
  'max_rows_to_read',
  'max_bytes_to_read',
  'max_memory_usage',
  'date_time_input_format',
  'date_time_output_format',
  'join_algorithm',
  'max_threads',
  'session_timezone',
  'insert_deduplicate',
  'async_insert',
  'wait_for_async_insert',
  'max_block_size',
  'log_comment',
  'http_headers_progress_interval_ms',
  'extremes',
] as const;

export const ALLOWED_PREFIXES = ['output_format_', 'input_format_', 'format_csv_', 'format_tsv_'];

export const ALLOWED_EXPERIMENTS = [
  'allow_experimental_analyzer',
  'allow_experimental_correlated_subqueries',
  'allow_experimental_join_right_table_sorting',
] as const;

export const LOAMS_SETTINGS = [
  'loams_table_class',
  'loams_snapshot_id',
  'loams_as_of',
  'loams_lake_commit_interval_ms',
  'loams_lake_commit_bytes',
  'loams_lake_target_file_bytes',
  'loams_dedup_window',
  'loams_pipe_max_rows',
  'loams_pipe_max_delay_ms',
  'loams_wake_timeout_ms',
  'loams_consistency_wait_ms',
  'loams_consistency_token',
  'loams_bucket_key',
  'loams_tiering_freshness',
] as const;

/** deny.rs `DENIED_SETTINGS`; a trailing `*` is a prefix. */
export const DENIED_SETTINGS = [
  'format_schema*',
  'user_files_path',
  'output_format_schema',
  'input_format_record_errors_file_path',
  'format_template_resultset',
  'format_template_row',
  'format_avro_schema_registry_url',
  'rename_files_after_processing',
  's3queue_default_zookeeper_path',
  'input_format_values_interpret_expressions',
  'input_format_values_deduce_templates_of_expressions',
  'into_outfile_create_parent_directories',
  'default_temporary_table_engine',
  'default_table_engine',
  'allow_introspection_functions',
  'allow_get_client_http_header',
  'format_display_secrets_in_show_and_select',
  'user_scripts_path',
  'url_base',
  'allow_insert_into_iceberg',
  'allow_experimental_insert_into_iceberg',
  'allow_experimental_iceberg_compaction',
  'allow_iceberg_remove_orphan_files',
  'allow_experimental_expire_snapshots',
];

/** Every name the picker offers (prefix settings are typed in). */
export const OFFERED_SETTINGS: string[] = [
  ...ALLOWED_SETTINGS,
  ...ALLOWED_EXPERIMENTS,
  ...LOAMS_SETTINGS,
];

const NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
const GIB = 1024 ** 3;

const eq = (a: string, b: string) => a.toLowerCase() === b.toLowerCase();

export function isDeniedSetting(name: string): boolean {
  return DENIED_SETTINGS.some((p) =>
    p.endsWith('*') ? name.toLowerCase().startsWith(p.slice(0, -1).toLowerCase()) : eq(p, name),
  );
}

export function isAllowedSetting(name: string): boolean {
  if (!NAME.test(name) || isDeniedSetting(name)) return false;
  if (OFFERED_SETTINGS.some((s) => eq(s, name))) return true;
  return ALLOWED_PREFIXES.some((p) => name.toLowerCase().startsWith(p));
}

/** Why the server would refuse `name = value`, or undefined when it would accept it. */
export function settingProblem(name: string, value: string): string | undefined {
  const n = name.trim();
  if (!n) return 'Name the setting.';
  if (!NAME.test(n)) return 'A setting name is letters, digits and underscores.';
  if (isDeniedSetting(n)) return `${n} cannot be changed on the House.`;
  if (!isAllowedSetting(n)) return `${n} is not on the House's list of allowed settings.`;
  const v = value.trim();
  if (!v) return 'Give a value.';
  const num = Number(v);
  switch (n.toLowerCase()) {
    case 'max_memory_usage':
      if (!Number.isFinite(num) || num <= 0 || num > 4 * GIB)
        return 'max_memory_usage is at most 4 GiB (4294967296), and 0 (unlimited) is over it.';
      break;
    case 'max_execution_time':
      if (!Number.isFinite(num) || num <= 0 || num > 300)
        return 'max_execution_time is at most 300 seconds, and 0 (unlimited) is over it.';
      break;
    case 'max_threads':
      if (!Number.isInteger(num) || num < 0) return 'max_threads is a whole number; 0 means auto.';
      break;
  }
  return undefined;
}

export interface SettingRow {
  id: number;
  name: string;
  value: string;
}

/** The rows as the request's `settings` map, or the first problem. */
export function settingsMap(
  rows: SettingRow[],
): { ok: true; settings: Record<string, string> } | { ok: false; problem: string } {
  const out: Record<string, string> = {};
  for (const r of rows) {
    if (!r.name.trim() && !r.value.trim()) continue;
    const problem = settingProblem(r.name, r.value);
    if (problem) return { ok: false, problem };
    const key = r.name.trim();
    if (Object.keys(out).some((k) => eq(k, key)))
      return { ok: false, problem: `${key} is set twice.` };
    out[key] = r.value.trim();
  }
  return { ok: true, settings: out };
}
