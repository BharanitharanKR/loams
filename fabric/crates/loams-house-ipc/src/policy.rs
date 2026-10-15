//! Which settings a statement may change: one list for the front's check and
//! the worker's settings profile (HS1 ruling R-ACL.1).
//!
//! The front refuses a setting outside these lists before anything runs (HS1
//! Task 4, R4.3). The worker writes the same lists into its chDB settings
//! profile: every other setting the engine has is pinned with a `<readonly/>`
//! constraint, so ClickHouse itself answers `452` to a `SET`, a URL parameter or
//! a `SETTINGS` clause that names one, whatever the front saw. The two cannot
//! drift, because they read this module.

/// §32 §8.5's allowlist, plus what HTTP clients routinely send (`max_block_size`,
/// `log_comment`, `http_headers_progress_interval_ms`, `extremes`): ruling R4.3.
pub const ALLOWED_SETTINGS: &[&str] = &[
    "max_execution_time",
    "max_result_rows",
    "max_result_bytes",
    "result_overflow_mode",
    "max_rows_to_read",
    "max_bytes_to_read",
    "max_memory_usage",
    "date_time_input_format",
    "date_time_output_format",
    "join_algorithm",
    "max_threads",
    "session_timezone",
    "insert_deduplicate",
    "async_insert",
    "wait_for_async_insert",
    "max_block_size",
    "log_comment",
    "http_headers_progress_interval_ms",
    "extremes",
];

/// Prefixes allowed whole (§32 §8.5): output and input format options, and the
/// CSV and TSV options, less the deny list's settings. `format_schema*` and
/// `format_template_*` name files and are not here.
pub const ALLOWED_PREFIXES: &[&str] = &[
    "output_format_",
    "input_format_",
    "format_csv_",
    "format_tsv_",
];

/// The analyzer experiments a query may switch (HS1 Task 4 fix round 1, M3). An
/// explicit list, not the `allow_experimental_` prefix, which also opens `eval`,
/// AI and URL-wildcard functions, catalogs and writers.
pub const ALLOWED_EXPERIMENTS: &[&str] = &[
    "allow_experimental_analyzer",
    "allow_experimental_correlated_subqueries",
    "allow_experimental_join_right_table_sorting",
];

/// Settings the House's own statements set on a worker's connections, which the
/// worker's profile must therefore leave changeable (R-ACL.1). None is on the
/// front's lists, so a request still cannot set them (`164`); with the front
/// bypassed, each changes only how the namespace's own data is read or shown.
///
/// * `readonly`, `allow_ddl`: every user connection is opened with `readonly = 2`
///   and `allow_ddl = 0` (R1.9, R5.6). Both are sticky by the engine's own rules:
///   neither can be set back on that connection (measured).
/// * `flatten_nested`: the front's `DESCRIBE TABLE null(…)` of a `CREATE TABLE`'s
///   columns (R10.3).
/// * `describe_compact_output`: the API's result header (R8.5).
/// * `iceberg_snapshot_id`: a time-travel view's own `SETTINGS` (R11.1). A view's
///   settings are checked against the profile when it is created (measured).
/// * `final`: a statement's `SETTINGS final = 1`, which the front reads to pick
///   merge views (R11.1, R11.4).
/// * `database`: `USE d` sets it (chDB 26.9, measured: with it pinned `USE` is
///   `452`), and the worker `USE`s the request's database (R11.2).
pub const HOUSE_SETTINGS: &[&str] = &[
    "readonly",
    "allow_ddl",
    "database",
    "flatten_nested",
    "describe_compact_output",
    "iceberg_snapshot_id",
    "final",
];

/// Whether `name` is on the front's lists (before the deny list, which removes
/// some names these lists would allow; HS1 Task 5).
pub fn on_the_lists(name: &str) -> bool {
    ALLOWED_SETTINGS.contains(&name)
        || ALLOWED_EXPERIMENTS.contains(&name)
        || ALLOWED_PREFIXES.iter().any(|p| name.starts_with(p))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_are_distinct_and_house_settings_are_not_on_them() {
        let mut names: Vec<&str> = ALLOWED_SETTINGS
            .iter()
            .chain(ALLOWED_EXPERIMENTS)
            .chain(HOUSE_SETTINGS)
            .copied()
            .collect();
        let all = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), all);
        for name in HOUSE_SETTINGS {
            assert!(!on_the_lists(name), "{name} would be a request's to set");
        }
        assert!(on_the_lists("output_format_json_quote_64bit_integers"));
        assert!(!on_the_lists("max_bytes_before_external_sort"));
    }
}
