//! ClickHouse's stateless tests filtered by the mined profile, and a runner
//! for the kept ones (plan HE1 Task 7, design §75 §15, D349).
//!
//! **Filter.** A `0_stateless` test is kept when every function, combinator,
//! table function, engine, type, format and setting it uses is in
//! `profile.toml` (Task 5), read by the same extractor as the mining (Task 2).
//! Tests that are not plain `.sql` (`.sh`, `.py`, `.j2`, `.expect`) are not
//! read. Tests tagged for a cluster, ZooKeeper, the stateful datasets or long
//! runs are left out, as are tests in `echo` mode (the client prints each
//! query before its result, which the runner does not emulate) and tests with
//! a statement neither parser reads. The manifest records every count, and
//! the features that kept the most tests out: the first HE1 work list.
//!
//! **Runner.** Each kept test runs on a target over HTTP the way
//! `clickhouse-test` runs it through `clickhouse-client`: in a fresh database,
//! one session, statement by statement, results in `TabSeparated`, compared
//! with the `.reference` file byte for byte. A statement marked
//! `-- { serverError X }` must fail with that code; `clientError` and `error`
//! accept any failure. A test passes on the House only when it also passes on
//! the baseline server, so the pass rate is the House's against what the
//! runner can run at all.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, bail};
use serde::{Deserialize, Serialize};
use sqlparser::tokenizer::{Token, Whitespace};

use crate::explain::Explainer;
use crate::features::Features;
use crate::inventory::{Path as ReadPath, read_statement};
use crate::tokens;

/// The manifest's schema id.
pub const SCHEMA: &str = "loams-house-mine/stateless/1";

/// Tags whose tests need what the House does not have: a cluster, ZooKeeper,
/// the stateful datasets, or long runs.
pub const EXCLUDED_TAGS: [&str; 7] = [
    "distributed",
    "shard",
    "replica",
    "zookeeper",
    "stateful",
    "long",
    "use-rocksdb",
];

/// The feature kinds the filter checks (the plan's list, with the two kinds
/// the extractor adds for functions, R2.1). Syntax is not checked.
pub const CHECKED_KINDS: [&str; 7] = [
    "function",
    "combinator",
    "table_function",
    "engine",
    "type",
    "format",
    "setting",
];

/// What a statement is expected to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expect {
    /// Succeed.
    Ok,
    /// Fail with one of these codes or names (`serverError`).
    ServerError(Vec<String>),
    /// Fail somehow (`clientError`, `error`): the HTTP interface cannot tell
    /// a client-side error apart.
    AnyError,
}

/// One statement of a test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    /// Its text, without the `;`.
    pub text: String,
    /// What it must do.
    pub expect: Expect,
}

/// A parsed test file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Test {
    /// `-- Tags:` on the first lines.
    pub tags: Vec<String>,
    /// Whether it uses `-- { echo }` modes.
    pub echo: bool,
    /// Its statements.
    pub statements: Vec<Statement>,
}

/// Reads a `{ serverError A, B }` / `{ clientError X }` / `{ error }` hint.
fn hint(comment: &str) -> Option<Expect> {
    let inner = comment.trim().strip_prefix('{')?.trim();
    let inner = inner.strip_suffix('}').unwrap_or(inner).trim();
    let (word, rest) = inner.split_once(char::is_whitespace).unwrap_or((inner, ""));
    match word {
        "serverError" => Some(Expect::ServerError(
            rest.split([',', '|', ' '])
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
        )),
        "clientError" | "error" => Some(Expect::AnyError),
        _ => None,
    }
}

fn is_echo(comment: &str) -> bool {
    let c = comment.trim();
    c.starts_with('{') && c.contains("echo")
}

/// Parses a test file. A hint comment applies to the statement it is inside
/// or to the statement that ends on its line, as `clickhouse-client` reads it.
///
/// # Errors
///
/// The tokenizer refuses the file.
pub fn parse_test(text: &str) -> anyhow::Result<Test> {
    let tags = text
        .lines()
        .take(5)
        .filter_map(|l| l.trim().strip_prefix("-- Tags:"))
        .flat_map(|t| t.split(','))
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty())
        .collect();
    let toks = tokens::tokenize(text).map_err(|e| anyhow::anyhow!("tokenizing: {e}"))?;
    let mut echo = false;
    let mut statements: Vec<Statement> = Vec::new();
    let mut start: Option<usize> = None;
    let mut end = 0;
    let mut expect = Expect::Ok;
    // The line the last `;` was on, so a comment on it applies to that
    // statement.
    let mut last_semicolon_line: Option<u64> = None;
    for t in &toks {
        match &t.token {
            Token::Whitespace(Whitespace::SingleLineComment { comment, .. })
            | Token::Whitespace(Whitespace::MultiLineComment(comment)) => {
                if is_echo(comment) {
                    echo = true;
                }
                if let Some(h) = hint(comment) {
                    if start.is_none() && last_semicolon_line == Some(t.line) {
                        if let Some(prev) = statements.last_mut() {
                            prev.expect = h;
                        }
                    } else {
                        expect = h;
                    }
                }
            }
            Token::Whitespace(_) => {}
            Token::SemiColon => {
                if let Some(s) = start.take() {
                    statements.push(Statement {
                        text: text[s..end].trim().to_owned(),
                        expect: std::mem::replace(&mut expect, Expect::Ok),
                    });
                }
                last_semicolon_line = Some(t.line);
            }
            _ => {
                if start.is_none() {
                    start = Some(t.range.start);
                }
                end = t.range.end;
            }
        }
    }
    if let Some(s) = start {
        statements.push(Statement {
            text: text[s..end].trim().to_owned(),
            expect,
        });
    }
    Ok(Test {
        tags,
        echo,
        statements,
    })
}

/// The profile's features, by `kind:name`, with their tier.
#[derive(Debug, Default)]
pub struct ProfileSet {
    features: BTreeMap<String, u8>,
    lower: BTreeMap<String, u8>,
}

impl ProfileSet {
    /// Reads `profile.toml`.
    ///
    /// # Errors
    ///
    /// An unreadable or malformed profile.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        #[derive(Deserialize)]
        struct File {
            #[serde(default)]
            feature: Vec<Feature>,
        }
        #[derive(Deserialize)]
        struct Feature {
            name: String,
            #[serde(default)]
            tier: u8,
        }
        let file: File =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        Ok(Self::from_features(
            file.feature.into_iter().map(|f| (f.name, f.tier)),
        ))
    }

    /// A set of `(kind:name, tier)`.
    pub fn from_features(features: impl IntoIterator<Item = (String, u8)>) -> Self {
        let mut set = Self::default();
        for (name, tier) in features {
            set.lower.insert(name.to_ascii_lowercase(), tier);
            set.features.insert(name, tier);
        }
        set
    }

    /// The tier of `feature`; functions also match without case, since
    /// tests spell `COUNT` and `count` alike.
    pub fn tier(&self, feature: &str) -> Option<u8> {
        self.features.get(feature).copied().or_else(|| {
            (feature.starts_with("function:") || feature.starts_with("combinator:"))
                .then(|| self.lower.get(&feature.to_ascii_lowercase()).copied())
                .flatten()
        })
    }
}

/// The verdict on one test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Kept: its category and the highest tier among its features.
    Kept { category: String, tier: u8 },
    /// Left out, with the reason.
    Dropped(Drop),
}

/// Why a test is left out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Drop {
    /// A tag in [`EXCLUDED_TAGS`].
    Tag(String),
    /// `echo` mode.
    Echo,
    /// A statement neither parser reads.
    Unparsed,
    /// Features outside the profile.
    Outside(BTreeSet<String>),
}

/// The test's category, from what it exercises (first match): `mutations`,
/// `ddl`, `formats`, `types`, else `queries`.
fn category(features: &Features) -> &'static str {
    let has = |p: &str| features.0.iter().any(|f| f.starts_with(p));
    if has("syntax:ALTER DELETE") || has("syntax:ALTER UPDATE") || has("syntax:DELETE") {
        "mutations"
    } else if has("syntax:CREATE") || has("syntax:ALTER") || has("engine:") {
        "ddl"
    } else if has("format:") {
        "formats"
    } else if has("type:") {
        "types"
    } else {
        "queries"
    }
}

/// Judges one parsed test against the profile.
pub fn judge(
    test: &Test,
    profile: &ProfileSet,
    mut explainer: Option<&mut dyn Explainer>,
) -> Verdict {
    if let Some(tag) = test
        .tags
        .iter()
        .find(|t| EXCLUDED_TAGS.contains(&t.as_str()))
    {
        return Verdict::Dropped(Drop::Tag(tag.clone()));
    }
    if test.echo {
        return Verdict::Dropped(Drop::Echo);
    }
    let mut all = Features::default();
    for s in &test.statements {
        let e: Option<&mut dyn Explainer> = match explainer.as_mut() {
            Some(e) => Some(&mut **e),
            None => None,
        };
        let (f, path) = read_statement(&s.text, e);
        if path == ReadPath::Unparsed {
            // A statement expected to fail may be deliberately malformed.
            if s.expect == Expect::Ok {
                return Verdict::Dropped(Drop::Unparsed);
            }
            continue;
        }
        all.extend(f);
    }
    let mut outside = BTreeSet::new();
    let mut tier = 0;
    for f in &all.0 {
        let kind = f.split(':').next().unwrap_or("");
        if !CHECKED_KINDS.contains(&kind) {
            continue;
        }
        match profile.tier(f) {
            Some(t) => tier = tier.max(t),
            None => {
                outside.insert(f.clone());
            }
        }
    }
    if outside.is_empty() {
        Verdict::Kept {
            category: category(&all).to_owned(),
            tier,
        }
    } else {
        Verdict::Dropped(Drop::Outside(outside))
    }
}

/// `conformance/clickhouse/stateless/manifest.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// [`SCHEMA`].
    pub schema: String,
    /// The ClickHouse tag the tests come from.
    pub clickhouse_tag: String,
    /// Its commit.
    pub clickhouse_commit: String,
    /// Counts by outcome.
    pub counts: Counts,
    /// The features that kept the most tests out, most first.
    #[serde(default)]
    pub missing: Vec<Missing>,
    /// The kept tests.
    #[serde(default)]
    pub test: Vec<Kept>,
}

/// Counts of the filter.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    /// Every file in the directory that is a test (`.sql`, `.sh`, ...).
    pub tests: u64,
    /// Not `.sql`.
    pub not_sql: u64,
    /// Excluded by tag.
    pub tagged: u64,
    /// In echo mode.
    pub echo: u64,
    /// With a statement neither parser reads.
    pub unparsed: u64,
    /// Using features outside the profile.
    pub outside_profile: u64,
    /// Kept.
    pub kept: u64,
}

/// A feature outside the profile and how many tests it alone or with others
/// kept out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Missing {
    /// `kind:name`.
    pub feature: String,
    /// Tests that use it.
    pub tests: u64,
    /// Tests it is the only missing feature of.
    pub only: u64,
}

/// A kept test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Kept {
    /// The file stem (`00001_select_1`).
    pub name: String,
    /// Its category.
    pub category: String,
    /// The highest tier of its features.
    pub tier: u8,
}

/// Extensions of test files `clickhouse-test` runs.
const TEST_EXTENSIONS: [&str; 6] = ["sql", "sh", "j2", "expect", "python", "py"];

/// Filters the tests in `dir` by `profile`.
///
/// # Errors
///
/// An unreadable directory.
pub fn filter(
    dir: &Path,
    tag: &str,
    commit: &str,
    profile: &ProfileSet,
    mut explainer: Option<&mut dyn Explainer>,
) -> anyhow::Result<Manifest> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .filter(|p| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            // `x.sql.j2` is a template; `x.lib` and data files are not tests.
            TEST_EXTENSIONS
                .iter()
                .any(|e| name.ends_with(&format!(".{e}")))
        })
        .collect();
    files.sort();
    let mut counts = Counts::default();
    let mut kept = Vec::new();
    let mut missing: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for file in files {
        counts.tests += 1;
        let name = file
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_owned();
        let Some(stem) = name.strip_suffix(".sql") else {
            counts.not_sql += 1;
            continue;
        };
        let bytes = std::fs::read(&file).with_context(|| format!("reading {}", file.display()))?;
        let Ok(text) = String::from_utf8(bytes) else {
            counts.unparsed += 1;
            continue;
        };
        let Ok(test) = parse_test(&text) else {
            counts.unparsed += 1;
            continue;
        };
        let e: Option<&mut dyn Explainer> = match explainer.as_mut() {
            Some(e) => Some(&mut **e),
            None => None,
        };
        match judge(&test, profile, e) {
            Verdict::Kept { category, tier } => {
                counts.kept += 1;
                kept.push(Kept {
                    name: stem.to_owned(),
                    category,
                    tier,
                });
            }
            Verdict::Dropped(Drop::Tag(_)) => counts.tagged += 1,
            Verdict::Dropped(Drop::Echo) => counts.echo += 1,
            Verdict::Dropped(Drop::Unparsed) => counts.unparsed += 1,
            Verdict::Dropped(Drop::Outside(features)) => {
                counts.outside_profile += 1;
                let only = features.len() == 1;
                for f in features {
                    let m = missing.entry(f).or_default();
                    m.0 += 1;
                    if only {
                        m.1 += 1;
                    }
                }
            }
        }
    }
    let mut missing: Vec<Missing> = missing
        .into_iter()
        .map(|(feature, (tests, only))| Missing {
            feature,
            tests,
            only,
        })
        .collect();
    missing.sort_by(|a, b| {
        b.only
            .cmp(&a.only)
            .then(b.tests.cmp(&a.tests))
            .then(a.feature.cmp(&b.feature))
    });
    missing.truncate(100);
    Ok(Manifest {
        schema: SCHEMA.to_owned(),
        clickhouse_tag: tag.to_owned(),
        clickhouse_commit: commit.to_owned(),
        counts,
        missing,
        test: kept,
    })
}

/// What one statement did on a target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// Its output.
    Output(String),
    /// Its error code.
    Error(String),
}

/// A target the runner sends statements to.
pub trait Session {
    /// Runs one statement in the test's database and session.
    fn run(&mut self, statement: &str) -> anyhow::Result<Answer>;
}

/// Whether a test passed, given its statements' answers and the reference.
/// `codes` maps error names (`UNKNOWN_TABLE`) to their codes.
pub fn passes(
    test: &Test,
    session: &mut dyn Session,
    reference: &str,
    codes: &BTreeMap<String, String>,
) -> anyhow::Result<bool> {
    let mut output = String::new();
    for s in &test.statements {
        let answer = session.run(&s.text)?;
        match (&s.expect, answer) {
            (Expect::Ok, Answer::Output(o)) => output.push_str(&o),
            (Expect::Ok, Answer::Error(_)) => return Ok(false),
            (Expect::AnyError, Answer::Error(_)) => {}
            (Expect::ServerError(wanted), Answer::Error(code)) => {
                let ok = wanted
                    .iter()
                    .any(|w| w == &code || codes.get(w).is_some_and(|c| c == &code));
                if !ok {
                    return Ok(false);
                }
            }
            (_, Answer::Output(_)) => return Ok(false),
        }
    }
    Ok(output == reference)
}

/// A ClickHouse-compatible HTTP endpoint, one session per test.
#[derive(Debug)]
pub struct HttpSession {
    agent: ureq::Agent,
    url: String,
    user: Option<String>,
    key: Option<String>,
    database: String,
    session: String,
}

impl HttpSession {
    /// Opens a session on `url` and creates `database` in it.
    ///
    /// # Errors
    ///
    /// The target refuses the database.
    pub fn open(
        url: &str,
        user: Option<&str>,
        key: Option<&str>,
        database: &str,
        session: &str,
    ) -> anyhow::Result<Self> {
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(std::time::Duration::from_secs(60)))
            .build()
            .new_agent();
        let mut s = Self {
            agent,
            url: url.to_owned(),
            user: user.map(str::to_owned),
            key: key.map(str::to_owned),
            database: "default".to_owned(),
            session: session.to_owned(),
        };
        if let Answer::Error(code) = s.run(&format!("CREATE DATABASE IF NOT EXISTS {database}"))? {
            bail!("{url}: CREATE DATABASE {database}: error {code}");
        }
        s.database = database.to_owned();
        Ok(s)
    }

    /// Drops the test's database.
    pub fn close(mut self) {
        let db = std::mem::take(&mut self.database);
        self.database = "default".to_owned();
        let _ = self.run(&format!("DROP DATABASE IF EXISTS {db}"));
    }

    /// Runs a query outside a test (`system.errors`).
    ///
    /// # Errors
    ///
    /// The target is unreachable.
    pub fn query(&mut self, sql: &str) -> anyhow::Result<Answer> {
        self.run(sql)
    }
}

impl Session for HttpSession {
    fn run(&mut self, statement: &str) -> anyhow::Result<Answer> {
        let mut request = self.agent.post(&self.url).query_pairs([
            ("database", self.database.as_str()),
            ("session_id", self.session.as_str()),
            ("default_format", "TabSeparated"),
        ]);
        if let Some(u) = &self.user {
            request = request.header("X-ClickHouse-User", u);
        }
        if let Some(k) = &self.key {
            request = request.header("X-ClickHouse-Key", k);
        }
        let mut response = request
            .send(statement)
            .map_err(|e| anyhow::anyhow!("{}: {e}", self.url))?;
        let code = response
            .headers()
            .get("X-ClickHouse-Exception-Code")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let status = response.status().as_u16();
        let text = response
            .body_mut()
            .with_config()
            .limit(64 * 1024 * 1024)
            .read_to_string()
            .unwrap_or_default();
        Ok(match code {
            Some(c) => Answer::Error(c),
            None if status != 200 => Answer::Error(format!("http {status}")),
            None => Answer::Output(text),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hints_apply_to_their_statement() {
        let t = parse_test(
            "-- Tags: no-fasttest, long\nSELECT 1;\nSELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }\nSELECT x -- { clientError 62 }\n;\nSELECT 2",
        )
        .unwrap();
        assert_eq!(t.tags, ["no-fasttest", "long"]);
        let e: Vec<&Expect> = t.statements.iter().map(|s| &s.expect).collect();
        assert_eq!(
            e,
            [
                &Expect::Ok,
                &Expect::ServerError(vec!["FUNCTION_THROW_IF_VALUE_IS_NON_ZERO".into()]),
                &Expect::AnyError,
                &Expect::Ok
            ]
        );
        assert!(!t.echo);
        assert!(parse_test("-- { echoOn }\nSELECT 1").unwrap().echo);
    }

    #[test]
    fn a_test_is_kept_only_inside_the_profile() {
        let profile = ProfileSet::from_features([
            ("function:count".to_owned(), 0),
            ("function:plus".to_owned(), 1),
            ("format:JSON".to_owned(), 0),
        ]);
        let kept = parse_test("SELECT COUNT() + 1 FROM t FORMAT JSON;").unwrap();
        assert_eq!(
            judge(&kept, &profile, None),
            Verdict::Kept {
                category: "formats".into(),
                tier: 1
            }
        );
        let out = parse_test("SELECT sum(1) FROM t SETTINGS max_threads = 1").unwrap();
        let Verdict::Dropped(Drop::Outside(f)) = judge(&out, &profile, None) else {
            panic!("dropped");
        };
        assert_eq!(
            f.into_iter().collect::<Vec<_>>(),
            ["function:sum", "setting:max_threads"]
        );
        let tagged = parse_test("-- Tags: zookeeper\nSELECT 1").unwrap();
        assert_eq!(
            judge(&tagged, &profile, None),
            Verdict::Dropped(Drop::Tag("zookeeper".into()))
        );
    }

    struct Fake(Vec<Answer>);
    impl Session for Fake {
        fn run(&mut self, _: &str) -> anyhow::Result<Answer> {
            Ok(self.0.remove(0))
        }
    }

    #[test]
    fn pass_needs_the_reference_and_the_expected_errors() {
        let t = parse_test("SELECT 1; SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }\nSELECT 2").unwrap();
        let codes = BTreeMap::from([(
            "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO".to_owned(),
            "395".to_owned(),
        )]);
        let ok = || {
            Fake(vec![
                Answer::Output("1\n".into()),
                Answer::Error("395".into()),
                Answer::Output("2\n".into()),
            ])
        };
        assert!(passes(&t, &mut ok(), "1\n2\n", &codes).unwrap());
        assert!(!passes(&t, &mut ok(), "1\n3\n", &codes).unwrap());
        let wrong_code = Fake(vec![
            Answer::Output("1\n".into()),
            Answer::Error("60".into()),
            Answer::Output("2\n".into()),
        ]);
        assert!(!passes(&t, &mut { wrong_code }, "1\n2\n", &codes).unwrap());
        let no_error = Fake(vec![
            Answer::Output("1\n".into()),
            Answer::Output("0\n".into()),
            Answer::Output("2\n".into()),
        ]);
        assert!(!passes(&t, &mut { no_error }, "1\n2\n", &codes).unwrap());
    }
}
