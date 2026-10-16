//! `loams.house.v1` (HS1 Task 8, §49 §18.2) on real workers: every call here goes
//! over a socket, through the generated Connect client, to the House front's HTTP
//! listener, and every statement runs in a real `loams-house-worker` process. The
//! tests live in this package for the reason `http.rs` does (R3.2): only its
//! integration tests have the worker binary.

mod common;

use std::collections::HashMap;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, AsArray};
use arrow::datatypes::UInt64Type;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use buffa::Message as _;
use common::http::{request, target};
use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::{ConnectError, ErrorCode};
use loams_house::api::FixtureCatalog;
use loams_house::config::{HouseConfig, UserMap};
use loams_house::http::{HouseHandle, serve_with_catalog};
use loams_house::{ExitReason, ProcessLauncher, WorkerPool};
use loams_house_proto::loams::errors::v1::ErrorInfo;
use loams_house_proto::loams::house::v1 as pb;
use loams_house_proto::loams::house::v1::__buffa::oneof::{execute_query_response, result_batch};
use loams_house_proto::loams::house::v1::HouseServiceClient;

fn users() -> Vec<UserMap> {
    vec![
        UserMap::dev("alice", "secret", 2, false),
        UserMap::dev("bob", "builder", 2, false),
        UserMap::dev("reader", "books", 2, true),
    ]
}

async fn house(test: &str, catalog: FixtureCatalog) -> (HouseHandle, WorkerPool) {
    let launcher = ProcessLauncher::new(common::WORKER, common::tmp_root(test));
    let pool = WorkerPool::start(common::small(3), Arc::new(launcher))
        .await
        .expect("pool");
    let config = HouseConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        users: users(),
        tmp_dir: common::tmp_root(&format!("{test}-spool")),
        ..HouseConfig::default()
    };
    let house = serve_with_catalog(config, pool.clone(), Arc::new(catalog))
        .await
        .expect("serves");
    (house, pool)
}

type Client = HouseServiceClient<HttpClient>;

fn client(addr: SocketAddr, user: &str, password: &str) -> Client {
    let basic = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    let uri: http::Uri = format!("http://{addr}").parse().expect("uri");
    HouseServiceClient::new(
        HttpClient::plaintext(),
        ClientConfig::new(uri).with_default_header("authorization", format!("Basic {basic}")),
    )
}

/// A whole result, frame by frame.
#[derive(Debug, Default)]
struct Result_ {
    order: Vec<&'static str>,
    header: pb::ResultHeader,
    arrow: Vec<u8>,
    json: Vec<u8>,
    batch_rows: u64,
    summary: pb::QuerySummary,
}

async fn execute(
    client: &Client,
    request: pb::ExecuteQueryRequest,
) -> Result<Result_, ConnectError> {
    let mut stream = client.execute_query(request).await?;
    let mut out = Result_::default();
    while let Some(message) = stream.message().await? {
        match message.to_owned_message().frame {
            Some(execute_query_response::Frame::Header(header)) => {
                out.order.push("header");
                out.header = *header;
            }
            Some(execute_query_response::Frame::Batch(batch)) => {
                out.order.push("batch");
                out.batch_rows += batch.rows;
                match batch.data {
                    Some(result_batch::Data::ArrowIpc(bytes)) => {
                        out.arrow.extend_from_slice(&bytes)
                    }
                    Some(result_batch::Data::RowsJson(bytes)) => out.json.extend_from_slice(&bytes),
                    None => panic!("a batch without data"),
                }
            }
            Some(execute_query_response::Frame::Progress(_)) => out.order.push("progress"),
            Some(execute_query_response::Frame::Summary(summary)) => {
                out.order.push("summary");
                out.summary = *summary;
            }
            None => panic!("a frame without a frame"),
        }
    }
    Ok(out)
}

fn query(sql: &str, format: pb::ResultFormat) -> pb::ExecuteQueryRequest {
    pb::ExecuteQueryRequest {
        sql: sql.to_string(),
        format: format.into(),
        ..Default::default()
    }
}

const ARROW: pb::ResultFormat = pb::ResultFormat::RESULT_FORMAT_ARROW_IPC;
const JSON: pb::ResultFormat = pb::ResultFormat::RESULT_FORMAT_JSON_ROWS;

fn arrow_batches(bytes: &[u8]) -> Vec<RecordBatch> {
    StreamReader::try_new(Cursor::new(bytes.to_vec()), None)
        .expect("an Arrow IPC stream")
        .map(|batch| batch.expect("a record batch"))
        .collect()
}

fn json_rows(bytes: &[u8]) -> Vec<Vec<serde_json::Value>> {
    bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("a JSON row"))
        .collect()
}

/// The error's `ErrorInfo`.
fn info(err: &ConnectError) -> ErrorInfo {
    let detail = err
        .details
        .iter()
        .find(|d| d.type_url.ends_with("loams.errors.v1.ErrorInfo"))
        .unwrap_or_else(|| panic!("no ErrorInfo in {err:?}"));
    let value = detail.value.as_deref().expect("a detail value");
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(value.trim_end_matches('='))
        .expect("base64");
    ErrorInfo::decode_from_slice(&bytes).expect("an ErrorInfo")
}

fn assert_refused(
    err: &ConnectError,
    code: ErrorCode,
    reason: &str,
    clickhouse: Option<(i32, &str)>,
) {
    assert_eq!(err.code, code, "{err:?}");
    let info = info(err);
    assert_eq!(info.reason, reason, "{err:?}");
    if let Some((number, name)) = clickhouse {
        assert_eq!(
            info.metadata.get("clickhouse_code").map(String::as_str),
            Some(number.to_string().as_str()),
            "{err:?}"
        );
        assert_eq!(
            info.metadata.get("clickhouse_name").map(String::as_str),
            Some(name),
            "{err:?}"
        );
    }
}

/// A value as text, whichever way JSON carried it (64-bit integers are strings).
fn text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_query_arrow_and_json_agree() {
    let (house, _pool) = house("api-agree", FixtureCatalog::empty()).await;
    let alice = client(house.local_addr(), "alice", "secret");
    let sql = "SELECT number AS n, toString(number * 7) AS s, toDateTime(number, 'UTC') AS t, \
               number % 3 = 0 AS b FROM numbers(2500)";
    let mut settings = HashMap::new();
    // Several engine blocks, so several chunks and batches.
    settings.insert("max_block_size".to_string(), "700".to_string());
    let request = |format| pb::ExecuteQueryRequest {
        settings: settings.clone().into_iter().collect(),
        max_rows: 10_000,
        ..query(sql, format)
    };
    let arrow = execute(&alice, request(ARROW)).await.expect("arrow");
    let json = execute(&alice, request(JSON)).await.expect("json");

    for result in [&arrow, &json] {
        assert_eq!(result.order.first(), Some(&"header"), "{:?}", result.order);
        assert_eq!(result.order.last(), Some(&"summary"), "{:?}", result.order);
        assert_eq!(result.summary.result_rows, 2500);
        assert_eq!(result.batch_rows, 2500);
        assert!(!result.summary.truncated);
        assert_eq!(result.summary.query_id, result.header.query_id);
        assert!(!result.summary.query_id.is_empty());
    }
    assert!(arrow.order.iter().filter(|f| **f == "batch").count() >= 1);
    // Both headers are ClickHouse's own names and types.
    let columns = |r: &Result_| {
        r.header
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.clickhouse_type.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(columns(&arrow), columns(&json));
    assert_eq!(
        columns(&json),
        vec![
            ("n".to_string(), "UInt64".to_string()),
            ("s".to_string(), "String".to_string()),
            ("t".to_string(), "DateTime('UTC')".to_string()),
            ("b".to_string(), "UInt8".to_string()),
        ]
    );
    assert_eq!(
        arrow.header.format,
        pb::ResultFormat::RESULT_FORMAT_ARROW_IPC
    );
    assert_eq!(
        json.header.format,
        pb::ResultFormat::RESULT_FORMAT_JSON_ROWS
    );

    // The same rows, value for value.
    let batches = arrow_batches(&arrow.arrow);
    let rows = json_rows(&json.json);
    assert_eq!(rows.len(), 2500);
    let mut at = 0;
    for batch in &batches {
        assert_eq!(batch.schema().field(0).name(), "n");
        let n = batch.column(0).as_primitive::<UInt64Type>();
        let s = batch.column(1);
        for i in 0..batch.num_rows() {
            assert_eq!(text(&rows[at][0]), n.value(i).to_string(), "row {at}");
            let s_value = match s.data_type() {
                arrow::datatypes::DataType::Binary => {
                    String::from_utf8(s.as_binary::<i32>().value(i).to_vec()).expect("utf8")
                }
                _ => s.as_string::<i32>().value(i).to_string(),
            };
            assert_eq!(text(&rows[at][1]), s_value, "row {at}");
            assert!(!s.is_null(i));
            at += 1;
        }
    }
    assert_eq!(at, 2500);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_rows_truncates_and_says_so() {
    let (house, pool) = house("api-truncate", FixtureCatalog::empty()).await;
    let alice = client(house.local_addr(), "alice", "secret");

    for format in [ARROW, JSON] {
        let result = execute(
            &alice,
            pb::ExecuteQueryRequest {
                max_rows: 1500,
                ..query("SELECT number FROM numbers(1000000)", format)
            },
        )
        .await
        .expect("runs");
        assert!(result.summary.truncated, "{format:?}");
        assert_eq!(result.summary.result_rows, 1500, "{format:?}");
        assert_eq!(result.batch_rows, 1500, "{format:?}");
        let values: Vec<u64> = if format == ARROW {
            arrow_batches(&result.arrow)
                .iter()
                .flat_map(|b| b.column(0).as_primitive::<UInt64Type>().values().to_vec())
                .collect()
        } else {
            json_rows(&result.json)
                .iter()
                .map(|r| text(&r[0]).parse().expect("a number"))
                .collect()
        };
        assert_eq!(values, (0..1500).collect::<Vec<u64>>(), "{format:?}");
    }

    // The default is 1 000 rows.
    let default = execute(&alice, query("SELECT number FROM numbers(5000)", JSON))
        .await
        .expect("runs");
    assert_eq!(default.summary.result_rows, 1000);
    assert!(default.summary.truncated);

    // A result that fits is not truncated, and the cap cuts only the outermost
    // result: a subquery larger than max_rows is counted whole.
    let whole = execute(
        &alice,
        pb::ExecuteQueryRequest {
            max_rows: 10,
            ..query(
                "SELECT count() FROM (SELECT number FROM numbers(1000000) ORDER BY number DESC)",
                JSON,
            )
        },
    )
    .await
    .expect("runs");
    assert!(!whole.summary.truncated);
    assert_eq!(text(&json_rows(&whole.json)[0][0]), "1000000");

    // max_bytes cuts at a row boundary, and says so.
    let bytes = execute(
        &alice,
        pb::ExecuteQueryRequest {
            max_bytes: 100,
            ..query("SELECT number FROM numbers(1000)", JSON)
        },
    )
    .await
    .expect("runs");
    assert!(bytes.summary.truncated);
    assert!(bytes.json.len() <= 100 && bytes.json.ends_with(b"\n"));
    assert_eq!(bytes.summary.result_bytes, bytes.json.len() as u64);

    // Over the caps is refused, not clamped.
    let err = execute(
        &alice,
        pb::ExecuteQueryRequest {
            max_rows: 100_001,
            ..query("SELECT 1", JSON)
        },
    )
    .await
    .expect_err("over the cap");
    assert_refused(&err, ErrorCode::InvalidArgument, "invalid_argument", None);

    // chDB stopped at the cap's block (`result_overflow_mode = 'break'`): the
    // truncated statements were drained, not killed.
    assert_eq!(pool.stats().kills_for(ExitReason::Cancel), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_refuses_writes_with_reason() {
    let (house, _pool) = house("api-read-only", FixtureCatalog::empty()).await;
    let alice = client(house.local_addr(), "alice", "secret");
    let insert = "INSERT INTO FUNCTION null('x UInt8') VALUES (1)";

    // `read_only` absent is true: a write is refused before it runs.
    for sql in [insert, "CREATE TEMPORARY TABLE t (a UInt8) ENGINE = Memory"] {
        let err = execute(&alice, query(sql, JSON)).await.expect_err(sql);
        assert_refused(
            &err,
            ErrorCode::FailedPrecondition,
            "house_read_only",
            Some((164, "READONLY")),
        );
    }
    let explicit = execute(
        &alice,
        pb::ExecuteQueryRequest {
            read_only: Some(true),
            ..query(insert, ARROW)
        },
    )
    .await
    .expect_err("read_only = true");
    assert_refused(
        &explicit,
        ErrorCode::FailedPrecondition,
        "house_read_only",
        None,
    );

    // Reads run read-only.
    let read = execute(&alice, query("SELECT 1 AS one", JSON))
        .await
        .expect("reads");
    assert_eq!(read.json, b"[1]\n");

    // read_only = false lets the write run.
    let written = execute(
        &alice,
        pb::ExecuteQueryRequest {
            read_only: Some(false),
            ..query(insert, JSON)
        },
    )
    .await
    .expect("writes");
    assert!(written.header.columns.is_empty());
    assert_eq!(written.order, vec!["header", "summary"]);

    // A read-only user is read-only whatever the request says.
    let reader = client(house.local_addr(), "reader", "books");
    let err = execute(
        &reader,
        pb::ExecuteQueryRequest {
            read_only: Some(false),
            ..query(insert, JSON)
        },
    )
    .await
    .expect_err("a read-only user");
    assert_refused(
        &err,
        ErrorCode::PermissionDenied,
        "house_access_denied",
        Some((164, "READONLY")),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_query_kills_worker() {
    let (house, pool) = house("api-cancel", FixtureCatalog::empty()).await;
    let alice = client(house.local_addr(), "alice", "secret");
    let bob = client(house.local_addr(), "bob", "builder");

    // An unknown id.
    let err = alice
        .cancel_query(pb::CancelQueryRequest {
            query_id: "nobody".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("unknown");
    assert_refused(&err, ErrorCode::NotFound, "house_query_not_found", None);

    let running = {
        let alice = alice.clone();
        tokio::spawn(async move {
            execute(
                &alice,
                pb::ExecuteQueryRequest {
                    query_id: "cancel-me".to_string(),
                    ..query(
                        "SELECT sum(cityHash64(number)) FROM numbers(1000000000000)",
                        JSON,
                    )
                },
            )
            .await
        })
    };
    let cancel = pb::CancelQueryRequest {
        query_id: "cancel-me".to_string(),
        ..Default::default()
    };
    // Running once the front knows it.
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        match bob.cancel_query(cancel.clone()).await {
            Err(err) if err.code == ErrorCode::NotFound => {
                assert!(Instant::now() < until, "the query never started");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // Someone else's query is not bob's to cancel.
            Err(err) => {
                assert_refused(
                    &err,
                    ErrorCode::PermissionDenied,
                    "house_access_denied",
                    None,
                );
                break;
            }
            Ok(_) => panic!("bob cancelled alice's query"),
        }
    }
    // Give it time to be on its worker, then cancel it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let cancelled_at = Instant::now();
    alice.cancel_query(cancel.clone()).await.expect("cancels");
    let err = tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the stream ends")
        .expect("the task")
        .expect_err("cancelled");
    assert!(
        cancelled_at.elapsed() < Duration::from_secs(2),
        "{:?}",
        cancelled_at.elapsed()
    );
    assert_refused(
        &err,
        ErrorCode::Canceled,
        "house_cancelled",
        Some((394, "QUERY_WAS_CANCELLED")),
    );
    assert!(
        common::eventually(Duration::from_secs(5), || pool
            .stats()
            .kills_for(ExitReason::Cancel)
            >= 1)
        .await,
        "the worker was killed as `cancel`"
    );
    // The id is free again.
    let err = alice.cancel_query(cancel).await.expect_err("gone");
    assert_refused(&err, ErrorCode::NotFound, "house_query_not_found", None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn errors_carry_clickhouse_code_metadata() {
    let (house, _pool) = house("api-errors", FixtureCatalog::empty()).await;
    let alice = client(house.local_addr(), "alice", "secret");

    let cases: Vec<(pb::ExecuteQueryRequest, ErrorCode, &str, i32, &str)> = vec![
        (
            query("SELECT * FROM nope", JSON),
            ErrorCode::NotFound,
            "house_unknown_table",
            60,
            "UNKNOWN_TABLE",
        ),
        (
            query("SELEC 1", ARROW),
            ErrorCode::InvalidArgument,
            "house_syntax_error",
            62,
            "SYNTAX_ERROR",
        ),
        (
            query("SELECT 1; SELECT 2", JSON),
            ErrorCode::InvalidArgument,
            "house_syntax_error",
            62,
            "SYNTAX_ERROR",
        ),
        (
            pb::ExecuteQueryRequest {
                database: "other".to_string(),
                ..query("SELECT 1", JSON)
            },
            ErrorCode::NotFound,
            "house_unknown_database",
            81,
            "UNKNOWN_DATABASE",
        ),
        (
            pb::ExecuteQueryRequest {
                settings: [("no_such_setting_at_all".to_string(), "1".to_string())]
                    .into_iter()
                    .collect(),
                ..query("SELECT 1", JSON)
            },
            ErrorCode::InvalidArgument,
            "invalid_argument",
            115,
            "UNKNOWN_SETTING",
        ),
        (
            query("SELECT throwIf(number = 3, 'boom') FROM numbers(10)", JSON),
            ErrorCode::Unknown,
            "house_query_failed",
            395,
            "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
        ),
    ];
    for (request, code, reason, number, name) in cases {
        let sql = request.sql.clone();
        let err = execute(&alice, request).await.expect_err(&sql);
        assert_refused(&err, code, reason, Some((number, name)));
        assert!(
            err.message.as_deref().is_some_and(|m| !m.is_empty()),
            "{sql}"
        );
    }

    // A wrong password.
    let stranger = client(house.local_addr(), "alice", "wrong");
    let err = execute(&stranger, query("SELECT 1", JSON))
        .await
        .expect_err("unauthenticated");
    assert_refused(
        &err,
        ErrorCode::Unauthenticated,
        "unauthenticated",
        Some((516, "AUTHENTICATION_FAILED")),
    );

    // ExplainQuery runs the same checks, and explains.
    let explained = alice
        .explain_query(pb::ExplainQueryRequest {
            sql: "SELECT number FROM numbers(10) WHERE number > 3".to_string(),
            kind: pb::ExplainKind::EXPLAIN_KIND_SYNTAX.into(),
            ..Default::default()
        })
        .await
        .expect("explains")
        .into_owned();
    assert!(explained.text.contains("numbers(10)"), "{}", explained.text);
    let err = alice
        .explain_query(pb::ExplainQueryRequest {
            sql: "SELECT * FROM file('/etc/passwd')".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("denied");
    assert_refused(
        &err,
        ErrorCode::PermissionDenied,
        "house_disabled_function",
        Some((344, "SUPPORT_IS_DISABLED")),
    );

    // A preview of a table that does not exist.
    let err = alice
        .preview_table(pb::PreviewTableRequest {
            table: "nope".to_string(),
            ..Default::default()
        })
        .await;
    let err = match err {
        Ok(mut stream) => loop {
            match stream.message().await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("a preview of no table succeeded"),
                Err(err) => break err,
            }
        },
        Err(err) => err,
    };
    assert_refused(
        &err,
        ErrorCode::NotFound,
        "house_unknown_table",
        Some((60, "UNKNOWN_TABLE")),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_and_http_share_the_deny_list() {
    let (house, _pool) = house("api-deny", FixtureCatalog::empty()).await;
    let addr = house.local_addr();
    let alice = client(addr, "alice", "secret");
    let denied = [
        "SELECT * FROM file('/etc/passwd', LineAsString)",
        "SELECT * FROM url('http://127.0.0.1:1/x', CSV, 'a String')",
        "SELECT file('/etc/hostname')",
        "SELECT * FROM system.disks",
        "SELECT getMacro('x')",
        "SELECT 1 INTO OUTFILE '/tmp/loams-api-x'",
        "SELECT 1 SETTINGS format_schema = 'x'",
        "SELECT * FROM numbers(1) WHERE 1 IN (SELECT * FROM s3('http://127.0.0.1:1/b/k'))",
    ];
    for sql in denied {
        let http = tokio::task::spawn_blocking({
            let sql = sql.to_string();
            move || {
                request(
                    addr,
                    "POST",
                    &target(&[("user", "alice"), ("password", "secret")]),
                    &[],
                    sql.as_bytes(),
                )
            }
        })
        .await
        .expect("http");
        let http_code: i32 = http
            .header("X-ClickHouse-Exception-Code")
            .unwrap_or_else(|| panic!("{sql}: HTTP answered {}", http.text()))
            .parse()
            .expect("a code");
        assert!(http_code == 344 || http_code == 164, "{sql}: {http_code}");
        for format in [ARROW, JSON] {
            let err = execute(&alice, query(sql, format)).await.expect_err(sql);
            let info = info(&err);
            assert_eq!(
                info.metadata.get("clickhouse_code").map(String::as_str),
                Some(http_code.to_string().as_str()),
                "{sql}: the API and HTTP agree"
            );
            let expected = if http_code == 344 {
                "house_disabled_function"
            } else {
                "invalid_argument"
            };
            assert_eq!(info.reason, expected, "{sql}");
        }
    }

    // A denied setting in the request's settings map is the URL parameter's 164.
    let err = execute(
        &alice,
        pb::ExecuteQueryRequest {
            settings: [("user_files_path".to_string(), "/".to_string())]
                .into_iter()
                .collect(),
            ..query("SELECT 1", JSON)
        },
    )
    .await
    .expect_err("a denied setting");
    assert_refused(
        &err,
        ErrorCode::InvalidArgument,
        "invalid_argument",
        Some((164, "READONLY")),
    );

    // The host functions answer the House's values on both surfaces.
    let host = execute(&alice, query("SELECT hostName(), currentUser()", JSON))
        .await
        .expect("host functions");
    assert_eq!(
        json_rows(&host.json),
        vec![vec![
            serde_json::json!("loams-house"),
            serde_json::json!("alice")
        ]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browsing_answers_from_the_catalog_and_names_later_tasks() {
    let table = |name: &str| pb::DescribeTableResponse {
        table: buffa::MessageField::some(pb::Table {
            database: "default".to_string(),
            name: name.to_string(),
            table_class: pb::TableClass::TABLE_CLASS_LAKE.into(),
            engine: "MergeTree".to_string(),
            total_rows: 3,
            ..Default::default()
        }),
        columns: vec![pb::ColumnDescription {
            name: "id".to_string(),
            clickhouse_type: "UInt64".to_string(),
            ..Default::default()
        }],
        create_table_query: format!("CREATE TABLE default.{name} (id UInt64) ENGINE = MergeTree"),
        ..Default::default()
    };
    let mut catalog = FixtureCatalog::empty().with_database(2, "sales", "Sales data");
    for name in ["events", "orders", "order_items", "users"] {
        catalog = catalog.with_table(2, table(name));
    }
    let (house, _pool) = house("api-browse", catalog).await;
    let alice = client(house.local_addr(), "alice", "secret");

    let databases = alice
        .list_databases(pb::ListDatabasesRequest::default())
        .await
        .expect("databases")
        .into_owned();
    let names: Vec<_> = databases
        .databases
        .iter()
        .map(|d| d.name.as_str())
        .collect();
    assert_eq!(names, vec!["default", "sales"]);
    assert_eq!(databases.databases[0].table_count, 4);

    // Filtered and paged, by name.
    let first = alice
        .list_tables(pb::ListTablesRequest {
            filter: "ORDER".to_string(),
            page_size: 1,
            ..Default::default()
        })
        .await
        .expect("tables")
        .into_owned();
    assert_eq!(first.tables.len(), 1);
    assert_eq!(first.tables[0].name, "order_items");
    assert!(!first.next_page_token.is_empty());
    let second = alice
        .list_tables(pb::ListTablesRequest {
            filter: "ORDER".to_string(),
            page_size: 1,
            page_token: first.next_page_token.clone(),
            ..Default::default()
        })
        .await
        .expect("tables")
        .into_owned();
    assert_eq!(second.tables[0].name, "orders");
    assert!(second.next_page_token.is_empty());

    let described = alice
        .describe_table(pb::DescribeTableRequest {
            table: "events".to_string(),
            ..Default::default()
        })
        .await
        .expect("described")
        .into_owned();
    assert_eq!(described.columns[0].clickhouse_type, "UInt64");
    let err = alice
        .describe_table(pb::DescribeTableRequest {
            table: "missing".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("missing");
    assert_refused(
        &err,
        ErrorCode::NotFound,
        "house_unknown_table",
        Some((60, "UNKNOWN_TABLE")),
    );
    let err = alice
        .list_tables(pb::ListTablesRequest {
            database: "nope".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("no database");
    assert_refused(&err, ErrorCode::NotFound, "house_unknown_database", None);

    // What later tasks serve answers `unimplemented` and names the task.
    let err = alice
        .list_query_history(pb::ListQueryHistoryRequest::default())
        .await
        .expect_err("no query log yet");
    assert_refused(&err, ErrorCode::Unimplemented, "not_implemented", None);
    assert_eq!(
        info(&err).metadata.get("task").map(String::as_str),
        Some("HS1 Task 23")
    );
    // A House without a catalog has no pipes to list.
    let err = alice
        .list_pipes(pb::ListPipesRequest::default())
        .await
        .expect_err("no pipes without a catalog");
    assert_refused(&err, ErrorCode::Unimplemented, "not_implemented", None);
    let err = alice
        .restore_table(pb::RestoreTableRequest {
            table: "events".to_string(),
            idempotency_key: "01J".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("no restore yet");
    assert_refused(&err, ErrorCode::Unimplemented, "not_implemented", None);
}
