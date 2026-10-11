//! Results of many blocks over the ClickHouse HTTP interface are one document
//! (HS1 R8.4, R8.12).
//!
//! chDB's streaming C ABI formats every engine block as a complete output of its
//! own: a `FORMAT JSON` result of four blocks came back as four JSON documents, a
//! `Parquet` result as four files, an `ArrowStream` result as four streams, and the
//! `…WithNames` formats repeated their header lines at every block. Each test here
//! runs a statement that produces at least three blocks (`max_block_size` small,
//! or chDB's default block of 65 409 rows with 200 000 rows) and reads the body
//! with a real reader for its format, which fails on a second document.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::http::{Response, request, target};
use loams_house::config::{HouseConfig, UserMap};
use loams_house::http::{HouseHandle, serve};
use loams_house::{ProcessLauncher, WorkerPool};

/// Rows in the small-block statements, and the block size that makes them ten
/// blocks.
const ROWS: u64 = 10_000;
const BLOCK: &str = "1000";

async fn house(test: &str, adjust: impl FnOnce(&mut HouseConfig)) -> HouseHandle {
    let launcher = ProcessLauncher::new(common::WORKER, common::tmp_root(test));
    let pool = WorkerPool::start(common::small(3), Arc::new(launcher))
        .await
        .expect("pool");
    let mut config = HouseConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        users: vec![UserMap::dev("default", "", 1, false)],
        tmp_dir: common::tmp_root(&format!("{test}-spool")),
        ..HouseConfig::default()
    };
    adjust(&mut config);
    serve(config, pool).await.expect("serves")
}

/// `SELECT … FORMAT <format>` over GET, with `max_block_size` when given.
async fn select(house: &HouseHandle, sql: &str, format: &str, block: Option<&str>) -> Response {
    let addr = house.local_addr();
    let query = format!("{sql} FORMAT {format}");
    let mut params = vec![("query", query.as_str())];
    if let Some(block) = block {
        params.push(("max_block_size", block));
    }
    let target = target(&params);
    let response = tokio::task::spawn_blocking(move || request(addr, "GET", &target, &[], &[]))
        .await
        .expect("client");
    assert_eq!(
        response.status,
        200,
        "{format}: {}",
        &response.text()[..response.text().len().min(400)]
    );
    assert!(response.complete, "{format}: the body ended properly");
    response
}

/// Two columns, `n` (`UInt64`) and `s` (`String`), `rows` rows in order.
fn numbers(rows: u64) -> String {
    format!("SELECT number AS n, toString(number) AS s FROM numbers({rows})")
}

fn summary_rows(response: &Response) -> String {
    let summary: serde_json::Value = serde_json::from_str(
        response
            .header("X-ClickHouse-Summary")
            .expect("a summary header"),
    )
    .expect("the summary is JSON");
    summary["result_rows"]
        .as_str()
        .expect("result_rows")
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_family_is_one_document() {
    let house = house("doc-json", |_| {}).await;
    for format in ["JSON", "JSONStrings", "JSONCompact", "JSONCompactStrings"] {
        let response = select(&house, &numbers(ROWS), format, Some(BLOCK)).await;
        // `from_slice` refuses trailing data, so a second document fails here.
        let doc: serde_json::Value = serde_json::from_slice(&response.body)
            .unwrap_or_else(|err| panic!("{format} is one JSON document: {err}"));
        assert_eq!(doc["rows"], ROWS, "{format}");
        let data = doc["data"].as_array().expect("data");
        assert_eq!(data.len() as u64, ROWS, "{format}");
        assert_eq!(doc["meta"].as_array().expect("meta").len(), 2, "{format}");
        assert_eq!(summary_rows(&response), ROWS.to_string(), "{format}");
    }

    let columns = select(&house, &numbers(ROWS), "JSONColumns", Some(BLOCK)).await;
    let doc: serde_json::Value =
        serde_json::from_slice(&columns.body).expect("JSONColumns is one document");
    assert_eq!(doc["n"].as_array().expect("n").len() as u64, ROWS);

    let objects = select(&house, &numbers(ROWS), "JSONObjectEachRow", Some(BLOCK)).await;
    let doc: serde_json::Value =
        serde_json::from_slice(&objects.body).expect("JSONObjectEachRow is one document");
    let doc = doc.as_object().expect("an object");
    assert_eq!(doc.len() as u64, ROWS);
    assert_eq!(
        doc[&format!("row_{ROWS}")]["n"],
        ROWS - 1,
        "rows are numbered once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_of_default_blocks_is_one_document() {
    // The measured case of R8.4: 200 000 rows in chDB's default blocks.
    let house = house("doc-json-default", |_| {}).await;
    let response = select(&house, "SELECT number FROM numbers(200000)", "JSON", None).await;
    let doc: serde_json::Value = serde_json::from_slice(&response.body).expect("one JSON document");
    assert_eq!(doc["rows"], 200_000);
    assert_eq!(doc["data"].as_array().expect("data").len(), 200_000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parquet_is_one_file() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let house = house("doc-parquet", |_| {}).await;
    for (sql, rows, block) in [
        (numbers(ROWS), ROWS, Some(BLOCK)),
        (numbers(200_000), 200_000, None),
    ] {
        let response = select(&house, &sql, "Parquet", block).await;
        let bytes = Bytes::from(response.body.clone());
        // A reader finds the footer at the end: of the last file, when several
        // were concatenated, which the row count below would show.
        assert_eq!(&bytes[..4], b"PAR1");
        let reader = SerializedFileReader::new(bytes.clone()).expect("a Parquet file");
        let metadata = reader.metadata();
        assert_eq!(metadata.file_metadata().num_rows() as u64, rows);
        assert!(metadata.num_row_groups() >= 3, "one row group per block");

        let batches = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("arrow reader")
            .build()
            .expect("reader");
        let mut expected = 0u64;
        for batch in batches {
            let batch = batch.expect("a batch");
            let n = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .expect("n is UInt64");
            for value in n.values() {
                assert_eq!(*value, expected);
                expected += 1;
            }
        }
        assert_eq!(expected, rows, "every row, in order");
        if block.is_some() {
            // Small enough to be held to its end, so the summary is final.
            assert_eq!(summary_rows(&response), rows.to_string());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arrow_file_and_stream_are_one_each() {
    use arrow::ipc::reader::{FileReader, StreamReader};

    let house = house("doc-arrow", |_| {}).await;

    let file = select(&house, &numbers(ROWS), "Arrow", Some(BLOCK)).await;
    let reader =
        FileReader::try_new(std::io::Cursor::new(file.body.clone()), None).expect("an Arrow file");
    let rows: usize = reader.map(|b| b.expect("a batch").num_rows()).sum();
    assert_eq!(rows as u64, ROWS);

    let stream = select(&house, &numbers(ROWS), "ArrowStream", Some(BLOCK)).await;
    let mut cursor = std::io::Cursor::new(stream.body.clone());
    let reader = StreamReader::try_new(&mut cursor, None).expect("an Arrow stream");
    let rows: usize = reader.map(|b| b.expect("a batch").num_rows()).sum();
    assert_eq!(
        rows as u64, ROWS,
        "the stream's end-of-stream marker is its last"
    );
    assert_eq!(
        cursor.position() as usize,
        stream.body.len(),
        "nothing follows the end-of-stream marker"
    );
    assert_eq!(summary_rows(&stream), ROWS.to_string());
}

/// The number of lines equal to `line` in `text`.
fn count_lines(text: &str, line: &str) -> usize {
    text.lines().filter(|l| *l == line).count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_names_formats_have_one_header() {
    let house = house("doc-names", |_| {}).await;
    // A name that needs quoting in CSV and escaping in TSV and JSON, so the
    // header cannot be found by a naive search for the first newline.
    let sql = format!(
        "SELECT number AS n, toString(number) AS `tricky \"name\",\nwith\ttab` FROM numbers({ROWS})"
    );
    let cases: [(&str, &[&str]); 7] = [
        ("TSVWithNames", &["n\ttricky \"name\",\\nwith\\ttab"]),
        (
            "TabSeparatedWithNamesAndTypes",
            &["n\ttricky \"name\",\\nwith\\ttab", "UInt64\tString"],
        ),
        ("CSVWithNames", &[]),
        ("CSVWithNamesAndTypes", &["\"UInt64\",\"String\""]),
        (
            "JSONCompactEachRowWithNames",
            &["[\"n\", \"tricky \\\"name\\\",\\nwith\\ttab\"]"],
        ),
        (
            "JSONCompactEachRowWithNamesAndTypes",
            &[
                "[\"n\", \"tricky \\\"name\\\",\\nwith\\ttab\"]",
                "[\"UInt64\", \"String\"]",
            ],
        ),
        (
            "JSONCompactStringsEachRowWithNames",
            &["[\"n\", \"tricky \\\"name\\\",\\nwith\\ttab\"]"],
        ),
    ];
    for (format, header) in cases {
        let response = select(&house, &sql, format, Some(BLOCK)).await;
        let text = response.text();
        for line in header {
            assert_eq!(count_lines(&text, line), 1, "{format}: {line:?} once");
        }
        if format.starts_with("CSV") {
            // The CSV name is quoted and holds a raw newline, so the header is
            // two physical lines; the data rows are the rest.
            assert_eq!(
                text.matches("\"tricky \"\"name\"\",\nwith\ttab\"").count(),
                1,
                "{format}: the quoted name once"
            );
            let header_lines = 2 + header.len();
            assert_eq!(
                text.lines().count() as u64,
                ROWS + header_lines as u64,
                "{format}"
            );
        } else {
            assert_eq!(
                text.lines().count() as u64,
                ROWS + header.len() as u64,
                "{format}"
            );
        }
        let last = text.lines().last().expect("a last line");
        assert!(last.contains(&(ROWS - 1).to_string()), "{format}: {last}");
    }

    // RowBinaryWithNamesAndTypes: the header, then the rows, read field by field.
    let response = select(
        &house,
        &numbers(ROWS),
        "RowBinaryWithNamesAndTypes",
        Some(BLOCK),
    )
    .await;
    let body = &response.body;
    let mut at = 0usize;
    let string = |at: &mut usize| -> Vec<u8> {
        let len = body[*at] as usize; // every string here is shorter than 128 bytes
        *at += 1;
        let out = body[*at..*at + len].to_vec();
        *at += len;
        out
    };
    assert_eq!(body[at], 2, "two columns");
    at += 1;
    let header: Vec<Vec<u8>> = (0..4).map(|_| string(&mut at)).collect();
    assert_eq!(header, [&b"n"[..], b"s", b"UInt64", b"String"]);
    for expected in 0..ROWS {
        let n = u64::from_le_bytes(body[at..at + 8].try_into().expect("8 bytes"));
        at += 8;
        assert_eq!(n, expected, "row {expected} follows without a header");
        assert_eq!(string(&mut at), expected.to_string().into_bytes());
    }
    assert_eq!(at, body.len(), "nothing after the last row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn other_envelopes_are_one_document() {
    let house = house("doc-other", |_| {}).await;
    let sql = numbers(ROWS);

    let values = select(&house, &sql, "Values", Some(BLOCK)).await;
    let expected: Vec<String> = (0..ROWS).map(|n| format!("({n},'{n}')")).collect();
    assert_eq!(
        values.text(),
        expected.join(","),
        "rows separated by commas"
    );

    let hash = select(&house, &sql, "Hash", Some(BLOCK)).await;
    assert_eq!(
        hash.text().lines().count(),
        1,
        "one hash of the whole result"
    );

    let xml = select(&house, &sql, "XML", Some(BLOCK)).await;
    let text = xml.text();
    assert_eq!(text.matches("<?xml").count(), 1);
    assert_eq!(text.matches(&format!("<rows>{ROWS}</rows>")).count(), 1);

    let markdown = select(&house, &sql, "Markdown", Some(BLOCK)).await;
    assert_eq!(count_lines(&markdown.text(), "| n | s |"), 1);

    // `Vertical` shows the first `output_format_pretty_max_rows` (1 000) rows,
    // numbered once across blocks of 100.
    let vertical = select(&house, &sql, "Vertical", Some("100")).await;
    assert_eq!(count_lines(&vertical.text(), "Row 1:"), 1);
    assert_eq!(count_lines(&vertical.text(), "Row 1000:"), 1);
    assert_eq!(count_lines(&vertical.text(), "Showed first 1000."), 1);

    let avro = select(&house, &sql, "Avro", Some(BLOCK)).await;
    assert_eq!(
        avro.body.windows(4).filter(|w| *w == b"Obj\x01").count(),
        1,
        "one Avro container header"
    );

    // The streamable formats are unchanged: plain rows, whatever the blocks.
    let tsv = select(&house, &sql, "TSV", Some(BLOCK)).await;
    assert_eq!(tsv.text().lines().count() as u64, ROWS);
    let lines = select(&house, &sql, "JSONEachRow", Some(BLOCK)).await;
    assert_eq!(lines.text().lines().count() as u64, ROWS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whole_documents_respect_the_result_cap() {
    // A format that needs the whole result is buffered in the worker, under the
    // House's existing result cap (`wait_end_of_query_max_bytes`, R3.4).
    let house = house("doc-cap", |config| {
        config.wait_end_of_query_max_bytes = 10_000
    })
    .await;
    let addr = house.local_addr();
    let query = format!("{} FORMAT JSON", numbers(ROWS));
    let target = target(&[("query", query.as_str())]);
    let over = tokio::task::spawn_blocking(move || request(addr, "GET", &target, &[], &[]))
        .await
        .expect("client");
    assert_eq!(
        over.header("X-ClickHouse-Exception-Code"),
        Some("36"),
        "{}",
        over.text()
    );
    assert!(
        over.text().contains("wait_end_of_query_max_bytes"),
        "{}",
        over.text()
    );

    // Streamed formats are not held to it.
    let tsv = select(&house, &numbers(ROWS), "TSVWithNames", Some(BLOCK)).await;
    assert_eq!(tsv.text().lines().count() as u64, ROWS + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn totals_and_extremes_keep_one_header() {
    // chDB sends totals and extremes in a fetch of their own, followed by the header
    // of a new, empty output: the header still goes out once.
    let house = house("doc-totals", |_| {}).await;
    let totals = select(
        &house,
        "SELECT number % 3 AS k, count() AS c FROM numbers(10) GROUP BY k WITH TOTALS ORDER BY k",
        "TSVWithNamesAndTypes",
        Some("1"),
    )
    .await;
    assert_eq!(
        totals.text(),
        "k\tc\nUInt8\tUInt64\n0\t4\n1\t3\n2\t3\n\n0\t10\n"
    );
    let extremes = select(
        &house,
        "SELECT number AS n FROM numbers(5) SETTINGS extremes = 1",
        "CSVWithNames",
        Some("2"),
    )
    .await;
    assert_eq!(extremes.text(), "\"n\"\n0\n1\n2\n3\n4\n\n0\n4\n");
}
