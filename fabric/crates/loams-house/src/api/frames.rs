//! A statement's output as `loams.house.v1` batches (HS1 Task 8, §49 §18.2).
//!
//! chDB writes the result as one byte stream, split into `Chunk` frames at engine
//! blocks (and inside a block past 4 MiB, R2.3). A [`Framer`] turns that stream
//! into batches a client can decode one at a time, and holds the result to the
//! request's `max_rows` and `max_bytes`:
//!
//! - **`ARROW_IPC`** is chDB's `ArrowStream`, uncompressed. Each batch holds whole
//!   encapsulated IPC messages (the schema with the first record batch), so the
//!   batches concatenated are one IPC stream. A record batch that crosses a cap is
//!   decoded, sliced to the rows that fit and encoded again, so the cut is exact;
//!   one larger than [`SPLIT_BYTES`] is sliced into pieces of about
//!   [`TARGET_BATCH_BYTES`], so a frame stays under the 4 MiB a default Connect
//!   client accepts. The stream always ends with the IPC end-of-stream marker.
//! - **`JSON_ROWS`** is chDB's `JSONCompactEachRow`; a read is run as
//!   `JSONCompactEachRowWithNamesAndTypes`, whose first two lines are the header.
//!   Batches hold whole lines, up to [`TARGET_BATCH_BYTES`] each.
//!
//! **chDB's streaming output is per block** (measured on 26.9, HS1 Task 8):
//! `chdb_stream_query` formats each engine block on its own, so a result of
//! several blocks is several complete outputs back to back — for `ArrowStream`
//! a schema, a record batch and an end-of-stream marker per block, for a
//! `…WithNamesAndTypes` format the names and types lines again before every
//! block. The framers take that into account: an Arrow schema that repeats is
//! dropped and only the last end-of-stream marker is written; the names and
//! types lines that open each block (the caller says where a block starts) are
//! read once and skipped after.
//!
//! Past a cap the framer stops taking rows and says so ([`Framer::truncated`]);
//! the caller drains or stops the statement.

use std::io::Cursor;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, write_message,
};
use arrow_ipc::{MessageHeader, root_as_message};

use crate::errors::{ChError, HouseError};

/// The size a batch frame aims for.
pub const TARGET_BATCH_BYTES: usize = 1024 * 1024;
/// An Arrow record batch larger than this is sliced into smaller ones.
pub const SPLIT_BYTES: usize = 4 * 1024 * 1024;
/// The Arrow IPC end-of-stream marker: a continuation token and a zero length.
pub const ARROW_EOS: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

/// One column of the header: its name and ClickHouse type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The column's name.
    pub name: String,
    /// ClickHouse's type name.
    pub clickhouse_type: String,
}

/// Some rows, ready to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// Whole IPC messages or whole lines.
    pub bytes: Vec<u8>,
    /// The rows in them.
    pub rows: u64,
}

/// The caps a result is held to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caps {
    /// Rows at most.
    pub max_rows: u64,
    /// Encoded bytes at most.
    pub max_bytes: u64,
}

/// What a result's bytes are.
#[derive(Debug)]
pub enum Framer {
    /// `ArrowStream`.
    Arrow(ArrowFramer),
    /// `JSONCompactEachRow`, with the names and types first when `header` is set.
    Json(JsonFramer),
}

impl Framer {
    /// Takes the next output bytes and returns the batches they complete.
    /// `block_start`: these bytes open an engine block (the previous `Chunk` frame,
    /// if any, did not say `continued`).
    pub fn push(&mut self, bytes: &[u8], block_start: bool) -> Result<Vec<Batch>, HouseError> {
        match self {
            Self::Arrow(framer) => framer.push(bytes),
            Self::Json(framer) => framer.push(bytes, block_start),
        }
    }

    /// The statement ended: whatever is left, as batches.
    pub fn finish(&mut self) -> Result<Vec<Batch>, HouseError> {
        match self {
            Self::Arrow(framer) => framer.finish(),
            Self::Json(framer) => framer.finish(),
        }
    }

    /// The header the output itself carried (`JSONCompactEachRowWithNamesAndTypes`),
    /// once both of its lines have arrived.
    pub fn take_header(&mut self) -> Option<Vec<Column>> {
        match self {
            Self::Arrow(_) => None,
            Self::Json(framer) => framer.header.take(),
        }
    }

    /// Whether the output is still waiting for its header lines.
    pub fn header_pending(&self) -> bool {
        matches!(self, Self::Json(framer) if framer.with_names && !framer.header_read)
    }

    /// Whether a cap cut the result.
    pub fn truncated(&self) -> bool {
        match self {
            Self::Arrow(framer) => framer.truncated,
            Self::Json(framer) => framer.truncated,
        }
    }

    /// Rows returned so far.
    pub fn rows(&self) -> u64 {
        match self {
            Self::Arrow(framer) => framer.rows,
            Self::Json(framer) => framer.rows,
        }
    }

    /// Bytes returned so far.
    pub fn bytes(&self) -> u64 {
        match self {
            Self::Arrow(framer) => framer.bytes,
            Self::Json(framer) => framer.bytes,
        }
    }
}

fn malformed(what: impl std::fmt::Display) -> HouseError {
    HouseError::from(ChError::network_error(format!(
        "the engine's result could not be read: {what}"
    )))
}

// ---------------------------------------------------------------------------
// JSON rows
// ---------------------------------------------------------------------------

/// `JSONCompactEachRow` lines into batches of whole lines.
#[derive(Debug)]
pub struct JsonFramer {
    caps: Caps,
    pending: Vec<u8>,
    /// Every block opens with the names and types lines.
    with_names: bool,
    /// Lines still to skip at the start of the current block.
    skip_lines: u8,
    /// The first block's names and types lines have been read.
    header_read: bool,
    names: Vec<String>,
    header: Option<Vec<Column>>,
    rows: u64,
    bytes: u64,
    truncated: bool,
}

impl JsonFramer {
    /// A framer whose output starts with the names and types lines when `header`.
    pub fn new(caps: Caps, header: bool) -> Self {
        Self {
            caps,
            pending: Vec::new(),
            with_names: header,
            skip_lines: 0,
            header_read: false,
            names: Vec::new(),
            header: None,
            rows: 0,
            bytes: 0,
            truncated: false,
        }
    }

    fn push(&mut self, bytes: &[u8], block_start: bool) -> Result<Vec<Batch>, HouseError> {
        if self.truncated {
            return Ok(Vec::new());
        }
        if block_start && self.with_names {
            // A block's output is whole lines, so nothing is pending here.
            self.skip_lines = 2;
        }
        self.pending.extend_from_slice(bytes);
        let Some(last_newline) = self.pending.iter().rposition(|b| *b == b'\n') else {
            return Ok(Vec::new());
        };
        let complete: Vec<u8> = self.pending.drain(..=last_newline).collect();
        self.lines(&complete)
    }

    fn finish(&mut self) -> Result<Vec<Batch>, HouseError> {
        if self.truncated || self.pending.is_empty() {
            return Ok(Vec::new());
        }
        let mut rest = std::mem::take(&mut self.pending);
        rest.push(b'\n');
        self.lines(&rest)
    }

    fn lines(&mut self, complete: &[u8]) -> Result<Vec<Batch>, HouseError> {
        let mut batches = Vec::new();
        let mut current = Batch {
            bytes: Vec::new(),
            rows: 0,
        };
        for line in complete.split_inclusive(|b| *b == b'\n') {
            if self.skip_lines > 0 {
                self.skip_lines -= 1;
                if !self.header_read {
                    self.header_line(line)?;
                }
                continue;
            }
            if self.rows >= self.caps.max_rows
                || self.bytes + line.len() as u64 > self.caps.max_bytes
            {
                self.truncated = true;
                break;
            }
            if current.bytes.len() + line.len() > TARGET_BATCH_BYTES && current.rows > 0 {
                batches.push(std::mem::replace(
                    &mut current,
                    Batch {
                        bytes: Vec::new(),
                        rows: 0,
                    },
                ));
            }
            current.bytes.extend_from_slice(line);
            current.rows += 1;
            self.rows += 1;
            self.bytes += line.len() as u64;
        }
        if current.rows > 0 {
            batches.push(current);
        }
        Ok(batches)
    }

    fn header_line(&mut self, line: &[u8]) -> Result<(), HouseError> {
        let values: Vec<String> = serde_json::from_slice(line)
            .map_err(|err| malformed(format!("the names and types lines: {err}")))?;
        if self.skip_lines == 1 {
            self.names = values;
        } else {
            self.header_read = true;
            if values.len() != self.names.len() {
                return Err(malformed("the names and types lines differ in length"));
            }
            self.header = Some(
                std::mem::take(&mut self.names)
                    .into_iter()
                    .zip(values)
                    .map(|(name, clickhouse_type)| Column {
                        name,
                        clickhouse_type,
                    })
                    .collect(),
            );
        }
        Ok(())
    }
}

/// Reads the `[name, type]` lines of `DESCRIBE … SETTINGS describe_compact_output
/// = 1 FORMAT JSONCompactEachRow`, or the first two lines of
/// `JSONCompactEachRowWithNamesAndTypes`, as columns.
pub fn header_from_json(bytes: &[u8], names_and_types: bool) -> Result<Vec<Column>, HouseError> {
    let mut lines = bytes.split(|b| *b == b'\n').filter(|line| !line.is_empty());
    if names_and_types {
        let parse = |line: Option<&[u8]>| -> Result<Vec<String>, HouseError> {
            match line {
                Some(line) => serde_json::from_slice(line)
                    .map_err(|err| malformed(format!("the names and types lines: {err}"))),
                None => Ok(Vec::new()),
            }
        };
        let names = parse(lines.next())?;
        let types = parse(lines.next())?;
        if names.len() != types.len() {
            return Err(malformed("the names and types lines differ in length"));
        }
        return Ok(names
            .into_iter()
            .zip(types)
            .map(|(name, clickhouse_type)| Column {
                name,
                clickhouse_type,
            })
            .collect());
    }
    lines
        .map(|line| {
            let pair: (String, String) = serde_json::from_slice(line)
                .map_err(|err| malformed(format!("a DESCRIBE line: {err}")))?;
            Ok(Column {
                name: pair.0,
                clickhouse_type: pair.1,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Arrow IPC
// ---------------------------------------------------------------------------

/// `ArrowStream` bytes into batches of whole IPC messages.
#[derive(Debug)]
pub struct ArrowFramer {
    caps: Caps,
    pending: Vec<u8>,
    /// The schema message, kept to decode a record batch that must be sliced.
    schema: Vec<u8>,
    /// Dictionary messages so far, for the same reason.
    dictionaries: Vec<u8>,
    /// Bytes decided but not yet in a batch (the schema waits for the first rows).
    out: Batch,
    rows: u64,
    bytes: u64,
    truncated: bool,
}

/// One encapsulated message at the front of a buffer.
struct Message {
    /// Its length in the buffer, prefix, metadata and body.
    len: usize,
    kind: MessageHeader,
    rows: u64,
}

impl ArrowFramer {
    /// A framer for a result of at most `caps`.
    pub fn new(caps: Caps) -> Self {
        Self {
            caps,
            pending: Vec::new(),
            schema: Vec::new(),
            dictionaries: Vec::new(),
            out: Batch {
                bytes: Vec::new(),
                rows: 0,
            },
            rows: 0,
            bytes: 0,
            truncated: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<Vec<Batch>, HouseError> {
        self.pending.extend_from_slice(bytes);
        let mut batches = Vec::new();
        let mut at = 0;
        while let Some(message) = next_message(&self.pending[at..])? {
            let raw = self.pending[at..at + message.len].to_vec();
            at += message.len;
            match message.kind {
                // A block's end-of-stream marker: the framer writes one, in
                // `finish`.
                MessageHeader::NONE => {}
                // Every block repeats the schema; a second one that differs would
                // not be one stream.
                MessageHeader::Schema if !self.schema.is_empty() => {
                    if raw != self.schema {
                        return Err(malformed("the Arrow schema changed between blocks"));
                    }
                }
                MessageHeader::Schema => {
                    self.schema = raw.clone();
                    self.emit_meta(&raw);
                }
                MessageHeader::DictionaryBatch => {
                    self.dictionaries.extend_from_slice(&raw);
                    self.emit_meta(&raw);
                }
                MessageHeader::RecordBatch => {
                    if !self.truncated {
                        self.record_batch(raw, message.rows, &mut batches)?;
                    }
                }
                other => {
                    return Err(malformed(format!("an unexpected IPC message {other:?}")));
                }
            }
        }
        self.pending.drain(..at);
        Ok(batches)
    }

    fn finish(&mut self) -> Result<Vec<Batch>, HouseError> {
        if !self.pending.is_empty() {
            return Err(malformed("the Arrow stream ended inside a message"));
        }
        self.pending.clear();
        let mut out = std::mem::replace(
            &mut self.out,
            Batch {
                bytes: Vec::new(),
                rows: 0,
            },
        );
        if self.schema.is_empty() && out.bytes.is_empty() {
            // No output at all (a write): no stream to end.
            return Ok(Vec::new());
        }
        out.bytes.extend_from_slice(&ARROW_EOS);
        self.bytes += ARROW_EOS.len() as u64;
        Ok(vec![out])
    }

    /// A schema or dictionary message: sent with the next rows.
    fn emit_meta(&mut self, raw: &[u8]) {
        self.out.bytes.extend_from_slice(raw);
        self.bytes += raw.len() as u64;
    }

    fn record_batch(
        &mut self,
        raw: Vec<u8>,
        rows: u64,
        batches: &mut Vec<Batch>,
    ) -> Result<(), HouseError> {
        let rows_left = self.caps.max_rows.saturating_sub(self.rows);
        let bytes_left = self.caps.max_bytes.saturating_sub(self.bytes);
        let fits = rows <= rows_left && raw.len() as u64 <= bytes_left;
        if fits && raw.len() <= SPLIT_BYTES {
            self.add(raw, rows, batches);
            return Ok(());
        }
        // Decoded once, then cut to what fits and into pieces a client accepts.
        let batch = self.decode(&raw)?;
        let mut taken = 0usize;
        let total = batch.num_rows();
        let mut room_rows = usize::try_from(rows_left).unwrap_or(usize::MAX);
        while taken < total && room_rows > 0 {
            let bytes_left = self.caps.max_bytes.saturating_sub(self.bytes);
            let per_row = (raw.len() / total.max(1)).max(1);
            let by_size = TARGET_BATCH_BYTES / per_row;
            let mut take = (total - taken).min(room_rows).min(by_size.max(1));
            let mut encoded = encode(&batch.slice(taken, take))?;
            while encoded.len() as u64 > bytes_left && take > 0 {
                take = take * 2 / 3;
                encoded = if take == 0 {
                    Vec::new()
                } else {
                    encode(&batch.slice(taken, take))?
                };
            }
            if take == 0 {
                break;
            }
            self.add(encoded, take as u64, batches);
            taken += take;
            room_rows -= take;
        }
        if taken < total {
            self.truncated = true;
        }
        Ok(())
    }

    fn add(&mut self, raw: Vec<u8>, rows: u64, batches: &mut Vec<Batch>) {
        self.bytes += raw.len() as u64;
        self.rows += rows;
        self.out.bytes.extend_from_slice(&raw);
        self.out.rows += rows;
        if self.out.bytes.len() >= TARGET_BATCH_BYTES {
            batches.push(std::mem::replace(
                &mut self.out,
                Batch {
                    bytes: Vec::new(),
                    rows: 0,
                },
            ));
        }
    }

    fn decode(&self, raw: &[u8]) -> Result<RecordBatch, HouseError> {
        let mut stream =
            Vec::with_capacity(self.schema.len() + self.dictionaries.len() + raw.len());
        stream.extend_from_slice(&self.schema);
        stream.extend_from_slice(&self.dictionaries);
        stream.extend_from_slice(raw);
        stream.extend_from_slice(&ARROW_EOS);
        let mut reader = StreamReader::try_new(Cursor::new(stream), None)
            .map_err(|err| malformed(format!("the Arrow schema: {err}")))?;
        match reader.next() {
            Some(Ok(batch)) => Ok(batch),
            Some(Err(err)) => Err(malformed(format!("an Arrow record batch: {err}"))),
            None => Err(malformed("an Arrow record batch is missing")),
        }
    }
}

/// The encapsulated IPC message at the front of `buf`, when it is all there.
fn next_message(buf: &[u8]) -> Result<Option<Message>, HouseError> {
    if buf.len() < 8 {
        return Ok(None);
    }
    let word = |at: usize| u32::from_le_bytes([buf[at], buf[at + 1], buf[at + 2], buf[at + 3]]);
    // The current format opens with the continuation token; the pre-1.0 one
    // with the length alone.
    let (prefix, meta_len) = if word(0) == 0xffff_ffff {
        (8usize, word(4) as usize)
    } else {
        (4usize, word(0) as usize)
    };
    if meta_len == 0 {
        return Ok(Some(Message {
            len: prefix,
            kind: MessageHeader::NONE,
            rows: 0,
        }));
    }
    if buf.len() < prefix + meta_len {
        return Ok(None);
    }
    let message = root_as_message(&buf[prefix..prefix + meta_len])
        .map_err(|err| malformed(format!("an IPC message header: {err}")))?;
    let body = usize::try_from(message.bodyLength())
        .map_err(|_| malformed("a negative IPC body length"))?;
    let len = prefix + meta_len + body;
    if buf.len() < len {
        return Ok(None);
    }
    let kind = message.header_type();
    let rows = message
        .header_as_record_batch()
        .map_or(0, |batch| u64::try_from(batch.length()).unwrap_or(0));
    Ok(Some(Message { len, kind, rows }))
}

/// One record batch as an encapsulated IPC message (and any dictionaries it needs).
fn encode(batch: &RecordBatch) -> Result<Vec<u8>, HouseError> {
    let options = IpcWriteOptions::default();
    let mut tracker = DictionaryTracker::new(false);
    let (dictionaries, encoded) = IpcDataGenerator::default()
        .encode(
            batch,
            &mut tracker,
            &options,
            &mut IpcWriteContext::default(),
        )
        .map_err(|err| malformed(format!("encoding a sliced batch: {err}")))?;
    let mut out = Vec::new();
    for dictionary in dictionaries {
        write_message(&mut out, dictionary, &options)
            .map_err(|err| malformed(format!("encoding a dictionary: {err}")))?;
    }
    write_message(&mut out, encoded, &options)
        .map_err(|err| malformed(format!("encoding a sliced batch: {err}")))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn ipc_stream(blocks: &[(i64, usize)]) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let mut out = Vec::new();
        let mut writer = StreamWriter::try_new(&mut out, &schema).expect("writer");
        for (start, rows) in blocks {
            let n: Vec<i64> = (*start..*start + *rows as i64).collect();
            let s: Vec<String> = n.iter().map(|v| format!("row-{v:08}")).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(n)),
                    Arc::new(StringArray::from(s)),
                ],
            )
            .expect("batch");
            writer.write(&batch).expect("write");
        }
        writer.finish().expect("finish");
        drop(writer);
        out
    }

    fn read_rows(stream: &[u8]) -> Vec<i64> {
        let reader = StreamReader::try_new(Cursor::new(stream.to_vec()), None).expect("reader");
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.expect("batch");
            let n = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("n");
            out.extend(n.values().iter().copied());
        }
        out
    }

    fn run(framer: &mut Framer, bytes: &[u8], piece: usize) -> Vec<Batch> {
        let mut batches = Vec::new();
        for (i, chunk) in bytes.chunks(piece).enumerate() {
            batches.extend(framer.push(chunk, i == 0).expect("push"));
        }
        batches.extend(framer.finish().expect("finish"));
        batches
    }

    const ALL: Caps = Caps {
        max_rows: u64::MAX,
        max_bytes: u64::MAX,
    };

    #[test]
    fn arrow_batches_are_whole_messages_in_any_chunking() {
        let stream = ipc_stream(&[(0, 10), (10, 10), (20, 5)]);
        for piece in [1, 7, 64, stream.len()] {
            let mut framer = Framer::Arrow(ArrowFramer::new(ALL));
            let batches = run(&mut framer, &stream, piece);
            let joined: Vec<u8> = batches.iter().flat_map(|b| b.bytes.clone()).collect();
            assert_eq!(
                read_rows(&joined),
                (0..25).collect::<Vec<_>>(),
                "piece {piece}"
            );
            assert_eq!(framer.rows(), 25);
            assert!(!framer.truncated());
            // Every batch on its own is whole messages: the first decodes with
            // its schema, and none ends inside a message.
            let mut at = 0;
            let mut buf = Vec::new();
            for batch in &batches {
                buf.extend_from_slice(&batch.bytes);
                while let Some(message) = next_message(&buf[at..]).expect("message") {
                    at += message.len;
                }
                assert_eq!(at, buf.len(), "a batch ends on a message boundary");
            }
        }
    }

    #[test]
    fn arrow_max_rows_cuts_inside_a_record_batch() {
        let stream = ipc_stream(&[(0, 10), (10, 10), (20, 10)]);
        let mut framer = Framer::Arrow(ArrowFramer::new(Caps {
            max_rows: 15,
            max_bytes: u64::MAX,
        }));
        let batches = run(&mut framer, &stream, 13);
        let joined: Vec<u8> = batches.iter().flat_map(|b| b.bytes.clone()).collect();
        assert_eq!(read_rows(&joined), (0..15).collect::<Vec<_>>());
        assert!(framer.truncated());
        assert_eq!(batches.iter().map(|b| b.rows).sum::<u64>(), 15);
        assert_eq!(framer.bytes(), joined.len() as u64);
    }

    #[test]
    fn arrow_max_bytes_cuts_at_a_row_boundary() {
        let stream = ipc_stream(&[(0, 1000)]);
        let cap = 6_000;
        let mut framer = Framer::Arrow(ArrowFramer::new(Caps {
            max_rows: u64::MAX,
            max_bytes: cap,
        }));
        let batches = run(&mut framer, &stream, 4096);
        let joined: Vec<u8> = batches.iter().flat_map(|b| b.bytes.clone()).collect();
        let rows = read_rows(&joined);
        assert!(framer.truncated());
        assert!(!rows.is_empty() && rows.len() < 1000, "{} rows", rows.len());
        assert_eq!(rows, (0..rows.len() as i64).collect::<Vec<_>>());
        // The end-of-stream marker is the only thing past the cap.
        assert!(joined.len() as u64 <= cap + ARROW_EOS.len() as u64);
    }

    #[test]
    fn arrow_big_batches_are_split_under_the_frame_limit() {
        // About 10 MiB in one record batch.
        let stream = ipc_stream(&[(0, 500_000)]);
        let mut framer = Framer::Arrow(ArrowFramer::new(ALL));
        let batches = run(&mut framer, &stream, 1 << 20);
        assert!(batches.len() > 4, "{} batches", batches.len());
        for batch in &batches {
            assert!(batch.bytes.len() <= SPLIT_BYTES, "{}", batch.bytes.len());
        }
        let joined: Vec<u8> = batches.iter().flat_map(|b| b.bytes.clone()).collect();
        assert_eq!(read_rows(&joined).len(), 500_000);
        assert!(!framer.truncated());
    }

    #[test]
    fn json_lines_header_and_caps() {
        let text = b"[\"n\",\"s\"]\n[\"UInt64\",\"String\"]\n[0,\"a\"]\n[1,\"b\"]\n[2,\"c\"]\n";
        let mut framer = Framer::Json(JsonFramer::new(
            Caps {
                max_rows: 2,
                max_bytes: u64::MAX,
            },
            true,
        ));
        let batches = run(&mut framer, text, 5);
        assert_eq!(
            framer.take_header(),
            Some(vec![
                Column {
                    name: "n".into(),
                    clickhouse_type: "UInt64".into()
                },
                Column {
                    name: "s".into(),
                    clickhouse_type: "String".into()
                },
            ])
        );
        let joined: Vec<u8> = batches.iter().flat_map(|b| b.bytes.clone()).collect();
        assert_eq!(joined, b"[0,\"a\"]\n[1,\"b\"]\n");
        assert!(framer.truncated());

        let mut bytes_capped = Framer::Json(JsonFramer::new(
            Caps {
                max_rows: 100,
                max_bytes: 10,
            },
            false,
        ));
        let batches = run(&mut bytes_capped, b"[0,\"a\"]\n[1,\"b\"]\n", 3);
        let joined: Vec<u8> = batches.iter().flat_map(|b| b.bytes.clone()).collect();
        assert_eq!(joined, b"[0,\"a\"]\n");
        assert!(bytes_capped.truncated());
    }

    #[test]
    fn arrow_per_block_streams_are_one_stream() {
        // chDB writes each block as a stream of its own (schema, batch, EOS).
        let mut chdb = ipc_stream(&[(0, 4)]);
        chdb.extend(ipc_stream(&[(4, 3)]));
        chdb.extend(ipc_stream(&[(7, 5)]));
        let mut framer = Framer::Arrow(ArrowFramer::new(ALL));
        let batches = run(&mut framer, &chdb, 9);
        let joined: Vec<u8> = batches.iter().flat_map(|b| b.bytes.clone()).collect();
        assert_eq!(read_rows(&joined), (0..12).collect::<Vec<_>>());
        // One schema, one end-of-stream marker.
        let mut at = 0;
        let mut kinds = Vec::new();
        while let Some(message) = next_message(&joined[at..]).expect("message") {
            kinds.push(message.kind);
            at += message.len;
        }
        assert_eq!(
            kinds
                .iter()
                .filter(|k| **k == MessageHeader::Schema)
                .count(),
            1
        );
        assert_eq!(
            kinds.iter().filter(|k| **k == MessageHeader::NONE).count(),
            1
        );
        assert_eq!(kinds.last(), Some(&MessageHeader::NONE));
    }

    #[test]
    fn json_names_and_types_repeat_per_block() {
        let block = |rows: &[u32]| {
            let mut out = b"[\"n\"]\n[\"UInt32\"]\n".to_vec();
            for r in rows {
                out.extend(format!("[{r}]\n").into_bytes());
            }
            out
        };
        let mut framer = Framer::Json(JsonFramer::new(ALL, true));
        let mut batches = Vec::new();
        for rows in [&[0u32, 1][..], &[2], &[3, 4]] {
            let bytes = block(rows);
            // A block split into two chunks: only the first starts it.
            let (a, b) = bytes.split_at(bytes.len() / 2);
            batches.extend(framer.push(a, true).expect("push"));
            batches.extend(framer.push(b, false).expect("push"));
        }
        batches.extend(framer.finish().expect("finish"));
        let joined: Vec<u8> = batches.iter().flat_map(|b| b.bytes.clone()).collect();
        assert_eq!(joined, b"[0]\n[1]\n[2]\n[3]\n[4]\n");
        assert_eq!(framer.rows(), 5);
        assert_eq!(framer.take_header().map(|h| h.len()), Some(1));
    }

    #[test]
    fn describe_lines_are_columns() {
        let columns = header_from_json(b"[\"t\",\"DateTime('UTC')\"]\n[\"u\",\"UInt8\"]\n", false)
            .expect("columns");
        assert_eq!(columns[0].clickhouse_type, "DateTime('UTC')");
        assert_eq!(columns[1].name, "u");
        let named = header_from_json(b"[\"a\"]\n[\"String\"]\n[\"x\"]\n", true).expect("named");
        assert_eq!(named.len(), 1);
        assert_eq!(named[0].clickhouse_type, "String");
    }
}
