//! One document per result, whatever the format and however many blocks (HS1
//! R8.4, R8.12).
//!
//! chDB's streaming ABI formats every engine block as a complete output of its
//! own (R8.4, measured on 26.9.2.1 for every output format chDB has): `JSON`
//! repeats `meta`, `data` and `statistics` per block, `Parquet` writes a file per
//! block, `ArrowStream` a stream per block, the `…WithNames` formats repeat their
//! header lines, `Values` drops the comma between blocks, and `Vertical`,
//! `JSONObjectEachRow` and `PrettyCompact` number rows from 1 in every block.
//! `WITH TOTALS` and `extremes = 1` add one more twist: their rows come in a fetch
//! of their own, followed by the prefix of a new, empty output (a `…WithNames`
//! header at the end of that fetch). Concatenating the fetches is therefore right
//! only for the plain row formats.
//!
//! A [`Shaper`] turns the fetches of one result into one document:
//!
//! * **Row formats** (`TSV`, `CSV`, `JSONEachRow`, `RowBinary`, `Native`, …) pass
//!   through: a fetch is rows and nothing else.
//! * **`Values`** gets the comma between fetches back.
//! * **`JSONEachRow` with `output_format_json_array_of_rows = 1`** is re-framed
//!   from its rows (one per line, since JSON escapes newlines): `[`, the rows
//!   joined by `,` and a newline, `]`.
//! * **`…WithNames` and `…WithNamesAndTypes`** (TSV, CSV, JSONCompact(Strings)
//!   EachRow, RowBinary): the header is read once from the first fetch, exactly
//!   per format, and taken off every later fetch, at its start, or at its end
//!   after totals or extremes. A fetch that has it at neither end is an error,
//!   never wrong output.
//! * **`ArrowStream`**: one schema message and one end-of-stream marker; the
//!   messages between are kept as they are (dictionary batches included).
//! * **`Parquet`**: the per-fetch files' row groups are spliced, column chunk
//!   bytes copied without decoding, into one file whose footer is written at the
//!   end, so only one fetch is held at a time.
//! * **Every other format** is [`Shape::Whole`]: chDB formats the whole result
//!   with one output format (`chdb_query`, the engine doing it right), held to the
//!   House's result cap. That covers the formats that need the whole result
//!   (`JSONColumns`, `Hash`, `Arrow`'s file footer, `ORC`, `Avro`), those with a
//!   document envelope (`JSON*`, `XML`, `Markdown`, `Pretty*`, `Vertical`,
//!   `JSONObjectEachRow`, `Template`, `CustomSeparated*`, `MySQLWire`, …) and any
//!   format this list does not know.

use std::io::Write;
use std::sync::{Arc, Mutex};

use arrow_ipc::{MessageHeader, root_as_message};
use bytes::Bytes;
use loams_house_ipc::{EngineError, Output};
use parquet::bloom_filter::Sbbf;
use parquet::column::writer::ColumnCloseResult;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;

/// How one format's fetches become one document.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// Rows only: fetches concatenate.
    Rows,
    /// `Values`: rows separated by commas, also between fetches.
    Values,
    /// `JSONEachRow` and its kin: rows, unless the first fetch shows the
    /// `output_format_json_array_of_rows` framing.
    JsonRows,
    /// A header once, then rows.
    Header(HeaderKind),
    /// One Arrow IPC stream.
    ArrowStream,
    /// One Parquet file.
    Parquet,
    /// Formatted by chDB in one piece.
    Whole,
}

/// How a `…WithNames` header is laid out, so its length can be read exactly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderKind {
    /// `lines` newline-terminated lines whose values are escaped (TSV, JSON).
    Lines(usize),
    /// `lines` CSV records; names and types are quoted and may hold newlines.
    Csv(usize),
    /// A column count, then the names, then (with types) the type names, each a
    /// length-prefixed string.
    RowBinary { types: bool },
}

impl Shape {
    /// The shape of an output format, by its name (any case, ClickHouse's
    /// aliases included). A name this list does not know is [`Shape::Whole`].
    pub fn of(format: &str) -> Shape {
        match format.to_ascii_lowercase().as_str() {
            "tabseparated"
            | "tsv"
            | "tabseparatedraw"
            | "tsvraw"
            | "raw"
            | "csv"
            | "jsoncompacteachrow"
            | "jsoncompactstringseachrow"
            | "rowbinary"
            | "native"
            | "bsoneachrow"
            | "tskv"
            | "lineasstring"
            | "null"
            | "msgpack"
            | "rawblob"
            | "sqlinsert" => Shape::Rows,
            "values" => Shape::Values,
            "jsoneachrow" | "jsonlines" | "ndjson" | "jsonstringseachrow" => Shape::JsonRows,
            "tabseparatedwithnames"
            | "tsvwithnames"
            | "jsoncompacteachrowwithnames"
            | "jsoncompactstringseachrowwithnames" => Shape::Header(HeaderKind::Lines(1)),
            "tabseparatedwithnamesandtypes"
            | "tsvwithnamesandtypes"
            | "jsoncompacteachrowwithnamesandtypes"
            | "jsoncompactstringseachrowwithnamesandtypes" => Shape::Header(HeaderKind::Lines(2)),
            "csvwithnames" => Shape::Header(HeaderKind::Csv(1)),
            "csvwithnamesandtypes" => Shape::Header(HeaderKind::Csv(2)),
            "rowbinarywithnames" => Shape::Header(HeaderKind::RowBinary { types: false }),
            "rowbinarywithnamesandtypes" => Shape::Header(HeaderKind::RowBinary { types: true }),
            "arrowstream" => Shape::ArrowStream,
            "parquet" => Shape::Parquet,
            // `…RawWithNames` writes names unescaped, so a name with a newline
            // would make its header unreadable: whole, with the rest.
            _ => Shape::Whole,
        }
    }
}

/// Turns the fetches of one result into what goes to the front.
#[derive(Debug)]
pub struct Shaper {
    format: String,
    state: State,
}

#[derive(Debug)]
enum State {
    /// `Output::Blocks`, or a row format: every fetch as it came.
    PassThrough,
    Values {
        rows_sent: bool,
    },
    JsonRows {
        /// `None` until the first fetch shows which framing it has.
        array: Option<bool>,
        /// Whether a row has gone out (a separator goes before the next).
        rows_sent: bool,
    },
    Header {
        kind: HeaderKind,
        header: Header,
    },
    Arrow {
        schema: Option<Vec<u8>>,
        carry: Vec<u8>,
    },
    Parquet(Box<Option<ParquetSplice>>),
}

#[derive(Debug)]
enum Header {
    /// Nothing seen yet.
    Unknown,
    /// The header's exact bytes.
    Known(Vec<u8>),
    /// The first fetch, whose header could not be read: sent alone if it is the
    /// only one, refused if another follows.
    Held(Bytes),
}

/// What a [`Shaper`] is for one statement.
#[derive(Debug)]
pub enum Plan {
    /// Stream the fetches through the shaper.
    Stream(Shaper),
    /// Run the statement with `chdb_query` and send its output whole, refused
    /// past `max_bytes`.
    Whole { max_bytes: u64 },
}

impl Shaper {
    /// The plan for a statement's output.
    pub fn plan(output: &Output, format: &str) -> Plan {
        let state = match output {
            Output::Blocks => State::PassThrough,
            Output::Document { max_buffered_bytes } => match Shape::of(format) {
                Shape::Whole => {
                    return Plan::Whole {
                        max_bytes: *max_buffered_bytes,
                    };
                }
                Shape::Rows => State::PassThrough,
                Shape::Values => State::Values { rows_sent: false },
                Shape::JsonRows => State::JsonRows {
                    array: None,
                    rows_sent: false,
                },
                Shape::Header(kind) => State::Header {
                    kind,
                    header: Header::Unknown,
                },
                Shape::ArrowStream => State::Arrow {
                    schema: None,
                    carry: Vec::new(),
                },
                Shape::Parquet => State::Parquet(Box::new(None)),
            },
        };
        Plan::Stream(Shaper {
            format: format.to_string(),
            state,
        })
    }

    /// The bytes to send for one fetch (possibly none).
    pub fn push(&mut self, fetch: Bytes) -> Result<Bytes, EngineError> {
        if fetch.is_empty() {
            return Ok(Bytes::new());
        }
        let format = &self.format;
        match &mut self.state {
            State::PassThrough => Ok(fetch),
            State::Values { rows_sent } => {
                if !*rows_sent {
                    *rows_sent = true;
                    return Ok(fetch);
                }
                let mut out = Vec::with_capacity(fetch.len() + 1);
                out.push(b',');
                out.extend_from_slice(&fetch);
                Ok(Bytes::from(out))
            }
            State::JsonRows { array, rows_sent } => {
                let first = array.is_none();
                let is_array = *array.get_or_insert(fetch.first() == Some(&b'['));
                if !is_array {
                    return Ok(fetch);
                }
                let mut out = Vec::with_capacity(fetch.len() + 2);
                if first {
                    out.extend_from_slice(b"[\n");
                }
                for line in fetch.split(|b| *b == b'\n') {
                    if line.first() != Some(&b'{') {
                        continue;
                    }
                    let row = line.strip_suffix(b",").unwrap_or(line);
                    if *rows_sent {
                        out.extend_from_slice(b",\n");
                    }
                    out.extend_from_slice(row);
                    *rows_sent = true;
                }
                Ok(Bytes::from(out))
            }
            State::Header { kind, header } => match header {
                Header::Unknown => {
                    *header = match header_len(*kind, &fetch) {
                        Some(len) => Header::Known(fetch[..len].to_vec()),
                        None => {
                            *header = Header::Held(fetch);
                            return Ok(Bytes::new());
                        }
                    };
                    Ok(fetch)
                }
                Header::Known(bytes) => {
                    if fetch.starts_with(bytes) {
                        Ok(fetch.slice(bytes.len()..))
                    } else if fetch.ends_with(bytes) {
                        // Totals or extremes, then the header of a new, empty output.
                        Ok(fetch.slice(..fetch.len() - bytes.len()))
                    } else {
                        Err(document_error(
                            format,
                            "a block did not repeat the result's header",
                        ))
                    }
                }
                Header::Held(_) => Err(document_error(
                    format,
                    "its header could not be read (are the types encoded in binary?), so \
                     a result of more than one block cannot be joined",
                )),
            },
            State::Arrow { schema, carry } => {
                carry.extend_from_slice(&fetch);
                arrow_messages(format, schema, carry)
            }
            State::Parquet(splice) => {
                let splice = &mut **splice;
                let file = parquet_file(format, &fetch)?;
                let writer = match splice {
                    Some(writer) => writer,
                    None => splice.insert(ParquetSplice::new(format, &file)?),
                };
                writer.append(format, &fetch, &file)
            }
        }
    }

    /// What ends the document, once the last fetch is in.
    pub fn finish(&mut self) -> Result<Bytes, EngineError> {
        let format = &self.format;
        match &mut self.state {
            State::PassThrough | State::Values { .. } => Ok(Bytes::new()),
            // `[`, the rows, `]`: chDB's own framing, `[\n\n]\n` when empty.
            State::JsonRows { array, .. } => Ok(match array {
                Some(true) => Bytes::from_static(b"\n]\n"),
                _ => Bytes::new(),
            }),
            State::Header { header, .. } => match std::mem::replace(header, Header::Unknown) {
                Header::Held(fetch) => Ok(fetch),
                _ => Ok(Bytes::new()),
            },
            State::Arrow { schema, carry } => {
                if !carry.is_empty() {
                    return Err(document_error(format, "the stream ended inside a message"));
                }
                Ok(if schema.is_some() {
                    Bytes::from_static(&ARROW_EOS)
                } else {
                    Bytes::new()
                })
            }
            State::Parquet(splice) => match splice.take() {
                Some(writer) => writer.close(format),
                None => Ok(Bytes::new()),
            },
        }
    }
}

/// The Arrow IPC end-of-stream marker.
const ARROW_EOS: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

fn document_error(format: &str, what: impl std::fmt::Display) -> EngineError {
    EngineError {
        code: 0,
        name: "LOAMS_DOCUMENT".to_string(),
        message: format!("the {format} result could not be made one document: {what}"),
    }
}

/// The length of the header at the start of `fetch`, when it can be read.
pub fn header_len(kind: HeaderKind, fetch: &[u8]) -> Option<usize> {
    match kind {
        HeaderKind::Lines(lines) => {
            let mut at = 0;
            for _ in 0..lines {
                at += fetch[at..].iter().position(|b| *b == b'\n')? + 1;
            }
            Some(at)
        }
        HeaderKind::Csv(records) => {
            let mut at = 0;
            for _ in 0..records {
                let mut quoted = false;
                loop {
                    let byte = *fetch.get(at)?;
                    at += 1;
                    match byte {
                        b'"' => quoted = !quoted,
                        b'\n' if !quoted => break,
                        _ => {}
                    }
                }
            }
            Some(at)
        }
        HeaderKind::RowBinary { types } => {
            let mut at = 0;
            let columns = varuint(fetch, &mut at)?;
            for _ in 0..columns {
                let len = usize::try_from(varuint(fetch, &mut at)?).ok()?;
                at = at.checked_add(len).filter(|end| *end <= fetch.len())?;
            }
            if types {
                for _ in 0..columns {
                    let len = usize::try_from(varuint(fetch, &mut at)?).ok()?;
                    let end = at.checked_add(len).filter(|end| *end <= fetch.len())?;
                    // With `output_format_binary_encode_types_in_binary_format`
                    // the types are not names, and their length cannot be read
                    // without decoding them: the header is then unreadable.
                    if !is_type_name(&fetch[at..end]) {
                        return None;
                    }
                    at = end;
                }
            }
            Some(at)
        }
    }
}

/// Whether bytes look like a ClickHouse type name (`UInt64`,
/// `Nullable(DateTime64(6, 'UTC'))`, `Tuple(a String)`, …).
fn is_type_name(bytes: &[u8]) -> bool {
    bytes.first().is_some_and(u8::is_ascii_uppercase)
        && bytes.iter().all(|b| (0x20..0x7f).contains(b))
}

/// ClickHouse's unsigned LEB128.
fn varuint(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut value: u64 = 0;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*at)?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Some(value);
        }
    }
    None
}

/// Moves the whole messages at the front of `carry` out: the first schema, and
/// every message but a later schema or an end-of-stream marker.
fn arrow_messages(
    format: &str,
    schema: &mut Option<Vec<u8>>,
    carry: &mut Vec<u8>,
) -> Result<Bytes, EngineError> {
    let mut out = Vec::with_capacity(carry.len());
    let mut at = 0;
    while let Some((len, kind)) = next_message(format, &carry[at..])? {
        let message = &carry[at..at + len];
        at += len;
        match kind {
            MessageHeader::NONE => {} // end of one block's stream
            MessageHeader::Schema => match schema {
                None => {
                    *schema = Some(message.to_vec());
                    out.extend_from_slice(message);
                }
                Some(first) if first.as_slice() == message => {}
                Some(_) => {
                    return Err(document_error(format, "a block's schema differs"));
                }
            },
            _ => out.extend_from_slice(message),
        }
    }
    carry.drain(..at);
    Ok(Bytes::from(out))
}

/// The length and kind of the encapsulated message at the front of `buf`, when
/// it is all there; an end-of-stream marker is `NONE`.
fn next_message(format: &str, buf: &[u8]) -> Result<Option<(usize, MessageHeader)>, EngineError> {
    let word = |at: usize| {
        buf.get(at..at + 4)
            .map(|w| u32::from_le_bytes([w[0], w[1], w[2], w[3]]))
    };
    let Some(first) = word(0) else {
        return Ok(None);
    };
    // The current format opens with the continuation token; the pre-1.0 one with
    // the length alone.
    let (prefix, meta_len) = if first == 0xffff_ffff {
        match word(4) {
            Some(len) => (8usize, len as usize),
            None => return Ok(None),
        }
    } else {
        (4usize, first as usize)
    };
    if meta_len == 0 {
        return Ok(Some((prefix, MessageHeader::NONE)));
    }
    let Some(meta) = buf.get(prefix..prefix + meta_len) else {
        return Ok(None);
    };
    let message = root_as_message(meta)
        .map_err(|err| document_error(format, format!("an IPC message header: {err}")))?;
    let body = usize::try_from(message.bodyLength())
        .map_err(|_| document_error(format, "a negative IPC body length"))?;
    let len = prefix + meta_len + body;
    if buf.len() < len {
        return Ok(None);
    }
    Ok(Some((len, message.header_type())))
}

/// One fetch's Parquet file, read from its footer.
fn parquet_file(format: &str, fetch: &Bytes) -> Result<ParquetMetaData, EngineError> {
    // Each fetch is one whole file (measured): a fetch that is not one is refused
    // rather than spliced at the wrong offsets.
    if !fetch.starts_with(b"PAR1") || !fetch.ends_with(b"PAR1") {
        return Err(document_error(format, "a block is not one Parquet file"));
    }
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(fetch)
        .map_err(|err| document_error(format, format!("a block's Parquet footer: {err}")))
}

/// Where the spliced file goes: drained after each fetch.
#[derive(Clone, Debug, Default)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl Sink {
    fn take(&self) -> Bytes {
        let mut buf = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Bytes::from(std::mem::take(&mut *buf))
    }
}

impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// One Parquet file made of the row groups of every fetch.
struct ParquetSplice {
    writer: SerializedFileWriter<Sink>,
    sink: Sink,
    first: ParquetMetaData,
}

impl std::fmt::Debug for ParquetSplice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetSplice").finish_non_exhaustive()
    }
}

impl ParquetSplice {
    /// A writer with the first file's schema, `created_by`, key-value metadata
    /// and format version, so the result reads as chDB's own file.
    fn new(format: &str, first: &ParquetMetaData) -> Result<Self, EngineError> {
        let meta = first.file_metadata();
        let mut props = WriterProperties::builder()
            .set_key_value_metadata(meta.key_value_metadata().cloned())
            .set_writer_version(if meta.version() >= 2 {
                WriterVersion::PARQUET_2_0
            } else {
                WriterVersion::PARQUET_1_0
            });
        if let Some(created_by) = meta.created_by() {
            props = props.set_created_by(created_by.to_string());
        }
        let sink = Sink::default();
        let writer = SerializedFileWriter::new(
            sink.clone(),
            meta.schema_descr().root_schema_ptr(),
            Arc::new(props.build()),
        )
        .map_err(|err| document_error(format, err))?;
        Ok(Self {
            writer,
            sink,
            first: first.clone(),
        })
    }

    /// Copies every row group of `file` (whose bytes are `fetch`) and returns
    /// what the writer has written since the last call.
    fn append(
        &mut self,
        format: &str,
        fetch: &Bytes,
        file: &ParquetMetaData,
    ) -> Result<Bytes, EngineError> {
        if file.file_metadata().schema() != self.first.file_metadata().schema() {
            return Err(document_error(format, "a block's schema differs"));
        }
        let fail = |err: parquet::errors::ParquetError| document_error(format, err);
        for (rg_index, rg) in file.row_groups().iter().enumerate() {
            let column_indexes = file.column_index().and_then(|ci| ci.get(rg_index));
            let offset_indexes = file.offset_index().and_then(|oi| oi.get(rg_index));
            let mut out = self.writer.next_row_group().map_err(fail)?;
            for (col_index, column) in rg.columns().iter().enumerate() {
                let close = ColumnCloseResult {
                    bytes_written: u64::try_from(column.compressed_size()).unwrap_or(0),
                    rows_written: u64::try_from(rg.num_rows()).unwrap_or(0),
                    metadata: column.clone(),
                    bloom_filter: Sbbf::read_from_column_chunk(column, fetch).ok().flatten(),
                    column_index: column_indexes.and_then(|row| row.get(col_index)).cloned(),
                    offset_index: offset_indexes.and_then(|row| row.get(col_index)).cloned(),
                };
                out.append_column(fetch, close).map_err(fail)?;
            }
            out.close().map_err(fail)?;
        }
        self.writer
            .flush()
            .map_err(|err| document_error(format, err))?;
        Ok(self.sink.take())
    }

    /// Writes the footer and returns the rest of the file.
    fn close(self, format: &str) -> Result<Bytes, EngineError> {
        let sink = self.sink.clone();
        self.writer
            .close()
            .map_err(|err| document_error(format, err))?;
        Ok(sink.take())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(output: Output, format: &str, fetches: &[&[u8]]) -> Result<Vec<u8>, EngineError> {
        let Plan::Stream(mut shaper) = Shaper::plan(&output, format) else {
            panic!("{format} streams");
        };
        let mut out = Vec::new();
        for fetch in fetches {
            out.extend_from_slice(&shaper.push(Bytes::copy_from_slice(fetch))?);
        }
        out.extend_from_slice(&shaper.finish()?);
        Ok(out)
    }

    const DOC: Output = Output::Document {
        max_buffered_bytes: 1 << 20,
    };

    #[test]
    fn shapes_follow_the_format_names() {
        assert_eq!(Shape::of("TSV"), Shape::Rows);
        assert_eq!(Shape::of("tabseparated"), Shape::Rows);
        assert_eq!(Shape::of("NDJSON"), Shape::JsonRows);
        assert_eq!(
            Shape::of("TabSeparatedWithNamesAndTypes"),
            Shape::Header(HeaderKind::Lines(2))
        );
        assert_eq!(Shape::of("CSVWithNames"), Shape::Header(HeaderKind::Csv(1)));
        assert_eq!(Shape::of("TSVRawWithNames"), Shape::Whole);
        assert_eq!(Shape::of("JSON"), Shape::Whole);
        assert_eq!(Shape::of("Arrow"), Shape::Whole);
        assert_eq!(Shape::of("NoSuchFormat"), Shape::Whole);
        assert!(matches!(
            Shaper::plan(&DOC, "JSON"),
            Plan::Whole { max_bytes } if max_bytes == 1 << 20
        ));
        // Blocks mode never changes a byte.
        let blocks = run(Output::Blocks, "TSVWithNames", &[b"n\n0\n", b"n\n1\n"]).unwrap();
        assert_eq!(blocks, b"n\n0\nn\n1\n");
    }

    #[test]
    fn headers_are_read_exactly() {
        assert_eq!(
            header_len(HeaderKind::Lines(2), b"a\tb\nT\tU\n1\t2\n"),
            Some(8)
        );
        assert_eq!(header_len(HeaderKind::Lines(1), b"a\tb"), None);
        // A quoted name with a newline, a comma and doubled quotes.
        let csv = b"\"x\"\"y,\nz\",\"s\"\n1,\"a\"\n";
        assert_eq!(header_len(HeaderKind::Csv(1), csv), Some(14));
        assert_eq!(
            header_len(HeaderKind::Csv(2), b"\"n\"\n\"UInt64\"\n0\n"),
            Some(13)
        );
        let rb = b"\x02\x01k\x01c\x05UInt8\x06UInt64\x00\x04\x00\x00\x00\x00\x00\x00\x00";
        assert_eq!(
            header_len(HeaderKind::RowBinary { types: true }, rb),
            Some(18)
        );
        assert_eq!(
            header_len(HeaderKind::RowBinary { types: false }, rb),
            Some(5)
        );
        // Types encoded in binary are not names.
        let binary = b"\x01\x01n\x04\x00\x00\x00\x00\x00\x00\x00\x00";
        assert_eq!(
            header_len(HeaderKind::RowBinary { types: true }, binary),
            None
        );
    }

    #[test]
    fn headers_go_once_with_totals_and_extremes() {
        // As measured: totals in a fetch of their own, then a new output's header.
        let out = run(
            DOC,
            "TSVWithNamesAndTypes",
            &[
                b"k\tc\nUInt8\tUInt64\n0\t4\n",
                b"k\tc\nUInt8\tUInt64\n1\t3\n",
                b"\n0\t10\nk\tc\nUInt8\tUInt64\n",
            ],
        )
        .unwrap();
        assert_eq!(out, b"k\tc\nUInt8\tUInt64\n0\t4\n1\t3\n\n0\t10\n");
        let csv = run(
            DOC,
            "CSVWithNames",
            &[b"\"a\nb\"\n0\n", b"\"a\nb\"\n1\n", b"\"a\nb\"\n"],
        )
        .unwrap();
        assert_eq!(csv, b"\"a\nb\"\n0\n1\n");
        let lost = run(DOC, "TSVWithNames", &[b"n\n0\n", b"m\n1\n"]).unwrap_err();
        assert_eq!(lost.name, "LOAMS_DOCUMENT");
    }

    #[test]
    fn an_unreadable_header_is_sent_alone_or_refused() {
        let binary: &[u8] = b"\x01\x01n\x04\x00\x00\x00\x00\x00\x00\x00\x00";
        let one = run(DOC, "RowBinaryWithNamesAndTypes", &[binary]).unwrap();
        assert_eq!(one, binary);
        let two = run(DOC, "RowBinaryWithNamesAndTypes", &[binary, binary]).unwrap_err();
        assert_eq!(two.name, "LOAMS_DOCUMENT");
    }

    #[test]
    fn values_and_json_arrays_are_rejoined() {
        let values = run(DOC, "Values", &[b"(0,'0'),(1,'1')", b"(2,'2')"]).unwrap();
        assert_eq!(values, b"(0,'0'),(1,'1'),(2,'2')");
        let lines = run(DOC, "JSONEachRow", &[b"{\"n\":0}\n", b"{\"n\":1}\n"]).unwrap();
        assert_eq!(lines, b"{\"n\":0}\n{\"n\":1}\n");
        let array = run(
            DOC,
            "JSONEachRow",
            &[
                b"[\n{\"n\":0},\n{\"n\":1}\n]\n",
                b"[\n{\"n\":2}\n]\n",
                b"\n]\n[\n",
            ],
        )
        .unwrap();
        assert_eq!(array, b"[\n{\"n\":0},\n{\"n\":1},\n{\"n\":2}\n]\n");
        let empty = run(DOC, "JSONEachRow", &[b"[\n\n]\n"]).unwrap();
        assert_eq!(empty, b"[\n\n]\n", "chDB's own empty array");
    }

    /// `rows` as one Arrow IPC stream of one batch (schema, batch, end).
    fn ipc_stream(rows: std::ops::Range<u64>) -> Vec<u8> {
        use arrow::array::UInt64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from_iter_values(rows))],
        )
        .unwrap();
        let mut out = Vec::new();
        let mut writer = StreamWriter::try_new(&mut out, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        out
    }

    /// `rows` as one Parquet file of one row group.
    fn parquet_file_of(rows: std::ops::Range<u64>) -> Vec<u8> {
        use arrow::array::UInt64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from_iter_values(rows))],
        )
        .unwrap();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        out
    }

    fn values(batches: impl Iterator<Item = arrow::record_batch::RecordBatch>) -> Vec<u64> {
        batches
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[test]
    fn arrow_streams_become_one() {
        let (a, b) = (ipc_stream(0..3), ipc_stream(3..5));
        // Whole streams per fetch, and the same bytes cut anywhere.
        let whole = run(DOC, "ArrowStream", &[&a, &b]).unwrap();
        let mut both = a.clone();
        both.extend_from_slice(&b);
        let (x, y) = both.split_at(a.len() + 5);
        let cut = run(DOC, "ArrowStream", &[&x[..7], &x[7..], y]).unwrap();
        assert_eq!(whole, cut);
        let mut cursor = std::io::Cursor::new(whole.clone());
        let reader = arrow::ipc::reader::StreamReader::try_new(&mut cursor, None).unwrap();
        assert_eq!(values(reader.map(Result::unwrap)), [0, 1, 2, 3, 4]);
        assert_eq!(
            cursor.position() as usize,
            whole.len(),
            "one end-of-stream marker, last"
        );
        let truncated = run(DOC, "ArrowStream", &[&a[..a.len() - 3]]).unwrap_err();
        assert_eq!(truncated.name, "LOAMS_DOCUMENT");
    }

    #[test]
    fn parquet_files_become_one() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let files = [
            parquet_file_of(0..3),
            parquet_file_of(3..4),
            parquet_file_of(4..9),
        ];
        let spliced = run(DOC, "Parquet", &[&files[0], &files[1], &files[2]]).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(spliced))
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            values(reader.map(Result::unwrap)),
            (0..9).collect::<Vec<_>>()
        );
        let torn = run(DOC, "Parquet", &[&files[0][..files[0].len() - 1]]).unwrap_err();
        assert_eq!(torn.name, "LOAMS_DOCUMENT");
    }
}
