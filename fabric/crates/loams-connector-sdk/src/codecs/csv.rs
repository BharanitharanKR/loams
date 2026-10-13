//! CSV over `arrow-csv` 59: bytes to `RecordBatch`es of a declared schema,
//! and back. Nulls are empty fields; a header row is read and written by
//! default.

use std::io::Cursor;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::{Codec, CodecError, CodecStats, offset_schema, relabel};

/// The default rows per decoded batch (D356's `max_batch_rows`).
pub const DEFAULT_BATCH_ROWS: usize = 65_536;

#[derive(Debug)]
pub struct CsvCodec {
    schema: SchemaRef,
    header: bool,
    delimiter: u8,
    batch_rows: usize,
    stats: CodecStats,
}

impl CsvCodec {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            header: true,
            delimiter: b',',
            batch_rows: DEFAULT_BATCH_ROWS,
            stats: CodecStats::default(),
        }
    }

    pub fn with_header(mut self, header: bool) -> Self {
        self.header = header;
        self
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    pub fn with_batch_rows(mut self, rows: usize) -> Self {
        self.batch_rows = rows.max(1);
        self
    }
}

impl Codec for CsvCodec {
    fn content_type(&self) -> &'static str {
        "text/csv"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode(&self, bytes: &[u8]) -> Result<Vec<RecordBatch>, CodecError> {
        let reader = arrow_csv::ReaderBuilder::new(offset_schema(&self.schema))
            .with_header(self.header)
            .with_delimiter(self.delimiter)
            .with_batch_size(self.batch_rows)
            .build(Cursor::new(bytes))
            .map_err(|e| CodecError::Decode(e.to_string()))?;
        reader
            .map(|b| {
                b.map_err(|e| CodecError::Decode(e.to_string()))
                    .and_then(|b| relabel(b, &self.schema))
            })
            .collect()
    }

    fn encode(&self, batches: &[RecordBatch]) -> Result<Vec<u8>, CodecError> {
        let wire = offset_schema(&self.schema);
        let mut out = Vec::new();
        {
            let mut w = arrow_csv::WriterBuilder::new()
                .with_header(self.header)
                .with_delimiter(self.delimiter)
                .build(&mut out);
            for b in batches {
                if b.schema().fields() != self.schema.fields() {
                    return Err(CodecError::Encode(
                        "the batch's schema is not the codec's".to_owned(),
                    ));
                }
                w.write(&relabel(b.clone(), &wire)?)
                    .map_err(|e| CodecError::Encode(e.to_string()))?;
            }
        }
        Ok(out)
    }

    fn stats(&self) -> &CodecStats {
        &self.stats
    }
}
