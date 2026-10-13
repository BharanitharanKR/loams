//! NDJSON over `arrow-json` 59: one JSON object per line, decoded by
//! `arrow-json`'s tape decoder straight into the columns of a declared
//! schema (no per-row `serde_json::Value`), and written back with nulls
//! omitted.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::csv::DEFAULT_BATCH_ROWS;
use super::{Codec, CodecError, CodecStats, offset_schema, relabel};

#[derive(Debug)]
pub struct NdjsonCodec {
    schema: SchemaRef,
    batch_rows: usize,
    stats: CodecStats,
}

impl NdjsonCodec {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batch_rows: DEFAULT_BATCH_ROWS,
            stats: CodecStats::default(),
        }
    }

    pub fn with_batch_rows(mut self, rows: usize) -> Self {
        self.batch_rows = rows.max(1);
        self
    }
}

impl Codec for NdjsonCodec {
    fn content_type(&self) -> &'static str {
        "application/x-ndjson"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode(&self, mut bytes: &[u8]) -> Result<Vec<RecordBatch>, CodecError> {
        let err = |e: arrow_schema::ArrowError| CodecError::Decode(e.to_string());
        let mut decoder = arrow_json::ReaderBuilder::new(offset_schema(&self.schema))
            .with_batch_size(self.batch_rows)
            .build_decoder()
            .map_err(err)?;
        let mut out = Vec::new();
        while !bytes.is_empty() {
            let read = decoder.decode(bytes).map_err(err)?;
            bytes = &bytes[read..];
            // The decoder stops at a full batch: flush it and go on.
            if read == 0 || decoder.len() >= self.batch_rows {
                if let Some(b) = decoder.flush().map_err(err)? {
                    out.push(b);
                } else if read == 0 {
                    return Err(CodecError::Decode(
                        "the decoder made no progress".to_owned(),
                    ));
                }
            }
        }
        if decoder.has_partial_record() {
            return Err(CodecError::Decode(
                "a truncated JSON object at the end".to_owned(),
            ));
        }
        if let Some(b) = decoder.flush().map_err(err)? {
            out.push(b);
        }
        out.into_iter().map(|b| relabel(b, &self.schema)).collect()
    }

    fn encode(&self, batches: &[RecordBatch]) -> Result<Vec<u8>, CodecError> {
        let wire = offset_schema(&self.schema);
        let mut out = Vec::new();
        {
            let mut w = arrow_json::LineDelimitedWriter::new(&mut out);
            for b in batches {
                if b.schema().fields() != self.schema.fields() {
                    return Err(CodecError::Encode(
                        "the batch's schema is not the codec's".to_owned(),
                    ));
                }
                w.write(&relabel(b.clone(), &wire)?)
                    .map_err(|e| CodecError::Encode(e.to_string()))?;
            }
            w.finish().map_err(|e| CodecError::Encode(e.to_string()))?;
        }
        Ok(out)
    }

    fn stats(&self) -> &CodecStats {
        &self.stats
    }
}
