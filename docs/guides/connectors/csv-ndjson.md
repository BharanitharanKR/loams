# CSV and NDJSON

The CSV and NDJSON codecs decode text straight into Arrow record batches of a declared schema, and encode batches back to text. A format is a codec that other connectors use (§33 Appendix A.6), such as the objects family reading `s3://…/export.csv`. Built in CN2 Task 5 as `loams_connector_sdk::codecs::{csv::CsvCodec, ndjson::NdjsonCodec}`, on `arrow-csv` and `arrow-json` 59.

## The bulk path

`Codec::decode_bulk` turns bytes into one `EventBatch::Arrow` per batch (65,536 rows by default). Each batch carries one envelope with `datacontenttype: application/vnd.apache.arrow.stream`, and the caller derives the envelope's id from the batch's row range with `ids::rows`.

The bulk path never decodes a row on its own:

- CSV goes through arrow-csv's column parser.
- NDJSON goes through arrow-json's tape decoder, with no per-row `serde_json::Value`.

`Codec::decode_rows` is the row path, with one CloudEvent per row and the row as a JSON object. It exists for consumers that need individual events. `CodecStats` counts rows per path.

## Types

| Arrow | CSV | NDJSON |
|---|---|---|
| `Boolean` | `true` / `false` | `true` / `false` |
| integers, `UInt64` | decimal text | numbers |
| `Float32`, `Float64` | decimal or exponent text | numbers |
| `Decimal128(p,s)` | decimal text | a number written as exact decimal text |
| `Utf8` | quoted when it holds the delimiter, a quote or a newline | string |
| `Date32` | `YYYY-MM-DD` | string |
| `Time64(µs)` | `HH:MM:SS.ffffff` | string |
| `Timestamp(µs, "UTC")` | RFC 3339 with `+00:00` | RFC 3339 string |
| `Binary` | — | hex string |
| `List`, `Struct`, `Map` | — | arrays and objects |
| null | an empty field | the key is omitted |

Notes on these types:

- **Nulls and empty strings in CSV.** CSV cannot tell a null `Utf8` from an empty string; both are an empty field, which decodes as null.
- **Timezones.** `UTC` (and `Z`, `Etc/UTC`, `GMT`) timestamps are written with the fixed offset `+00:00`, because Arrow's text formats resolve only offsets without a time-zone database. They decode back to the declared `UTC` column.

Input that does not fit the schema is a decode error, which becomes `Poison` for the record's DLQ. The codecs never coerce such input.

## Settings

`CsvCodec` takes `with_header` (default `true`), `with_delimiter` (default `,`) and `with_batch_rows`. `NdjsonCodec` takes `with_batch_rows`.

## Registry status

The `csv-ndjson` manifest is still the generated `planned` stub on Camel. CN2 Task 2's runtime assignment moves it to `native` (see the CN2 plan's ruling R5.6).
