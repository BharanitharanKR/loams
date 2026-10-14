//! Untrusted JSON as the typed params of any ndjson method: decoding never
//! panics, and what decodes re-encodes the same.
//! See loams_agentd_connect::fuzz::ndjson_params.
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| loams_agentd_connect::fuzz::ndjson_params(data));
