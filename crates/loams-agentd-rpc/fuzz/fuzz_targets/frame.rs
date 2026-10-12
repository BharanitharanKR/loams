//! ndjson RPC frames from untrusted bytes: never panics; what decodes
//! re-encodes the same. See loams_agentd_rpc::fuzz::frame.
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| loams_agentd_rpc::fuzz::frame(data));
