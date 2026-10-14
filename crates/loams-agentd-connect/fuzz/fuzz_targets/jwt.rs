//! Untrusted bearer tokens for the remote listener against a fixed key set:
//! never panics, and only the enrolled subject's signed token is accepted.
//! See loams_agentd_connect::fuzz::jwt.
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| loams_agentd_connect::fuzz::jwt(data));
