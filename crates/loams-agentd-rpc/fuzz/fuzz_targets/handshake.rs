//! The WebSocket upgrade request head from untrusted bytes: never panics; what
//! passes names the loopback host. See loams_agentd_rpc::fuzz::handshake.
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| loams_agentd_rpc::fuzz::handshake(data));
