//! A call to any loams.agentd.v1 method with an untrusted body, in any codec,
//! through the real router and handlers over a recording fake engine: never
//! panics or hangs, and a handler only calls its own twin with params that
//! parse. See loams_agentd_connect::fuzz::connect_decode.
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    loams_agentd_connect::fuzz::connect_decode(data);
});
