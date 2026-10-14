//! The daemon's checks on untrusted paths, clone URLs, chat ids, remotes and
//! review text: nothing accepted escapes its jail.
//! See loams_agentd_sessions::fuzz::path_jail.
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| loams_agentd_sessions::fuzz::path_jail(data));
