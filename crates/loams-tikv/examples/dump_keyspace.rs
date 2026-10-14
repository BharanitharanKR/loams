//! Dumps every key of one keyspace (under an empty root) as JSON lines,
//! `{"k":"<hex>","v":"<hex>"}`, in key order, from one snapshot.
//!
//! UP1 Task 0 uses it to capture a preview.2 stack's metastore and Live
//! keyspaces as test fixtures (`crates/loams-migrate/tests/fixtures/preview2/`):
//!
//! ```text
//! cargo run -p loams-tikv --example dump_keyspace -- <pd host:port> <keyspace> > dump.jsonl
//! ```
//!
//! Read-only: it opens a snapshot and scans; it writes nothing.

use std::fmt::Write as _;

use loams_tikv::{Tikv, TikvConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(pd), Some(keyspace)) = (args.next(), args.next()) else {
        eprintln!("usage: dump_keyspace <pd host:port> <keyspace>");
        std::process::exit(2);
    };
    let tikv = Tikv::connect(TikvConfig::new(vec![pd], keyspace)).await?;
    let mut snap = tikv.snapshot(tikv.now().await?).await?;
    let pairs = snap.scan(b"", None, usize::MAX).await?;
    let mut out = String::new();
    for (key, value) in pairs {
        out.clear();
        out.push_str("{\"k\":\"");
        hex(&mut out, &key);
        out.push_str("\",\"v\":\"");
        hex(&mut out, &value);
        out.push_str("\"}");
        println!("{out}");
    }
    Ok(())
}

fn hex(out: &mut String, bytes: &[u8]) {
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
}
