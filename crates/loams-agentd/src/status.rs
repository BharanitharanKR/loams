//! `loams-agentd status`: the daemon directory and whether a daemon runs
//! there, confirmed with `Hello` (design §50 §5.4).

use std::path::Path;

use loams_agentd::paths::Paths;
use loams_agentd::{discovery, hello, lock};

pub fn status(config: &Path) -> anyhow::Result<()> {
    let paths = Paths::of_config(config)?;
    println!("Directory: {}", paths.dir().display());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let Some(found) = discovery::read(&paths) else {
        match lock::holder(&paths) {
            Some(pid) => println!("Daemon:    starting or not answering (pid {pid})"),
            None => println!("Daemon:    not running"),
        }
        return Ok(());
    };
    match runtime.block_on(hello::probe(found.port)) {
        Ok(reply) => println!(
            "Daemon:    running (pid {}, version {}, protocol {}, mode {:?}, port {})",
            reply.pid, reply.version, reply.protocol, reply.mode, found.port
        ),
        Err(error) => println!(
            "Daemon:    pid {} holds the lock but port {} did not answer Hello: {error}",
            found.pid, found.port
        ),
    }
    Ok(())
}
