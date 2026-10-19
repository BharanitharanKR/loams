//! `pgdog-config-sync` (see the library's docs).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use loams_pgdog_sync::{Server, load_persisted, server_tls};

/// The sidecar beside a PgDog replica: pg-control's rendered config in,
/// over mutual TLS, written atomically where PgDog reads it.
#[derive(Parser)]
#[command(name = "pgdog-config-sync")]
struct Args {
    /// Where to listen (mTLS).
    #[arg(long, default_value = "0.0.0.0:7695")]
    listen: SocketAddr,
    /// The directory PgDog reads pgdog.toml and users.toml from.
    #[arg(long)]
    dir: PathBuf,
    /// The persisted copy (the mounted Secret), loaded at start.
    #[arg(long)]
    persisted: Option<PathBuf>,
    /// This server's certificate and key (PEM).
    #[arg(long)]
    tls_cert: PathBuf,
    #[arg(long)]
    tls_key: PathBuf,
    /// The CA a client's certificate must chain to (pg-control's).
    #[arg(long)]
    client_ca: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let tls = server_tls(
        &std::fs::read(&args.tls_cert)?,
        &std::fs::read(&args.tls_key)?,
        &std::fs::read(&args.client_ca)?,
    )?;
    if let Some(persisted) = &args.persisted {
        let loaded = load_persisted(&args.dir, persisted)?;
        tracing::info!(generation = ?loaded, "persisted config loaded");
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(args.listen).await?;
        tracing::info!(listen = %args.listen, dir = %args.dir.display(), "pgdog-config-sync listening");
        Server {
            dir: args.dir,
            tls: Arc::new(tls),
        }
        .serve(listener, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
        Ok::<_, std::io::Error>(())
    })?;
    Ok(())
}
