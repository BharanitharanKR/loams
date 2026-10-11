//! The daemon's run loop and RPC wiring (design §50 §5.4, §6 and §7).
//!
//! The caller holds `agentd.lock`. In order, [`serve`]:
//! 1. rotates the token;
//! 2. binds the RPC listener on `127.0.0.1:0` (loopback only, an ephemeral port);
//! 3. assembles the sessions engine on `store/`;
//! 4. serves `Hello` and the engine's methods on the listener;
//! 5. publishes `agentd.json`;
//! 6. runs until SIGTERM, Ctrl-C or `StopEngine`, then closes the listener and
//!    every connection (awaiting their requests), stops the engine and removes
//!    `agentd.json`.

use std::sync::Arc;

use async_trait::async_trait;
use loams_agentd_proto::daemon::{DaemonMode, Discovery};
use loams_agentd_proto::rpc::AGENTD_PROTOCOL;
use loams_agentd_rpc::{RpcError, RpcReply, RpcService, methods};
use loams_agentd_sessions::{Engine, EngineConfig, EngineProfile, HarnessId};

use crate::config::Config;
use crate::discovery;
use crate::hello::{DaemonInfo, HelloService, VERSION};
use crate::paths::Paths;

/// What [`serve`] runs with.
#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub paths: Paths,
    pub config: Config,
    pub mode: DaemonMode,
    /// The harness for chats without a config row.
    pub default_harness: HarnessId,
}

/// Run the daemon until it is asked to stop. See the module docs.
pub async fn serve(options: ServeOptions) -> anyhow::Result<()> {
    let ServeOptions {
        paths,
        config,
        mode,
        default_harness,
    } = options;
    // Held for the daemon's life; the RPC checks it from DD1 Task 7.
    let _token = crate::token::rotate(&paths)?;

    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();

    let store = paths.store();
    std::fs::create_dir_all(&store)?;
    let engine_config = EngineConfig {
        data_dir: store.clone(),
        ipc_port: port,
        default_harness,
        terminal_shell: None,
        tool_outputs: config.tool_outputs.limits(),
    };
    let runtime = Engine::assemble_runtime(&engine_config, EngineProfile::local(&store)?).await?;
    // The MCP shim injected into harness runs dials this port.
    runtime.core().sessions.set_ipc_port(port);

    let info = DaemonInfo {
        mode,
        pid: std::process::id(),
        started_at_ms: now_ms(),
    };
    let (stop_tx, mut stop_rx) = tokio::sync::mpsc::unbounded_channel();
    let lifecycle = Lifecycle {
        inner: runtime.core().rpc_service(),
        stop_tx,
    };
    let service = Arc::new(HelloService::new(Arc::new(lifecycle), info));
    let (server_stop, server_stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(loams_agentd_rpc::serve_ws_until(listener, service, async {
        let _ = server_stopped.await;
    }));
    // Stops accepting, ends every connection and awaits their requests, so
    // nothing calls into the engine while it shuts down.
    let stop_server = || async {
        let _ = server_stop.send(());
        if let Err(error) = server.await {
            tracing::warn!(%error, "the RPC server task failed");
        }
    };

    let published = discovery::publish(
        &paths,
        &Discovery {
            protocol: AGENTD_PROTOCOL,
            version: VERSION.to_string(),
            pid: info.pid,
            port,
            started_at_ms: info.started_at_ms,
            mode,
        },
    );
    if let Err(error) = published {
        stop_server().await;
        runtime.shutdown().await;
        return Err(anyhow::Error::new(error).context("publish agentd.json"));
    }
    tracing::info!(port, ?mode, pid = info.pid, "loams-agentd ready");

    tokio::select! {
        signal = shutdown_signal() => match signal {
            Ok(()) => tracing::info!("stop signal received"),
            Err(error) => tracing::error!(%error, "cannot wait for stop signals; stopping"),
        },
        Some(()) = stop_rx.recv() => tracing::info!("stop requested over the RPC"),
    }
    tracing::info!("shutting down");
    stop_server().await;
    runtime.shutdown().await;
    if let Err(error) = discovery::remove(&paths) {
        tracing::warn!(%error, "agentd.json was not removed");
    }
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

/// SIGTERM or Ctrl-C. systemd and launchd stop with SIGTERM; without catching
/// it the daemon would die mid-write and every stop would take the
/// crash-recovery path instead of the clean one.
async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = sigterm.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

/// `StopEngine` stops the daemon; every other method goes to the engine.
/// (The fork's headless `StopEngine`; Shared contracts rename it `Shutdown`,
/// DD1 Task 9.)
struct Lifecycle {
    inner: Arc<dyn RpcService>,
    stop_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

#[async_trait]
impl RpcService for Lifecycle {
    async fn handle(&self, method: &str, params: serde_json::Value) -> Result<RpcReply, RpcError> {
        if method != methods::STOP_ENGINE {
            return self.inner.handle(method, params).await;
        }
        let stop_tx = self.stop_tx.clone();
        // Let the reply reach the client before the server is aborted.
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let _ = stop_tx.send(());
        });
        RpcReply::value(&serde_json::json!({ "ok": true }))
    }
}
