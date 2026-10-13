//! [`JsLoader`]: the function runtime behind `loams-live`'s deployments
//! (LV1 plan Task 6). A deploy validates its bundle by loading it, in this
//! process or in sandboxed workers as [`JsConfig::isolation`] says, and the
//! loaded [`Bundle`] then serves the deployment's functions. A bundle on
//! isolated workers hands its degraded signal to the deployment, which
//! records it, and its functions' metadata to the deployment's record (LV1
//! plan Task 10).

use std::sync::Arc;

use futures::future::BoxFuture;
use loams_live::deploy::{BundleLoader, FunctionInfo, LoadedBundle};
use loams_live::{Function, Isolation, LiveError};
use tokio::sync::watch;

use crate::{Bundle, JsConfig, WorkerCommand, WorkerPool};

/// Loads deployments' bundles with one [`JsConfig`].
#[derive(Debug, Clone)]
pub struct JsLoader {
    config: JsConfig,
    /// The worker pool, with [`Isolation::Isolated`].
    workers: Option<WorkerPool>,
}

impl JsLoader {
    /// Bundles load with `config`; isolated ones in workers of this binary
    /// run as `<binary> live-worker` ([`WorkerCommand::current_exe`]),
    /// which the `loams` binary serves.
    pub fn new(config: JsConfig) -> Result<Self, LiveError> {
        let workers = match config.isolation {
            Isolation::InProcess => None,
            Isolation::Isolated => Some(WorkerPool::new(WorkerCommand::current_exe()?)),
        };
        Ok(JsLoader { config, workers })
    }

    /// Bundles load with `config`; isolated ones on `workers`.
    pub fn with_workers(config: JsConfig, workers: WorkerPool) -> Self {
        JsLoader {
            config,
            workers: Some(workers),
        }
    }

    /// The configuration bundles load with.
    pub fn config(&self) -> &JsConfig {
        &self.config
    }
}

impl BundleLoader for JsLoader {
    fn load<'a>(
        &'a self,
        app: &'a str,
        bundle: &'a [u8],
    ) -> BoxFuture<'a, Result<Arc<dyn LoadedBundle>, LiveError>> {
        Box::pin(async move {
            let loaded = match (&self.workers, self.config.isolation) {
                (Some(workers), Isolation::Isolated) => workers
                    .spawn(app, bundle, self.config.clone())
                    .await
                    .map(Bundle::from)?,
                // Never run an isolated deployment's code in this process.
                (None, Isolation::Isolated) => {
                    return Err(LiveError::Internal(
                        "an isolated loader has no worker pool".into(),
                    ));
                }
                (_, Isolation::InProcess) => {
                    let source = std::str::from_utf8(bundle).map_err(|_| {
                        LiveError::InvalidArgument("the bundle is not UTF-8".into())
                    })?;
                    Bundle::load(source, self.config.clone()).await?
                }
            };
            Ok(Arc::new(loaded) as Arc<dyn LoadedBundle>)
        })
    }
}

impl LoadedBundle for Bundle {
    fn function(&self, path: &str) -> Option<Arc<dyn Function>> {
        Bundle::function(self, path)
    }

    fn degraded(&self) -> Option<watch::Receiver<bool>> {
        self.workers().map(|w| w.degraded_signal())
    }

    fn functions(&self) -> Vec<FunctionInfo> {
        Bundle::functions(self)
            .into_iter()
            .map(|m| FunctionInfo {
                path: m.path,
                kind: m.kind,
                visibility: m.visibility,
                args: m.args,
            })
            .collect()
    }
}
