//! A `ForwardTarget`'s per-worker endpoint is opened only by a launch, and a
//! launch that fails releases it (HS1 Task 9, PR #439 review).

use std::net::SocketAddr;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};

use loams_house::{ForwardSlot, ForwardTarget, Launcher, ProcessLauncher};

#[derive(Debug, Default)]
struct Recorder {
    endpoints: Mutex<Vec<String>>,
    gone: Mutex<Vec<String>>,
}

impl ForwardTarget for Recorder {
    fn accept(&self, _worker: &str, _stream: UnixStream, _slot: ForwardSlot) {}

    fn endpoint(&self, worker: &str) -> std::io::Result<SocketAddr> {
        self.endpoints
            .lock()
            .expect("lock")
            .push(worker.to_string());
        Ok("127.0.0.1:1".parse().expect("addr"))
    }

    fn bound(&self, _worker: &str, _namespace: &str) {}

    fn gone(&self, worker: &str) {
        self.gone.lock().expect("lock").push(worker.to_string());
    }
}

#[test]
fn a_failed_launch_releases_the_endpoint_and_args_opens_none() {
    let target = Arc::new(Recorder::default());
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("forward-target");
    let launcher = ProcessLauncher::new("/nonexistent/loams-house-worker", &root)
        .unsandboxed()
        .with_forward_target(target.clone());

    let args = launcher.args("w0");
    assert!(!args.iter().any(|a| a == "--s3-endpoint"), "{args:?}");
    assert!(
        target.endpoints.lock().expect("lock").is_empty(),
        "args has no side effect"
    );

    assert!(launcher.launch("w1").is_err(), "no such binary");
    assert_eq!(*target.endpoints.lock().expect("lock"), ["w1"]);
    assert_eq!(
        *target.gone.lock().expect("lock"),
        ["w1"],
        "released on failure"
    );
}
