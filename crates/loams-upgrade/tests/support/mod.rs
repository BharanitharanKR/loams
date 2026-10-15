//! A recording container runtime and stack directories for the tests.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Mutex;

use loams_upgrade::errors::Result;
use loams_upgrade::runtime::{Output, Runtime};
use loams_upgrade::stackdir::StackDir;
use tempfile::TempDir;

type Responder = Box<dyn Fn(&[String]) -> Output + Send + Sync>;

/// Records every call; answers with `respond`.
pub struct Fake {
    pub calls: Mutex<Vec<Vec<String>>>,
    respond: Responder,
}

impl Fake {
    pub fn new(respond: impl Fn(&[String]) -> Output + Send + Sync + 'static) -> Self {
        Fake {
            calls: Mutex::new(Vec::new()),
            respond: Box::new(respond),
        }
    }

    pub fn calls(&self) -> Vec<Vec<String>> {
        self.calls.lock().expect("calls").clone()
    }
}

impl Runtime for Fake {
    fn compose(&self, _dir: &Path, args: &[String]) -> Result<Output> {
        let mut call = vec!["compose".to_string()];
        call.extend(args.iter().cloned());
        let out = (self.respond)(&call);
        self.calls.lock().expect("calls").push(call);
        Ok(out)
    }

    fn engine(&self, args: &[String]) -> Result<Output> {
        let out = (self.respond)(args);
        self.calls.lock().expect("calls").push(args.to_vec());
        Ok(out)
    }
}

pub fn ok(stdout: &str) -> Output {
    Output {
        status: 0,
        stdout: stdout.to_string(),
        stderr: String::new(),
    }
}

pub fn failed(status: i32, stderr: &str) -> Output {
    Output {
        status,
        stdout: String::new(),
        stderr: stderr.to_string(),
    }
}

/// A preview.2-shaped stack directory: `compose.yaml` (project `loams`) and
/// a `.env` with a secret-looking value.
pub fn stack_dir(env: &str) -> TempDir {
    let dir = TempDir::new().expect("dir");
    std::fs::write(
        dir.path().join("compose.yaml"),
        "name: loams\nservices:\n  loams:\n    image: ghcr.io/ostrium-labs/loams:${LOAMS_VERSION:-0.1.0-preview.2}\n",
    )
    .expect("compose");
    std::fs::write(dir.path().join(".env"), env).expect("env");
    dir
}

pub fn stack(dir: &TempDir) -> StackDir {
    StackDir::discover(Some(dir.path()), None, dir.path()).expect("a stack")
}
