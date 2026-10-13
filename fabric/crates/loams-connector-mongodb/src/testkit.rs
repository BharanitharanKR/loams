//! A loopback fake `mongod` for the unit tests: it answers each `OP_MSG`
//! command with whatever the test's handler returns. Tests that need a real
//! server run against the compose estate instead (tests/).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use crate::bson::Document;
use crate::wire::{Connection, DEFAULT_MAX_MESSAGE, decode, encode, read_frame};

type Handler = Arc<dyn Fn(&Document) -> Document + Send + Sync>;

pub struct FakeServer {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeServer {
    /// Serve `handler` on 127.0.0.1 until dropped. The handler runs on a
    /// blocking thread, so it may sleep to simulate a slow server.
    pub async fn start(handler: impl Fn(&Document) -> Document + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handler: Handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                let h = handler.clone();
                tokio::spawn(serve(sock, h));
            }
        });
        Self { addr, task }
    }

    pub fn host(&self) -> String {
        format!("127.0.0.1:{}", self.addr.port())
    }

    /// A raw connection to the fake (no handshake).
    pub async fn connect(&self, timeout: Duration) -> Connection {
        let s = TcpStream::connect(self.addr).await.expect("connect");
        Connection::new(Box::new(s), self.host(), timeout)
    }
}

async fn serve(mut sock: TcpStream, h: Handler) {
    loop {
        let Ok(frame) = read_frame(&mut sock, DEFAULT_MAX_MESSAGE).await else {
            return;
        };
        let Ok(msg) = decode(&frame) else { return };
        let mut cmd = msg.body.clone();
        // Fold document sequences into the body, as the server does.
        for s in msg.sequences {
            cmd.insert(
                s.identifier,
                s.documents
                    .into_iter()
                    .map(crate::bson::Bson::Document)
                    .collect::<Vec<_>>(),
            );
        }
        let hh = h.clone();
        let Ok(reply) = tokio::task::spawn_blocking(move || hh(&cmd)).await else {
            return;
        };
        let Ok(out) = encode(1_000_000 + msg.request_id, msg.request_id, &reply, &[]) else {
            return;
        };
        if sock.write_all(&out).await.is_err() {
            return;
        }
    }
}
