//! In-process channels: tests, and apps that run both ends in one process.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::{Channel, Dialer, FrameRx, FrameTx, RemoteError};

struct MemTx(mpsc::UnboundedSender<Vec<u8>>);
struct MemRx(mpsc::UnboundedReceiver<Vec<u8>>);

#[async_trait]
impl FrameTx for MemTx {
    async fn send(&mut self, message: Vec<u8>) -> Result<(), RemoteError> {
        self.0.send(message).map_err(|_| RemoteError::Closed)
    }
}

#[async_trait]
impl FrameRx for MemRx {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, RemoteError> {
        Ok(self.0.recv().await)
    }
}

/// Two connected channel ends.
pub fn memory_pair() -> (Channel, Channel) {
    let (a_tx, b_rx) = mpsc::unbounded_channel();
    let (b_tx, a_rx) = mpsc::unbounded_channel();
    (Channel::new(Box::new(MemTx(a_tx)), Box::new(MemRx(a_rx))), Channel::new(Box::new(MemTx(b_tx)), Box::new(MemRx(b_rx))))
}

type Handler = Arc<dyn Fn(Channel, String) + Send + Sync>;

/// A [`Dialer`] over [`memory_pair`]: each routed node gets a handler that receives the serving
/// end and the protocol (it usually spawns [`super::infer::serve`] or [`super::task::serve`]).
/// Nodes without a route are unreachable.
#[derive(Default, Clone)]
pub struct MemoryDialer {
    routes: Arc<Mutex<HashMap<String, Handler>>>,
}

impl MemoryDialer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn route(&self, node: &str, handler: impl Fn(Channel, String) + Send + Sync + 'static) {
        self.routes.lock().unwrap().insert(node.to_string(), Arc::new(handler));
    }

    pub fn unroute(&self, node: &str) {
        self.routes.lock().unwrap().remove(node);
    }
}

#[async_trait]
impl Dialer for MemoryDialer {
    async fn open(&self, node: &str, protocol: &str) -> Result<Channel, RemoteError> {
        let handler = self.routes.lock().unwrap().get(node).cloned();
        let Some(handler) = handler else {
            return Err(RemoteError::Unreachable { message: format!("{node} is not reachable") });
        };
        let (ours, theirs) = memory_pair();
        handler(theirs, protocol.to_string());
        Ok(ours)
    }
}
