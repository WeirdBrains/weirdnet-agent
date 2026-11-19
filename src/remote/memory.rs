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

#[derive(Clone)]
struct Route {
    /// `None`: every protocol.
    protocols: Option<Vec<String>>,
    handler: Handler,
}

/// A [`Dialer`] over [`memory_pair`]: each routed node gets a handler that receives the serving
/// end and the protocol (it usually spawns [`super::infer::serve`] or [`super::task::serve`]).
/// Nodes without a route are unreachable.
#[derive(Default, Clone)]
pub struct MemoryDialer {
    routes: Arc<Mutex<HashMap<String, Route>>>,
}

impl MemoryDialer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn route(&self, node: &str, handler: impl Fn(Channel, String) + Send + Sync + 'static) {
        self.routes.lock().unwrap().insert(node.to_string(), Route { protocols: None, handler: Arc::new(handler) });
    }

    /// Like [`route`](Self::route), but the node serves only `protocols`; opening another one
    /// fails with [`RemoteError::Unsupported`] (like an older node).
    pub fn route_only(&self, node: &str, protocols: &[&str], handler: impl Fn(Channel, String) + Send + Sync + 'static) {
        let protocols = Some(protocols.iter().map(|p| p.to_string()).collect());
        self.routes.lock().unwrap().insert(node.to_string(), Route { protocols, handler: Arc::new(handler) });
    }

    pub fn unroute(&self, node: &str) {
        self.routes.lock().unwrap().remove(node);
    }
}

#[async_trait]
impl Dialer for MemoryDialer {
    async fn open(&self, node: &str, protocol: &str) -> Result<Channel, RemoteError> {
        let route = self.routes.lock().unwrap().get(node).cloned();
        let Some(route) = route else {
            return Err(RemoteError::Unreachable { message: format!("{node} is not reachable") });
        };
        if route.protocols.as_ref().is_some_and(|p| !p.iter().any(|p| p == protocol)) {
            return Err(RemoteError::Unsupported { message: format!("{node} does not accept {protocol}") });
        }
        let (ours, theirs) = memory_pair();
        (route.handler)(theirs, protocol.to_string());
        Ok(ours)
    }
}
