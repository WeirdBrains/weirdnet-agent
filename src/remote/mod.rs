//! Remote inference and remote tasks between one user's own nodes.
//!
//! Two protocols over any ordered, reliable message channel (Weirdnet streams in the apps):
//!
//! - [`infer`]: offload one model turn to a stronger node. The asking side sends the request,
//!   tokens stream back, a cancel travels the other way, and [`infer::RemoteModel`] falls back
//!   to the next node or a local backend when a node cannot take the turn.
//! - [`task`]: run a whole agent task on another node (Node companions). Agent events stream
//!   back, approval cards are answered on the asking device, and cancel travels the other way.
//!
//! Transport and identity belong to the app: a [`Dialer`] opens a [`Channel`] to a node, and the
//! serving side must check that the caller is one of the user's linked devices before it calls
//! [`infer::serve`] or [`task::serve`]. Messages are JSON; [`codec`] frames them on byte streams.

pub mod codec;
pub mod infer;
mod memory;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub use memory::{MemoryDialer, memory_pair};

/// Why a remote call did not complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum RemoteError {
    #[error("node unreachable: {message}")]
    Unreachable { message: String },
    #[error("node busy")]
    Busy,
    #[error("node refused: {message}")]
    Refused { message: String },
    #[error("model unavailable on the node: {message}")]
    Unavailable { message: String },
    #[error("remote run failed: {message}")]
    Failed { message: String },
    #[error("protocol error: {message}")]
    Protocol { message: String },
    #[error("connection closed")]
    Closed,
    #[error("timed out")]
    Timeout,
}

impl RemoteError {
    pub fn protocol(message: impl Into<String>) -> Self {
        RemoteError::Protocol { message: message.into() }
    }
}

/// The sending half of a channel: one message per call.
#[async_trait]
pub trait FrameTx: Send {
    async fn send(&mut self, message: Vec<u8>) -> Result<(), RemoteError>;
}

/// The receiving half of a channel. `recv` must be cancel-safe: it is raced against other
/// futures, so a dropped call must not lose a message (back it with a reader task and a queue).
/// `Ok(None)` means the peer closed its side.
#[async_trait]
pub trait FrameRx: Send {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, RemoteError>;
}

/// A two-way message channel to one node.
pub struct Channel {
    pub tx: Box<dyn FrameTx>,
    pub rx: Box<dyn FrameRx>,
}

impl Channel {
    pub fn new(tx: Box<dyn FrameTx>, rx: Box<dyn FrameRx>) -> Self {
        Self { tx, rx }
    }

    pub async fn send<T: Serialize + Sync>(&mut self, msg: &T) -> Result<(), RemoteError> {
        send_json(self.tx.as_mut(), msg).await
    }

    pub async fn recv<T: DeserializeOwned>(&mut self) -> Result<Option<T>, RemoteError> {
        recv_json(self.rx.as_mut()).await
    }
}

/// Opens channels to the user's nodes. The app implements it over its network and checks the
/// far end is a linked device; tests use [`MemoryDialer`].
#[async_trait]
pub trait Dialer: Send + Sync {
    async fn open(&self, node: &str, protocol: &str) -> Result<Channel, RemoteError>;
}

pub(crate) async fn send_json<T: Serialize + Sync + ?Sized>(tx: &mut dyn FrameTx, msg: &T) -> Result<(), RemoteError> {
    let bytes = serde_json::to_vec(msg).map_err(|e| RemoteError::protocol(e.to_string()))?;
    tx.send(bytes).await
}

pub(crate) async fn recv_json<T: DeserializeOwned>(rx: &mut dyn FrameRx) -> Result<Option<T>, RemoteError> {
    match rx.recv().await? {
        None => Ok(None),
        Some(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|e| RemoteError::protocol(e.to_string())),
    }
}

/// A request id unique enough for one channel (time plus a process counter).
pub(crate) fn request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or_default();
    format!("{nanos:x}-{}", SEQ.fetch_add(1, Ordering::Relaxed))
}
