//! Remote inference and remote tasks between one user's own nodes.
//!
//! Protocols over any ordered, reliable message channel (Weirdnet streams in the apps):
//!
//! - [`infer`]: offload one model turn to a stronger node. `/weirdnet-agent/infer/1` (JSON) is
//!   the original protocol and is still served; [`infer::v2`] (`/weirdnet-agent/infer/2`, CBOR)
//!   adds priorities and a real queue, jobs with attempts and resume from an assistant prefix,
//!   offsets on every text frame, reasoning and tool-call frames, usage and metadata, raw mode
//!   and embeddings.
//! - [`catalog`]: `/weirdnet-agent/catalog/1`, the models an inference node offers, queried once
//!   or subscribed to.
//! - [`pool`]: the asking side over both. It tries inference nodes in order, fails over before
//!   and (opt-in) during an answer, resumes from what it has already shown, cancels a stalled
//!   stream, and runs jobs. [`infer::RemoteModel`] is a thin [`ModelBackend`](crate::ModelBackend)
//!   over it.
//! - [`task`]: run a whole agent task on another node (Node companions). Agent events stream
//!   back, approval cards are answered on the asking device, and cancel travels the other way.
//!
//! Transport and identity belong to the app: a [`Dialer`] opens a [`Channel`] to a node, and the
//! serving side passes the transport-authenticated peer as a [`Caller`] to the serve functions,
//! which ask the host to authorize it before anything else. Nothing about the caller is taken
//! from a message body except the certificate it presents, which the host checks against the
//! peer. The `pool` and `serve` cargo features add the Weirdnet (`weirdnet-net`,
//! `weirdnet-sync`) halves; this module has no networking of its own. [`codec`] frames messages
//! on byte streams.

pub mod catalog;
pub mod codec;
pub mod infer;
mod memory;
pub mod pool;
pub mod task;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub use memory::{MemoryDialer, memory_pair};

/// Why a remote call did not complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[non_exhaustive]
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
    /// The node does not serve this protocol (an older or different app).
    #[error("not supported by the node: {message}")]
    Unsupported { message: String },
    /// The request was malformed (for example raw mode with a `context_ref`).
    #[error("bad request: {message}")]
    BadRequest { message: String },
    /// Cancelled by the asker.
    #[error("cancelled")]
    Cancelled,
    /// No frame arrived within the stall timeout; the asker cancelled the stream.
    #[error("stalled: no frame for {ms} ms")]
    Stalled { ms: u64 },
}

impl RemoteError {
    pub fn protocol(message: impl Into<String>) -> Self {
        RemoteError::Protocol { message: message.into() }
    }

    pub fn refused(message: impl Into<String>) -> Self {
        RemoteError::Refused { message: message.into() }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        RemoteError::Unavailable { message: message.into() }
    }

    /// The request itself can't be served (`BadRequest`) or the asker gave up (`Cancelled`):
    /// another node would answer the same, so a pool returns it at once instead of failing over.
    pub fn is_request_error(&self) -> bool {
        matches!(self, RemoteError::BadRequest { .. } | RemoteError::Cancelled)
    }

    /// A node's own answer about this request (it does not serve it, refuses the caller, lacks the
    /// model or is busy), as opposed to a transport or engine failure. When every node fails, a
    /// pool reports the last such answer before any transport error, so callers see why.
    pub fn is_node_answer(&self) -> bool {
        matches!(self, RemoteError::Unsupported { .. } | RemoteError::Refused { .. } | RemoteError::Unavailable { .. } | RemoteError::Busy)
            || self.is_request_error()
    }

    /// Infer/1 peers (0.3 and older) know only the original kinds; newer ones are sent there as
    /// `failed`.
    pub(crate) fn for_v1(self) -> Self {
        match self {
            e @ (RemoteError::Unsupported { .. } | RemoteError::BadRequest { .. } | RemoteError::Cancelled | RemoteError::Stalled { .. }) => {
                RemoteError::Failed { message: e.to_string() }
            }
            e => e,
        }
    }
}

/// Who is calling a served protocol. `peer` is the transport-authenticated peer id (Noise on
/// Weirdnet), never one the caller sent. `cert_bytes` is the certificate the caller presented
/// (for Weirdnet, a CBOR `weirdnet_net::user::NodeCert`), if any: hosts check it against `peer`
/// (for example with `weirdnet_sync::Roster::check_caller`, see the `serve` feature) and must
/// not trust it on its own.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Caller {
    pub peer: String,
    pub cert_bytes: Option<Vec<u8>>,
}

impl Caller {
    pub fn new(peer: impl Into<String>) -> Self {
        Self { peer: peer.into(), cert_bytes: None }
    }

    pub fn with_cert(mut self, cert_bytes: Vec<u8>) -> Self {
        self.cert_bytes = Some(cert_bytes);
        self
    }

    /// Takes the certificate a request presented when the transport gave none.
    pub(crate) fn presenting(mut self, cert: Option<Vec<u8>>) -> Self {
        if self.cert_bytes.is_none() {
            self.cert_bytes = cert;
        }
        self
    }
}

/// Decides whether a caller may use a served protocol. Hosts implement it (for Weirdnet with
/// `serve::RosterAuth`); a closure works too.
pub trait Authorize: Send + Sync {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError>;
}

impl<F> Authorize for F
where
    F: Fn(&Caller) -> Result<(), RemoteError> + Send + Sync,
{
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        self(caller)
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

    /// Sends one JSON message (infer/1, task/1).
    pub async fn send<T: Serialize + Sync>(&mut self, msg: &T) -> Result<(), RemoteError> {
        send_json(self.tx.as_mut(), msg).await
    }

    /// Receives one JSON message (infer/1, task/1).
    pub async fn recv<T: DeserializeOwned>(&mut self) -> Result<Option<T>, RemoteError> {
        recv_json(self.rx.as_mut()).await
    }

    /// Sends one CBOR message (infer/2, catalog/1).
    pub async fn send_cbor<T: Serialize + Sync>(&mut self, msg: &T) -> Result<(), RemoteError> {
        let bytes = to_cbor(msg)?;
        self.tx.send(bytes).await
    }

    /// Receives one CBOR message (infer/2, catalog/1).
    pub async fn recv_cbor<T: DeserializeOwned>(&mut self) -> Result<Option<T>, RemoteError> {
        match self.rx.recv().await? {
            None => Ok(None),
            Some(bytes) => from_cbor(&bytes).map(Some),
        }
    }
}

/// Opens channels to the user's nodes. The app implements it over its network and checks the
/// far end is a linked device; tests use [`MemoryDialer`]. A node that does not serve
/// `protocol` should answer [`RemoteError::Unsupported`], so the pool can fall back to infer/1.
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

/// CBOR encoding used by infer/2 and catalog/1.
pub fn to_cbor<T: Serialize>(msg: &T) -> Result<Vec<u8>, RemoteError> {
    cbor4ii::serde::to_vec(Vec::new(), msg).map_err(|e| RemoteError::protocol(e.to_string()))
}

/// CBOR decoding used by infer/2 and catalog/1.
pub fn from_cbor<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, RemoteError> {
    cbor4ii::serde::from_slice(bytes).map_err(|e| RemoteError::protocol(e.to_string()))
}

/// A request id unique enough for one channel (time plus a process counter).
pub(crate) fn request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or_default();
    format!("{nanos:x}-{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

/// Serde helpers that write `Vec<u8>` as a CBOR byte string (not an array of numbers).
pub(crate) mod bytes {
    use serde::de::{self, SeqAccess, Visitor};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        d.deserialize_byte_buf(BytesVisitor)
    }

    pub(super) struct BytesVisitor;

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a byte string")
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
            Ok(v.to_vec())
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
            Ok(v)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
            let mut out = Vec::new();
            while let Some(b) = seq.next_element::<u8>()? {
                out.push(b);
            }
            Ok(out)
        }
    }

    /// `Option<Vec<u8>>` as an optional byte string.
    pub mod opt {
        use serde::{Deserialize, Deserializer, Serializer};

        pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
            match v {
                Some(b) => s.serialize_some(&Wrap(b)),
                None => s.serialize_none(),
            }
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
            Ok(Option::<Owned>::deserialize(d)?.map(|o| o.0))
        }

        struct Wrap<'a>(&'a [u8]);

        impl serde::Serialize for Wrap<'_> {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_bytes(self.0)
            }
        }

        struct Owned(Vec<u8>);

        impl<'de> Deserialize<'de> for Owned {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                d.deserialize_byte_buf(super::BytesVisitor).map(Owned)
            }
        }
    }
}
