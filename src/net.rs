//! Weirdnet glue shared by the `pool` and `serve` features: [`Channel`]s over
//! `weirdnet-net` streams, and certificate bytes.

use futures::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use weirdnet_net::user::NodeCert;
use weirdnet_net::{Limits, Stream};

use crate::remote::codec::{FrameDecoder, encode_frame};
use crate::remote::{Channel, FrameRx, FrameTx, RemoteError, from_cbor, to_cbor};

/// The app name inference nodes announce (`Node::serve_app`), so the pool can find them among
/// the user's linked devices. Model nodes do not announce it: apps never call them.
pub const INFERENCE_APP: &str = "weirdnet-agent-infer";

/// Stream limits for the agent protocols: `weirdnet-net` defaults (30 s resume window), no
/// idle reset (a queued request can be quiet for a while; `Queued` keepalives come every 3 s).
pub fn stream_limits() -> Limits {
    Limits { idle: None, ..Limits::default() }
}

/// A certificate as presented with a request (`Caller::cert_bytes`, `PoolOptions::cert`).
pub fn encode_cert(cert: &NodeCert) -> Vec<u8> {
    to_cbor(cert).unwrap_or_default()
}

pub fn decode_cert(bytes: &[u8]) -> Result<NodeCert, RemoteError> {
    from_cbor(bytes)
}

struct Tx(mpsc::Sender<Vec<u8>>);
struct Rx(mpsc::Receiver<Result<Vec<u8>, RemoteError>>);

#[async_trait::async_trait]
impl FrameTx for Tx {
    async fn send(&mut self, message: Vec<u8>) -> Result<(), RemoteError> {
        self.0.send(message).await.map_err(|_| RemoteError::Closed)
    }
}

#[async_trait::async_trait]
impl FrameRx for Rx {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, RemoteError> {
        match self.0.recv().await {
            Some(Ok(m)) => Ok(Some(m)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

/// A [`Channel`] over a stream: length-prefixed messages ([`crate::remote::codec`]). A task
/// drives the stream; dropping the channel closes our side, and once both sides are done the
/// stream is dropped. Call from a Tokio runtime.
pub fn channel(stream: Stream) -> Channel {
    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(16);
    let (in_tx, in_rx) = mpsc::channel::<Result<Vec<u8>, RemoteError>>(64);
    tokio::spawn(async move {
        let (mut r, mut w) = stream.split();
        let mut decoder = FrameDecoder::new();
        let mut buf = vec![0u8; 64 * 1024];
        let mut writing = true;
        let mut reader = Some(in_tx);
        loop {
            if !writing && reader.as_ref().is_none_or(|t| t.is_closed()) {
                break;
            }
            tokio::select! {
                m = out_rx.recv(), if writing => match m {
                    Some(message) => {
                        let ok = match encode_frame(&message) {
                            Ok(bytes) => w.write_all(&bytes).await.is_ok() && w.flush().await.is_ok(),
                            Err(_) => false,
                        };
                        if !ok {
                            writing = false;
                            if let Some(t) = reader.take() {
                                let _ = t.send(Err(RemoteError::Closed)).await;
                            }
                        }
                    }
                    None => {
                        writing = false;
                        let _ = w.close().await;
                    }
                },
                n = r.read(&mut buf), if reader.is_some() => match n {
                    Ok(0) => reader = None,
                    Ok(n) => {
                        decoder.push(&buf[..n]);
                        loop {
                            match decoder.next_frame() {
                                Ok(Some(m)) => {
                                    if let Some(t) = &reader && t.send(Ok(m)).await.is_err() {
                                        reader = None;
                                        break;
                                    }
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    if let Some(t) = reader.take() {
                                        let _ = t.send(Err(e)).await;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if let Some(t) = reader.take() {
                            let _ = t.send(Err(RemoteError::Unreachable { message: e.to_string() })).await;
                        }
                    }
                },
                _ = async { match &reader { Some(t) => t.closed().await, None => std::future::pending().await } }, if !writing => {
                    reader = None;
                }
            }
        }
    });
    Channel::new(Box::new(Tx(out_tx)), Box::new(Rx(in_rx)))
}

/// Maps a stream open error: a peer that does not accept the protocol is
/// [`RemoteError::Unsupported`], anything else unreachable.
#[cfg(feature = "pool")]
pub(crate) fn open_error(e: weirdnet_net::NetError) -> RemoteError {
    let message = e.to_string();
    if message.contains("does not accept") { RemoteError::Unsupported { message } } else { RemoteError::Unreachable { message } }
}
