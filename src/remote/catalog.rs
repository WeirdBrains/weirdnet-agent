//! `/weirdnet-agent/catalog/1`: the models an inference node offers.
//!
//! ```text
//! asking node                     inference node
//!   Query { cert } ->             <- Catalog | Failed
//!   Subscribe { cert } ->         <- Catalog, then Catalog again on every change (until closed)
//! ```
//!
//! One entry per model with its state, an estimated speed and how many replicas serve it, but
//! no model-node addresses: apps only ever talk to inference nodes. Messages are CBOR.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::{Authorize, Caller, Channel, RemoteError, bytes};

/// Stream protocol id.
pub const PROTOCOL: &str = "/weirdnet-agent/catalog/1";

/// What a model is for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Purpose {
    #[default]
    Chat,
    Embedding,
}

/// A model's state across the pool.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ModelState {
    /// Loaded somewhere and ready.
    Warm,
    /// Not loaded, but a node can load it.
    #[default]
    Loadable,
    Loading,
    /// Loaded, but every replica is full.
    Busy,
}

/// One catalog entry.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CatalogModel {
    pub id: String,
    #[serde(default)]
    pub purpose: Purpose,
    #[serde(default)]
    pub state: ModelState,
    /// Model nodes that have it loaded.
    #[serde(default)]
    pub replicas: u32,
    /// Estimated decode speed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens_per_sec: Option<f32>,
    /// Context window in tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// The model that answers while this one loads, if the pool policy names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standin: Option<String>,
}

impl CatalogModel {
    pub fn new(id: impl Into<String>, purpose: Purpose, state: ModelState) -> Self {
        Self { id: id.into(), purpose, state, ..Default::default() }
    }

    pub fn with_replicas(mut self, replicas: u32) -> Self {
        self.replicas = replicas;
        self
    }

    pub fn with_speed(mut self, tokens_per_sec: f32) -> Self {
        self.tokens_per_sec = Some(tokens_per_sec);
        self
    }

    pub fn with_context(mut self, tokens: u32) -> Self {
        self.context = Some(tokens);
        self
    }
}

/// The models on offer. `rev` grows with every change.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Catalog {
    pub rev: u64,
    pub models: Vec<CatalogModel>,
}

impl Catalog {
    pub fn new(rev: u64, models: Vec<CatalogModel>) -> Self {
        Self { rev, models }
    }

    pub fn find(&self, id: &str) -> Option<&CatalogModel> {
        self.models.iter().find(|m| m.id == id)
    }
}

/// Asking node to inference node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ClientMsg {
    Query {
        #[serde(default, skip_serializing_if = "Option::is_none", with = "bytes::opt")]
        cert: Option<Vec<u8>>,
    },
    Subscribe {
        #[serde(default, skip_serializing_if = "Option::is_none", with = "bytes::opt")]
        cert: Option<Vec<u8>>,
    },
}

/// Inference node to asking node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ServerMsg {
    Catalog(Catalog),
    Failed { error: RemoteError },
}

/// The serving side's catalog (an inference node's merged view).
pub trait CatalogHost: Send + Sync {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError>;

    /// The catalog for this caller, and its changes.
    fn watch(&self, caller: &Caller) -> Result<watch::Receiver<Catalog>, RemoteError>;
}

/// A ready [`CatalogHost`]: publish the models, and subscribers hear about it.
#[derive(Clone)]
pub struct CatalogFeed {
    tx: Arc<watch::Sender<Catalog>>,
    auth: Arc<dyn Authorize>,
}

impl CatalogFeed {
    /// A feed whose callers `auth` admits.
    pub fn new(auth: Arc<dyn Authorize>) -> Self {
        Self { tx: Arc::new(watch::channel(Catalog::default()).0), auth }
    }

    /// Replaces the models; `rev` goes up by one.
    pub fn publish(&self, models: Vec<CatalogModel>) {
        self.tx.send_modify(|c| {
            c.rev += 1;
            c.models = models;
        });
    }

    pub fn current(&self) -> Catalog {
        self.tx.borrow().clone()
    }
}

impl CatalogHost for CatalogFeed {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        self.auth.authorize(caller)
    }

    fn watch(&self, _caller: &Caller) -> Result<watch::Receiver<Catalog>, RemoteError> {
        Ok(self.tx.subscribe())
    }
}

/// Serve one catalog/1 stream. `caller` is the transport-authenticated peer; the certificate the
/// request presents is attached when the transport gave none, and the host authorizes it first.
pub async fn serve(mut ch: Channel, caller: Caller, host: &dyn CatalogHost) -> Result<(), RemoteError> {
    let (cert, follow) = match ch.recv_cbor::<ClientMsg>().await? {
        Some(ClientMsg::Query { cert }) => (cert, false),
        Some(ClientMsg::Subscribe { cert }) => (cert, true),
        None => return Ok(()),
    };
    let caller = caller.presenting(cert);
    let mut rx = match host.authorize(&caller).and_then(|()| host.watch(&caller)) {
        Ok(rx) => rx,
        Err(error) => return ch.send_cbor(&ServerMsg::Failed { error }).await,
    };
    let now = rx.borrow_and_update().clone();
    ch.send_cbor(&ServerMsg::Catalog(now)).await?;
    if !follow {
        return Ok(());
    }
    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                let now = rx.borrow_and_update().clone();
                ch.send_cbor(&ServerMsg::Catalog(now)).await?;
            }
            m = ch.rx.recv() => match m {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => return Ok(()),
            },
        }
    }
}

/// Asks once.
pub async fn query(ch: &mut Channel, cert: Option<Vec<u8>>) -> Result<Catalog, RemoteError> {
    ch.send_cbor(&ClientMsg::Query { cert }).await?;
    match ch.recv_cbor::<ServerMsg>().await? {
        Some(ServerMsg::Catalog(c)) => Ok(c),
        Some(ServerMsg::Failed { error }) => Err(error),
        None => Err(RemoteError::Closed),
    }
}

/// Subscribes and hands every catalog to `on`, until the stream ends (`Ok`) or fails.
pub async fn subscribe(ch: &mut Channel, cert: Option<Vec<u8>>, mut on: impl FnMut(Catalog)) -> Result<(), RemoteError> {
    ch.send_cbor(&ClientMsg::Subscribe { cert }).await?;
    loop {
        match ch.recv_cbor::<ServerMsg>().await? {
            Some(ServerMsg::Catalog(c)) => on(c),
            Some(ServerMsg::Failed { error }) => return Err(error),
            None => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::memory_pair;

    fn anyone() -> Arc<dyn Authorize> {
        Arc::new(|_: &Caller| Ok(()))
    }

    #[tokio::test]
    async fn query_and_subscribe_see_changes() {
        let feed = CatalogFeed::new(anyone());
        feed.publish(vec![CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm).with_replicas(2)]);
        let (mut ours, theirs) = memory_pair();
        let f = feed.clone();
        tokio::spawn(async move { serve(theirs, Caller::new("phone"), &f).await });
        let c = query(&mut ours, None).await.unwrap();
        assert_eq!(c.rev, 1);
        assert_eq!(c.find("gemma").unwrap().replicas, 2);

        let (mut ours, theirs) = memory_pair();
        let f = feed.clone();
        tokio::spawn(async move { serve(theirs, Caller::new("phone"), &f).await });
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move { subscribe(&mut ours, None, |c| tx.send(c).unwrap()).await });
        assert_eq!(rx.recv().await.unwrap().rev, 1);
        feed.publish(vec![CatalogModel::new("gemma", Purpose::Chat, ModelState::Loading)]);
        let next = rx.recv().await.unwrap();
        assert_eq!((next.rev, next.models[0].state), (2, ModelState::Loading));
    }

    #[tokio::test]
    async fn the_host_authorizes_with_the_presented_cert() {
        let auth: Arc<dyn Authorize> = Arc::new(|c: &Caller| {
            if c.peer == "laptop" && c.cert_bytes.as_deref() == Some(b"cert") { Ok(()) } else { Err(RemoteError::refused("unknown-device")) }
        });
        let feed = CatalogFeed::new(auth);
        for (peer, cert, ok) in [("laptop", Some(b"cert".to_vec()), true), ("laptop", None, false), ("stranger", Some(b"cert".to_vec()), false)] {
            let (mut ours, theirs) = memory_pair();
            let f = feed.clone();
            tokio::spawn(async move { serve(theirs, Caller::new(peer), &f).await });
            let r = query(&mut ours, cert).await;
            assert_eq!(r.is_ok(), ok, "{peer}");
            if !ok {
                assert_eq!(r.unwrap_err(), RemoteError::refused("unknown-device"));
            }
        }
    }

    #[test]
    fn wire_is_cbor_with_byte_string_certs() {
        let m = ClientMsg::Query { cert: Some(vec![1, 2, 3]) };
        let b = crate::remote::to_cbor(&m).unwrap();
        // map(1) "query" map(1) "cert" bytes(3)
        assert!(b.windows(4).any(|w| w == [0x43, 1, 2, 3]), "{b:x?}");
        assert_eq!(crate::remote::from_cbor::<ClientMsg>(&b).unwrap(), m);
    }
}
