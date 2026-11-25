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
//!
//! Each entry says which inputs beyond text it takes ([`CatalogModel::inputs`]: images, audio).
//! Entries from nodes before 0.5 have none, so they count as text-only. [`Catalog::route`] picks
//! a model for a request that carries images or audio, and the pool uses it so such a request
//! never lands on a text-only model.
//!
//! Entries may also say where the model came from ([`CatalogModel::origin`]) and its license
//! ([`CatalogModel::license`]); both are left off the wire when unset, so older peers read them.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::{Authorize, Caller, Channel, RemoteError, bytes};
use crate::model::Modalities;

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
    /// Inputs it takes beyond text (images, audio). Empty: text only.
    #[serde(default, skip_serializing_if = "Modalities::is_text")]
    pub inputs: Modalities,
    /// For a LoRA adapter served on a base model: the base's id. Requests name the adapter's own
    /// id; apps may show it as a variant of the base. Not sent for models; catalogs from 0.5.1
    /// and earlier lack it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// Where the model came from: `hub`, `upload` or `train` (the origins of a
    /// [weirdnet-model](https://crates.io/crates/weirdnet-model) provenance record), or an app's
    /// own. Absent when the node does not say, and in catalogs from 0.5.3 and earlier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    /// The model's license as an SPDX id, or `unknown` when the uploader said so. Absent when the
    /// node does not say, and in catalogs from 0.5.3 and earlier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
}

/// [`CatalogModel::origin`]: downloaded from a Hub.
pub const ORIGIN_HUB: &str = "hub";
/// [`CatalogModel::origin`]: sent from another machine.
pub const ORIGIN_UPLOAD: &str = "upload";
/// [`CatalogModel::origin`]: made by a training run.
pub const ORIGIN_TRAIN: &str = "train";

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

    /// The inputs it takes beyond text.
    pub fn with_inputs(mut self, inputs: Modalities) -> Self {
        self.inputs = inputs;
        self
    }

    /// Marks it as a LoRA adapter on base model `base`.
    pub fn with_base(mut self, base: impl Into<String>) -> Self {
        self.base = Some(base.into());
        self
    }

    /// Where it came from ([`ORIGIN_HUB`], [`ORIGIN_UPLOAD`], [`ORIGIN_TRAIN`] or an app's own).
    pub fn with_origin(mut self, origin: impl Into<String>) -> Self {
        self.origin = Some(origin.into());
        self
    }

    /// Its license (an SPDX id, or `unknown`).
    pub fn with_license(mut self, license: impl Into<String>) -> Self {
        self.license = Some(license.into());
        self
    }

    /// Whether it is a LoRA adapter on another model.
    pub fn is_adapter(&self) -> bool {
        self.base.is_some()
    }

    /// A chat model that can read input needing `needed`.
    pub fn accepts(&self, needed: Modalities) -> bool {
        self.purpose == Purpose::Chat && self.inputs.covers(needed)
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

    /// The model for a request that needs `needed`: text requests keep `model` as is. Otherwise
    /// a named model must take those inputs, and with none named the best chat model that does
    /// is picked (warm, then busy, loading, loadable; then more replicas). A named model the
    /// catalog does not list is left to the node. `BadRequest` when nothing here can read it.
    pub fn route(&self, model: Option<&str>, needed: Modalities) -> Result<Option<String>, RemoteError> {
        if needed.is_text() {
            return Ok(model.map(str::to_string));
        }
        if let Some(id) = model {
            return match self.find(id) {
                Some(m) if !m.accepts(needed) => {
                    let what = m.inputs.lacking(needed).unwrap_or("this input");
                    Err(RemoteError::BadRequest { message: format!("model `{id}` does not take {what}; pick one whose catalog entry lists them") })
                }
                _ => Ok(Some(id.to_string())),
            };
        }
        let rank = |s: ModelState| match s {
            ModelState::Warm => 0,
            ModelState::Busy => 1,
            ModelState::Loading => 2,
            _ => 3,
        };
        self.models
            .iter()
            .filter(|m| m.accepts(needed))
            .min_by_key(|m| (rank(m.state), std::cmp::Reverse(m.replicas)))
            .map(|m| Some(m.id.clone()))
            .ok_or_else(|| {
                let what = Modalities::TEXT.lacking(needed).unwrap_or("this input");
                RemoteError::BadRequest { message: format!("no model in the pool takes {what}") }
            })
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
    fn route_keeps_image_requests_off_text_only_models() {
        let c = Catalog::new(
            1,
            vec![
                CatalogModel::new("qwen", Purpose::Chat, ModelState::Warm),
                CatalogModel::new("gemma-cold", Purpose::Chat, ModelState::Loadable).with_inputs(Modalities::new(true, true)),
                CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm).with_inputs(Modalities::VISION),
                CatalogModel::new("embed", Purpose::Embedding, ModelState::Warm).with_inputs(Modalities::new(true, true)),
            ],
        );
        assert_eq!(c.route(None, Modalities::TEXT).unwrap(), None);
        assert_eq!(c.route(Some("qwen"), Modalities::TEXT).unwrap().as_deref(), Some("qwen"));
        assert_eq!(c.route(None, Modalities::VISION).unwrap().as_deref(), Some("gemma"), "warm first");
        assert_eq!(c.route(None, Modalities::AUDIO).unwrap().as_deref(), Some("gemma-cold"));
        let e = c.route(Some("qwen"), Modalities::VISION).unwrap_err();
        assert!(matches!(&e, RemoteError::BadRequest { message } if message.contains("`qwen` does not take images")), "{e}");
        assert_eq!(c.route(Some("not-listed"), Modalities::VISION).unwrap().as_deref(), Some("not-listed"));
        let text_only = Catalog::new(1, vec![CatalogModel::new("qwen", Purpose::Chat, ModelState::Warm)]);
        let e = text_only.route(None, Modalities::new(true, true)).unwrap_err();
        assert!(matches!(&e, RemoteError::BadRequest { message } if message == "no model in the pool takes images and audio"), "{e}");
    }

    #[test]
    fn inputs_are_optional_on_the_wire() {
        let plain = CatalogModel::new("qwen", Purpose::Chat, ModelState::Warm);
        let b = crate::remote::to_cbor(&plain).unwrap();
        assert!(!b.windows(6).any(|w| w == b"inputs"), "text-only entries look as before");
        let v = CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm).with_inputs(Modalities::VISION);
        assert_eq!(crate::remote::from_cbor::<CatalogModel>(&crate::remote::to_cbor(&v).unwrap()).unwrap(), v);
    }

    #[test]
    fn base_is_optional_on_the_wire() {
        let plain = CatalogModel::new("qwen", Purpose::Chat, ModelState::Warm);
        let b = crate::remote::to_cbor(&plain).unwrap();
        assert!(!b.windows(4).any(|w| w == b"base"), "models look as before");
        assert!(!plain.is_adapter());
        let a = CatalogModel::new("qwen+tune", Purpose::Chat, ModelState::Warm).with_base("qwen");
        assert!(a.is_adapter());
        assert_eq!(crate::remote::from_cbor::<CatalogModel>(&crate::remote::to_cbor(&a).unwrap()).unwrap(), a);
    }

    /// A catalog/1 entry as a 0.5.3 node sends it (and reads it): no origin or license.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct OldModel {
        id: String,
        #[serde(default)]
        purpose: Purpose,
        #[serde(default)]
        state: ModelState,
        #[serde(default)]
        replicas: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        revision: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base: Option<String>,
    }

    #[test]
    fn origin_and_license_are_optional_on_the_wire() {
        let plain = CatalogModel::new("qwen", Purpose::Chat, ModelState::Warm);
        let b = crate::remote::to_cbor(&plain).unwrap();
        assert!(!b.windows(6).any(|w| w == b"origin"), "entries without an origin look as before");
        assert!(!b.windows(7).any(|w| w == b"license"), "entries without a license look as before");
        let m = CatalogModel::new("qwen+tune", Purpose::Chat, ModelState::Warm).with_origin(ORIGIN_TRAIN).with_license("Apache-2.0");
        assert_eq!((m.origin.as_deref(), m.license.as_deref()), (Some("train"), Some("Apache-2.0")));
        assert_eq!(crate::remote::from_cbor::<CatalogModel>(&crate::remote::to_cbor(&m).unwrap()).unwrap(), m);
    }

    #[test]
    fn old_peers_read_and_send_entries_without_origin() {
        // An old node's entry reads with neither field.
        let old =
            OldModel { id: "qwen".into(), purpose: Purpose::Chat, state: ModelState::Warm, replicas: 1, revision: Some("abc".into()), base: None };
        let m = crate::remote::from_cbor::<CatalogModel>(&crate::remote::to_cbor(&old).unwrap()).unwrap();
        assert_eq!((m.origin, m.license, m.revision.as_deref()), (None, None, Some("abc")));
        // A new node's entry with both fields still reads on an old peer, which skips them.
        let new = CatalogModel::new("qwen", Purpose::Chat, ModelState::Warm).with_replicas(1).with_origin(ORIGIN_UPLOAD).with_license("unknown");
        let seen = crate::remote::from_cbor::<OldModel>(&crate::remote::to_cbor(&new).unwrap()).unwrap();
        assert_eq!(seen, OldModel { revision: None, ..old });
        // And a whole catalog message from a new node reads on an old peer's catalog type.
        let msg = ServerMsg::Catalog(Catalog::new(3, vec![new]));
        assert!(matches!(
            crate::remote::from_cbor::<ServerMsg>(&crate::remote::to_cbor(&msg).unwrap()).unwrap(),
            ServerMsg::Catalog(c) if c.models[0].origin.as_deref() == Some("upload")
        ));
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
