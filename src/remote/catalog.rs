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
//!
//! Entries may carry the model's media and request limits ([`CatalogModel::limits`], a
//! [`MediaLimits`]: images per request, token cost per image and its edge, audio length and token
//! rate, request bytes), each absent when the node doesn't know it. Unknown limits are left off
//! the wire, so peers before 0.5.7 read and send entries unchanged.
//!
//! A model may be a speech-to-text model ([`Purpose::Transcribe`], `transcribe` on the wire):
//! audio in, its transcript out, so a soundtrack can become text for models without audio. It is
//! never picked for chat; [`Catalog::route_transcribe`] picks one for a transcription request
//! (infer/2 `Transcribe`). Peers before 0.5.8 can't read that purpose, so [`serve`] leaves such
//! entries out for askers that don't say they skip what they can't read; [`query`] and
//! [`subscribe`] say so, and from 0.5.8 an entry this version can't read (a purpose from a newer
//! version, say) is skipped instead of failing the whole catalog.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::{Authorize, Caller, Channel, RemoteError, bytes};
use crate::limits::MediaLimits;
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
    /// Speech to text: audio in, its transcript out (infer/2 `Transcribe`). Never picked as a
    /// chat model. Since 0.5.8; older peers don't see these entries (see the module docs).
    Transcribe,
}

impl Purpose {
    /// The name on the wire and in settings: `chat`, `embedding` or `transcribe` (the same names
    /// as [weirdnet-model](https://crates.io/crates/weirdnet-model)'s `hw::Purpose`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Purpose::Chat => "chat",
            Purpose::Embedding => "embedding",
            Purpose::Transcribe => "transcribe",
        }
    }

    /// Parses [`as_str`](Self::as_str)'s names; `None` for anything else.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "chat" => Some(Purpose::Chat),
            "embedding" => Some(Purpose::Embedding),
            "transcribe" => Some(Purpose::Transcribe),
            _ => None,
        }
    }

    /// Known to peers before 0.5.8.
    fn before_transcribe(self) -> bool {
        matches!(self, Purpose::Chat | Purpose::Embedding)
    }
}

impl std::fmt::Display for Purpose {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
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
    /// Context window in tokens: the one the node runs the model with (such as llama.cpp's
    /// `--ctx-size` per slot) when it knows it, not the model's trained maximum.
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
    /// Media and request limits, each absent when the node doesn't know it (never defaulted).
    /// Left off the wire when none is known; absent in catalogs from 0.5.6 and earlier.
    #[serde(default, skip_serializing_if = "MediaLimits::is_unknown")]
    pub limits: MediaLimits,
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

    /// Its media and request limits.
    pub fn with_limits(mut self, limits: MediaLimits) -> Self {
        self.limits = limits;
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

    /// A speech-to-text model ([`Purpose::Transcribe`]).
    pub fn transcribes(&self) -> bool {
        self.purpose == Purpose::Transcribe
    }
}

/// Placement order: warm, then busy, loading, loadable.
fn rank(s: ModelState) -> u8 {
    match s {
        ModelState::Warm => 0,
        ModelState::Busy => 1,
        ModelState::Loading => 2,
        _ => 3,
    }
}

/// The models on offer. `rev` grows with every change.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Catalog {
    pub rev: u64,
    /// The entries. Reading one this version can't (a purpose from a newer version, say) skips
    /// that entry instead of failing the catalog (since 0.5.8).
    #[serde(deserialize_with = "readable_models")]
    pub models: Vec<CatalogModel>,
}

/// The entries this version can read; the rest are skipped.
fn readable_models<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<CatalogModel>, D::Error> {
    struct Entry(Option<CatalogModel>);
    impl<'de> Deserialize<'de> for Entry {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            // Entries hold only strings, numbers, booleans and maps, so a JSON value carries
            // them whole from CBOR as well as from JSON.
            let v = serde_json::Value::deserialize(d)?;
            Ok(Entry(serde_json::from_value(v).ok()))
        }
    }
    Ok(Vec::<Entry>::deserialize(d)?.into_iter().filter_map(|e| e.0).collect())
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

    /// The model for a transcription request (infer/2 `Transcribe`): a named model must be a
    /// speech-to-text model ([`Purpose::Transcribe`]), and with none named the best one is picked
    /// (warm, then busy, loading, loadable; then more replicas). A named model the catalog does
    /// not list is left to the node. `BadRequest` when it names a model that does not transcribe
    /// or nothing here does.
    pub fn route_transcribe(&self, model: Option<&str>) -> Result<String, RemoteError> {
        if let Some(id) = model {
            return match self.find(id) {
                Some(m) if !m.transcribes() => Err(RemoteError::BadRequest {
                    message: format!("model `{id}` does not transcribe audio; pick one whose catalog purpose is `transcribe`"),
                }),
                _ => Ok(id.to_string()),
            };
        }
        self.transcribers()
            .min_by_key(|m| (rank(m.state), std::cmp::Reverse(m.replicas)))
            .map(|m| m.id.clone())
            .ok_or_else(|| RemoteError::BadRequest { message: "no model in the pool transcribes audio".into() })
    }

    /// The speech-to-text models.
    pub fn transcribers(&self) -> impl Iterator<Item = &CatalogModel> {
        self.models.iter().filter(|m| m.transcribes())
    }

    /// The catalog as a peer before 0.5.8 can read it: without entries whose purpose it lacks.
    fn for_older(&self) -> Catalog {
        Catalog { rev: self.rev, models: self.models.iter().filter(|m| m.purpose.before_transcribe()).cloned().collect() }
    }
}

/// Asking node to inference node. [`query`] and [`subscribe`] also say that the asker skips
/// entries it can't read (a field older nodes ignore); a node leaves entries peers before 0.5.8
/// can't read ([`Purpose::Transcribe`]) out of its answer to a message without it, so an app that
/// sends this type itself sees no transcription models.
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

/// [`ClientMsg`] as sent and read from 0.5.8: `skips_unknown` says the asker skips entries it
/// can't read. Older nodes ignore the field; older askers leave it out.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Ask {
    Query {
        #[serde(default, skip_serializing_if = "Option::is_none", with = "bytes::opt")]
        cert: Option<Vec<u8>>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        skips_unknown: bool,
    },
    Subscribe {
        #[serde(default, skip_serializing_if = "Option::is_none", with = "bytes::opt")]
        cert: Option<Vec<u8>>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        skips_unknown: bool,
    },
}

/// The serving side's catalog (an inference node's merged view).
pub trait CatalogHost: Send + Sync {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError>;

    /// The catalog for this caller, and its changes.
    fn watch(&self, caller: &Caller) -> Result<watch::Receiver<Catalog>, RemoteError>;

    /// Whether a subscriber may still follow the catalog (since 0.5.9): asked before every
    /// update a subscription sends and every [`RECHECK_EVERY`]; an error closes the subscription
    /// with that refusal. Hosts that admit guests return their current verdict (usually
    /// [`authorize`](Self::authorize) again) so revoking a guest ends its subscriptions at once.
    /// Default: `Ok` (admitted once, followed until closed, as before 0.5.9).
    fn recheck(&self, caller: &Caller) -> Result<(), RemoteError> {
        let _ = caller;
        Ok(())
    }
}

/// How often a subscription is re-checked when no update comes ([`CatalogHost::recheck`]).
pub const RECHECK_EVERY: std::time::Duration = std::time::Duration::from_secs(5);

/// A ready [`CatalogHost`]: publish the models, and subscribers hear about it.
#[derive(Clone)]
pub struct CatalogFeed {
    tx: Arc<watch::Sender<Catalog>>,
    auth: Arc<dyn Authorize>,
    recheck: bool,
}

impl CatalogFeed {
    /// A feed whose callers `auth` admits.
    pub fn new(auth: Arc<dyn Authorize>) -> Self {
        Self { tx: Arc::new(watch::channel(Catalog::default()).0), auth, recheck: false }
    }

    /// Re-check subscribers with the authorizer ([`CatalogHost::recheck`]), so one it stops
    /// admitting (a revoked guest or device) loses its subscription within [`RECHECK_EVERY`].
    /// Since 0.5.9; off by default.
    pub fn rechecking(mut self) -> Self {
        self.recheck = true;
        self
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

    fn recheck(&self, caller: &Caller) -> Result<(), RemoteError> {
        if self.recheck { self.auth.authorize(caller) } else { Ok(()) }
    }
}

/// Serve one catalog/1 stream. `caller` is the transport-authenticated peer; the certificate the
/// request presents is attached when the transport gave none, and the host authorizes it first.
/// An asker that doesn't say it skips entries it can't read (one before 0.5.8) gets the catalog
/// without [`Purpose::Transcribe`] entries, which it could not decode.
pub async fn serve(mut ch: Channel, caller: Caller, host: &dyn CatalogHost) -> Result<(), RemoteError> {
    let (cert, follow, skips_unknown) = match ch.recv_cbor::<Ask>().await? {
        Some(Ask::Query { cert, skips_unknown }) => (cert, false, skips_unknown),
        Some(Ask::Subscribe { cert, skips_unknown }) => (cert, true, skips_unknown),
        None => return Ok(()),
    };
    let readable = |c: Catalog| if skips_unknown { c } else { c.for_older() };
    let caller = caller.presenting(cert);
    let mut rx = match host.authorize(&caller).and_then(|()| host.watch(&caller)) {
        Ok(rx) => rx,
        Err(error) => return ch.send_cbor(&ServerMsg::Failed { error }).await,
    };
    let now = readable(rx.borrow_and_update().clone());
    ch.send_cbor(&ServerMsg::Catalog(now)).await?;
    if !follow {
        return Ok(());
    }
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + RECHECK_EVERY, RECHECK_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                if let Err(error) = host.recheck(&caller) {
                    return ch.send_cbor(&ServerMsg::Failed { error }).await;
                }
                let now = readable(rx.borrow_and_update().clone());
                ch.send_cbor(&ServerMsg::Catalog(now)).await?;
            }
            _ = tick.tick() => {
                if let Err(error) = host.recheck(&caller) {
                    return ch.send_cbor(&ServerMsg::Failed { error }).await;
                }
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
    ch.send_cbor(&Ask::Query { cert, skips_unknown: true }).await?;
    match ch.recv_cbor::<ServerMsg>().await? {
        Some(ServerMsg::Catalog(c)) => Ok(c),
        Some(ServerMsg::Failed { error }) => Err(error),
        None => Err(RemoteError::Closed),
    }
}

/// Subscribes and hands every catalog to `on`, until the stream ends (`Ok`) or fails.
pub async fn subscribe(ch: &mut Channel, cert: Option<Vec<u8>>, mut on: impl FnMut(Catalog)) -> Result<(), RemoteError> {
    ch.send_cbor(&Ask::Subscribe { cert, skips_unknown: true }).await?;
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

    #[tokio::test(start_paused = true)]
    async fn a_rechecking_feed_closes_subscriptions_it_stops_admitting() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let allowed = Arc::new(AtomicBool::new(true));
        let auth: Arc<dyn Authorize> = {
            let a = allowed.clone();
            Arc::new(move |_: &Caller| if a.load(Ordering::SeqCst) { Ok(()) } else { Err(RemoteError::refused("guest-revoked")) })
        };
        // On an update: refused before the update is sent.
        let feed = CatalogFeed::new(auth.clone()).rechecking();
        feed.publish(vec![CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm)]);
        let (mut ours, theirs) = memory_pair();
        let f = feed.clone();
        tokio::spawn(async move { serve(theirs, Caller::new("guest"), &f).await });
        let mut seen = Vec::new();
        let sub = subscribe(&mut ours, None, |c| seen.push(c.rev));
        let flip = async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            allowed.store(false, Ordering::SeqCst);
            feed.publish(vec![]);
        };
        let (r, ()) = tokio::join!(sub, flip);
        assert_eq!(r.unwrap_err(), RemoteError::refused("guest-revoked"));
        assert_eq!(seen, [1], "nothing after the revocation");

        // With no update: refused on the next tick.
        allowed.store(true, Ordering::SeqCst);
        let (mut ours, theirs) = memory_pair();
        let f = feed.clone();
        tokio::spawn(async move { serve(theirs, Caller::new("guest"), &f).await });
        let mut seen = 0;
        let sub = subscribe(&mut ours, None, |_| seen += 1);
        let flip = async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            allowed.store(false, Ordering::SeqCst);
        };
        let started = tokio::time::Instant::now();
        let (r, ()) = tokio::join!(sub, flip);
        assert_eq!(r.unwrap_err(), RemoteError::refused("guest-revoked"));
        assert_eq!(seen, 1);
        assert!(started.elapsed() <= RECHECK_EVERY + std::time::Duration::from_millis(100));

        // A feed that doesn't re-check (the default) keeps following, as before 0.5.9.
        allowed.store(true, Ordering::SeqCst);
        let feed = CatalogFeed::new(auth);
        let (mut ours, theirs) = memory_pair();
        let f = feed.clone();
        tokio::spawn(async move { serve(theirs, Caller::new("guest"), &f).await });
        let mut seen = 0;
        let sub = subscribe(&mut ours, None, |_| seen += 1);
        let flip = async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            allowed.store(false, Ordering::SeqCst);
            tokio::time::sleep(RECHECK_EVERY * 3).await;
            feed.publish(vec![]);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        };
        tokio::select! {
            r = sub => panic!("ended: {r:?}"),
            () = flip => {}
        }
        assert_eq!(seen, 2);
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
    fn limits_round_trip_and_stay_off_the_wire_when_unknown() {
        let plain = CatalogModel::new("qwen", Purpose::Chat, ModelState::Warm).with_context(8192);
        let b = crate::remote::to_cbor(&plain).unwrap();
        assert!(!b.windows(6).any(|w| w == b"limits"), "entries without limits look as before");
        assert!(!serde_json::to_string(&plain).unwrap().contains("limits"));
        let limits = MediaLimits::new()
            .with_max_images(8)
            .with_image_tokens(256)
            .with_image_edge(896)
            .with_audio_max_seconds(30)
            .with_audio_tokens_per_minute(1500)
            .with_max_request_bytes(32 << 20);
        let m = CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm).with_inputs(Modalities::new(true, true)).with_limits(limits);
        assert_eq!(crate::remote::from_cbor::<CatalogModel>(&crate::remote::to_cbor(&m).unwrap()).unwrap(), m);
        assert_eq!(serde_json::from_str::<CatalogModel>(&serde_json::to_string(&m).unwrap()).unwrap(), m);
        let j = serde_json::to_value(&m).unwrap();
        assert_eq!(j["limits"]["max_images"], 8);
        assert_eq!(j["limits"]["max_request_bytes"], 32 << 20);
        assert!(j["limits"].get("image_patch").is_none(), "unknown fields stay off");
        // Only some limits known: the rest stay absent.
        let some = CatalogModel::new("llava", Purpose::Chat, ModelState::Warm).with_limits(MediaLimits::new().with_image_tokens(576));
        let back = crate::remote::from_cbor::<CatalogModel>(&crate::remote::to_cbor(&some).unwrap()).unwrap();
        assert_eq!((back.limits.image_tokens, back.limits.max_images), (Some(576), None));
    }

    #[test]
    fn old_peers_read_and_send_entries_without_limits() {
        // JSON and CBOR from a node before 0.5.7 read with no limits.
        let old_json = r#"{"id":"qwen","purpose":"chat","state":"warm","replicas":1,"context":4096,"inputs":{"vision":true}}"#;
        let m: CatalogModel = serde_json::from_str(old_json).unwrap();
        assert!(m.limits.is_unknown());
        assert_eq!(m.context, Some(4096));
        let old =
            OldModel { id: "qwen".into(), purpose: Purpose::Chat, state: ModelState::Warm, replicas: 1, revision: Some("abc".into()), base: None };
        let m = crate::remote::from_cbor::<CatalogModel>(&crate::remote::to_cbor(&old).unwrap()).unwrap();
        assert!(m.limits.is_unknown());
        // A new entry with limits still reads on an old peer, which skips them.
        let new = CatalogModel::new("qwen", Purpose::Chat, ModelState::Warm)
            .with_replicas(1)
            .with_limits(MediaLimits::new().with_max_images(2).with_max_request_bytes(1 << 20));
        let seen = crate::remote::from_cbor::<OldModel>(&crate::remote::to_cbor(&new).unwrap()).unwrap();
        assert_eq!(seen, OldModel { revision: None, ..old });
        let msg = ServerMsg::Catalog(Catalog::new(4, vec![new]));
        assert!(matches!(
            crate::remote::from_cbor::<ServerMsg>(&crate::remote::to_cbor(&msg).unwrap()).unwrap(),
            ServerMsg::Catalog(c) if c.models[0].limits.max_images == Some(2)
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

    fn transcribe_catalog() -> Catalog {
        Catalog::new(
            7,
            vec![
                CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm).with_inputs(Modalities::new(true, true)),
                CatalogModel::new("nomic", Purpose::Embedding, ModelState::Warm),
                CatalogModel::new("voxtral-cold", Purpose::Transcribe, ModelState::Loadable).with_inputs(Modalities::new(false, true)),
                CatalogModel::new("voxtral", Purpose::Transcribe, ModelState::Warm).with_replicas(1).with_inputs(Modalities::new(false, true)),
                CatalogModel::new("whisper", Purpose::Transcribe, ModelState::Warm).with_replicas(2),
            ],
        )
    }

    #[test]
    fn transcribe_purpose_names_match_weirdnet_model() {
        for (p, name) in [(Purpose::Chat, "chat"), (Purpose::Embedding, "embedding"), (Purpose::Transcribe, "transcribe")] {
            assert_eq!(p.as_str(), name);
            assert_eq!(p.to_string(), name);
            assert_eq!(Purpose::parse(name), Some(p));
            assert_eq!(serde_json::to_string(&p).unwrap(), format!("\"{name}\""));
            assert_eq!(serde_json::from_str::<Purpose>(&format!("\"{name}\"")).unwrap(), p);
            assert_eq!(crate::remote::from_cbor::<Purpose>(&crate::remote::to_cbor(&p).unwrap()).unwrap(), p);
        }
        assert_eq!(Purpose::parse("speech"), None);
        let m = CatalogModel::new("voxtral", Purpose::Transcribe, ModelState::Warm);
        assert!(m.transcribes() && !m.accepts(Modalities::TEXT) && !m.accepts(Modalities::AUDIO));
    }

    #[test]
    fn route_transcribe_picks_the_best_transcriber() {
        let c = transcribe_catalog();
        // Warm before loadable, then more replicas.
        assert_eq!(c.route_transcribe(None).unwrap(), "whisper");
        assert_eq!(c.transcribers().map(|m| m.id.as_str()).collect::<Vec<_>>(), ["voxtral-cold", "voxtral", "whisper"]);
        assert_eq!(c.route_transcribe(Some("voxtral-cold")).unwrap(), "voxtral-cold");
        // A model the catalog doesn't list is left to the node.
        assert_eq!(c.route_transcribe(Some("elsewhere")).unwrap(), "elsewhere");
        for id in ["gemma", "nomic"] {
            let e = c.route_transcribe(Some(id)).unwrap_err();
            assert!(matches!(&e, RemoteError::BadRequest { message } if message.contains(id) && message.contains("transcribe")), "{e:?}");
        }
        let none = Catalog::new(1, vec![CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm)]);
        assert!(matches!(none.route_transcribe(None), Err(RemoteError::BadRequest { message }) if message.contains("transcribes")));
    }

    #[test]
    fn chat_routing_never_picks_a_transcriber() {
        let c = Catalog::new(1, vec![CatalogModel::new("voxtral", Purpose::Transcribe, ModelState::Warm).with_inputs(Modalities::new(false, true))]);
        assert!(matches!(c.route(None, Modalities::AUDIO), Err(RemoteError::BadRequest { .. })));
        assert!(matches!(c.route(Some("voxtral"), Modalities::AUDIO), Err(RemoteError::BadRequest { .. })));
        assert_eq!(transcribe_catalog().route(None, Modalities::AUDIO).unwrap().as_deref(), Some("gemma"));
    }

    #[test]
    fn transcribers_round_trip_and_unreadable_entries_are_skipped() {
        let c = transcribe_catalog();
        assert_eq!(crate::remote::from_cbor::<Catalog>(&crate::remote::to_cbor(&c).unwrap()).unwrap(), c);
        assert_eq!(serde_json::from_str::<Catalog>(&serde_json::to_string(&c).unwrap()).unwrap(), c);
        // An entry from a newer version (an unknown purpose) is skipped, not fatal.
        let json = r#"{"rev":3,"models":[{"id":"a","purpose":"chat","tokens_per_sec":12.5},{"id":"b","purpose":"diarize"},{"id":"c","purpose":"transcribe"}]}"#;
        let read: Catalog = serde_json::from_str(json).unwrap();
        assert_eq!(read.models.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(), ["a", "c"]);
        assert_eq!(read.models[0].tokens_per_sec, Some(12.5));
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        let read: Catalog = crate::remote::from_cbor(&crate::remote::to_cbor(&v).unwrap()).unwrap();
        assert_eq!(read.models.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(), ["a", "c"]);
        // The whole entry still round-trips through CBOR with every field set.
        let full = Catalog::new(
            9,
            vec![
                CatalogModel::new("q", Purpose::Chat, ModelState::Busy)
                    .with_speed(31.25)
                    .with_context(8192)
                    .with_base("base")
                    .with_origin(ORIGIN_UPLOAD)
                    .with_license("MIT")
                    .with_limits(MediaLimits::new().with_max_images(4).with_max_request_bytes(1 << 25)),
            ],
        );
        assert_eq!(crate::remote::from_cbor::<Catalog>(&crate::remote::to_cbor(&full).unwrap()).unwrap(), full);
    }

    /// The purposes a peer before 0.5.8 knows, and its strict catalog.
    #[derive(Debug, PartialEq, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum OldPurpose {
        Chat,
        Embedding,
    }

    #[derive(Debug, Deserialize)]
    struct OldEntry {
        id: String,
        #[allow(dead_code)]
        purpose: OldPurpose,
    }

    #[derive(Debug, Deserialize)]
    struct OldCatalog {
        models: Vec<OldEntry>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum OldServerMsg {
        Catalog(OldCatalog),
    }

    #[tokio::test]
    async fn older_askers_get_the_catalog_without_transcribers() {
        let feed = CatalogFeed::new(anyone());
        feed.publish(transcribe_catalog().models);
        // An old peer could not read the full catalog at all.
        assert!(crate::remote::from_cbor::<OldServerMsg>(&crate::remote::to_cbor(&ServerMsg::Catalog(feed.current())).unwrap()).is_err());

        for follow in [false, true] {
            // An asker before 0.5.8 sends plain `ClientMsg`s.
            let (mut ours, theirs) = memory_pair();
            let f = feed.clone();
            tokio::spawn(async move { serve(theirs, Caller::new("phone"), &f).await });
            let ask = if follow { ClientMsg::Subscribe { cert: None } } else { ClientMsg::Query { cert: None } };
            ours.send_cbor(&ask).await.unwrap();
            let bytes = ours.rx.recv().await.unwrap().unwrap();
            let OldServerMsg::Catalog(old) = crate::remote::from_cbor::<OldServerMsg>(&bytes).unwrap();
            assert_eq!(old.models.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(), ["gemma", "nomic"]);
        }

        // 0.5.8 askers see everything, on query and on every change.
        let (mut ours, theirs) = memory_pair();
        let f = feed.clone();
        tokio::spawn(async move { serve(theirs, Caller::new("phone"), &f).await });
        assert_eq!(query(&mut ours, None).await.unwrap().transcribers().count(), 3);
        let (mut ours, theirs) = memory_pair();
        let f = feed.clone();
        tokio::spawn(async move { serve(theirs, Caller::new("phone"), &f).await });
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move { subscribe(&mut ours, None, |c| tx.send(c).unwrap()).await });
        assert_eq!(rx.recv().await.unwrap().models.len(), 5);
        feed.publish(vec![CatalogModel::new("whisper", Purpose::Transcribe, ModelState::Busy)]);
        assert_eq!(rx.recv().await.unwrap().route_transcribe(None).unwrap(), "whisper");
    }

    #[test]
    fn the_new_ask_reads_on_old_nodes_and_old_asks_read_on_new_ones() {
        let new = Ask::Query { cert: Some(vec![1, 2]), skips_unknown: true };
        assert_eq!(
            crate::remote::from_cbor::<ClientMsg>(&crate::remote::to_cbor(&new).unwrap()).unwrap(),
            ClientMsg::Query { cert: Some(vec![1, 2]) }
        );
        let new = Ask::Subscribe { cert: None, skips_unknown: true };
        assert_eq!(crate::remote::from_cbor::<ClientMsg>(&crate::remote::to_cbor(&new).unwrap()).unwrap(), ClientMsg::Subscribe { cert: None });
        let old = ClientMsg::Subscribe { cert: Some(vec![3]) };
        assert_eq!(
            crate::remote::from_cbor::<Ask>(&crate::remote::to_cbor(&old).unwrap()).unwrap(),
            Ask::Subscribe { cert: Some(vec![3]), skips_unknown: false }
        );
        // Unset, the flag stays off the wire: the bytes are what 0.5.7 sends.
        let quiet = Ask::Query { cert: None, skips_unknown: false };
        assert_eq!(crate::remote::to_cbor(&quiet).unwrap(), crate::remote::to_cbor(&ClientMsg::Query { cert: None }).unwrap());
    }
}
