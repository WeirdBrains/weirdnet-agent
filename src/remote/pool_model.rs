//! A model cloud as one model: [`PoolModel`] and [`PoolWatch`].
//!
//! A pool of inference nodes (for example a [Ymir](https://github.com/WeirdBrains/ymir) cloud on
//! the user's own devices) is reached through a [`Pool`]. This module adds what an agent app
//! needs on top:
//!
//! - [`PoolModel`]: a [`ModelBackend`] over [`Pool::infer`] (infer/2). Text and reasoning stream
//!   to the [`DeltaSink`], tool-call frames come back in the turn, cancel stops the node, and a
//!   node that dies mid-answer is resumed on another one (the pool's
//!   [`PoolOptions::resume`](super::pool::PoolOptions::resume), on by default), shown as
//!   [`ModelNotice::Continued`]. When the pool can't take a turn before any text streamed, an
//!   optional fallback (usually the local model) runs it, shown as [`ModelNotice::FellBack`];
//!   with no fallback the error is [`ModelError::PoolUnreachable`] when no node could be reached.
//! - [`PoolWatch`]: follows the pool catalog (catalog/1 subscribe) as a [`PoolStatus`] for a model
//!   picker (live, searching or unreachable, with each model's state and speed), and knows which
//!   nodes answered, as a [`NodeSource`] ([`PoolWatch::nodes`]) so turns go only to real
//!   inference nodes.
//!
//! Like the rest of [`remote`](super), it has no networking of its own: the [`Pool`]'s dialer
//! and node source come from the app.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::RemoteError;
use super::catalog::{self, Catalog, CatalogModel, ModelState};
use super::infer::Route;
use super::infer::v2::Request;
use super::pool::{EventSink, Interrupted, NodeSource, Pool, PoolEvent, reported};
use crate::cancel::CancelToken;
use crate::model::{DeltaSink, Message, Modalities, ModelBackend, ModelError, ModelNotice, ModelTurn};
use crate::tool::ToolSpec;

/// A [`ModelBackend`] over a model pool, with an optional fallback.
pub struct PoolModel {
    pool: Pool,
    model: Option<String>,
    fallback: Option<Arc<dyn ModelBackend>>,
    reroute: bool,
    last: Mutex<Option<Route>>,
}

impl PoolModel {
    /// Runs turns on `pool` with its options as they are (resume is on by default). For a
    /// pool of inference nodes only, turn off
    /// [`infer_v1_fallback`](super::pool::PoolOptions::infer_v1_fallback).
    pub fn new(pool: Pool) -> Self {
        Self { pool, model: None, fallback: None, reroute: true, last: Mutex::new(None) }
    }

    /// Asks for this catalog model instead of the pool's default.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Runs here when the pool can't take a turn before any text streamed.
    pub fn with_fallback(mut self, fallback: Arc<dyn ModelBackend>) -> Self {
        self.fallback = Some(fallback);
        self
    }

    /// When the chosen model can't read a turn's images or audio, let the pool pick one that
    /// can ([`ModelNotice::Rerouted`]) instead of refusing the turn. On by default.
    pub fn with_reroute(mut self, on: bool) -> Self {
        self.reroute = on;
        self
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// Where the last completed turn ran.
    pub fn last_route(&self) -> Option<Route> {
        self.last.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn set_last(&self, route: Route) {
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some(route);
    }

    /// Drops a chosen model that can't read the request's inputs, so the pool routes it.
    async fn fit(&self, request: &mut Request, deltas: &DeltaSink) {
        let needed = request.needs();
        let Some(model) = request.model.clone().filter(|_| self.reroute && !needed.is_text()) else { return };
        let Ok(catalog) = self.pool.catalog().await else { return };
        if catalog.find(&model).is_some_and(|m| m.accepts(needed)) {
            return;
        }
        if catalog.models.iter().any(|m| m.accepts(needed)) {
            request.model = None;
            deltas.notice(ModelNotice::Rerouted { from: model, needs: Modalities::TEXT.lacking(needed).unwrap_or("these inputs").to_string() });
        }
    }
}

/// Every node tried was unreachable rather than answering, or there was none to try (a request
/// the pool refused itself, such as images no model reads, is not).
pub fn is_unreachable(err: &Interrupted) -> bool {
    if err.tried.is_empty() {
        return !err.error.is_request_error();
    }
    let transport = |e: &RemoteError| {
        matches!(
            e,
            RemoteError::Unreachable { .. }
                | RemoteError::Timeout
                | RemoteError::Closed
                | RemoteError::Unsupported { .. }
                | RemoteError::Stalled { .. }
        )
    };
    err.tried.iter().all(|(_, e)| transport(e))
}

/// Why the pool did not take a turn, in one line.
fn describe(err: &Interrupted) -> String {
    if err.tried.is_empty() {
        return err.error.to_string();
    }
    err.tried.iter().map(|(n, e)| format!("{n}: {e}")).collect::<Vec<_>>().join("; ")
}

struct Forward<'a>(&'a DeltaSink);

impl EventSink for Forward<'_> {
    fn event(&self, event: PoolEvent) {
        match event {
            PoolEvent::Text(t) => self.0.text(t),
            PoolEvent::Reasoning(t) => self.0.reasoning(t),
            PoolEvent::Resumed { node, offset } => self.0.notice(ModelNotice::Continued { node, offset }),
            _ => {}
        }
    }
}

#[async_trait]
impl ModelBackend for PoolModel {
    /// `model@node` of the last turn (traces record it after each turn).
    fn name(&self) -> String {
        match self.last_route() {
            Some(Route::Node { node, model }) => format!("{model}@{node}"),
            Some(Route::Fallback) => self.fallback.as_ref().map(|f| f.name()).unwrap_or_default(),
            _ => format!("{}@pool", self.model.as_deref().unwrap_or("model")),
        }
    }

    async fn complete(&self, messages: &[Message], tools: &[ToolSpec], deltas: &DeltaSink, cancel: &CancelToken) -> Result<ModelTurn, ModelError> {
        let mut request = Request::messages(messages.to_vec()).with_tools(tools.to_vec());
        request.model.clone_from(&self.model);
        self.fit(&mut request, deltas).await;
        let err = match self.pool.infer(request, &Forward(deltas), cancel).await {
            Ok(answer) => {
                self.set_last(Route::Node { node: answer.node.clone(), model: answer.model.clone() });
                return Ok(answer.turn());
            }
            Err(e) => e,
        };
        if err.error == RemoteError::Cancelled || cancel.is_cancelled() {
            return Err(ModelError::Cancelled);
        }
        if err.streamed() {
            // Text was shown and could not be resumed: it can't be taken back.
            let node = err.tried.last().map(|(n, _)| n.as_str()).unwrap_or("pool");
            return Err(ModelError::Failed(format!("{node}: {}", err.error)));
        }
        let error = if is_unreachable(&err) { ModelError::PoolUnreachable(describe(&err)) } else { ModelError::Unavailable(describe(&err)) };
        let Some(fallback) = &self.fallback else { return Err(error) };
        deltas.notice(ModelNotice::FellBack { reason: error.to_string(), to: fallback.name() });
        let r = fallback.complete(messages, tools, deltas, cancel).await;
        if r.is_ok() {
            self.set_last(Route::Fallback);
        }
        r
    }
}

/// The pool as a model picker sees it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum PoolStatus {
    /// Looking for an inference node: at the start, and after the followed one went away.
    /// `last` is the catalog seen before, if any.
    Searching { last: Option<Catalog> },
    /// Following `node`'s catalog.
    Live { node: String, catalog: Catalog },
    /// No inference node answered in a whole round; it keeps trying with backoff.
    Unreachable { error: RemoteError, last: Option<Catalog> },
}

impl PoolStatus {
    pub fn is_live(&self) -> bool {
        matches!(self, PoolStatus::Live { .. })
    }

    /// The live catalog, or the last one seen.
    pub fn catalog(&self) -> Option<&Catalog> {
        match self {
            PoolStatus::Live { catalog, .. } => Some(catalog),
            PoolStatus::Searching { last } | PoolStatus::Unreachable { last, .. } => last.as_ref(),
        }
    }

    /// Chat models that can read input needing `needed`, ready ones first (warm, loading,
    /// loadable, busy), then by id. Models that can't read it are left out.
    pub fn chat_models(&self, needed: Modalities) -> Vec<&CatalogModel> {
        let mut models: Vec<&CatalogModel> = self.catalog().map(|c| c.models.iter().filter(|m| m.accepts(needed)).collect()).unwrap_or_default();
        models.sort_by(|a, b| state_rank(a.state).cmp(&state_rank(b.state)).then_with(|| a.id.cmp(&b.id)));
        models
    }
}

fn state_rank(s: ModelState) -> u8 {
    match s {
        ModelState::Warm => 0,
        ModelState::Loading => 1,
        ModelState::Loadable => 2,
        ModelState::Busy => 3,
    }
}

/// The nodes that answered the pool catalog, the followed one first: a [`NodeSource`] for the
/// [`Pool`] that runs turns.
#[derive(Clone, Default)]
pub struct CatalogNodes(Arc<Mutex<Vec<String>>>);

impl CatalogNodes {
    fn first(&self, node: &str) {
        let mut v = self.0.lock().unwrap_or_else(|e| e.into_inner());
        v.retain(|n| n != node);
        v.insert(0, node.to_string());
    }

    fn forget(&self, node: &str) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).retain(|n| n != node);
    }

    pub fn get(&self) -> Vec<String> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[async_trait]
impl NodeSource for CatalogNodes {
    async fn nodes(&self) -> Vec<String> {
        self.get()
    }
}

/// Follows a pool's catalog in the background; stops when dropped.
pub struct PoolWatch {
    rx: watch::Receiver<PoolStatus>,
    nodes: CatalogNodes,
    task: JoinHandle<()>,
}

/// The first retry after a round with no catalog; it doubles up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

impl PoolWatch {
    /// Asks `probe`'s nodes in order for the catalog and follows the first that answers; when
    /// it goes away, the next. Call from a Tokio runtime.
    pub fn spawn(probe: Pool) -> Self {
        let (tx, rx) = watch::channel(PoolStatus::Searching { last: None });
        let nodes = CatalogNodes::default();
        let known = nodes.clone();
        let task = tokio::spawn(async move {
            let (timeout, cert) = (probe.options().accept_timeout, probe.options().cert.clone());
            let mut last: Option<Catalog> = None;
            let mut backoff = FIRST_BACKOFF;
            loop {
                let mut tried: Vec<RemoteError> = Vec::new();
                for node in probe.nodes().await {
                    let opened = tokio::select! {
                        r = tokio::time::timeout(timeout, probe.dialer().open(&node, catalog::PROTOCOL)) => r.unwrap_or(Err(RemoteError::Timeout)),
                        _ = tx.closed() => return,
                    };
                    let mut ch = match opened {
                        Ok(ch) => ch,
                        Err(e) => {
                            known.forget(&node);
                            tried.push(e);
                            continue;
                        }
                    };
                    let mut live = false;
                    let r = {
                        let sub = catalog::subscribe(&mut ch, cert.clone(), |c| {
                            live = true;
                            known.first(&node);
                            last = Some(c.clone());
                            tx.send_replace(PoolStatus::Live { node: node.clone(), catalog: c });
                        });
                        tokio::select! {
                            r = sub => r,
                            _ = tx.closed() => return,
                        }
                    };
                    if live {
                        backoff = FIRST_BACKOFF;
                        tx.send_replace(PoolStatus::Searching { last: last.clone() });
                    } else {
                        known.forget(&node);
                    }
                    tried.push(r.err().unwrap_or(RemoteError::Closed));
                }
                tx.send_replace(PoolStatus::Unreachable { error: reported(tried.iter(), "no inference node to ask"), last: last.clone() });
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = tx.closed() => return,
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        });
        Self { rx, nodes, task }
    }

    pub fn status(&self) -> PoolStatus {
        self.rx.borrow().clone()
    }

    /// A receiver to follow the status elsewhere (a UI thread).
    pub fn receiver(&self) -> watch::Receiver<PoolStatus> {
        self.rx.clone()
    }

    /// Waits for the next status; `None` once the watch has stopped.
    pub async fn changed(&mut self) -> Option<PoolStatus> {
        self.rx.changed().await.ok()?;
        Some(self.rx.borrow_and_update().clone())
    }

    /// The nodes that answered, the followed one first, for the [`Pool`] that runs turns.
    pub fn nodes(&self) -> CatalogNodes {
        self.nodes.clone()
    }
}

impl Drop for PoolWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
#[path = "pool_model_tests.rs"]
mod tests;
