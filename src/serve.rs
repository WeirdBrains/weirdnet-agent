//! The `serve` feature: the inference side of remote inference over `weirdnet-net`.
//!
//! [`Server::start`] accepts `/weirdnet-agent/infer/1`, `/weirdnet-agent/infer/2` and
//! `/weirdnet-agent/catalog/1` streams on a node and serves each through the transport-neutral
//! serve functions, with the stream's Noise-authenticated peer as the [`Caller`]. It announces
//! [`INFERENCE_APP`] so pools find the node (model nodes serving raw infer/2 to inference nodes
//! pass `announce: false`).
//!
//! [`RosterAuth`] is the authorize hook for hosts: `weirdnet_sync::Roster::check_caller` with
//! the peer and the certificate the caller presented. Hosts call it from
//! [`InferHost::authorize`] and [`CatalogHost::authorize`].

use std::sync::Arc;

use weirdnet_net::node::Message;
use weirdnet_net::profile::Profile;
use weirdnet_net::{AppBody, AppResponse, Incoming, Node, PublicKey};
use weirdnet_sync::{CallerCheck, Roster};

pub use crate::net::{INFERENCE_APP, channel, decode_cert, encode_cert, stream_limits};
pub use crate::remote::catalog::{CatalogFeed, CatalogHost};
pub use crate::remote::infer::v2::{EmbedBackend, InferBackend, ModelAdapter, Output};
pub use crate::remote::infer::{Admission, InferHost};
use crate::remote::{Authorize, Caller, RemoteError, catalog, infer};

/// The roster as it is now (the app keeps it current from `weirdnet-sync`).
pub type RosterView = Arc<dyn Fn() -> Roster + Send + Sync>;

/// Admits only the user's linked devices (`Roster::check_caller`). Refusals carry the roster's
/// reason (`unknown-device`, `revoked`, `wrong-user`, `needs-admission`).
pub struct RosterAuth {
    profile: &'static Profile,
    user: PublicKey,
    roster: RosterView,
}

impl RosterAuth {
    pub fn new(profile: &'static Profile, user: PublicKey, roster: RosterView) -> Self {
        Self { profile, user, roster }
    }

    pub fn check(&self, caller: &Caller) -> CallerCheck {
        let presented = match caller.cert_bytes.as_deref().map(decode_cert) {
            None => None,
            Some(Ok(c)) => Some(c),
            Some(Err(_)) => return CallerCheck::WrongUser,
        };
        (self.roster)().check_caller(self.profile, &self.user, &caller.peer, presented.as_ref())
    }
}

impl Authorize for RosterAuth {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        match self.check(caller).reason() {
            None => Ok(()),
            Some(reason) => Err(RemoteError::refused(reason)),
        }
    }
}

/// Serving tasks on a node; dropping it stops accepting (calls under way carry on).
pub struct Server {
    tasks: Vec<tokio::task::JoinHandle<()>>,
    announced: Option<Box<dyn Fn() + Send + Sync>>,
}

/// What a [`Server`] serves.
#[derive(Clone)]
pub struct Serving {
    /// infer/1 and infer/2, sharing the admission slots.
    pub infer: Option<(Arc<dyn InferHost>, Admission)>,
    pub catalog: Option<Arc<dyn CatalogHost>>,
    /// Announce [`INFERENCE_APP`] (inference nodes; not model nodes).
    pub announce: bool,
}

fn accept_loop(mut incoming: Incoming, serve: impl Fn(crate::remote::Channel, Caller) + Send + 'static) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(stream) = incoming.next().await {
            let caller = Caller::new(stream.peer().to_string());
            serve(channel(stream), caller);
        }
    })
}

impl Server {
    /// Starts serving on `node`. Call from a Tokio runtime.
    pub fn start<Q: Message, R: Message>(node: &Arc<Node<Q, R>>, serving: Serving) -> Result<Server, RemoteError> {
        let net = |e: weirdnet_net::NetError| RemoteError::Failed { message: e.to_string() };
        let mut tasks = Vec::new();
        if let Some((host, admission)) = serving.infer {
            let (h, a) = (host.clone(), admission.clone());
            tasks.push(accept_loop(node.accept_streams(infer::PROTOCOL, stream_limits()).map_err(net)?, move |ch, caller| {
                let (h, a) = (h.clone(), a.clone());
                tokio::spawn(async move {
                    let _ = infer::serve(ch, &caller, h.as_ref(), &a).await;
                });
            }));
            tasks.push(accept_loop(node.accept_streams(infer::v2::PROTOCOL, stream_limits()).map_err(net)?, move |ch, caller| {
                let (h, a) = (host.clone(), admission.clone());
                tokio::spawn(async move {
                    let _ = infer::v2::serve(ch, caller, h.as_ref(), &a).await;
                });
            }));
        }
        if let Some(host) = serving.catalog {
            tasks.push(accept_loop(node.accept_streams(catalog::PROTOCOL, stream_limits()).map_err(net)?, move |ch, caller| {
                let h = host.clone();
                tokio::spawn(async move {
                    let _ = catalog::serve(ch, caller, h.as_ref()).await;
                });
            }));
        }
        let mut announced: Option<Box<dyn Fn() + Send + Sync>> = None;
        if serving.announce {
            let protocols = vec![infer::PROTOCOL, infer::v2::PROTOCOL, catalog::PROTOCOL];
            let body = crate::remote::to_cbor(&protocols)?;
            node.serve_app(
                INFERENCE_APP,
                Arc::new(move |_peer, _req| {
                    let body = body.clone();
                    async move { Ok(AppResponse::new(200).header("content-type", "application/cbor").body(AppBody::bytes(body))) }
                }),
            )
            .map_err(|e| RemoteError::Failed { message: e.to_string() })?;
            let n = node.clone();
            announced = Some(Box::new(move || {
                n.stop_serving_app(INFERENCE_APP);
            }));
        }
        Ok(Server { tasks, announced })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
        if let Some(stop) = self.announced.take() {
            stop();
        }
    }
}
