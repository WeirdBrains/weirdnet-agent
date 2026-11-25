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
//! [`InferHost::authorize`] and [`CatalogHost::authorize`]. With
//! [`require_roles`](RosterAuth::require_roles) it also needs one of the device roles the user
//! granted the caller (the grants in the `weirdnet-sync` 0.7 or later roster, `Roster::roles_of`).
//!
//! [`Server::start_with`] also serves `/weirdnet-agent/task/1` ([`ServingExtras::tasks`]): each
//! task is admitted by the given [`Authorize`] and built by [`TaskHost::session_for`].

use std::sync::Arc;

use weirdnet_net::node::Message;
use weirdnet_net::profile::Profile;
use weirdnet_net::{AppBody, AppResponse, Incoming, Node, PublicKey};
use weirdnet_sync::{CallerCheck, Roster};

pub use crate::net::{INFERENCE_APP, channel, decode_cert, encode_cert, stream_limits};
pub use crate::remote::catalog::{CatalogFeed, CatalogHost};
pub use crate::remote::infer::v2::{EmbedBackend, InferBackend, ModelAdapter, Output};
pub use crate::remote::infer::{Admission, InferHost};
pub use crate::remote::task::TaskHost;
use crate::remote::{Admitted, Authorize, Caller, RemoteError, catalog, infer, task};

/// The roster as it is now (the app keeps it current from `weirdnet-sync`).
pub type RosterView = Arc<dyn Fn() -> Roster + Send + Sync>;

/// The device roles a peer holds, `None` without a grant. With `weirdnet-sync` 0.7 or later:
/// `Arc::new(move |peer| identity.roster().roles_of(peer))`.
pub type RoleView = Arc<dyn Fn(&str) -> Option<Vec<String>> + Send + Sync>;

/// Admits only the user's linked devices (`Roster::check_caller`). Refusals carry the roster's
/// reason (`unknown-device`, `revoked`, `wrong-user`, `needs-admission`).
///
/// With [`require_roles`](Self::require_roles) a linked caller also needs one of the roles
/// (refused with `role` otherwise); without it any linked device is let in, as before.
pub struct RosterAuth {
    profile: &'static Profile,
    user: PublicKey,
    roster: RosterView,
    roles: Option<RoleView>,
    any_of: Vec<String>,
}

impl RosterAuth {
    pub fn new(profile: &'static Profile, user: PublicKey, roster: RosterView) -> Self {
        Self { profile, user, roster, roles: None, any_of: Vec::new() }
    }

    /// Needs one of `any_of` among the caller's granted roles. An empty `any_of` needs none but
    /// still reports roles in [`admit`](Authorize::admit).
    pub fn require_roles(mut self, any_of: &[&str], roles: RoleView) -> Self {
        self.any_of = any_of.iter().map(|r| r.to_string()).collect();
        self.roles = Some(roles);
        self
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
        self.admit(caller).map(|_| ())
    }

    fn admit(&self, caller: &Caller) -> Result<Admitted, RemoteError> {
        if let Some(reason) = self.check(caller).reason() {
            return Err(RemoteError::refused(reason));
        }
        let roles = self.roles.as_ref().and_then(|view| view(&caller.peer));
        if !self.any_of.is_empty() && !roles.as_ref().is_some_and(|have| have.iter().any(|r| self.any_of.contains(r))) {
            return Err(RemoteError::refused("role"));
        }
        Ok(Admitted::new(caller.peer.clone(), roles))
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

/// What a [`Server`] serves beyond [`Serving`] (a separate struct so `Serving` literals keep
/// compiling in 0.4.x; it folds into `Serving` in 0.5).
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct ServingExtras {
    /// task/1: whole agent tasks, admitted by the `Authorize` (usually a [`RosterAuth`]).
    pub tasks: Option<(Arc<dyn TaskHost>, Arc<dyn Authorize>)>,
}

impl ServingExtras {
    pub fn tasks(mut self, host: Arc<dyn TaskHost>, auth: Arc<dyn Authorize>) -> Self {
        self.tasks = Some((host, auth));
        self
    }
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
        Self::start_with(node, serving, ServingExtras::default())
    }

    /// [`start`](Self::start) plus [`ServingExtras`]. When tasks are served, task/1 is added to
    /// the [`INFERENCE_APP`] announcement.
    pub fn start_with<Q: Message, R: Message>(node: &Arc<Node<Q, R>>, serving: Serving, extras: ServingExtras) -> Result<Server, RemoteError> {
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
        let serves_tasks = extras.tasks.is_some();
        if let Some((host, auth)) = extras.tasks {
            tasks.push(accept_loop(node.accept_streams(task::PROTOCOL, stream_limits()).map_err(net)?, move |ch, caller| {
                let (h, a) = (host.clone(), auth.clone());
                tokio::spawn(async move {
                    let _ = task::serve_for(ch, caller, a.as_ref(), h.as_ref()).await;
                });
            }));
        }
        let mut announced: Option<Box<dyn Fn() + Send + Sync>> = None;
        if serving.announce {
            let mut protocols = vec![infer::PROTOCOL, infer::v2::PROTOCOL, catalog::PROTOCOL];
            if serves_tasks {
                protocols.push(task::PROTOCOL);
            }
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
