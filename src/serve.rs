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
//!
//! # Guests: sharing a pool with another Weirdnet identity (since 0.5.9)
//!
//! [`GuestAuth`] admits a caller whose certificate chains to a *different* Weirdnet user (a
//! guest), with the same checks as the roster makes for the user's own devices: the guest
//! presents a credential ([`encode_credential`]: its `NodeCert` plus its user's public key);
//! the key must hash to the certificate's user id, the user's signature over the peer id must
//! verify, the certificate must be for the transport-authenticated peer, and it must not be
//! revoked (a revocation must carry its signature) nor issued in the future (beyond the clock
//! skew). Only then does the host's guest policy (a closure over its synced guest list) see the
//! [`Guest`] and the [`GuestScope`] (`catalog` or `infer`) and decide. Guests are never
//! members of the roster, never get the root seed or synced types, and are never admitted to
//! task/1 or any protocol the host does not ask [`GuestAuth`] about.
//!
//! [`SharedPoolAuth`] puts the two together for an inference node's [`InferHost::authorize`]
//! and [`CatalogHost::authorize`]: the user's own devices by the roster ([`RosterAuth`],
//! unchanged), guests by [`GuestAuth`]. Without [`with_guests`](SharedPoolAuth::with_guests) it
//! is exactly the roster check: guests are refused (`wrong-user`), as by every host before
//! 0.5.9. A guest credential naming the host's own user always goes to the roster.
//!
//! Revocation is live: the policy runs on every infer/1 and infer/2 request (one per stream) and
//! every catalog query; a catalog subscription is re-checked before each update and every few
//! seconds when the host re-checks ([`CatalogHost::recheck`], [`CatalogFeed::rechecking`]), and
//! closed with the policy's refusal. An answer already streaming finishes.

use std::sync::Arc;

use weirdnet_net::node::Message;
use weirdnet_net::profile::Profile;
use weirdnet_net::user::NodeCert;
use weirdnet_net::{AppBody, AppResponse, Incoming, Node, PublicKey};
use weirdnet_sync::{CallerCheck, Roster};

pub use crate::net::{INFERENCE_APP, Presented, channel, decode_cert, decode_presented, encode_cert, encode_credential, stream_limits};
pub use crate::remote::catalog::{CatalogFeed, CatalogHost};
pub use crate::remote::infer::v2::{EmbedBackend, InferBackend, ModelAdapter, Output, TranscribeBackend};
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

    /// The roster's verdict on `caller`. The presented certificate may be bare or (since 0.5.9)
    /// inside a guest credential; either way it is checked against this user's key, so a
    /// certificate of another user is `WrongUser`.
    pub fn check(&self, caller: &Caller) -> CallerCheck {
        let presented = match caller.cert_bytes.as_deref().map(decode_presented) {
            None => None,
            Some(Ok(p)) => Some(p.cert),
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

/// What a guest may use: the protocol being asked for. A guest policy decides per scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GuestScope {
    /// catalog/1: query or subscribe.
    Catalog,
    /// infer/1 and infer/2: chat, embeddings, transcription.
    Infer,
}

impl GuestScope {
    pub fn as_str(self) -> &'static str {
        match self {
            GuestScope::Catalog => "catalog",
            GuestScope::Infer => "infer",
        }
    }
}

/// A verified guest: a device of another Weirdnet user, its certificate checked against that
/// user's key and the transport peer. Who may use the pool is the guest policy's decision.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Guest {
    /// The guest's Weirdnet user id (32 hex characters).
    pub user_id: String,
    /// The guest's user public key (protobuf), which hashes to `user_id`.
    pub user_key: Vec<u8>,
    /// The transport-authenticated peer id (equal to `cert.peer_id`).
    pub peer: String,
    /// The device label its user gave it (not authenticated: the signature covers user, peer and
    /// time only).
    pub label: String,
    /// When its user certified the device (Unix ms).
    pub issued_at: i64,
    pub cert: NodeCert,
}

/// The host's guest policy: `Ok` admits the guest for the scope, an error is sent back as the
/// refusal (for example `RemoteError::refused("not-shared")`). Called on every check, so it
/// should read the current synced guest list (revoking there takes effect on the next request).
pub type GuestPolicy = Arc<dyn Fn(&Guest, GuestScope) -> Result<(), RemoteError> + Send + Sync>;

/// How far in the future a certificate's `issued_at` may be by default (clock skew): 10 minutes.
pub const DEFAULT_CLOCK_SKEW: std::time::Duration = std::time::Duration::from_secs(600);

/// Verifies guests (devices of another Weirdnet user) and asks the [`GuestPolicy`]. See the
/// [module docs](self#guests-sharing-a-pool-with-another-weirdnet-identity-since-059).
///
/// Refusal reasons: `guest-no-cert` (nothing presented), `guest-no-key` (a bare certificate,
/// not a guest credential), `guest-bad-key` (the key does not decode or does not hash to the
/// certificate's user id), `wrong-peer` (the certificate is for another peer), `guest-bad-cert`
/// (the signature, or a revocation's, does not verify), `revoked`, `guest-future-cert`, and
/// `own-user` (the host's own user: that is the roster's call, never a guest's); then whatever
/// the policy answers.
pub struct GuestAuth {
    profile: &'static Profile,
    host_user_id: String,
    policy: GuestPolicy,
    skew_ms: i64,
}

impl GuestAuth {
    /// Guests of the pool owned by `host_user`, admitted by `policy`.
    pub fn new(profile: &'static Profile, host_user: &PublicKey, policy: GuestPolicy) -> Self {
        Self { profile, host_user_id: weirdnet_net::user::user_id(host_user), policy, skew_ms: DEFAULT_CLOCK_SKEW.as_millis() as i64 }
    }

    /// How far in the future `issued_at` may be ([`DEFAULT_CLOCK_SKEW`]).
    pub fn with_clock_skew(mut self, skew: std::time::Duration) -> Self {
        self.skew_ms = i64::try_from(skew.as_millis()).unwrap_or(i64::MAX);
        self
    }

    /// The cryptographic checks only (no policy): who the caller is, if a valid guest.
    pub fn verify(&self, caller: &Caller) -> Result<Guest, RemoteError> {
        self.verify_at(caller, weirdnet_net::now_ms())
    }

    fn verify_at(&self, caller: &Caller, now_ms: i64) -> Result<Guest, RemoteError> {
        let bytes = caller.cert_bytes.as_deref().ok_or_else(|| RemoteError::refused("guest-no-cert"))?;
        let presented = decode_presented(bytes).map_err(|_| RemoteError::refused("guest-bad-cert"))?;
        let cert = presented.cert;
        let key_bytes = presented.user_key.ok_or_else(|| RemoteError::refused("guest-no-key"))?;
        let key = weirdnet_net::user::decode_public(&key_bytes).map_err(|_| RemoteError::refused("guest-bad-key"))?;
        let user_id = weirdnet_net::user::user_id(&key);
        if user_id != cert.user_id {
            return Err(RemoteError::refused("guest-bad-key"));
        }
        if user_id == self.host_user_id {
            return Err(RemoteError::refused("own-user"));
        }
        if cert.peer_id != caller.peer {
            return Err(RemoteError::refused("wrong-peer"));
        }
        if !self.profile.verify_cert(&key, &cert) {
            return Err(RemoteError::refused("guest-bad-cert"));
        }
        if !cert.active() {
            return Err(RemoteError::refused("revoked"));
        }
        if cert.issued_at > now_ms.saturating_add(self.skew_ms) {
            return Err(RemoteError::refused("guest-future-cert"));
        }
        Ok(Guest { user_id, user_key: key_bytes, peer: caller.peer.clone(), label: cert.label.clone(), issued_at: cert.issued_at, cert })
    }

    /// [`verify`](Self::verify), then the policy for `scope`.
    pub fn admit(&self, caller: &Caller, scope: GuestScope) -> Result<Guest, RemoteError> {
        let guest = self.verify(caller)?;
        (self.policy)(&guest, scope)?;
        Ok(guest)
    }
}

/// Who an inference node let in ([`SharedPoolAuth::identify`]).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum CallerIdentity {
    /// One of the user's own linked devices, by the roster.
    Own(Admitted),
    /// A guest of another Weirdnet user, by the guest policy.
    Guest(Guest),
}

impl CallerIdentity {
    pub fn is_guest(&self) -> bool {
        matches!(self, CallerIdentity::Guest(_))
    }

    pub fn guest(&self) -> Option<&Guest> {
        match self {
            CallerIdentity::Guest(g) => Some(g),
            CallerIdentity::Own(_) => None,
        }
    }

    pub fn peer(&self) -> &str {
        match self {
            CallerIdentity::Own(a) => &a.peer,
            CallerIdentity::Guest(g) => &g.peer,
        }
    }
}

/// An inference node's caller check with optional guests: the roster for the user's own
/// devices, [`GuestAuth`] for other users' (only with [`with_guests`](Self::with_guests);
/// without it this is exactly [`RosterAuth`]). Use [`authorize_for`](Self::authorize_for) in
/// [`InferHost::authorize`] (with [`GuestScope::Infer`]) and give
/// [`scoped`](Self::scoped)`(GuestScope::Catalog)` to a [`CatalogFeed`]. It deliberately does not
/// implement [`Authorize`] itself, so it can't be handed to task/1 or another protocol by
/// mistake: hand those the [`RosterAuth`].
#[derive(Clone)]
pub struct SharedPoolAuth {
    roster: Arc<RosterAuth>,
    guests: Option<Arc<GuestAuth>>,
}

impl SharedPoolAuth {
    /// The roster only (guests refused).
    pub fn new(roster: Arc<RosterAuth>) -> Self {
        Self { roster, guests: None }
    }

    /// Also admit guests by `guests`.
    pub fn with_guests(mut self, guests: Arc<GuestAuth>) -> Self {
        self.guests = Some(guests);
        self
    }

    pub fn admits_guests(&self) -> bool {
        self.guests.is_some()
    }

    /// Who `caller` is, if let in for `scope`. A caller presenting a guest credential of another
    /// user goes to the guest check (when guests are on); everyone else to the roster.
    pub fn identify(&self, caller: &Caller, scope: GuestScope) -> Result<CallerIdentity, RemoteError> {
        if let Some(guests) = &self.guests
            && let Some(Ok(p)) = caller.cert_bytes.as_deref().map(decode_presented)
            && p.user_key.is_some()
            && p.cert.user_id != guests.host_user_id
        {
            return guests.admit(caller, scope).map(CallerIdentity::Guest);
        }
        self.roster.admit(caller).map(CallerIdentity::Own)
    }

    pub fn authorize_for(&self, caller: &Caller, scope: GuestScope) -> Result<(), RemoteError> {
        self.identify(caller, scope).map(|_| ())
    }

    /// An [`Authorize`] for one scope (for [`CatalogFeed::new`]). Its `admit` reports a guest's
    /// peer with no roles.
    pub fn scoped(&self, scope: GuestScope) -> Arc<dyn Authorize> {
        Arc::new(Scoped { auth: self.clone(), scope })
    }
}

struct Scoped {
    auth: SharedPoolAuth,
    scope: GuestScope,
}

impl Authorize for Scoped {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        self.auth.authorize_for(caller, self.scope)
    }

    fn admit(&self, caller: &Caller) -> Result<Admitted, RemoteError> {
        match self.auth.identify(caller, self.scope)? {
            CallerIdentity::Own(a) => Ok(a),
            CallerIdentity::Guest(g) => Ok(Admitted::new(g.peer, None)),
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

#[cfg(test)]
mod guest_tests {
    use std::sync::Mutex;

    use weirdnet_net::{Keypair, WEIRDNET};

    use super::*;

    const PEER: &str = "12D3KooWGuestPeer";

    struct Fixture {
        host: Keypair,
        guest: Keypair,
        cert: NodeCert,
        allowed: Arc<Mutex<Vec<(String, GuestScope)>>>,
    }

    fn fixture() -> Fixture {
        let (host, guest) = (Keypair::generate_ed25519(), Keypair::generate_ed25519());
        let uid = weirdnet_net::user::user_id(&guest.public());
        let cert = WEIRDNET.certify(&guest, &uid, PEER, "morph", weirdnet_net::now_ms()).unwrap();
        let allowed = Arc::new(Mutex::new(vec![(uid, GuestScope::Catalog), (weirdnet_net::user::user_id(&guest.public()), GuestScope::Infer)]));
        Fixture { host, guest, cert, allowed }
    }

    fn guests(f: &Fixture) -> GuestAuth {
        let allowed = f.allowed.clone();
        let policy: GuestPolicy = Arc::new(move |g: &Guest, scope| {
            if allowed.lock().unwrap().iter().any(|(u, s)| *u == g.user_id && *s == scope) { Ok(()) } else { Err(RemoteError::refused("not-shared")) }
        });
        GuestAuth::new(&WEIRDNET, &f.host.public(), policy)
    }

    fn caller(peer: &str, key: &Keypair, cert: &NodeCert) -> Caller {
        Caller::new(peer).with_cert(encode_credential(&key.public(), cert))
    }

    fn refused(r: Result<impl std::fmt::Debug, RemoteError>) -> String {
        match r {
            Err(RemoteError::Refused { message }) => message,
            other => panic!("not refused: {other:?}"),
        }
    }

    #[test]
    fn a_valid_guest_is_verified_and_admitted_per_scope() {
        let f = fixture();
        let auth = guests(&f);
        let g = auth.admit(&caller(PEER, &f.guest, &f.cert), GuestScope::Infer).unwrap();
        assert_eq!((g.user_id.as_str(), g.peer.as_str(), g.label.as_str()), (f.cert.user_id.as_str(), PEER, "morph"));
        assert_eq!(g.user_key, f.guest.public().encode_protobuf());
        // Scope: catalog only.
        f.allowed.lock().unwrap().retain(|(_, s)| *s == GuestScope::Catalog);
        assert!(auth.admit(&caller(PEER, &f.guest, &f.cert), GuestScope::Catalog).is_ok());
        assert_eq!(refused(auth.admit(&caller(PEER, &f.guest, &f.cert), GuestScope::Infer)), "not-shared");
        // Revoked in the policy: refused on the very next check.
        f.allowed.lock().unwrap().clear();
        assert_eq!(refused(auth.admit(&caller(PEER, &f.guest, &f.cert), GuestScope::Catalog)), "not-shared");
        assert!(auth.verify(&caller(PEER, &f.guest, &f.cert)).is_ok(), "verify alone does not ask the policy");
    }

    #[test]
    fn forged_wrong_peer_revoked_and_future_certs_are_refused() {
        let f = fixture();
        let auth = guests(&f);
        let s = GuestScope::Infer;
        // Nothing, or a bare certificate (no key to check it with).
        assert_eq!(refused(auth.admit(&Caller::new(PEER), s)), "guest-no-cert");
        assert_eq!(refused(auth.admit(&Caller::new(PEER).with_cert(encode_cert(&f.cert)), s)), "guest-no-key");
        assert_eq!(refused(auth.admit(&Caller::new(PEER).with_cert(b"junk".to_vec()), s)), "guest-bad-cert");
        // Presented by another peer than the certified one.
        assert_eq!(refused(auth.admit(&caller("12D3KooWOther", &f.guest, &f.cert), s)), "wrong-peer");
        // A key that is not the certificate's user.
        let other = Keypair::generate_ed25519();
        assert_eq!(refused(auth.admit(&caller(PEER, &other, &f.cert), s)), "guest-bad-key");
        // A bad signature: tampered bytes, or a certificate claiming the guest's id signed by another key.
        let mut bad = f.cert.clone();
        bad.cert[0] ^= 1;
        assert_eq!(refused(auth.admit(&caller(PEER, &f.guest, &bad), s)), "guest-bad-cert");
        let forged = WEIRDNET.certify(&other, &f.cert.user_id, PEER, "x", weirdnet_net::now_ms()).unwrap();
        assert_eq!(refused(auth.admit(&caller(PEER, &f.guest, &forged), s)), "guest-bad-cert");
        let mut relabeled = f.cert.clone();
        relabeled.issued_at += 1;
        assert_eq!(refused(auth.admit(&caller(PEER, &f.guest, &relabeled), s)), "guest-bad-cert");
        // Revoked by its user (signed), or a revocation time without its signature.
        let mut revoked = f.cert.clone();
        let at = weirdnet_net::now_ms();
        revoked.revoked_at = Some(at);
        revoked.revocation = Some(WEIRDNET.revoke(&f.guest, &f.cert.user_id, PEER, at).unwrap());
        assert_eq!(refused(auth.admit(&caller(PEER, &f.guest, &revoked), s)), "revoked");
        revoked.revocation = None;
        assert_eq!(refused(auth.admit(&caller(PEER, &f.guest, &revoked), s)), "guest-bad-cert");
        // Issued in the future beyond the skew.
        let later = WEIRDNET.certify(&f.guest, &f.cert.user_id, PEER, "x", weirdnet_net::now_ms() + 3_600_000).unwrap();
        assert_eq!(refused(auth.admit(&caller(PEER, &f.guest, &later), s)), "guest-future-cert");
        let lenient = guests(&f).with_clock_skew(std::time::Duration::from_secs(7200));
        assert!(lenient.admit(&caller(PEER, &f.guest, &later), s).is_ok());
        // The host's own user is never a guest.
        let own = WEIRDNET.certify(&f.host, &weirdnet_net::user::user_id(&f.host.public()), PEER, "x", weirdnet_net::now_ms()).unwrap();
        assert_eq!(refused(auth.admit(&caller(PEER, &f.host, &own), s)), "own-user");
        // A credential of an unknown version.
        let v2 = crate::remote::to_cbor(&serde_json::json!({"guest": 2, "key": f.guest.public().encode_protobuf(), "cert": f.cert})).unwrap();
        assert_eq!(refused(auth.admit(&Caller::new(PEER).with_cert(v2), s)), "guest-bad-cert");
    }

    #[test]
    fn shared_pool_auth_is_the_roster_unless_guests_are_on() {
        let f = fixture();
        let host_uid = weirdnet_net::user::user_id(&f.host.public());
        let own_peer = "12D3KooWOwnDevice";
        let own = weirdnet_sync::Roster::self_certify(&WEIRDNET, &f.host, own_peer, "phone", weirdnet_net::now_ms()).unwrap();
        let mut roster = weirdnet_sync::Roster::new();
        roster.upsert(own.clone());
        let roster = Arc::new(Mutex::new(roster));
        let view: RosterView = {
            let r = roster.clone();
            Arc::new(move || r.lock().unwrap().clone())
        };
        let roster_auth = Arc::new(RosterAuth::new(&WEIRDNET, f.host.public(), view));
        let guest = caller(PEER, &f.guest, &f.cert);

        // Default deny: no guests, the guest credential is another user's certificate.
        let plain = SharedPoolAuth::new(roster_auth.clone());
        assert!(!plain.admits_guests());
        for scope in [GuestScope::Catalog, GuestScope::Infer] {
            assert_eq!(refused(plain.identify(&guest, scope)), "wrong-user");
        }
        assert_eq!(refused(roster_auth.authorize(&guest)), "wrong-user", "RosterAuth itself is unchanged");
        assert_eq!(refused(plain.scoped(GuestScope::Catalog).authorize(&guest)), "wrong-user");

        // Guests on: the guest by the policy, own devices by the roster as before.
        let shared = SharedPoolAuth::new(roster_auth.clone()).with_guests(Arc::new(guests(&f)));
        let who = shared.identify(&guest, GuestScope::Infer).unwrap();
        assert!(who.is_guest());
        assert_eq!((who.peer(), who.guest().unwrap().user_id.as_str()), (PEER, f.cert.user_id.as_str()));
        let admitted = shared.scoped(GuestScope::Catalog).admit(&guest).unwrap();
        assert_eq!((admitted.peer.as_str(), admitted.roles.clone()), (PEER, None));
        let mine = Caller::new(own_peer).with_cert(encode_cert(&own));
        assert!(matches!(shared.identify(&mine, GuestScope::Infer).unwrap(), CallerIdentity::Own(a) if a.peer == own_peer));
        // An own device presenting its certificate inside a credential still goes to the roster.
        let mine_cred = caller(own_peer, &f.host, &own);
        assert!(!shared.identify(&mine_cred, GuestScope::Infer).unwrap().is_guest());
        assert_eq!(own.user_id, host_uid);
        // A revoked own device is not let in through the guest path.
        let at = weirdnet_net::now_ms();
        let mut gone = own.clone();
        gone.revoked_at = Some(at);
        gone.revocation = Some(WEIRDNET.revoke(&f.host, &host_uid, own_peer, at).unwrap());
        roster.lock().unwrap().upsert(gone);
        assert_eq!(refused(shared.identify(&mine_cred, GuestScope::Infer)), "revoked");
        // Revoking the guest: refused at once.
        f.allowed.lock().unwrap().clear();
        assert_eq!(refused(shared.identify(&guest, GuestScope::Infer)), "not-shared");
        // A stranger with a bare certificate of its own user: the roster's answer.
        let stranger = Keypair::generate_ed25519();
        let sc = WEIRDNET.certify(&stranger, &weirdnet_net::user::user_id(&stranger.public()), "12D3KooWS", "x", 1).unwrap();
        assert_eq!(refused(shared.identify(&Caller::new("12D3KooWS").with_cert(encode_cert(&sc)), GuestScope::Infer)), "wrong-user");
    }

    #[test]
    fn credentials_round_trip_and_bare_certs_still_decode() {
        let f = fixture();
        let p = decode_presented(&encode_credential(&f.guest.public(), &f.cert)).unwrap();
        assert_eq!((p.cert, p.user_key), (f.cert.clone(), Some(f.guest.public().encode_protobuf())));
        let p = decode_presented(&encode_cert(&f.cert)).unwrap();
        assert_eq!((p.cert, p.user_key), (f.cert.clone(), None));
        assert!(decode_cert(&encode_credential(&f.guest.public(), &f.cert)).is_err(), "older hosts can't read a credential as a cert");
    }
}
