//! Pool share links: how someone with a different Weirdnet identity finds a pool (since 0.5.9).
//!
//! A [`PoolShare`] names the pool's owner (a Weirdnet user id), an optional label, the pool's
//! inference nodes (peer ids and addresses to dial them, direct or relay circuit) and an
//! optional expiry. It holds no secret: no root seed, no keys, no certificates. Whoever holds the
//! link can *dial* the nodes; only the owner's nodes decide who may *use* them, by the guest's
//! own certificate and the owner's synced guest policy (`serve::GuestAuth`), re-checked on every
//! request, so revoking a guest there takes effect at once and the link needs no revocation.
//!
//! The string forms follow the Weirdnet token conventions (a prefix and base64url JSON, safe to
//! paste anywhere):
//!
//! ```text
//! token  wnet-pool1:<base64url(JSON), no padding>
//! link   weirdnet-pool://share/<the same base64url>
//! JSON   {"v":1,"owner":"<32 hex>","name":"...","nodes":[{"peer_id":"12D3Koo...","addrs":["/ip4/.../p2p/..."]}],"expires_at":<ms>}
//! ```
//!
//! `name` and `expires_at` are left out when unset; unknown fields are ignored, so later 0.5.x
//! versions can add some without a new `v`. A link with a higher `v` is refused.
//!
//! Over Weirdnet (the `pool` feature), `pool::connect_shared` turns a share into a
//! [`Pool`](crate::remote::pool::Pool) that presents the guest's own certificate.

use std::fmt;
use std::str::FromStr;

use base64::Engine;
use serde::{Deserialize, Serialize};

/// The token prefix.
pub const SHARE_PREFIX: &str = "wnet-pool1:";
/// The link prefix (the app scheme `weirdnet-pool://`, never the reserved bare `weirdnet://`).
pub const SHARE_LINK: &str = "weirdnet-pool://share/";
/// The share format version this crate writes and reads.
pub const SHARE_VERSION: u32 = 1;
/// The longest token or link read (bytes, before decoding).
pub const MAX_SHARE_LEN: usize = 16 * 1024;
/// The most nodes a share may list.
pub const MAX_SHARE_NODES: usize = 64;

/// Why a share could not be read or used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("pool share: {0}")]
pub struct ShareError(pub String);

fn err(m: impl Into<String>) -> ShareError {
    ShareError(m.into())
}

/// One inference node of a shared pool: its peer id and addresses to dial it (the shape of
/// `weirdnet_net::PeerHint`). Addresses may be empty when the guest's node finds the peer some
/// other way (a relay or discovery).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SharedNode {
    pub peer_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addrs: Vec<String>,
}

impl SharedNode {
    pub fn new(peer_id: impl Into<String>, addrs: Vec<String>) -> Self {
        Self { peer_id: peer_id.into(), addrs }
    }
}

/// A pool share link. See the [module docs](self) for the wire and string forms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PoolShare {
    /// The format version ([`SHARE_VERSION`]).
    pub v: u32,
    /// The pool owner's Weirdnet user id (32 lowercase hex characters,
    /// `weirdnet_net::user::user_id` of the owner's key).
    pub owner: String,
    /// A label for people (for example "Ymir at home").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The pool's inference nodes, best first.
    pub nodes: Vec<SharedNode>,
    /// When the link stops being offered (Unix ms). A hint for the guest's app: the owner's guest
    /// policy, not this field, decides who is let in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
}

fn b64() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
}

impl PoolShare {
    /// A share of `owner`'s pool, with no nodes yet.
    pub fn new(owner: impl Into<String>) -> Self {
        Self { v: SHARE_VERSION, owner: owner.into(), name: None, nodes: Vec::new(), expires_at: None }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Adds an inference node (a peer id and the addresses that reach it).
    pub fn with_node(mut self, peer_id: impl Into<String>, addrs: Vec<String>) -> Self {
        self.nodes.push(SharedNode::new(peer_id, addrs));
        self
    }

    pub fn with_expires_at(mut self, at_ms: i64) -> Self {
        self.expires_at = Some(at_ms);
        self
    }

    /// The nodes' peer ids, in order (a [`NodeSource`](crate::remote::pool::NodeSource) as a
    /// `Vec<String>`).
    pub fn node_ids(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.peer_id.clone()).collect()
    }

    /// Past its expiry at `now_ms`.
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_at.is_some_and(|at| now_ms >= at)
    }

    /// The version is known, the owner is a user id, and there are 1 to [`MAX_SHARE_NODES`]
    /// nodes with non-empty, distinct peer ids. (Peer ids and addresses are parsed when dialing.)
    pub fn validate(&self) -> Result<(), ShareError> {
        if self.v == 0 || self.v > SHARE_VERSION {
            return Err(err(format!("version {} is not supported (this reads up to {SHARE_VERSION})", self.v)));
        }
        if self.owner.len() != 32 || !self.owner.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return Err(err("the owner is not a Weirdnet user id (32 lowercase hex characters)"));
        }
        if self.nodes.is_empty() {
            return Err(err("no inference nodes"));
        }
        if self.nodes.len() > MAX_SHARE_NODES {
            return Err(err(format!("more than {MAX_SHARE_NODES} nodes")));
        }
        for (i, n) in self.nodes.iter().enumerate() {
            if n.peer_id.trim().is_empty() || n.peer_id.trim() != n.peer_id {
                return Err(err("a node has no peer id"));
            }
            if self.nodes[..i].iter().any(|m| m.peer_id == n.peer_id) {
                return Err(err(format!("node {} is listed twice", n.peer_id)));
            }
        }
        Ok(())
    }

    fn body(&self) -> Result<String, ShareError> {
        self.validate()?;
        let json = serde_json::to_vec(self).map_err(|e| err(e.to_string()))?;
        Ok(b64().encode(json))
    }

    /// The token form, `wnet-pool1:<base64url>`.
    pub fn to_token(&self) -> Result<String, ShareError> {
        Ok(format!("{SHARE_PREFIX}{}", self.body()?))
    }

    /// The link form, `weirdnet-pool://share/<base64url>`.
    pub fn to_link(&self) -> Result<String, ShareError> {
        Ok(format!("{SHARE_LINK}{}", self.body()?))
    }

    /// Reads a token or a link (whitespace and line breaks ignored, a trailing `/` allowed) and
    /// validates it.
    pub fn parse(input: &str) -> Result<Self, ShareError> {
        if input.len() > MAX_SHARE_LEN {
            return Err(err("too long"));
        }
        let s: String = input.split_whitespace().collect();
        let body = if let Some(b) = s.strip_prefix(SHARE_PREFIX) {
            b
        } else if let Some(b) = s.get(..SHARE_LINK.len()).filter(|h| h.eq_ignore_ascii_case(SHARE_LINK)).map(|_| &s[SHARE_LINK.len()..]) {
            b.trim_end_matches('/')
        } else {
            return Err(err(format!("not a pool share (they start with `{SHARE_PREFIX}` or `{SHARE_LINK}`)")));
        };
        let raw = b64().decode(body).map_err(|_| err("damaged (base64)"))?;
        let share: PoolShare = serde_json::from_slice(&raw).map_err(|e| err(format!("damaged ({e})")))?;
        share.validate()?;
        Ok(share)
    }

    /// Does this look like a pool share (rather than something else pasted)?
    pub fn is_share(input: &str) -> bool {
        let t = input.trim_start();
        t.starts_with(SHARE_PREFIX) || t.get(..SHARE_LINK.len()).is_some_and(|h| h.eq_ignore_ascii_case(SHARE_LINK))
    }
}

impl fmt::Display for PoolShare {
    /// The token form; an invalid share shows as `wnet-pool1:<invalid>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_token() {
            Ok(t) => f.write_str(&t),
            Err(_) => write!(f, "{SHARE_PREFIX}<invalid>"),
        }
    }
}

impl FromStr for PoolShare {
    type Err = ShareError;

    fn from_str(s: &str) -> Result<Self, ShareError> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: &str = "0123456789abcdef0123456789abcdef";

    fn share() -> PoolShare {
        PoolShare::new(OWNER)
            .with_name("Ymir at home")
            .with_node("12D3KooWA", vec!["/ip4/192.0.2.1/udp/4001/quic-v1/p2p/12D3KooWA".into()])
            .with_node("12D3KooWB", vec![])
            .with_expires_at(1_900_000_000_000)
    }

    #[test]
    fn token_and_link_round_trip() {
        let s = share();
        let token = s.to_token().unwrap();
        assert!(token.starts_with(SHARE_PREFIX));
        assert_eq!(PoolShare::parse(&token).unwrap(), s);
        assert_eq!(token.parse::<PoolShare>().unwrap(), s);
        assert_eq!(s.to_string(), token);
        let link = s.to_link().unwrap();
        assert!(link.starts_with(SHARE_LINK));
        assert_eq!(PoolShare::parse(&link).unwrap(), s);
        assert_eq!(PoolShare::parse(&format!("{link}/")).unwrap(), s, "a trailing slash");
        assert_eq!(PoolShare::parse(&format!("WEIRDNET-POOL://SHARE/{}", &link[SHARE_LINK.len()..])).unwrap(), s, "the scheme is not");
        let wrapped: String = token.chars().collect::<Vec<_>>().chunks(20).map(|c| c.iter().collect::<String>() + "\n  ").collect();
        assert_eq!(PoolShare::parse(&wrapped).unwrap(), s, "line breaks and spaces are ignored");
        assert!(PoolShare::is_share(&token) && PoolShare::is_share(&link) && !PoolShare::is_share("wnet-x1-offer:abc"));
        assert_eq!(s.node_ids(), ["12D3KooWA", "12D3KooWB"]);
    }

    #[test]
    fn json_is_compact_and_unset_fields_are_left_out() {
        let s = PoolShare::new(OWNER).with_node("p", vec![]);
        let json = String::from_utf8(b64().decode(&s.to_token().unwrap()[SHARE_PREFIX.len()..]).unwrap()).unwrap();
        assert_eq!(json, format!(r#"{{"v":1,"owner":"{OWNER}","nodes":[{{"peer_id":"p"}}]}}"#));
    }

    #[test]
    fn unknown_fields_are_ignored_and_newer_versions_refused() {
        let with = |json: String| format!("{SHARE_PREFIX}{}", b64().encode(json));
        let extra = with(format!(r#"{{"v":1,"owner":"{OWNER}","nodes":[{{"peer_id":"p","addrs":[],"relay":true}}],"later":"x"}}"#));
        assert_eq!(PoolShare::parse(&extra).unwrap().nodes, [SharedNode::new("p", vec![])]);
        let newer = with(format!(r#"{{"v":2,"owner":"{OWNER}","nodes":[{{"peer_id":"p"}}]}}"#));
        assert!(PoolShare::parse(&newer).unwrap_err().0.contains("version 2"));
    }

    #[test]
    fn damaged_or_invalid_shares_are_refused() {
        for (input, why) in [
            ("wnet-x1-offer:abc".to_string(), "not a pool share"),
            (format!("{SHARE_PREFIX}!!!"), "base64"),
            (format!("{SHARE_PREFIX}{}", b64().encode("not json")), "damaged"),
            ("weirdnet://share/abc".to_string(), "not a pool share"),
            ("x".repeat(MAX_SHARE_LEN + 1), "too long"),
        ] {
            let e = PoolShare::parse(&input).unwrap_err();
            assert!(e.0.contains(why), "{input:.40}: {e}");
        }
        assert!(PoolShare::new("ABC").with_node("p", vec![]).to_token().unwrap_err().0.contains("user id"));
        assert!(PoolShare::new(OWNER.to_uppercase()).with_node("p", vec![]).validate().is_err());
        assert!(PoolShare::new(OWNER).to_token().unwrap_err().0.contains("no inference nodes"));
        assert!(PoolShare::new(OWNER).with_node("p", vec![]).with_node("p", vec![]).validate().unwrap_err().0.contains("twice"));
        assert!(PoolShare::new(OWNER).with_node(" ", vec![]).validate().is_err());
        let many = (0..=MAX_SHARE_NODES).fold(PoolShare::new(OWNER), |s, i| s.with_node(format!("p{i}"), vec![]));
        assert!(many.validate().is_err());
        assert_eq!(PoolShare::new("bad").to_string(), "wnet-pool1:<invalid>");
    }

    #[test]
    fn expiry() {
        let s = share();
        assert!(!s.is_expired(1_899_999_999_999));
        assert!(s.is_expired(1_900_000_000_000));
        assert!(!PoolShare::new(OWNER).is_expired(i64::MAX));
    }
}
