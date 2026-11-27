# Handoff

Running log for [weirdnet-agent](https://github.com/WeirdBrains/weirdnet-agent), newest first. Shared rules: the collective `AGENTS.md` and this repo's [AGENTS.md](AGENTS.md).

## 2026-11-27: released 0.5.10 (native video)

What changed: release commit [`ba5b3a7`](https://github.com/WeirdBrains/weirdnet-agent/commit/ba5b3a7b01e4d2e4c4d915d8eacbfb6a839cac37) (CI green on Linux, macOS and Windows), tag [`v0.5.10`](https://github.com/WeirdBrains/weirdnet-agent/tree/v0.5.10), then `scripts/weirdnet_release.py crates --tag v0.5.10`: on [crates.io](https://crates.io/crates/weirdnet-agent/0.5.10), docs on [docs.rs](https://docs.rs/weirdnet-agent/0.5.10). Release basis: m3talsmith's standing decision for Weirdnet releases ("Don't ask about publishing").

Checks run: the release script's checks, CI on the feature commit [`9afb195`](https://github.com/WeirdBrains/weirdnet-agent/commit/9afb195ec5732d7c87c454d685223a3535bbcb4d) and the release commit, `cargo publish` verification; the rest is in the entry below.

Not tested: as below.

What's left and next: the [net.weirdbrains.com](https://net.weirdbrains.com) pages; [weirdnet-backends](https://crates.io/crates/weirdnet-backends) (`Capabilities.video`, `video_url` forwarding) and weirdnet-openai (`video_url` in the bridge), then [Ymir](https://github.com/WeirdBrains/ymir) milestones 3 and 4 and [Bragi](https://github.com/WeirdBrains/bragi) V4.

## 2026-11-27: native video (for 0.5.10)

What changed: the commit that adds this entry adds the native video contract from [Ymir's video design](https://github.com/WeirdBrains/ymir/blob/main/docs/design/video.md) (phase 2, milestones 3 and 4) and [Bragi's](https://github.com/WeirdBrains/bragi/blob/main/docs/design/video.md) V4, so a whole clip can go to a model that reads video natively. All additive on 0.5.x (`ContentPart`, `Modalities` and `MediaLimits` are `#[non_exhaustive]`):

- Video part: `ContentPart::VideoUrl { video_url: VideoUrl { url } }`, `{"type": "video_url", "video_url": {"url": "data:video/mp4;base64,..."}}` in JSON and in infer/2 CBOR (the part vLLM and mistral.rs take, so bridges and backends can forward it unchanged). `ContentPart::video(url)`, `ContentPart::video_bytes(bytes, mime)` (base64 `data:` URL), `VideoUrl::mime_type` and `VideoUrl::data_len` (decoded size from the base64 length, for `video_max_bytes`). `Content::check` refuses anything but a `data:video/` URL (remote URLs are never fetched). `VideoUrl` is `#[non_exhaustive]`, so a content-root reference can be added later as another optional field. Small clips only for now: the whole request must fit one 32 MiB stream message.
- Modality and capability: `Modalities::video` (`video` on the wire, left off when false), `Modalities::VIDEO`, `Modalities::with_video`; `covers`, `union`, `is_text` and `lacking` (`"video"`, `"images and video"`, `"audio and video"`, `"images, audio and video"`) include it. catalog/1 `inputs.video` is that field: entries from nodes before 0.5.10 lack it and count as not reading video, and older peers ignore it. No `Capabilities` type lives in this crate; that flag is [weirdnet-backends](https://crates.io/crates/weirdnet-backends)' to add and publish as `inputs.video`.
- Routing: `Catalog::route` (and so `Pool::infer` and `PoolModel` rerouting) sends a request with a video part only to a chat model whose entry reads video; a named model that doesn't, or a pool with none, is a `BadRequest` whose message ends with the new `catalog::VIDEO_FALLBACK` (`send the video as frames (image_url parts) instead`). A node before 0.5.10 that gets one anyway (no catalog: the nodes decide) can't decode it and drops the stream; the pool fails over.
- Limits: `MediaLimits::video_max_seconds` (`u32`), `video_max_bytes` (`u64`, decoded) and `video_frames_per_minute` (`u32`; per minute so 0.5 fps stays exact, keeping `Eq` and `Hash`, as `audio_tokens_per_minute` does), each absent when unknown and left off the wire; builders `with_video_max_seconds`, `with_video_max_bytes`, `with_video_frames_per_minute`; `video_frame_rate()` (fps), `video_frames(ms)` and `check_video(ms, bytes)`; `or` and `tightest` (shorter, smaller, fewer frames) cover them. The first two match [weirdnet-model](https://crates.io/crates/weirdnet-model) 0.5.4's `ModelLimits` field names and types.

Checks run: `cargo fmt --check`; `cargo clippy --all-targets -D warnings` with default, all, no default, `pool`, `serve`, `jobs` and `testing` features; `cargo test` default and no default (160 unit), `--all-features` (166 unit plus 6 job, 2 linked, 2 network and 1 share); `cargo doc -D warnings` (default and all features); `cargo package --locked`; `cargo semver-checks` against 0.5.9 (default and all features): no semver update required. New tests: video part JSON and CBOR round trips and wire shape, `video_bytes`, MIME type and decoded size, `check` refusals, unknown parts failing a message as video does for older peers; `Modalities::VIDEO` covers, union, `lacking` names, wire form and a 0.5.9-shaped reader ignoring `video`; `MediaLimits` video fields off the wire when unknown, round trips, helpers, `or` and `tightest`; catalog `inputs.video` optional, old entries reading as no video, `route` picking only video models (never embedding or transcription), refusals ending with `VIDEO_FALLBACK`; the pool end to end over CBOR (clip arrives unchanged at the video model, refusals before any node runs, the frames fallback going to the image model, a node that can't read the request failing over) and `PoolModel` rerouting a clip.

Not tested: a real video model or engine (that is weirdnet-backends and weirdnet-openai next); live nodes.

What's left and next: CI, then release 0.5.10 and the [net.weirdbrains.com](https://net.weirdbrains.com) pages; weirdnet-backends adds `Capabilities.video` and `video_url` forwarding, weirdnet-openai `video_url` in the bridge, then Ymir milestones 3 and 4 and Bragi V4.

## 2026-11-27: released 0.5.9 (pool sharing)

What changed: release commit [`fa038ee`](https://github.com/WeirdBrains/weirdnet-agent/commit/fa038eeaf0f0d2d49210eb59407c6a6a0574904b) (CI green on Linux, macOS and Windows), tag [`v0.5.9`](https://github.com/WeirdBrains/weirdnet-agent/tree/v0.5.9), then `scripts/weirdnet_release.py crates --tag v0.5.9`: on [crates.io](https://crates.io/crates/weirdnet-agent/0.5.9), docs on [docs.rs](https://docs.rs/weirdnet-agent/0.5.9). Release basis: m3talsmith's standing decision for Weirdnet releases ("Don't ask about publishing").

Checks run: the release script's checks, CI on the feature commit [`bdb5be2`](https://github.com/WeirdBrains/weirdnet-agent/commit/bdb5be2b9457adc002dae7445f1a4e7c43bffaf3) and the release commit, `cargo publish` verification; the rest is in the entry below.

Not tested: as below.

What's left and next: the [net.weirdbrains.com](https://net.weirdbrains.com) pages; [Ymir](https://github.com/WeirdBrains/ymir) writes `docs/design/pool-sharing.md`, keeps the synced guest policy and wires `ymirctl pool share` / `unshare`.

## 2026-11-27: cross-identity pool sharing (for 0.5.9)

What changed: the commit that adds this entry lets another Weirdnet identity (for example a person's [Morph](https://github.com/WeirdBrains/morph) identity, or someone else's) use a pool such as [Ymir](https://github.com/WeirdBrains/ymir) with its own device certificate, never the pool's root identity, for catalog/1 and infer/2 only. All additive on 0.5.x, all in this crate (no [weirdnet-sync](https://crates.io/crates/weirdnet-sync) change: [weirdnet-net](https://crates.io/crates/weirdnet-net)'s `Profile::verify_cert` and `user::user_id` are the primitives):

- Guest credential (`serve` and `pool`): `encode_credential(user_key, cert)` is CBOR `{"guest": 1, "key": <user public key, protobuf>, "cert": <NodeCert>}`, because a `NodeCert` names its user only by id. `decode_presented` reads it or a bare certificate (`Presented { cert, user_key }`). Hosts before 0.5.9 can't read it as a certificate and refuse (`wrong-user`).
- Host side (`serve`): `GuestAuth::new(profile, &owner_key, policy)` verifies a guest with the roster's rigor: the key must hash to the certificate's user id, the user's signature must verify, the certificate must be for the transport (Noise) peer, not revoked (a revocation must carry its signature) and not issued more than `DEFAULT_CLOCK_SKEW` (10 min, `with_clock_skew`) in the future, and never the owner's own user id (`own-user`: that is the roster's call). Then the host's `GuestPolicy` (`Fn(&Guest, GuestScope) -> Result<(), RemoteError>`) decides per `GuestScope` (`Catalog`, `Infer`) on every check. `SharedPoolAuth::new(roster_auth).with_guests(guests)` combines it with `RosterAuth` (own devices unchanged): `identify(caller, scope)` gives `CallerIdentity::Own(Admitted)` or `CallerIdentity::Guest(Guest)`, `authorize_for` is for `InferHost::authorize`, `scoped(scope)` is the `Authorize` for a `CatalogFeed`. It does not implement `Authorize` itself so it can't be handed to task/1 by mistake. Without `with_guests` it is exactly `RosterAuth`: deny by default.
- Live revocation: infer/1 and infer/2 authorize every request (one per stream), catalog/1 every query. New `CatalogHost::recheck` (default `Ok`, as before) is asked before each update a subscription sends and every `catalog::RECHECK_EVERY` (5 s); `CatalogFeed::rechecking()` makes the feed re-ask its authorizer, so a revoked guest's subscription closes with the policy's refusal. An answer already streaming finishes.
- Share link (`remote::share`, no feature): `PoolShare { v, owner, name, nodes: Vec<SharedNode { peer_id, addrs }>, expires_at }`, `wnet-pool1:<base64url JSON>` (the Weirdnet token style) or `weirdnet-pool://share/<same>`; `to_token`, `to_link`, `parse` (whitespace ignored, 16 KiB and 64 node caps), `validate`, `is_expired`, `node_ids`, `Display`/`FromStr`. Unknown fields are ignored; a higher `v` is refused. No secrets: holding it only lets you dial; the owner's policy decides who is let in.
- Guest side (`pool`): `connect_shared(node, &share, &user_key, &cert)` checks the share (valid, not expired) and that the certificate is this node's and this user's, then builds a `Pool` over the share's nodes in order (failover as usual, no roster discovery) with a `share_dialer` (addresses as hints; an address naming another peer is refused) and the guest credential presented with every catalog/1 and infer/2 request (chat, embeddings, transcription). Dialing another user's node needs nothing special in weirdnet-net: streams are Noise-authenticated by peer id whoever its user is, and relay circuit addresses work as hints.
- `RosterAuth::check` also reads a certificate inside a guest credential (still checked against the owner's key, so another user's is `wrong-user`).

Checks run: `cargo fmt --check`; `cargo clippy --all-targets -D warnings` with default, all, no default, `pool`, `serve`, `jobs` and `testing` features; `cargo test` default and no default (150 unit), `--all-features` (156 unit, 6 job, 2 linked, 2 network and the new share test); `pool,serve,testing` clippy and tests against weirdnet-sync 0.5.3 and 0.7.1; `cargo doc -D warnings` (default and all features); `cargo package --locked`; `cargo semver-checks` against 0.5.8 (default and all features): no semver update required. New tests: share link round trips (token, link, case-insensitive scheme, wrapped text), compact JSON, unknown fields, newer versions, damaged and invalid shares, expiry; guest verification (valid guest per scope, policy revocation, no cert, bare cert, junk, wrong peer, wrong key, tampered signature, forged certificate, changed `issued_at`, signed revocation, unsigned revocation, future certificate and skew, the owner's own user, an unknown credential version); `SharedPoolAuth` deny by default, guests and own devices side by side, a revoked own device not let in through the guest path; credential round trips; a rechecking feed closing a subscription on an update and on the tick, and a default feed not. End to end over in-process Weirdnet nodes (`tests/share.rs`): an owner with two inference nodes, a guest of another user with the link as a string, a stranger and a roster-only node: the guest's catalog, chat and transcription, the host seeing a guest in `open_request` (and refusing it a server-side context), failover when the first node halts, revocation refusing catalog, chat and transcription at once and closing a subscription within the re-check, re-sharing, a forged credential (`guest-bad-cert`), a stolen credential from another peer (`wrong-peer`), a stranger with the link (`not-shared`), the roster-only node refusing the guest (`wrong-user`), and `connect_shared` refusing expired shares, another node's or user's certificate, mismatched addresses and bad peer ids.

Not tested: real networks and relays (memory transport only); Ymir's synced guest policy and `ymirctl pool share` / `unshare` (Ymir's side); a guest whose own user revokes its device while it still holds the unrevoked certificate (the owner can't see the guest's roster; the owner's policy can pin peers).

What's left and next: CI, then release 0.5.9 and the [net.weirdbrains.com](https://net.weirdbrains.com) pages; Ymir writes `docs/design/pool-sharing.md` and wires the guest policy and `ymirctl pool share` / `unshare`.

## 2026-11-26: released 0.5.8 (transcription)

What changed: release commit [`4449a72`](https://github.com/WeirdBrains/weirdnet-agent/commit/4449a72b6bbde5d209152e6639de11d211c03cf8) (CI green on Linux, macOS and Windows), tag [`v0.5.8`](https://github.com/WeirdBrains/weirdnet-agent/tree/v0.5.8), then `scripts/weirdnet_release.py crates --tag v0.5.8`: on [crates.io](https://crates.io/crates/weirdnet-agent/0.5.8), docs on [docs.rs](https://docs.rs/weirdnet-agent/0.5.8).

Checks run: the release script's checks, CI on the feature and release commits, `cargo package --locked` and `cargo publish` verification; the rest is in the entry below.

Not tested: as below.

What's left and next: [weirdnet-backends](https://crates.io/crates/weirdnet-backends) serves a `TranscribeBackend` for `transcribe` models and fills measured limits at load; [Ymir](https://github.com/WeirdBrains/ymir) wires the route (Milestone 2).

## 2026-11-26: transcription role and route (for 0.5.8)

What changed: the commit that adds this entry adds speech to text for Milestone 2 of the video-upload work in [Ymir](https://github.com/WeirdBrains/ymir) (the `transcribe` role and route in `docs/design/video.md`), all additive on 0.5.x:

- Catalog: `Purpose::Transcribe` (`transcribe` on the wire, the same name as [weirdnet-model](https://crates.io/crates/weirdnet-model) 0.5.4's `hw::Purpose::Transcribe`), `Purpose::as_str`, `Purpose::parse` and `Display`; `CatalogModel::transcribes`, `Catalog::transcribers` and `Catalog::route_transcribe(model)` (a named model must transcribe; with none named the best transcriber: warm, then busy, loading, loadable, then more replicas; a `BadRequest` otherwise). `CatalogModel::accepts` stays chat-only, so `Catalog::route` never picks a transcriber.
- infer/2: `ClientFrame::Transcribe(TranscribeRequest)` (`audio` as a CBOR byte string, `format`, optional `model`, `language`, `prompt`, `timestamps`, `priority`, `cert`; `check` refuses empty audio or format), answered by `Accepted`, `ServerFrame::Working` every `QUEUE_KEEPALIVE` while the backend runs, one `ServerFrame::Transcript` (`Transcript`: `text`, `language`, `duration_ms`, `segments` of `Segment { start_ms, end_ms, text }`) and `Done` with usage and timings. Hosts serve it through `InferHost::open_transcribe` (default: unavailable) and a `TranscribeBackend` (`transcribe`, `transcribe_counted`); `serve` re-exports `TranscribeBackend`.
- Pool: `Pool::transcribe` returns a `Transcription` (`transcript`, `model`, `node`, `meta`, `usage`), with failover; a `BadRequest` comes back at once, and a node before 0.5.8 (which closes the stream on the unknown frame) counts as `Unsupported`.
- Older peers: a peer before 0.5.8 can't decode a `transcribe` entry, and one such entry would fail its whole catalog. So `catalog::serve` leaves those entries out unless the asker says it skips entries it can't read, which `catalog::query` and `catalog::subscribe` now do (a `skips_unknown` field older nodes ignore), and from 0.5.8 an entry this version can't read is skipped instead of failing the catalog. `ClientMsg` is unchanged; an app that sends it itself sees no transcribers.

Checks run: `cargo fmt --check`; `cargo clippy --all-targets -D warnings` with default, all, no default, `pool`, `serve`, `jobs` and `testing` features; `cargo test` default and no default (144 unit), `--all-features` (146 unit plus integration); `cargo doc -D warnings` (default and all features); `cargo semver-checks` against 0.5.7 (default and all features): no semver update required. New tests: purpose names and JSON/CBOR round trips, `route_transcribe` picks and refusals, chat routing never picking a transcriber, unreadable entries skipped, older askers getting the catalog without transcribers (query and subscribe) while 0.5.8 askers see them, the new ask read by old nodes and old asks by new ones; transcription request and transcript wire round trips (audio as a byte string, unset options off the wire), request checks on both sides, the pool end to end with usage and metadata, refusals without failover, old and chat-only nodes failing over, `Working` keepalives through a 10 s backend under a 4 s stall cap, and cancel.

Not tested: a real speech-to-text model or backend (that is [weirdnet-backends](https://crates.io/crates/weirdnet-backends) next); live nodes.

What's left and next: release 0.5.8; then weirdnet-backends serves `TranscribeBackend` for a `transcribe` model (with measured limits from weirdnet-model at load) and Ymir wires the route.

## 2026-11-26: released 0.5.7 (media limits)

What changed: release commit [`c13308b`](https://github.com/WeirdBrains/weirdnet-agent/commit/c13308bd9820d45142184d51cdb851bcc10dadbb) (CI green on Linux, macOS and Windows), tag [`v0.5.7`](https://github.com/WeirdBrains/weirdnet-agent/tree/v0.5.7), then `scripts/weirdnet_release.py crates --tag v0.5.7`: on [crates.io](https://crates.io/crates/weirdnet-agent/0.5.7), docs on [docs.rs](https://docs.rs/weirdnet-agent/0.5.7).

Checks run: the release script's checks, CI on the release commit and `cargo publish` verification; the rest is in the entry below.

Not tested: as below.

What's left and next: [weirdnet-backends](https://crates.io/crates/weirdnet-backends) and [weirdnet-openai](https://crates.io/crates/weirdnet-openai) carry and report the limits; later `ContentPart::video`, `Modalities::VIDEO`, catalog `inputs.video` and video limits.

## 2026-11-26: per-model media and request limits (for 0.5.7)

What changed: [`282f9e1`](https://github.com/WeirdBrains/weirdnet-agent/commit/282f9e1c4557a95bff29e26afa322b86b8df71f2) adds `MediaLimits` (crate root, `limits` module) and `CatalogModel::limits` with `CatalogModel::with_limits`, for the video-upload work in Bragi and Ymir: `max_images`, `image_tokens`, `image_patch`, `image_edge`, `audio_max_seconds`, `audio_tokens_per_minute` (per minute so rates such as 6.25 per second stay exact and the type stays `Eq + Hash`) and `max_request_bytes`, each `Option`, absent when unknown and never defaulted. Helpers: `image_cost`, `scaled`, `audio_cost`, `audio_tokens_per_second`, `tightest` (stricter per field) and `or` (operator over measured). Unknown limits are left off the wire, so catalog entries look as before and older peers read new ones. The context window stays in `CatalogModel::context`, now documented as the window the node runs with.

Checks run: `cargo fmt --check`; `cargo clippy --all-targets -D warnings` (default and all features); `cargo test` default, `--all-features` (130 unit plus integration) and `--no-default-features`; `cargo doc -D warnings`; `cargo semver-checks` against 0.5.6 (default and all features): no semver update required. New tests cover JSON and CBOR round trips with and without limits, old entries without them, and the cost and merge helpers.

Not tested: real projectors or audio encoders; the numbers come from nodes.

What's left and next: release 0.5.7, then `weirdnet-backends` carries the limits in `Capabilities` and `weirdnet-openai` reports them. Later: `ContentPart::video`, `Modalities::VIDEO`, catalog `inputs.video` and video limits.

## 2026-11-26: released 0.5.6 (`jobs` feature)

What changed: release commit [`1ce5c32`](https://github.com/WeirdBrains/weirdnet-agent/commit/1ce5c321c58e) (CI green on Linux, macOS and Windows), tag [`v0.5.6`](https://github.com/WeirdBrains/weirdnet-agent/tree/v0.5.6), then `scripts/weirdnet_release.py crates --tag v0.5.6`: on [crates.io](https://crates.io/crates/weirdnet-agent/0.5.6) and [docs.rs](https://docs.rs/weirdnet-agent/0.5.6). No GitHub release page; the tag and the changelog carry the notes.

Checks run: CI on the release commit and `cargo publish` verification; the rest is in the entry below.

Not tested: as below.

What's left and next: the jobs guide on [net.weirdbrains.com](https://net.weirdbrains.com); Ymir swaps its job store stand-in for `JobBoard<TrainJob>`.

## 2026-11-25: releasing 0.5.6 (`jobs` feature)

What changed: the release of the `jobs` feature from [`51e7802`](https://github.com/WeirdBrains/weirdnet-agent/commit/51e7802c691fdf65ce3a24134bcba6c33eff2246) (CI green on Linux, macOS and Windows) with `scripts/weirdnet_release.py cut --patch`. The crate is published only after CI passes on the release commit (`--skip-crates`, then `crates --tag v0.5.6`). It ships as 0.5.6, not 0.6.0, for the graph reason in the entry below: Ymir also depends on weirdnet-backends `=0.4.5` (`weirdnet-agent ^0.5.2`), so a 0.6.0 would give it two weirdnet-agent crates.

Release basis: m3talsmith's 2026-11-25 decision for Weirdnet releases ("Don't ask about publishing"). `cargo semver-checks` against 0.5.5: no semver update required.

Checks run: `cargo fmt --check`; `cargo clippy --all-targets -D warnings` (default and all features); `cargo test --all-features` (124 unit, 6 job, 2 linked-device, 2 network tests, doc tests) and `--no-default-features` (122 unit); `cargo doc -D warnings`. The linked-device and network timing tests failed once on a heavily loaded machine and passed on three reruns. Ymir's set: a scratch crate with this tree (`jobs,pool,serve`), weirdnet-sync 0.8.2, weirdnet-net `=0.8.2`, weirdnet-backends `=0.4.5`, weirdnet-update 0.2.2 and weirdnet-openai 0.2.3 builds with one weirdnet-agent, one weirdnet-sync and weirdnet-net 0.8.2 (no net bump). weirdnet-sync 0.8.2's `tests/compat_07.rs` passes: a published 0.7.1 device and 0.8.2 share a sealed record both ways, byte for byte.

Not tested: real networks and real training checkpoints; voter sets mixing weirdnet-sync 0.8.0 and 0.8.2 devices.

What's left and next: CI on the release commit, publish, then the jobs guide on [net.weirdbrains.com](https://net.weirdbrains.com).

## 2026-11-25: jobs on weirdnet-sync 0.8.2; Windows CI fix (for 0.5.6)

What changed: [`ae1be3e`](https://github.com/WeirdBrains/weirdnet-agent/commit/ae1be3e0076636661d3d6c34a80386af07d8f805) and [`63de1c8`](https://github.com/WeirdBrains/weirdnet-agent/commit/63de1c8db6c3d21a6432480c68fa698ee7aae101). CI for the previous push failed on Windows in `retries_wait_out_the_backoff_until_the_attempts_run_out`: the 800 ms backoff ran out before the test checked it on a slow runner. The test now uses a 5 s backoff and waits until the record's `not_before_ms`. Running the job tests six at a time locally then showed a real race: an owner's pause could count the holder's claim as an attempt when the holder's lease ran out (voters stop renewing a paused job) before it recorded its own pause. That is fixed in the fold in [weirdnet-sync 0.8.2](https://crates.io/crates/weirdnet-sync/0.8.2), which `Cargo.lock` now uses and the docs name as the minimum for `jobs`. The tests also re-pull until a device's view settles instead of trusting one pull.

Checks run: fmt; clippy (default and all features, `-D warnings`); `cargo test --all-features` (124 unit, 6 job tests), `--no-default-features`; `cargo doc -D warnings`; the job tests 18 times six at a time against 0.8.2, all passing (6 of 18 failed on 0.8.1).

Not tested: as below.

What's left and next: CI, then release 0.5.6.

## 2026-11-25: `jobs` feature, synced training-style jobs (for 0.5.6)

What changed: a new opt-in `jobs` feature (requested for Ymir's resume-a-training-job-on-another-node work): `JobBoard<P>`, a typed layer over the [weirdnet-sync](https://crates.io/crates/weirdnet-sync/0.8.1) 0.8.1 job log and leases (not a second log), with `JobRecord<P>`, `NewJob<P>`, `JobPolicy`, `CheckpointRef`, `ContentHash`, `Claim<P>` (background renewal, `term`, `fence`, `checkpoint`, `set_holders`, `progress`, `finish`, `fail`, `pause`, `lost`), owner `pause`/`resume`/`cancel`, `list`, `checkpoints` and `watch`. Commits [`9d2abd0`](https://github.com/WeirdBrains/weirdnet-agent/commit/9d2abd0d09f8d13be37c4efc3a6e3b4a558d9c61) and [`154060f`](https://github.com/WeirdBrains/weirdnet-agent/commit/154060fb18a7dc936f4443cf86de188b5c78f6ec). CI now also runs the tests on macOS and Windows; the weirdnet-sync 0.5.3 and 0.7.1 steps test `pool`, `serve` and `testing` (the `jobs` feature needs 0.8.1).

Version: additive and opt-in, so it ships as 0.5.6 rather than 0.6.0, under the 2026-11-20 rule for additive pre-1.0 Weirdnet releases. A 0.6.0 would split the graph for apps that also use weirdnet-backends 0.4.5 (`weirdnet-agent ^0.5.2`) or weirdnet-openai 0.2.3 (`^0.5.4`): two weirdnet-agent crates whose `ModelBackend` traits differ. The weirdnet-sync range stays `>=0.5.2, <0.9`.

Checks run: `cargo fmt --check`; `cargo clippy --all-targets` (default and all features, `-D warnings`); `cargo test --all-features` (124 unit tests, 6 multi-node job tests: claim and renew, a dead holder's lapse and takeover after the wait with resume from the checkpoint, a split pool with fencing, retry backoff and max attempts, pause/resume/cancel without using attempts, eligibility by candidates, role and the app's check), `--no-default-features`; `cargo doc -D warnings`; sync 0.5.3 and 0.7.1 with `pool,serve,testing`; `cargo semver-checks` against 0.5.5 (no semver update required). A scratch crate depending on this tree with `jobs,pool,serve`, weirdnet-sync 0.8.1, weirdnet-net `=0.8.2`, weirdnet-backends `=0.4.5`, weirdnet-update 0.2.2 and weirdnet-openai 0.2.3 resolves to one weirdnet-agent and one weirdnet-sync and builds.

Not tested: real networks and real training checkpoints; voter sets mixing weirdnet-sync 0.8.0 and 0.8.1 devices.

What's left and next: release 0.5.6; [net.weirdbrains.com](https://net.weirdbrains.com) gets a jobs guide.

## 2026-11-25: released 0.5.5 (weirdnet-sync 0.8)

What changed: released with `scripts/weirdnet_release.py --patch` from [`7ab6ea3`](https://github.com/WeirdBrains/weirdnet-agent/commit/7ab6ea3a4910955fe32b6ccaf198ee83a6d3b0e2) (CI green). Release commit [`fc65eb9`](https://github.com/WeirdBrains/weirdnet-agent/commit/fc65eb9f0caa604592dce545cad6c1a6b6a79b13) (CI green), tag [`v0.5.5`](https://github.com/WeirdBrains/weirdnet-agent/tree/v0.5.5), on [crates.io](https://crates.io/crates/weirdnet-agent/0.5.5) and [docs.rs](https://docs.rs/weirdnet-agent/0.5.5). No GitHub release page; the tag and the changelog carry the notes.

Release basis: m3talsmith's 2026-11-25 decision to accept weirdnet-sync 0.8 here, under his standing rule of 2026-11-24 for additive Weirdnet patch releases. `cargo semver-checks --all-features` against 0.5.4: no semver update required.

Checks run: CI (including the sync 0.5.3 and 0.7.1 steps), the release script's checks and `cargo publish` verification. `cargo publish` no longer warns about yanked lock entries.

Not tested: no live nodes on mixed sync versions.

What's left and next: [weirdnet-backends](https://github.com/WeirdBrains/weirdnet-backends) 0.4.6 needs this release.

## 2026-11-25: accept weirdnet-sync 0.8 (for 0.5.5)

What changed: the optional `weirdnet-sync` requirement (features `pool` and `serve`) widens from `>=0.5.2, <0.8` to `>=0.5.2, <0.9`, so apps on [weirdnet-sync](https://crates.io/crates/weirdnet-sync) 0.8 can use this crate. No code changes were needed: the crate only uses `Roster`, `Roster::check_caller` and `CallerCheck`, which 0.8 keeps. Cargo.lock now holds weirdnet-sync 0.8.0, weirdnet-net 0.8.2 and weirdnet-keys 0.1.2 (the yanked 0.5.2, 0.8.0 and 0.1.1 are gone). CI tests the lock (0.8.0) and also 0.5.3 and 0.7.1. README and `serve` docs say 0.5.2 to 0.8. Commit SHA: see the commit that adds this entry.

Basis: m3talsmith decided on 2026-11-25 to widen the weirdnet-sync range in weirdnet-agent and weirdnet-backends to include 0.8, and his standing rule of 2026-11-24 covers additive Weirdnet patch releases. Widening an upper bound is additive, so this ships as 0.5.5.

Checks run: `cargo fmt --check`; `cargo clippy --all-targets --all-features -D warnings` and `cargo test --all-features` against weirdnet-sync 0.8.0, 0.7.1 and 0.5.3 (the oldest version in range that is not yanked); `cargo test` with default and no default features; `cargo doc -D warnings`.

Not tested: 0.5.2 itself (yanked); no live nodes on mixed sync versions.

What's left and next: release 0.5.5, then [weirdnet-backends](https://github.com/WeirdBrains/weirdnet-backends) 0.4.6 with the same range.

## 2026-11-25: released 0.5.4 (catalog origin and license)

What changed: [`feat/catalog-origin`](https://github.com/WeirdBrains/weirdnet-agent/tree/feat/catalog-origin) merged to main ([`d638088`](https://github.com/WeirdBrains/weirdnet-agent/commit/d63808834dfba652770ddc933faae5bf77366290), CI green) and released with `scripts/weirdnet_release.py --patch`. Release commit [`3728adf`](https://github.com/WeirdBrains/weirdnet-agent/commit/3728adfc4f9b), tag [`v0.5.4`](https://github.com/WeirdBrains/weirdnet-agent/tree/v0.5.4), on [crates.io](https://crates.io/crates/weirdnet-agent/0.5.4) and [docs.rs](https://docs.rs/weirdnet-agent/0.5.4). No GitHub release page (as with 0.5.2); the tag and the changelog carry the notes.

Release basis: m3talsmith's standing rule of 2026-11-24 for additive Weirdnet patch releases; `cargo semver-checks` against 0.5.3 and 0.5.2 found nothing that needs more than a patch.

Checks run: the release script's checks (fmt, clippy `-D warnings --all-features`, tests `--all-features`, `cargo doc -D warnings`) and `cargo publish` verification.

Not tested: no live Ymir node. `cargo publish` still warns about the yanked `weirdnet-sync` 0.5.2, `weirdnet-keys` 0.1.1 and `weirdnet-net` 0.8.0 in this repo's Cargo.lock (lock only).

What's left and next: Ymir sets `origin`/`license` on its catalog entries from the node records ([weirdnet-backends](https://github.com/WeirdBrains/weirdnet-backends) `ModelEntry::origin`/`license`).

## 2026-11-25: catalog/1 origin and license (for 0.5.4)

What changed ([`15d76ed`](https://github.com/WeirdBrains/weirdnet-agent/commit/15d76ed245430f56fd61e162aee9080f76d8fc43), branch `feat/catalog-origin`, merged to main with `--no-ff`): `CatalogModel::origin` and `CatalogModel::license` (`Option<String>`, serde default, left off the wire when unset), the builders `with_origin` and `with_license`, and the names `ORIGIN_HUB`, `ORIGIN_UPLOAD` and `ORIGIN_TRAIN` (the same strings as a [weirdnet-model](https://crates.io/crates/weirdnet-model) 0.5.3 provenance origin). Asked for by [Ymir](https://github.com/WeirdBrains/ymir), so apps can show where a pool model came from and its license. `CatalogModel` is `#[non_exhaustive]`, so the new fields are additive: a patch release, 0.5.4.

Checks run: `cargo fmt --check`, `cargo build`, `cargo clippy --all-targets` with and without `--all-features` (`-D warnings`), `cargo test --all-features` (122 unit tests, 2 new: the fields stay off the wire when unset and round-trip when set; an older peer's entry reads with neither field, and an older peer reads a new entry and catalog message), `cargo test --no-default-features`, `cargo doc --no-deps --all-features` with `-D warnings`, and `cargo semver-checks` against 0.5.3 and 0.5.2 (no semver update required).

Not tested: no live Ymir node; only CBOR round trips against a struct shaped like the 0.5.3 entry.

Release basis: m3talsmith's standing rule of 2026-11-24 allows additive Weirdnet patch releases without asking; minor bumps still need his OK.

What's left and next: release 0.5.4 once CI on main is green; [weirdnet-backends](https://github.com/WeirdBrains/weirdnet-backends) carries the fields in its capability record and [weirdnet-openai](https://github.com/WeirdBrains/weirdnet-openai) shows them in `/v1/models`; Ymir fills `origin`/`license` from the node records when it builds its catalog.

## 2026-11-25: released 0.5.3 (PoolModel and PoolWatch)

What changed: [`feat/pool-model`](https://github.com/WeirdBrains/weirdnet-agent/tree/feat/pool-model) merged to main with `--no-ff` ([`bfb5962`](https://github.com/WeirdBrains/weirdnet-agent/commit/bfb59622af2302b805a700f6965a4f44ba51fadd)) and released with `scripts/weirdnet_release.py --patch` (approved by m3talsmith, 2026-11-25). Release commit [`3ef94cb`](https://github.com/WeirdBrains/weirdnet-agent/commit/3ef94cbf7a60fe514a23d2c151d705facc7cdb42), tag [`v0.5.3`](https://github.com/WeirdBrains/weirdnet-agent/releases/tag/v0.5.3) (GitHub release with the changelog notes, made with `gh release create` because `release.toml` has no `[github]` files), on [crates.io](https://crates.io/crates/weirdnet-agent/0.5.3) and [docs.rs](https://docs.rs/weirdnet-agent/0.5.3). Additive, so [weirdnet-model](https://crates.io/crates/weirdnet-model) 0.5.3 and [weirdnet-mcp](https://crates.io/crates/weirdnet-mcp) 0.5.3 pick it up without new releases.

Checks run: on the merge, `cargo fmt --check`, `cargo build`, `cargo clippy --all-targets` with and without `--all-features` (`-D warnings`), `cargo test --all-features` (120 unit tests), `cargo test --no-default-features`, `cargo doc -D warnings`, and CI on main (green); then the release script's checks and `cargo publish` verification.

Not tested: no live Ymir node. `cargo publish` warned that this repo's Cargo.lock holds yanked `weirdnet-sync` 0.5.2, `weirdnet-keys` 0.1.1 and `weirdnet-net` 0.8.0 (dev and lock only; dependents resolve their own versions).

What's left and next: [Morph](https://github.com/WeirdBrains/morph) moves to `=0.5.3` and merges its Ymir pool branch; a `cargo update` here for the yanked lock entries when convenient.

## 2026-11-25: `PoolWatch::retry` (branch `feat/pool-model`)

What changed ([`429bcef`](https://github.com/WeirdBrains/weirdnet-agent/commit/429bcef580c8149c9fbe755f3b2b5bac3ff6b825)): `PoolWatch::retry` wakes the watch from its backoff (up to 30 s) and resets it, so a turn that wants the pool right after a node came back doesn't wait out the backoff. Found by Morph's `feat/ymir-pool` tests, where a turn right after the pool came up still saw it unreachable.

Checks run: `cargo fmt --check`, `cargo build`, `cargo clippy --all-targets` with and without `--all-features` (`-D warnings`), `cargo test --all-features` (120 unit tests, 1 new: a retry after three failed rounds finds the node before the 4 s backoff ends), `cargo test --no-default-features`, `cargo doc --no-deps --all-features` with `-D warnings`.

Not tested: no live Ymir node.

What's left and next: as below. Recommended release: 0.5.3 (all additions are on `#[non_exhaustive]` types, so [weirdnet-model](https://crates.io/crates/weirdnet-model) and [weirdnet-mcp](https://crates.io/crates/weirdnet-mcp), which take `^0.5` / `<0.6`, keep working without new releases); 0.6.0 would need them re-released. Waiting for m3talsmith's OK.

## 2026-11-25: PoolModel and PoolWatch for a model pool (branch `feat/pool-model`, for 0.5.3)

What changed ([`494661f`](https://github.com/WeirdBrains/weirdnet-agent/commit/494661f320fd418b0f830ab4e2d164c7ea0aecfe)): a new `remote::pool_model` module so apps such as [Morph](https://github.com/WeirdBrains/morph) can run companions on a [Ymir](https://github.com/WeirdBrains/ymir) pool. `PoolModel` is a `ModelBackend` over `Pool::infer` (infer/2): text and reasoning stream, tool calls come back in the turn, cancel stops the node, a node that dies mid-answer resumes on another (`ModelNotice::Continued`), a chosen model that can't read a turn's images or audio is swapped for one that can (`ModelNotice::Rerouted`, opt-out with `with_reroute(false)`), and before any text streamed an optional fallback runs the turn (`ModelNotice::FellBack`). With no fallback, an unreachable pool is the new `ModelError::PoolUnreachable`; a node's own answer (busy, refused) stays `Unavailable`. `PoolWatch` follows catalog/1 subscribe as a `PoolStatus` (searching, live, unreachable with the last catalog), with `chat_models(needs)` for pickers, and `PoolWatch::nodes` (`CatalogNodes`) is the `NodeSource` of the nodes that answered, so turns never go to linked devices that are not inference nodes. Notices reach the UI as `AgentEvent::ModelNotice` through `DeltaSink::notice`; task/1 does not forward them. All additions are on `#[non_exhaustive]` types.

Checks run: `cargo fmt --check`, `cargo clippy --all-targets` with and without `--all-features` (`-D warnings`), `cargo test --all-features` (119 unit tests, 9 new), `cargo test --no-default-features`, `cargo doc --no-deps --all-features` with `-D warnings`. New tests use in-memory nodes: streaming with reasoning and tool calls, mid-answer resume with the notice and no repeated text, unreachable and busy pools with and without the fallback, no fallback once text streamed, cancel, rerouting for images, and the watch (live, catalog changes, non-inference devices skipped, unreachable with the last catalog, forgotten nodes).

Not tested: no live Ymir node or Weirdnet network; only in-memory channels.

What's left and next: merge to main and release as 0.5.3 (additive, on `#[non_exhaustive]` types) once m3talsmith OKs it; Morph's `feat/ymir-pool` branch uses this branch until then.

## 2026-11-23: released 0.5.2 (adapter scale, catalog base)

What changed: `Request::adapter_scale`/`with_adapter_scale` (infer/2) and `CatalogModel::base`/`with_base`/`is_adapter` (catalog/1), both off the wire when unset, for LoRA adapters (weirdnet-backends 0.4.1, Ymir, Bragi). Checks: fmt, clippy `-D warnings --all-features`, tests `--all-features`, `cargo doc -D warnings`. Not tested: nothing beyond the wire round trips; the fields are optional, so older nodes read the requests. Released with `scripts/weirdnet_release.py` (approved by Michael, 2026-11-23). Release commit [`cd51630`](https://github.com/WeirdBrains/weirdnet-agent/commit/cd51630e280b6ebbd2dd5f8fdc6cf3114d7f36d3), tag [`v0.5.2`](https://github.com/WeirdBrains/weirdnet-agent/releases/tag/v0.5.2), on [crates.io](https://crates.io/crates/weirdnet-agent/0.5.2) and [docs.rs](https://docs.rs/weirdnet-agent/0.5.2).

## 2026-11-22: released 0.5.1

What changed: released with `scripts/weirdnet_release.py` (approved by Michael, 2026-11-22). Release commit [`c2b7b0a`](https://github.com/WeirdBrains/weirdnet-agent/commit/c2b7b0aea601bcda61a37dcf6bbd39f2efd66d13), tag [`v0.5.1`](https://github.com/WeirdBrains/weirdnet-agent/releases/tag/v0.5.1), on [crates.io](https://crates.io/crates/weirdnet-agent/0.5.1) and [docs.rs](https://docs.rs/weirdnet-agent/0.5.1). The script first stopped at `cargo publish` (no registry token in the build environment); the tag was already pushed, so it was finished with `weirdnet_release.py crates --tag v0.5.1`.

Checks run: the release script's checks (fmt, clippy `-D warnings`, tests, docs), `cargo publish` verification, CI on the release commit (green), the docs.rs pages for the new APIs.

Not tested: no live node.

What's left and next: nothing for this release; the [site pages](https://net.weirdbrains.com) are merged.

## 2026-11-22: per-request answer-cache mode (for 0.5.1)

What changed ([`d81ef92`](https://github.com/WeirdBrains/weirdnet-agent/commit/d81ef928e8b67d3566384bab719af1599e4d2b0a)): `remote::infer::v2` gains `CacheMode` (`use`, `bypass`, `refresh`; `#[non_exhaustive]`, serde snake_case) with `may_read` / `may_store`, a `Request::cache` field (serde default, left off the wire when `use`), and the builders `Request::with_cache` and `Request::with_wait_for_model`. Requested for [Ymir](https://github.com/WeirdBrains/ymir)'s training evals, which need to skip the answer cache without raw mode. `Request` is already `#[non_exhaustive]`, so this is additive: a patch release, 0.5.1.

Checks run: `cargo fmt --check`, `cargo clippy --all-targets` with and without `--all-features` (`-D warnings`), `cargo test --all-features`, `cargo test --no-default-features`, `cargo doc --no-deps --all-features` with `-D warnings`. New tests: the field stays off the wire by default, old requests read as `use`, `bypass`/`refresh` round-trip and survive retries, and a reader without the field still decodes the request.

Not tested: no live node; nodes older than 0.5.1 ignore the field and may still answer from their cache (`Meta::cache_hit` reports it).

What's left and next: release 0.5.1 once Michael OKs it, then [weirdnet-semcache](https://github.com/WeirdBrains/weirdnet-semcache) 0.2.1 and [weirdnet-openai](https://github.com/WeirdBrains/weirdnet-openai) 0.2.1 (branches `cache-mode`), which honour the field.
