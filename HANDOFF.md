# Handoff

Running log for [weirdnet-agent](https://github.com/WeirdBrains/weirdnet-agent), newest first. Shared rules: the collective `AGENTS.md` and this repo's [AGENTS.md](AGENTS.md).

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
