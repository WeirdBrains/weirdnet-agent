# Handoff

Running log for [weirdnet-agent](https://github.com/WeirdBrains/weirdnet-agent), newest first. Shared rules: the collective `AGENTS.md` and this repo's [AGENTS.md](AGENTS.md).

## 2026-11-25: `PoolWatch::retry` (branch `feat/pool-model`)

What changed ([`429bcef`](https://github.com/WeirdBrains/weirdnet-agent/commit/429bcef580c8149c9fbe755f3b2b5bac3ff6b825)): `PoolWatch::retry` wakes the watch from its backoff (up to 30 s) and resets it, so a turn that wants the pool right after a node came back doesn't wait out the backoff. Found by Morph's `feat/ymir-pool` tests, where a turn right after the pool came up still saw it unreachable.

Checks run: `cargo fmt --check`, `cargo build`, `cargo clippy --all-targets` with and without `--all-features` (`-D warnings`), `cargo test --all-features` (120 unit tests, 1 new: a retry after three failed rounds finds the node before the 4 s backoff ends), `cargo test --no-default-features`, `cargo doc --no-deps --all-features` with `-D warnings`.

Not tested: no live Ymir node.

What's left and next: as below. Recommended release: 0.5.3 (all additions are on `#[non_exhaustive]` types, so [weirdnet-model](https://crates.io/crates/weirdnet-model) and [weirdnet-mcp](https://crates.io/crates/weirdnet-mcp), which take `^0.5` / `<0.6`, keep working without new releases); 0.6.0 would need them re-released. Waiting for m3talsmith's OK.

## 2026-11-25: PoolModel and PoolWatch for a model pool (branch `feat/pool-model`, for 0.6.0)

What changed ([`494661f`](https://github.com/WeirdBrains/weirdnet-agent/commit/494661f320fd418b0f830ab4e2d164c7ea0aecfe)): a new `remote::pool_model` module so apps such as [Morph](https://github.com/WeirdBrains/morph) can run companions on a [Ymir](https://github.com/WeirdBrains/ymir) pool. `PoolModel` is a `ModelBackend` over `Pool::infer` (infer/2): text and reasoning stream, tool calls come back in the turn, cancel stops the node, a node that dies mid-answer resumes on another (`ModelNotice::Continued`), a chosen model that can't read a turn's images or audio is swapped for one that can (`ModelNotice::Rerouted`, opt-out with `with_reroute(false)`), and before any text streamed an optional fallback runs the turn (`ModelNotice::FellBack`). With no fallback, an unreachable pool is the new `ModelError::PoolUnreachable`; a node's own answer (busy, refused) stays `Unavailable`. `PoolWatch` follows catalog/1 subscribe as a `PoolStatus` (searching, live, unreachable with the last catalog), with `chat_models(needs)` for pickers, and `PoolWatch::nodes` (`CatalogNodes`) is the `NodeSource` of the nodes that answered, so turns never go to linked devices that are not inference nodes. Notices reach the UI as `AgentEvent::ModelNotice` through `DeltaSink::notice`; task/1 does not forward them. All additions are on `#[non_exhaustive]` types.

Checks run: `cargo fmt --check`, `cargo clippy --all-targets` with and without `--all-features` (`-D warnings`), `cargo test --all-features` (119 unit tests, 9 new), `cargo test --no-default-features`, `cargo doc --no-deps --all-features` with `-D warnings`. New tests use in-memory nodes: streaming with reasoning and tool calls, mid-answer resume with the notice and no repeated text, unreachable and busy pools with and without the fallback, no fallback once text streamed, cancel, rerouting for images, and the watch (live, catalog changes, non-inference devices skipped, unreachable with the last catalog, forgotten nodes).

Not tested: no live Ymir node or Weirdnet network; only in-memory channels.

What's left and next: merge to main and release as 0.6.0 once m3talsmith OKs it (it is additive, so 0.5.3 would also work); Morph's `feat/ymir-pool` branch uses this branch until then.

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
