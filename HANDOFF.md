# Handoff

Running log for [weirdnet-agent](https://github.com/WeirdBrains/weirdnet-agent), newest first. Shared rules: the collective `AGENTS.md` and this repo's [AGENTS.md](AGENTS.md).

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
