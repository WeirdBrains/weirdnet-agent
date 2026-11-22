# Handoff

Running log for [weirdnet-agent](https://github.com/WeirdBrains/weirdnet-agent), newest first. Shared rules: the collective `AGENTS.md` and this repo's [AGENTS.md](AGENTS.md).

## 2026-11-22: per-request answer-cache mode (for 0.5.1)

What changed ([`d81ef92`](https://github.com/WeirdBrains/weirdnet-agent/commit/d81ef928e8b67d3566384bab719af1599e4d2b0a)): `remote::infer::v2` gains `CacheMode` (`use`, `bypass`, `refresh`; `#[non_exhaustive]`, serde snake_case) with `may_read` / `may_store`, a `Request::cache` field (serde default, left off the wire when `use`), and the builders `Request::with_cache` and `Request::with_wait_for_model`. Requested for [Ymir](https://github.com/WeirdBrains/ymir)'s training evals, which need to skip the answer cache without raw mode. `Request` is already `#[non_exhaustive]`, so this is additive: a patch release, 0.5.1.

Checks run: `cargo fmt --check`, `cargo clippy --all-targets` with and without `--all-features` (`-D warnings`), `cargo test --all-features`, `cargo test --no-default-features`, `cargo doc --no-deps --all-features` with `-D warnings`. New tests: the field stays off the wire by default, old requests read as `use`, `bypass`/`refresh` round-trip and survive retries, and a reader without the field still decodes the request.

Not tested: no live node; nodes older than 0.5.1 ignore the field and may still answer from their cache (`Meta::cache_hit` reports it).

What's left and next: release 0.5.1 once Michael OKs it, then [weirdnet-semcache](https://github.com/WeirdBrains/weirdnet-semcache) 0.2.1 and [weirdnet-openai](https://github.com/WeirdBrains/weirdnet-openai) 0.2.1 (branches `cache-mode`), which honour the field.
