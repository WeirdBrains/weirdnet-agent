# weirdnet-agent agents

The collective `AGENTS.md` in `WeirdBrains/weirdnet-agents` is mandatory and wins on conflict.

- No app-specific types (browser tabs, panels, storage schemas). Apps implement the traits.
- Policy changes follow the Companions design (Morph) and ship with tests for each rule.
- Build outside the source checkout, with `CARGO_TARGET_DIR` on a scratch disk; clean targets afterwards.
