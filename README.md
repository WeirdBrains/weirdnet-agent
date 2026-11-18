# weirdnet-agent

> Note: this crate, `weirdnet-agent` (singular), is unrelated to the private `WeirdBrains/weirdnet-agents` repo (plural), which only holds the shared `AGENTS.md`.

The shared agent runtime for WeirdBrains apps. Morph's Companions are its first user. Apps
supply a model, tools and an approval UI; this crate runs the loop, enforces the policy and
records what happened.

- `AgentSession::run`: one task. Calls the `ModelBackend`, checks each tool call against the
  `Policy`, asks the `Approver` when needed, charges the `Budget`, runs the tool with a timeout
  and the task's `CancelToken`, and streams `AgentEvent`s (text deltas, approvals, skipped-approval
  notices, tool start/finish, trace steps). Tool results reach the model wrapped as untrusted
  data, with the trace step ids it may cite.
- `Tool`, `ToolSource`, `LocalTools`, `ToolRegistry`: in-process tools now; MCP servers and
  remote nodes plug in as further sources. Per-source and per-tool switches; tier overrides
  can only make a tool stricter.
- `Policy`: tiers `Read` (attached content), `Navigate`, `Research`, `Interact`, `Never`;
  per-agent, per-site `Off` / `Ask` / `Act` grants; task approvals; revocable standing
  allows ("Always allow") for navigate (one site or everywhere) and research (optionally one
  provider), for one agent or all agents. Off always wins, signed-in research is never covered
  by a standing allow, and an all-agents allow covers an External agent only after its first
  approval.
- `Budget` / `BudgetTracker`: searches, pages (+ extensions), concurrency, per-page and per-task
  time, tokens kept per page, model steps. Defaults: 3 searches, 12 pages (+5), 2 at a time,
  15 s per page, 90 s per task, 4k tokens per page.
- `Trace`, `TraceStep`, `Citation`: every search, fetch, tool call, approval and notice is a step
  with its agent, node and session (logged out or signed in). `validate_citations` rejects a cited
  URL that none of its steps fetched; `retain_valid` drops such citations before display.
- Everything that is stored or synced implements serde.

Inference (`weirdnet-model`), MCP client transports (`weirdnet-mcp`) and the remote-agent protocol
over `weirdnet-net` are separate; this crate only defines the traits they implement.

The `testing` feature exposes `ScriptedModel`, `ScriptedApprover` and `FnTool` for downstream tests.

## License

BSD-3-Clause.
