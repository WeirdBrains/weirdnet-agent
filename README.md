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
  15 s per page, 90 s of tool time per task (model turns and approval waits don't count), 4k
  tokens per page.
- `Trace`, `TraceStep`, `Citation`: every search, fetch, tool call, approval and notice is a step
  with its agent, node and session (logged out or signed in). `validate_citations` rejects a cited
  URL that none of its steps fetched; `retain_valid` drops such citations before display.
- Everything that is stored or synced implements serde.

- `remote`: protocols between one user's own nodes over an app-supplied `Dialer` / `Channel`
  (`codec` frames messages on byte streams, `MemoryDialer` for tests). The serving side gets a
  `Caller` (transport peer plus any presented certificate) and authorizes it.
  - `remote::infer::v2` (`/weirdnet-agent/infer/2`, CBOR): priorities with a queue
    (`Queued{position}`), sampling, seed, response format, tools, raw mode, reasoning and tool-call
    frames with character offsets, usage and metadata, embeddings, and an assistant prefix so an
    answer can resume on another node. `infer/1` (JSON) is still served for 0.3 callers.
  - `remote::pool::Pool`: tries nodes in order, cancels a stalled stream (max(15 s, 3× the token
    gap), under the transport's 30 s resume window) and resumes it elsewhere without repeating
    text; continues a stopped answer from a caller's `assistant_prefix` (`Request::continuing`)
    on nodes that resume; embeddings, background jobs and catalog subscribe with failover. `RemoteModel` is a
    `ModelBackend` over it, with a local fallback before any token has streamed.
  - `remote::catalog` (`/weirdnet-agent/catalog/1`): which models the nodes have and their state.
  - `remote::task` runs a whole task on another node (Node companions): events stream back as
    `AgentEvent`s, approval cards are answered on the asking device, cancel stops the task there.

Features:

- `pool`: the pool over `weirdnet-net`: `LinkedNodes` finds inference nodes among the user's
  linked devices (roster), `NetDialer` opens streams, `connect` builds the `Pool`.
- `serve`: `Server` serves infer/1, infer/2 and catalog/1 on a `weirdnet-net` node and announces
  it; `RosterAuth` admits only linked devices (`weirdnet-sync` `Roster::check_caller`).
- `testing`: `ScriptedModel`, `ScriptedApprover` and `FnTool` for downstream tests.

Migrating from 0.3: implement `InferHost::authorize` (for Weirdnet, `serve::RosterAuth`), add the
`caller` argument to `InferHost::open` and `infer::serve`, and add wildcard arms when matching
public enums (`AgentEvent::ReasoningDelta` is new).

Inference (`weirdnet-model`) and MCP client transports (`weirdnet-mcp`) are separate; the core
only defines the traits and protocols they plug into, and only the `pool` and `serve` features
depend on `weirdnet-net` and `weirdnet-sync`.

## License

BSD-3-Clause.
