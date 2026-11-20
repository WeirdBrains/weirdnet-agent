# weirdnet-agent

> Note: this crate, `weirdnet-agent` (singular), is unrelated to the private `WeirdBrains/weirdnet-agents` repo (plural), which only holds the shared `AGENTS.md`.

The shared agent runtime for [WeirdBrains](https://weirdbrains.com) apps. Morph's Companions are its first user. Apps
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
- `Message::content` is a `Content`: text, or OpenAI-style parts (`ContentPart::text`,
  `ContentPart::image` with a `data:image/...` URL, `ContentPart::audio`). Text still goes on the
  wire as a plain string, so text-only messages reach older peers as before; parts need 0.5 on
  both ends. `Message::needs()` / `Request::needs()` give the `Modalities` (vision, audio) a model
  must accept, and `Message::text()` the text alone.

  ```rust
  let ask = Message::user(vec![ContentPart::text("What is in this photo?"), ContentPart::image(data_url)]);
  ```

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
  - `remote::catalog` (`/weirdnet-agent/catalog/1`): which models the nodes have, their state and
    the inputs beyond text they take (`CatalogModel::inputs`; entries without it are text-only).
    `Catalog::route` picks the model for a request with images or audio. The pool uses it, so such
    a request never lands on a text-only model: a named model that can't read it, or a pool with
    no model that can, is a `BadRequest` before any node runs it. Requests with parts skip infer/1
    nodes. Frames may be up to 32 MiB (8 MiB before 0.5).
  - `remote::task` runs a whole task on another node (Node companions): events stream back as
    `AgentEvent`s, approval cards are answered on the asking device, cancel stops the task there.

Features:

- `pool`: the pool over `weirdnet-net`: `LinkedNodes` finds inference nodes among the user's
  linked devices (roster), `NetDialer` opens streams, `connect` builds the `Pool`. Known
  inference nodes are returned at once; other devices are probed together in the background,
  and unreachable ones are skipped for a while (`with_retry_after`).
- `serve`: `Server` serves infer/1, infer/2 and catalog/1 on a `weirdnet-net` node and announces
  it; `RosterAuth` admits only linked devices (`weirdnet-sync` `Roster::check_caller`).
  `RosterAuth::require_roles(&["ymir.inference"], roles)` also needs one of the device roles the
  user granted (with sync 0.7, `|peer| roster.roles_of(peer)`), refusing others with `role`.
  `Server::start_with(.., ServingExtras::default().tasks(host, auth))` serves task/1 too: each
  task is admitted (`Authorize::admit`, giving `Admitted { peer, roles }`) and built by
  `TaskHost::session_for`, so hosts can scope tools per device or role; the announcement then
  lists task/1.
- `testing`: `ScriptedModel`, `ScriptedApprover` and `FnTool` for downstream tests.

Migrating from 0.4: `Message::content` is a `Content`, not a `String`. Constructors take
`impl Into<Content>`, so `Message::user("hi")` is unchanged; read text with `m.text()` or
`m.content.text()`, and build parts with `ContentPart`. `AgentSession::run` takes
`impl Into<Content>`.

Migrating from 0.3: implement `InferHost::authorize` (for Weirdnet, `serve::RosterAuth`), add the
`caller` argument to `InferHost::open` and `infer::serve`, and add wildcard arms when matching
public enums (`AgentEvent::ReasoningDelta` is new).

Inference (`weirdnet-model`) and MCP client transports (`weirdnet-mcp`) are separate; the core
only defines the traits and protocols they plug into, and only the `pool` and `serve` features
depend on `weirdnet-net` and `weirdnet-sync` (0.5.2 to 0.7).

## License

BSD-3-Clause.
