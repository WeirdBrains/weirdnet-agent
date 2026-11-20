# Changelog

## [Unreleased]

### Added

- `LinkedNodes::with_retry_after` (default 30 s): how long an unreachable linked device is skipped before it is probed again. A connection from it, a network change or resuming clears it sooner.

### Changed

- The `pool` and `serve` features accept weirdnet-sync 0.5.2 up to 0.6 (`>=0.5.2, <0.7`), so apps on the 0.6 identity store share one roster type with the agent.

### Fixed

- `LinkedNodes::nodes` (called by every `Pool::infer` and `Pool::embed`) no longer waits `with_wait` for each offline or unreachable linked device in turn. Connected inference nodes are returned at once; other devices are probed concurrently in the background, a failed connect gives up at once, and only when no inference node is known does it wait, once, for all of them. Results are cached between calls, and the time each call took is logged at debug level.

## [0.4.2] - 2026-11-19

### Added

- Continue an answer: `Pool::infer` accepts a request that already carries an `assistant_prefix` (assistant prefill). The answer starts from it (`Answer::prefix`, `Answer::continuation`), events carry only new text, failover keeps it, and only nodes that say `resume: true` take it; others are skipped as `Unsupported`. `Request::continuing` turns a trailing assistant message into that prefix. No wire change.

### Changed

- Requires weirdnet-sync 0.5.2 (pool and serve features), so a device linked elsewhere reaches the others in one sync pass.

### Fixed

- `Pool::infer` no longer fails with missing characters when a request carries an `assistant_prefix` on its first attempt.
- `remote::task`: an approval card no longer overtakes the events sent before it (the asker could see the card before `TaskStarted`).

## [0.4.1] - 2026-11-19

### Added

- infer/2 `Input::Prompt` and `Request::prompt`: a raw text prompt for completions (no chat template, no tools; a prompt with tools is a bad request). `Request::raw_prompt` reads it. `ModelAdapter` answers `Unsupported`; 0.4.0 servers can't decode it and close the stream, so the pool moves on.
- infer/2 early metadata: `Output::meta` lets a backend report metadata before the answer (a cache hit before replaying a cached answer). Askers that set `Request::early_meta` get a `ServerFrame::Meta` frame; the pool sets it and reports `PoolEvent::Meta`. Either way `Done` carries it. Servers that predate it ignore the flag.
- Embedding usage: `EmbedBackend::embed_counted` (default: `embed` with no count) returns the prompt tokens; the server sums them into `Finish::usage` and the pool into the new `Embeddings::usage`.
- `RemoteError::is_request_error` (`BadRequest`, `Cancelled`) and `RemoteError::is_node_answer`.

### Fixed

- `Pool` keeps error kinds intact. `embed` and `catalog` no longer fold every node's error into one `Unavailable` (an embeddings `BadRequest` reached HTTP bridges as a 503). Request errors (`BadRequest`, `Cancelled`) are returned at once without trying other nodes; node and transport errors still fail over. When every node fails, `infer`, `embed` and `catalog` report the last node answer (`Unsupported`, `Refused`, `Unavailable`, `Busy`) before any transport error, so an unknown model is not hidden behind an unreachable node.

## [0.4.0] - 2026-11-19

### Added

- `remote::infer::v2` (`/weirdnet-agent/infer/2`, CBOR): priorities, `job_id`/`attempt`, inline messages or a `context_ref`, an assistant prefix to resume from, sampling, seed, `response_format`, tools and raw mode; `Queued`, `Accepted`, `Delta`/`Reasoning`/`ToolCall` frames with character offsets, `Usage`, and `Done` with metadata; f32 embeddings as byte strings. `InferBackend` and `EmbedBackend` for hosts that serve it; plain `ModelBackend`s are adapted.
- `Admission::with_queue`: a priority queue behind `Queued{position}` (interactive, batch, background); each `Queued` resets the caller's accept timeout.
- `remote::pool::Pool`: tries nodes, falls back to infer/1 on older nodes, cancels a stalled stream (no frame for max(15 s, 3× the recent token gap), capped at 25 s) and resumes on another node from the text already shown; also embeddings, background jobs, and catalog query/subscribe with failover.
- `remote::catalog` (`/weirdnet-agent/catalog/1`): models with purpose, state, replicas and speed; query and subscribe; `CatalogFeed` for hosts.
- `Caller` and `Authorize`: the serving side gets the transport peer and any presented certificate.
- `DeltaSink::reasoning` and `AgentEvent::ReasoningDelta`.
- `pool` feature: `NetDialer`, `LinkedNodes` (inference nodes among linked devices) and `connect` over `weirdnet-net` 0.8.
- `serve` feature: `Server` serves infer/1, infer/2 and catalog/1 on a `weirdnet-net` node; `RosterAuth` checks callers with `weirdnet-sync` 0.5 `Roster::check_caller`.
- `MemoryDialer::route_only`, to test peers that serve only some protocols.

### Changed

- **Breaking:** `InferHost` has a required `authorize(&Caller)`, and `open` takes the caller: `open(&self, caller: &Caller, model: Option<&str>)`. `infer::serve(ch, caller: &Caller, host, admission)`.
- **Breaking:** public enums are `#[non_exhaustive]`; `RemoteError` gains `Unsupported`, `BadRequest`, `Cancelled` and `Stalled` (sent as `failed` on infer/1).
- `RemoteModel` is built on `Pool`: it speaks infer/2 first and infer/1 to 0.3 nodes; resume is opt-in (`with_resume`).
- `remote::task` does not forward reasoning deltas.

## [0.3.0] - 2026-11-19

### Changed

- **Behaviour change:** `Budget::per_task` now counts tool time only, not wall-clock time; model turns and approval waits no longer count, so tasks can run longer than `per_task` in wall time (cancel on your own timer if you relied on it).
- A model turn is no longer cut off when the task time runs out; `max_steps` and cancellation bound turns.

### Added

- `ToolTimer` and `BudgetTracker::time_tool`, the guard that times each tool call.
- `BudgetTracker::wall_elapsed`, the task's wall-clock time.
- `Approver::extend_pages` (default: no) and `PagesPrompt`, to offer more pages when a task's page budget runs out; `remote::task` doesn't forward it yet.

## [0.2.0] - 2026-11-18

### Added

- `remote` module: protocols between one user's own nodes over an app-supplied `Dialer` and
  `Channel` (JSON messages; `remote::codec` frames them on byte streams; `MemoryDialer` and
  `memory_pair` for tests). The serving app checks that the caller is a linked device before it
  calls `serve`.
- `remote::infer` (`/weirdnet-agent/infer/1`): offload one model turn to another node, with the
  request, streamed tokens and cancel, and `Admission` to limit concurrent turns on the serving
  side. `RemoteModel` is a `ModelBackend` that tries nodes in order and then a local fallback
  whenever a node fails before any token has streamed (unreachable, busy, no such model, or no
  `Accepted` within the accept timeout); `last_route()` says where the last turn ran.
- `remote::task` (`/weirdnet-agent/task/1`): run a whole agent task on another node (Node
  companions). Agent events stream back, approval prompts are answered on the asking device, cancel
  stops the task there, and the updated conversation history comes back with the outcome.

## [0.1.0] - 2026-11-18

### Added

- Agent loop with streaming events and cancellation, tool sources and registry, tiered approval policy with per-site grants and Always allow, research budgets, and traces with citation checks.
