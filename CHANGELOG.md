# Changelog

## [Unreleased]

### Changed

- The task time budget (`Budget::per_task`, default 90 s) counts only time spent in tool calls.
  Model turns and waiting for the user to answer an approval card no longer count, so a local
  model on a CPU (most of a minute per turn) no longer ends research before it has researched.
  `BudgetTracker::elapsed`, `remaining`, `check_time`, `page_timeout` and `BudgetUsage::elapsed_ms`
  now report tool time. A model turn is no longer cut off when the task time runs out; turns are
  bounded by `max_steps` and cancellation. No signatures changed, but tasks can now run longer in
  wall time than `per_task`; callers that relied on it as a wall-clock limit should cancel on their
  own timer.

### Added

- `BudgetTracker::time_tool` (a `ToolTimer` guard the session holds around every tool call;
  overlapping timers count the shared time once) and `BudgetTracker::wall_elapsed`.

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
