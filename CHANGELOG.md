# Changelog

## [Unreleased]

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
