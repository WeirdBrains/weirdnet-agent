# Changelog

## [Unreleased]

## [0.5.11] - 2026-11-27

### Added

- Speech out (text to speech) for Ymir's audio design and the four target speech models (Qwen3-TTS, Kokoro-82M, VoxCPM2, Fish Audio Drama 3): the new `speech` module with `AudioFormat` (`wav`, `pcm16`, `mp3`, `opus`, `flac`; `as_str`, `parse`, `mime`, `ALL`), `CloneMode` (`audio`, `audio_and_text`, `audio_text_required`), `SpeechProfile` (presets, design, instructions, clone, script speakers, max voices, raw, speed, streaming, languages, formats, sample rate, input/reference/output limits, license; every field absent when unknown) with `tightest`, `narrowed_by` and `covers`, `SpeechNeeds` and `wav_duration_ms`; re-exported at the crate root.
- `Purpose::Speak` (`speak` on the wire, the same name as weirdnet-model's `hw::Purpose`), `CatalogModel::speech` (an optional `SpeechProfile`, off the wire when absent) with `with_speech`, `speaks` and `speech_covers`, `Catalog::speakers` and `Catalog::route_speak(model, &needs)`. Askers before 0.5.8 never see `speak` entries; askers from 0.5.8 skip them as unreadable.
- infer/2 `ClientFrame::Speak(SpeakRequest)` (`SpeakInput::{Text, Script, Raw}` with `ScriptTurn`, `Voice::{Preset, Design, Clone}` with the reference clip as a CBOR byte string and `consent`, per-speaker `SpeakerVoice`s, instructions, language, per-mille speed, seed, format, sample rate, stream, priority, cert; builders, `check`, `needs`, `all_voices`, `clones`), answered by `Working` keepalives, one `ServerFrame::SpeechStart { format, sample_rate, channels }`, `ServerFrame::Audio { offset, data }` chunks of at most `MAX_AUDIO_CHUNK` (256 KiB) and `Done`. `MAX_REFERENCE_BYTES` (30 MiB). `Usage::input_chars` and `Usage::audio_ms` (with `Usage::speech`), `Meta::ignored` (with `Meta::with_ignored`), all off the wire when unset.
- `SpeakBackend`, `AudioStream` (`channel`, `from_file`, `from_pieces`, `next`), `AudioSink`, `SpeechPiece` and `AudioHeader`; `InferHost::open_speak(caller, request)` (default: unavailable). `serve` re-exports them.
- `Pool::speak` (a whole `Speech`) and `Pool::speak_stream` (a `SpeechStream` once the audio has started: `header`, `next`, `collect`, `meta`, `usage`), failing over across nodes only before `SpeechStart`.

## [0.5.10] - 2026-11-27

### Added

- Native video, for Bragi V4 and Ymir's video milestones 3 and 4: `ContentPart::VideoUrl { video_url: VideoUrl }` (`{"type": "video_url", "video_url": {"url": "data:video/...;base64,..."}}` in JSON and CBOR, the part vLLM and mistral.rs take) with `ContentPart::video` and `ContentPart::video_bytes`, `VideoUrl::mime_type` and `VideoUrl::data_len`; `Content::check` refuses video that is not a `data:video/` URL.
- `Modalities::video` (`video` on the wire, left off when false, so entries from older nodes count as not reading video), `Modalities::VIDEO` and `Modalities::with_video`; catalog/1 `inputs.video`. `Catalog::route` and the pool send a request with a video part only to a chat model whose entry reads video, and a refused video request's message ends with the new `catalog::VIDEO_FALLBACK` (send frames instead).
- `MediaLimits::video_max_seconds`, `video_max_bytes` and `video_frames_per_minute` (each absent when unknown, never defaulted) with `with_video_max_seconds`, `with_video_max_bytes`, `with_video_frames_per_minute`, `video_frame_rate`, `video_frames` and `check_video`; `or` and `tightest` cover them.

### Changed

- `Modalities::lacking` also names video (`"video"`, `"images and video"`, `"audio and video"`, `"images, audio and video"`).

## [0.5.9] - 2026-11-27

### Added

- Cross-identity pool sharing: let another Weirdnet identity use a pool with its own device certificate (catalog/1 and infer/2 only), without the pool's root identity.
- `remote::share`: `PoolShare` and `SharedNode`, a versioned share link (owner user id, optional name and expiry, inference node peer ids and addresses) with `wnet-pool1:` token and `weirdnet-pool://share/` link forms (`to_token`, `to_link`, `parse`, `is_share`, `validate`, `is_expired`, `node_ids`, `Display`, `FromStr`), `ShareError`, and the `SHARE_PREFIX`, `SHARE_LINK`, `SHARE_VERSION`, `MAX_SHARE_LEN` and `MAX_SHARE_NODES` constants.
- `serve`: `GuestAuth` (verifies a guest credential against the guest's user key and the transport peer, then asks the host's `GuestPolicy` per `GuestScope`), `Guest`, `GuestScope`, `GuestPolicy`, `DEFAULT_CLOCK_SKEW`, `SharedPoolAuth` (roster plus optional guests; `identify`, `authorize_for`, `scoped`, `admits_guests`) and `CallerIdentity`.
- `serve` and `pool`: `encode_credential`, `decode_presented` and `Presented` (a guest credential: the device's `NodeCert` plus its user's public key).
- `pool`: `connect_shared` (a `Pool` over a `PoolShare`, presenting the guest credential, no roster discovery) and `share_dialer`; re-exports `PoolShare`, `SharedNode` and `ShareError`.
- `CatalogHost::recheck` (default `Ok`), `catalog::RECHECK_EVERY` and `CatalogFeed::rechecking`: a subscription is re-checked before each update and every 5 s and closed with the refusal.

### Changed

- `RosterAuth::check` also reads a certificate presented inside a guest credential (still checked against the host user's key, so another user's is `wrong-user` as before).

## [0.5.8] - 2026-11-26

### Added

- Transcription (speech to text) for the video-upload work: `Purpose::Transcribe` (`transcribe` on the wire, the same name as weirdnet-model's `hw::Purpose`) with `Purpose::as_str`, `Purpose::parse` and `Display`; `CatalogModel::transcribes`, `Catalog::transcribers` and `Catalog::route_transcribe`; infer/2 `ClientFrame::Transcribe(TranscribeRequest)` answered by `ServerFrame::Transcript` (a `Transcript` with `Segment`s) and `ServerFrame::Working` keepalives; `TranscribeBackend`, `InferHost::open_transcribe` (default: unavailable) and `Pool::transcribe` returning a `Transcription`. `serve` re-exports `TranscribeBackend`.

### Changed

- catalog/1 leaves `transcribe` entries out for askers before 0.5.8, which could not decode them; `catalog::query` and `catalog::subscribe` now say the asker skips entries it can't read (a field older nodes ignore), and a catalog entry this version can't read is skipped instead of failing the whole catalog.

## [0.5.7] - 2026-11-26

### Added

- `MediaLimits` and `CatalogModel::limits`: optional per-model limits for images per request, image token cost and edge, audio length and token rate, and request bytes, left off the wire when unknown.

## [0.5.6] - 2026-11-25

### Added

- `jobs` feature: synced jobs over the weirdnet-sync job log (`JobBoard`, `JobRecord`, `NewJob`, `JobPolicy`, `CheckpointRef`, `ContentHash`, `Claim`, `Fence`, `JobFilter`, `JobChange`, `SyncHandle`), claimed under majority-vote leases with a takeover wait, retries with backoff, pause and resume, and resuming on another device from the last checkpoint. Needs weirdnet-sync 0.8.2.

## [0.5.5] - 2026-11-25

### Changed

- Accepts weirdnet-sync 0.8 (the `pool` and `serve` features now take `>=0.5.2, <0.9`).

## [0.5.4] - 2026-11-25

### Added

- `CatalogModel::origin` and `license`, with `with_origin`, `with_license` and the `ORIGIN_HUB`, `ORIGIN_UPLOAD` and `ORIGIN_TRAIN` names: where a catalog model came from and its license. Left off the wire when unset.

## [0.5.3] - 2026-11-25

### Added

- `remote::pool_model::PoolModel`: a model pool (such as Ymir) as one `ModelBackend`, with mid-answer resume, rerouting for images and audio, and an optional local fallback.
- `remote::pool_model::PoolWatch`, `PoolStatus` and `CatalogNodes`: a live view of the pool catalog for model pickers, and the nodes that answered it.
- `PoolWatch::retry`: ask the nodes again now instead of after the backoff (for a turn that wants the pool while it is unreachable).
- `ModelError::PoolUnreachable`, `ModelNotice`, `AgentEvent::ModelNotice` and `DeltaSink::notice`.

## [0.5.2] - 2026-11-23

### Added

- `Request::adapter_scale` and `with_adapter_scale`: how strongly a LoRA adapter applies for one request. Left off the wire when unset.
- `CatalogModel::base`, `with_base` and `is_adapter`: a LoRA adapter's base model, so apps can label adapters. Left off the wire for models.

## [0.5.1] - 2026-11-22

### Added

- `CacheMode` (`use`, `bypass`, `refresh`) and `Request::cache`: skip or refresh a node's answer cache for one request, without raw mode. Left off the wire when `use`.
- `Request::with_cache` and `Request::with_wait_for_model`.

## [0.5.0] - 2026-11-20

### Added

- Image and audio content parts: `Content` (text or `ContentPart`s: text, `image_url` with a data URL, `input_audio`), carried by infer/2 and the pool.
- `Modalities` (vision, audio), `Message::needs`, `Request::needs`, `Content::check` (data URLs only).
- `CatalogModel::inputs` / `with_inputs` / `accepts` and `Catalog::route`: the pool routes image and audio requests only to models that take them, and refuses with `BadRequest` when none does.

### Changed

- Breaking: `Message::content` is a `Content`; plain text keeps its wire form. `AgentSession::run` takes `impl Into<Content>`.
- Frames may be up to 32 MiB (was 8 MiB).
- Requests with images or audio skip infer/1 nodes.

## [0.4.5] - 2026-11-20

### Changed

- Metadata and docs scrub; no API changes.

## [0.4.4] - 2026-11-20

### Added

- Device roles: `RosterAuth::require_roles(any_of, RoleView)` admits only linked devices holding one of the roles (refusal `role`); `Authorize::admit` (default method) and `Admitted { peer, roles }`.
- `TaskHost::session_for` (default calls `session`) and `task::serve_for(ch, caller, auth, host)`, which admits the caller before building its session.
- `Server::start_with` and `ServingExtras::tasks`: serve task/1 on the node; task/1 is then listed in the `INFERENCE_APP` announcement.

### Changed

- The `pool` and `serve` features accept weirdnet-sync 0.5.2 up to 0.7 (`>=0.5.2, <0.8`).

## [0.4.3] - 2026-11-20

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
