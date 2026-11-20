//! Remote tasks: a whole agent task on another of the user's nodes (Node companions).
//!
//! ```text
//! asking node                      serving node (runs the AgentSession)
//!   Run { request } ->
//!                                  <- Started, Delta, Step, ToolStarted, ToolFinished, Skipped, Blocked ...
//!                                  <- Ask { ask, prompt, tool }
//!   Answer { ask, answer } ->
//!   Cancel ->                      (optional, any time)
//!                                  <- Finished { outcome, messages } | Failed { error }
//! ```
//!
//! The serving node's tools, budget and policy apply (its grants sync from the user's other
//! devices). Approval cards are shown on the asking device: the serving session's [`Approver`]
//! forwards each prompt and waits for the answer. The conversation lives with the asker: it
//! sends its history with each task and gets the updated history back.

use std::collections::HashMap;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use super::{Admitted, Authorize, Caller, Channel, Dialer, RemoteError, request_id};
use crate::cancel::CancelToken;
use crate::identity::AgentId;
use crate::model::Message;
use crate::policy::{ApprovalPrompt, DenyReason, SkipNotice};
use crate::session::{AgentEvent, AgentSession, ApprovalAnswer, Approver, Outcome};
use crate::tool::ToolSpec;
use crate::trace::TraceStep;

/// Stream protocol id for remote tasks.
pub const PROTOCOL: &str = "/weirdnet-agent/task/1";

/// How long the asker waits for the node to wind down after a cancel.
pub const CANCEL_GRACE: Duration = Duration::from_secs(5);

/// One task for a companion that runs on the serving node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskRequest {
    pub id: String,
    pub companion: AgentId,
    pub input: String,
    /// The conversation so far (the asker keeps it).
    #[serde(default)]
    pub history: Vec<Message>,
}

impl TaskRequest {
    pub fn new(companion: AgentId, input: impl Into<String>, history: Vec<Message>) -> Self {
        Self { id: request_id(), companion, input: input.into(), history }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "msg")]
#[non_exhaustive]
pub enum ClientMsg {
    Run { request: TaskRequest },
    Answer { ask: u64, answer: ApprovalAnswer },
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "msg")]
#[non_exhaustive]
pub enum ServerMsg {
    Started { task_id: String },
    Delta { text: String },
    Ask { ask: u64, prompt: ApprovalPrompt, tool: ToolSpec },
    Skipped { notice: SkipNotice },
    Blocked { tool: String, reason: DenyReason },
    ToolStarted { call_id: String, tool: String },
    ToolFinished { call_id: String, tool: String, is_error: bool },
    Step { step: TraceStep },
    Finished { id: String, outcome: Outcome, messages: Vec<Message> },
    Failed { id: String, error: RemoteError },
}

/// What the serving app offers: a session for the requested companion, built with the given
/// approver (which forwards cards to the asker). Refuse companions that do not run here.
pub trait TaskHost: Send + Sync {
    fn session(&self, request: &TaskRequest, approver: Arc<dyn Approver>) -> Result<AgentSession, RemoteError>;

    /// The session for an admitted caller ([`serve_for`]). Override it to scope tools, budget or
    /// companions per device or role; the default ignores the caller and calls
    /// [`session`](Self::session).
    fn session_for(&self, caller: &Admitted, request: &TaskRequest, approver: Arc<dyn Approver>) -> Result<AgentSession, RemoteError> {
        let _ = caller;
        self.session(request, approver)
    }
}

struct Ask {
    prompt: ApprovalPrompt,
    tool: ToolSpec,
    reply: oneshot::Sender<ApprovalAnswer>,
}

/// The serving session's approver: asks the far end and waits. A lost asker denies.
struct ForwardingApprover {
    asks: mpsc::UnboundedSender<Ask>,
}

#[async_trait]
impl Approver for ForwardingApprover {
    async fn decide(&self, prompt: &ApprovalPrompt, tool: &ToolSpec) -> ApprovalAnswer {
        let (reply, answer) = oneshot::channel();
        if self.asks.send(Ask { prompt: prompt.clone(), tool: tool.clone(), reply }).is_err() {
            return ApprovalAnswer::Deny;
        }
        answer.await.unwrap_or(ApprovalAnswer::Deny)
    }
}

fn wire_event(ev: AgentEvent) -> Option<ServerMsg> {
    Some(match ev {
        AgentEvent::TaskStarted { task_id } => ServerMsg::Started { task_id },
        AgentEvent::TextDelta(text) => ServerMsg::Delta { text },
        // task/1 peers (0.3 and older) know no reasoning message; it stays on the serving node.
        AgentEvent::ReasoningDelta(_) => return None,
        // The Ask message carries the prompt with the tool.
        AgentEvent::ApprovalRequested(_) => return None,
        AgentEvent::ApprovalSkipped(notice) => ServerMsg::Skipped { notice },
        AgentEvent::Blocked { tool, reason } => ServerMsg::Blocked { tool, reason },
        AgentEvent::ToolStarted { call_id, tool } => ServerMsg::ToolStarted { call_id, tool },
        AgentEvent::ToolFinished { call_id, tool, is_error } => ServerMsg::ToolFinished { call_id, tool, is_error },
        AgentEvent::Step(step) => ServerMsg::Step { step },
        // Sent with the outcome.
        AgentEvent::Finished { .. } => return None,
    })
}

/// Serve one task on an accepted channel. The caller has already checked that the far end is a
/// linked device (prefer [`serve_for`], which checks it here). If the asker goes away the task
/// is cancelled.
pub async fn serve(ch: Channel, host: &dyn TaskHost) -> Result<(), RemoteError> {
    serve_inner(ch, None, host).await
}

/// Serve one task for `caller`: `auth` admits it first ([`Authorize::admit`]; a refusal is sent
/// back as `Failed`), then the session comes from [`TaskHost::session_for`].
pub async fn serve_for(ch: Channel, caller: Caller, auth: &dyn Authorize, host: &dyn TaskHost) -> Result<(), RemoteError> {
    serve_inner(ch, Some((caller, auth)), host).await
}

async fn serve_inner(mut ch: Channel, check: Option<(Caller, &dyn Authorize)>, host: &dyn TaskHost) -> Result<(), RemoteError> {
    let request = match ch.recv::<ClientMsg>().await? {
        Some(ClientMsg::Run { request }) => request,
        _ => return Ok(()),
    };
    let id = request.id.clone();
    let (ask_tx, mut ask_rx) = mpsc::unbounded_channel();
    let approver: Arc<dyn Approver> = Arc::new(ForwardingApprover { asks: ask_tx });
    let made = match check {
        None => host.session(&request, approver),
        Some((caller, auth)) => auth.admit(&caller).and_then(|who| host.session_for(&who, &request, approver)),
    };
    let mut session = match made {
        Ok(s) => s,
        Err(error) => return ch.send(&ServerMsg::Failed { id, error }).await,
    };
    session.restore_messages(request.history.clone());
    let cancel = CancelToken::new();
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel();
    let mut pending: HashMap<u64, oneshot::Sender<ApprovalAnswer>> = HashMap::new();
    let mut next_ask = 0u64;
    let mut asker_open = true;
    let mut send_failed = false;
    let outcome = {
        let mut run = pin!(session.run(request.input.clone(), Some(ev_tx), cancel.clone()));
        loop {
            // Biased, events first: an approval card must not overtake the events the session
            // sent before it (the asker would see the card before `TaskStarted`).
            tokio::select! {
                biased;
                Some(ev) = ev_rx.recv() => {
                    if let Some(msg) = wire_event(ev)
                        && !send_failed
                        && ch.send(&msg).await.is_err()
                    {
                        send_failed = true;
                        cancel.cancel();
                    }
                }
                Some(ask) = ask_rx.recv() => {
                    next_ask += 1;
                    let msg = ServerMsg::Ask { ask: next_ask, prompt: ask.prompt, tool: ask.tool };
                    pending.insert(next_ask, ask.reply);
                    if send_failed || ch.send(&msg).await.is_err() {
                        send_failed = true;
                        pending.clear();
                        cancel.cancel();
                    }
                }
                m = ch.rx.recv(), if asker_open => match m.map(|b| b.map(|b| serde_json::from_slice::<ClientMsg>(&b))) {
                    Ok(Some(Ok(ClientMsg::Answer { ask, answer }))) => {
                        if let Some(reply) = pending.remove(&ask) {
                            let _ = reply.send(answer);
                        }
                    }
                    Ok(Some(Ok(ClientMsg::Cancel))) => cancel.cancel(),
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => {
                        asker_open = false;
                        pending.clear();
                        cancel.cancel();
                    }
                },
                o = &mut run => break o,
            }
        }
    };
    if send_failed || !asker_open {
        return Ok(());
    }
    while let Ok(ev) = ev_rx.try_recv() {
        if let Some(msg) = wire_event(ev) {
            ch.send(&msg).await?;
        }
    }
    ch.send(&ServerMsg::Finished { id, outcome, messages: session.messages().to_vec() }).await
}

/// A finished remote task and the conversation to keep.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteOutcome {
    pub outcome: Outcome,
    pub messages: Vec<Message>,
}

/// Run a task on `node`. Events arrive as ordinary [`AgentEvent`]s (so the app's panel code is
/// the same as for a local session), approval cards go to `approver` here, and `cancel` stops
/// the task on the node.
pub async fn run(
    dialer: &dyn Dialer,
    node: &str,
    request: TaskRequest,
    events: Option<mpsc::UnboundedSender<AgentEvent>>,
    approver: &dyn Approver,
    cancel: &CancelToken,
) -> Result<RemoteOutcome, RemoteError> {
    let emit = |e: AgentEvent| {
        if let Some(tx) = &events {
            let _ = tx.send(e);
        }
    };
    let mut ch = dialer.open(node, PROTOCOL).await?;
    let id = request.id.clone();
    ch.send(&ClientMsg::Run { request }).await?;
    let mut cancel_sent = false;
    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled(), if !cancel_sent => {
                cancel_sent = true;
                let _ = ch.send(&ClientMsg::Cancel).await;
                continue;
            }
            m = async {
                if cancel_sent {
                    tokio::time::timeout(CANCEL_GRACE, ch.recv::<ServerMsg>()).await.unwrap_or(Err(RemoteError::Timeout))
                } else {
                    ch.recv::<ServerMsg>().await
                }
            } => m?,
        };
        let Some(msg) = msg else { return Err(RemoteError::Closed) };
        match msg {
            ServerMsg::Started { task_id } => emit(AgentEvent::TaskStarted { task_id }),
            ServerMsg::Delta { text } => emit(AgentEvent::TextDelta(text)),
            ServerMsg::Skipped { notice } => emit(AgentEvent::ApprovalSkipped(notice)),
            ServerMsg::Blocked { tool, reason } => emit(AgentEvent::Blocked { tool, reason }),
            ServerMsg::ToolStarted { call_id, tool } => emit(AgentEvent::ToolStarted { call_id, tool }),
            ServerMsg::ToolFinished { call_id, tool, is_error } => emit(AgentEvent::ToolFinished { call_id, tool, is_error }),
            ServerMsg::Step { step } => emit(AgentEvent::Step(step)),
            ServerMsg::Ask { ask, prompt, tool } => {
                emit(AgentEvent::ApprovalRequested(prompt.clone()));
                let answer = if cancel_sent {
                    ApprovalAnswer::Deny
                } else {
                    tokio::select! {
                        a = approver.decide(&prompt, &tool) => a,
                        _ = cancel.cancelled() => {
                            cancel_sent = true;
                            let _ = ch.send(&ClientMsg::Cancel).await;
                            ApprovalAnswer::Deny
                        }
                    }
                };
                ch.send(&ClientMsg::Answer { ask, answer }).await?;
            }
            ServerMsg::Finished { id: rid, outcome, messages } if rid == id => {
                emit(AgentEvent::Finished { task_id: outcome.task_id.clone(), stop: outcome.stop.clone() });
                return Ok(RemoteOutcome { outcome, messages });
            }
            ServerMsg::Failed { id: rid, error } if rid == id => return Err(error),
            other => return Err(RemoteError::protocol(format!("unexpected message {other:?}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;
    use crate::identity::{AgentIdentity, AgentKind};
    use crate::model::{DeltaSink, ModelBackend, ModelError, ModelTurn};
    use crate::policy::{Policy, Scope, SharedPolicy, Tier};
    use crate::remote::MemoryDialer;
    use crate::session::{AgentConfig, StopReason};
    use crate::testing::{FnTool, ScriptedApprover, ScriptedModel, call};
    use crate::tool::{CallTarget, LocalTools, ToolOutput, ToolRegistry};

    struct Host {
        tools: Arc<ToolRegistry>,
        policy: SharedPolicy,
        model: Mutex<Option<Arc<dyn ModelBackend>>>,
    }

    impl TaskHost for Host {
        fn session(&self, request: &TaskRequest, approver: Arc<dyn Approver>) -> Result<AgentSession, RemoteError> {
            if request.companion != AgentId::new("atlas") {
                return Err(RemoteError::Refused { message: format!("{} does not run here", request.companion) });
            }
            let model = self.model.lock().unwrap().take().ok_or(RemoteError::Unavailable { message: "used".into() })?;
            let identity = AgentIdentity::new("atlas", "Atlas", AgentKind::Node).with_node("desktop");
            Ok(AgentSession::new(AgentConfig::new(identity), model, self.tools.clone(), self.policy.clone(), approver))
        }
    }

    async fn host(model: Arc<dyn ModelBackend>, ran: Arc<Mutex<Vec<String>>>) -> Arc<Host> {
        let nav = FnTool::new(ToolSpec::new("browser.navigate", "go").tier(Tier::Navigate), move |args| {
            ran.lock().unwrap().push(args["url"].as_str().unwrap_or_default().to_string());
            Ok(ToolOutput::text("navigated"))
        })
        .with_target(|args| CallTarget::url(args["url"].as_str().unwrap_or_default()));
        let mut local = LocalTools::new("browser");
        local.add(Arc::new(nav));
        let mut r = ToolRegistry::new();
        r.add_source(Arc::new(local));
        r.refresh().await.unwrap();
        Arc::new(Host { tools: Arc::new(r), policy: Policy::new().shared(), model: Mutex::new(Some(model)) })
    }

    fn route(dialer: &MemoryDialer, node: &str, host: Arc<Host>) {
        dialer.route(node, move |ch, protocol| {
            assert_eq!(protocol, PROTOCOL);
            let host = host.clone();
            tokio::spawn(async move {
                let _ = serve(ch, host.as_ref()).await;
            });
        });
    }

    fn drain(rx: &mut mpsc::UnboundedReceiver<AgentEvent>) -> Vec<AgentEvent> {
        let mut v = Vec::new();
        while let Ok(e) = rx.try_recv() {
            v.push(e);
        }
        v
    }

    #[tokio::test]
    async fn a_task_runs_on_the_node_and_its_approval_is_answered_here() {
        let model = ScriptedModel::new(vec![
            ModelTurn { text: String::new(), tool_calls: vec![call("1", "browser.navigate", json!({"url": "https://tokio.rs"}))] },
            ModelTurn { text: "tokio is async".into(), tool_calls: vec![] },
        ]);
        let ran = Arc::new(Mutex::new(Vec::new()));
        let dialer = MemoryDialer::new();
        route(&dialer, "desktop", host(Arc::new(model), ran.clone()).await);
        let approver = ScriptedApprover::new(vec![ApprovalAnswer::Approve { scope: Scope::Once }]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let history = vec![Message::user("earlier"), Message::assistant("before", vec![])];
        let req = TaskRequest::new(AgentId::new("atlas"), "what is tokio", history.clone());
        let got = run(&dialer, "desktop", req, Some(tx), &approver, &CancelToken::new()).await.unwrap();

        assert_eq!(got.outcome.stop, StopReason::Completed);
        assert_eq!(got.outcome.text, "tokio is async");
        assert_eq!(*ran.lock().unwrap(), ["https://tokio.rs"], "the tool ran on the node");
        assert_eq!(approver.asked().len(), 1, "the card was answered on the asking device");
        assert_eq!(approver.asked()[0].request.site.as_deref(), Some("tokio.rs"));
        assert_eq!(&got.messages[..2], &history[..], "history went over and came back");
        assert_eq!(got.messages.last().unwrap().content, "tokio is async");
        assert!(got.outcome.trace.steps.iter().all(|s| s.node.as_deref() == Some("desktop")), "the trace names the node");

        let ev = drain(&mut rx);
        assert!(matches!(ev.first(), Some(AgentEvent::TaskStarted { .. })));
        assert!(ev.iter().any(|e| matches!(e, AgentEvent::ApprovalRequested(_))));
        assert!(ev.iter().any(|e| matches!(e, AgentEvent::ToolFinished { is_error: false, .. })));
        let text: String = ev.iter().filter_map(|e| if let AgentEvent::TextDelta(t) = e { Some(t.as_str()) } else { None }).collect();
        assert_eq!(text, "tokio is async");
        assert!(matches!(ev.last(), Some(AgentEvent::Finished { stop: StopReason::Completed, .. })));
    }

    #[tokio::test]
    async fn a_declined_card_blocks_the_tool_on_the_node() {
        let model = ScriptedModel::new(vec![
            ModelTurn { text: String::new(), tool_calls: vec![call("1", "browser.navigate", json!({"url": "https://x.org"}))] },
            ModelTurn { text: "ok".into(), tool_calls: vec![] },
        ]);
        let ran = Arc::new(Mutex::new(Vec::new()));
        let dialer = MemoryDialer::new();
        route(&dialer, "desktop", host(Arc::new(model), ran.clone()).await);
        let approver = ScriptedApprover::new(vec![ApprovalAnswer::Deny]);
        let got = run(&dialer, "desktop", TaskRequest::new(AgentId::new("atlas"), "go", vec![]), None, &approver, &CancelToken::new()).await.unwrap();
        assert_eq!(got.outcome.stop, StopReason::Completed);
        assert!(ran.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_node_refuses_companions_it_does_not_run_and_unreachable_nodes_say_so() {
        let dialer = MemoryDialer::new();
        route(&dialer, "desktop", host(Arc::new(ScriptedModel::new(vec![])), Arc::default()).await);
        let approver = ScriptedApprover::default();
        let err =
            run(&dialer, "desktop", TaskRequest::new(AgentId::new("other"), "q", vec![]), None, &approver, &CancelToken::new()).await.unwrap_err();
        assert!(matches!(err, RemoteError::Refused { .. }), "{err:?}");
        let err =
            run(&dialer, "laptop", TaskRequest::new(AgentId::new("atlas"), "q", vec![]), None, &approver, &CancelToken::new()).await.unwrap_err();
        assert!(matches!(err, RemoteError::Unreachable { .. }), "{err:?}");
    }

    struct Slow;

    #[async_trait]
    impl ModelBackend for Slow {
        fn name(&self) -> String {
            "slow".into()
        }
        async fn complete(&self, _m: &[Message], _t: &[ToolSpec], deltas: &DeltaSink, cancel: &CancelToken) -> Result<ModelTurn, ModelError> {
            deltas.text("hmm");
            cancel.cancelled().await;
            Err(ModelError::Cancelled)
        }
    }

    #[tokio::test]
    async fn cancel_here_stops_the_task_there() {
        let dialer = MemoryDialer::new();
        route(&dialer, "desktop", host(Arc::new(Slow), Arc::default()).await);
        let cancel = CancelToken::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let job = {
            let (dialer, cancel) = (dialer.clone(), cancel.clone());
            tokio::spawn(async move {
                let approver = ScriptedApprover::default();
                run(&dialer, "desktop", TaskRequest::new(AgentId::new("atlas"), "q", vec![]), Some(tx), &approver, &cancel).await
            })
        };
        loop {
            if let Some(AgentEvent::TextDelta(_)) = rx.recv().await {
                break;
            }
        }
        cancel.cancel();
        let got = job.await.unwrap().unwrap();
        assert_eq!(got.outcome.stop, StopReason::Cancelled);
    }
}
