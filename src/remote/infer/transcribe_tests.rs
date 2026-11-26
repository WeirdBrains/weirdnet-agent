//! Infer/2 transcription and `Pool::transcribe` over in-memory channels.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use super::v2::{self, ClientFrame, Priority, QUEUE_KEEPALIVE, Segment, ServerFrame, TranscribeBackend, TranscribeRequest, Transcript, Usage};
use super::{Admission, InferHost};
use crate::cancel::CancelToken;
use crate::model::ModelBackend;
use crate::remote::catalog::{Catalog, CatalogModel, ModelState, Purpose};
use crate::remote::pool::{Pool, PoolOptions, Stall};
use crate::remote::{Caller, MemoryDialer, RemoteError, from_cbor, memory_pair, to_cbor};

const WAV: &[u8] = b"RIFF\x24\x00\x00\x00WAVEfmt ";

/// A speech-to-text backend that echoes what it was asked, after `delay`.
struct Echo {
    name: String,
    delay: Duration,
    seen: Mutex<Vec<TranscribeRequest>>,
    cancelled: AtomicUsize,
}

impl Echo {
    fn new(name: &str, delay: Duration) -> Arc<Self> {
        Arc::new(Self { name: name.into(), delay, seen: Mutex::new(Vec::new()), cancelled: AtomicUsize::new(0) })
    }
}

#[async_trait]
impl TranscribeBackend for Echo {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn transcribe(&self, request: &TranscribeRequest, cancel: &CancelToken) -> Result<Transcript, RemoteError> {
        self.seen.lock().unwrap().push(request.clone());
        // Counts a run that was cancelled or dropped before it finished.
        struct Stopped<'a>(Option<&'a AtomicUsize>);
        impl Drop for Stopped<'_> {
            fn drop(&mut self) {
                if let Some(n) = self.0 {
                    n.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        let mut stopped = Stopped(Some(&self.cancelled));
        tokio::select! {
            _ = tokio::time::sleep(self.delay) => {}
            _ = cancel.cancelled() => return Err(RemoteError::Cancelled),
        }
        stopped.0 = None;
        let text = format!("{} bytes of {}", request.audio.len(), request.format);
        let mut t = Transcript::new(text.clone()).with_language(request.language.clone().unwrap_or_else(|| "en".into())).with_duration_ms(1500);
        if request.timestamps {
            t = t.with_segments(vec![Segment::new(0, 700, "first"), Segment::new(700, 1500, "second")]);
        }
        Ok(t)
    }

    async fn transcribe_counted(&self, request: &TranscribeRequest, cancel: &CancelToken) -> Result<(Transcript, Option<Usage>), RemoteError> {
        let t = self.transcribe(request, cancel).await?;
        Ok((t, Some(Usage::new(375, 6))))
    }
}

/// Serves transcription through a catalog: names must be speech-to-text models.
struct Host {
    catalog: Catalog,
    backend: Option<Arc<Echo>>,
    opened: Arc<Mutex<Vec<Option<String>>>>,
}

impl InferHost for Host {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        if caller.peer == "stranger" { Err(RemoteError::refused("unknown-device")) } else { Ok(()) }
    }

    fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
        Err(RemoteError::unavailable("transcription only"))
    }

    fn open_transcribe(&self, _caller: &Caller, model: Option<&str>) -> Result<Arc<dyn TranscribeBackend>, RemoteError> {
        self.opened.lock().unwrap().push(model.map(str::to_string));
        let id = self.catalog.route_transcribe(model)?;
        match &self.backend {
            Some(b) if self.catalog.find(&id).is_some() => Ok(b.clone()),
            _ => Err(RemoteError::unavailable(format!("unknown model `{id}`"))),
        }
    }
}

/// A host that predates transcription: the default `open_transcribe`.
struct ChatOnly;

impl InferHost for ChatOnly {
    fn authorize(&self, _caller: &Caller) -> Result<(), RemoteError> {
        Ok(())
    }

    fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
        Err(RemoteError::unavailable("no models"))
    }
}

fn catalog() -> Catalog {
    Catalog::new(
        1,
        vec![
            CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm),
            CatalogModel::new("voxtral", Purpose::Transcribe, ModelState::Warm).with_replicas(1),
        ],
    )
}

fn serving(dialer: &MemoryDialer, node: &str, backend: Option<Arc<Echo>>, peer: &'static str) -> Arc<Mutex<Vec<Option<String>>>> {
    let opened = Arc::new(Mutex::new(Vec::new()));
    let host = Arc::new(Host { catalog: catalog(), backend, opened: opened.clone() });
    let admission = Admission::new(1);
    dialer.route_only(node, &[v2::PROTOCOL], move |ch, _| {
        let (host, admission) = (host.clone(), admission.clone());
        tokio::spawn(async move {
            let _ = v2::serve(ch, Caller::new(peer), host.as_ref(), &admission).await;
        });
    });
    opened
}

/// A node before 0.5.8: it can't decode `Transcribe` and drops the stream, as `v2::serve` did.
fn old_node(dialer: &MemoryDialer, node: &str) -> Arc<AtomicUsize> {
    let asked = Arc::new(AtomicUsize::new(0));
    let a = asked.clone();
    dialer.route_only(node, &[v2::PROTOCOL], move |mut ch, _| {
        let a = a.clone();
        tokio::spawn(async move {
            a.fetch_add(1, Ordering::SeqCst);
            let _ = ch.rx.recv().await;
            drop(ch);
        });
    });
    asked
}

fn pool(dialer: MemoryDialer, nodes: &[&str]) -> Pool {
    let opts =
        PoolOptions::default().with_accept_timeout(Duration::from_secs(5)).with_stall(Stall::new(Duration::from_secs(2), 3, Duration::from_secs(4)));
    Pool::new(Arc::new(dialer), nodes.iter().map(|n| n.to_string()).collect::<Vec<_>>()).with_options(opts)
}

#[test]
fn requests_and_transcripts_round_trip_with_audio_as_bytes() {
    let r = TranscribeRequest::new(WAV.to_vec(), "wav")
        .with_model("voxtral")
        .with_language("de")
        .with_prompt("Weirdnet, Ymir")
        .with_timestamps(true)
        .with_priority(Priority::Batch);
    assert!(!r.id.is_empty());
    let frame = ClientFrame::Transcribe(r.clone());
    let b = to_cbor(&frame).unwrap();
    // The audio is one CBOR byte string (major type 2), not an array or base64 text.
    let mut header = vec![0x40 | WAV.len() as u8];
    header.extend_from_slice(WAV);
    assert!(b.windows(header.len()).any(|w| w == header), "{b:x?}");
    assert_eq!(from_cbor::<ClientFrame>(&b).unwrap(), frame);

    // Unset options stay off the wire.
    let bare = TranscribeRequest::new(vec![1, 2, 3], "mp3");
    let v: serde_json::Value = serde_json::to_value(&bare).unwrap();
    let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    assert_eq!(keys, ["audio", "format", "id", "priority"]);
    assert_eq!(serde_json::from_value::<TranscribeRequest>(v).unwrap(), bare);

    let t = Transcript::new("hello world").with_language("en").with_duration_ms(2000).with_segments(vec![Segment::new(0, 900, "hello")]);
    let f = ServerFrame::Transcript { id: "1".into(), transcript: t.clone() };
    assert_eq!(from_cbor::<ServerFrame>(&to_cbor(&f).unwrap()).unwrap(), f);
    assert_eq!(f.id(), "1");
    let plain = serde_json::to_value(Transcript::new("hi")).unwrap();
    assert_eq!(plain, serde_json::json!({"text": "hi"}));
    let w = ServerFrame::Working { id: "2".into() };
    assert_eq!((from_cbor::<ServerFrame>(&to_cbor(&w).unwrap()).unwrap(), w.id()), (w.clone(), "2"));
}

#[test]
fn requests_need_audio_and_a_format() {
    assert!(TranscribeRequest::new(WAV.to_vec(), "wav").check().is_ok());
    assert!(matches!(TranscribeRequest::new(vec![], "wav").check(), Err(RemoteError::BadRequest { message }) if message.contains("audio")));
    assert!(matches!(TranscribeRequest::new(vec![1], " ").check(), Err(RemoteError::BadRequest { message }) if message.contains("format")));
}

#[tokio::test]
async fn the_pool_gets_a_transcript_with_usage_and_metadata() {
    let dialer = MemoryDialer::new();
    let echo = Echo::new("voxtral@node-b", Duration::ZERO);
    let opened = serving(&dialer, "inference", Some(echo.clone()), "phone");
    let request = TranscribeRequest::new(WAV.to_vec(), "wav").with_language("fr").with_prompt("names").with_timestamps(true);
    let t = pool(dialer, &["inference"]).transcribe(request.clone(), &CancelToken::new()).await.unwrap();
    assert_eq!(t.transcript.text, format!("{} bytes of wav", WAV.len()));
    assert_eq!((t.transcript.language.as_deref(), t.transcript.duration_ms), (Some("fr"), Some(1500)));
    assert_eq!(t.transcript.segments, [Segment::new(0, 700, "first"), Segment::new(700, 1500, "second")]);
    assert_eq!((t.node.as_str(), t.model.as_str()), ("inference", "voxtral@node-b"));
    assert_eq!(t.usage, Some(Usage::new(375, 6)));
    assert_eq!(t.meta.served_by.as_deref(), Some("voxtral@node-b"));
    let seen = echo.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!((seen[0].audio.as_slice(), seen[0].prompt.as_deref(), seen[0].timestamps), (WAV, Some("names"), true));
    assert_eq!(seen[0].cert, None, "the certificate is taken off before the backend sees the request");
    assert_eq!(*opened.lock().unwrap(), [None]);
}

#[tokio::test]
async fn bad_requests_never_reach_a_node_or_fail_over() {
    let dialer = MemoryDialer::new();
    let first = serving(&dialer, "a", Some(Echo::new("v", Duration::ZERO)), "phone");
    let second = serving(&dialer, "b", Some(Echo::new("v", Duration::ZERO)), "phone");
    let p = pool(dialer, &["a", "b"]);
    let err = p.transcribe(TranscribeRequest::new(vec![], "wav"), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err, RemoteError::BadRequest { .. }), "{err:?}");
    assert!(first.lock().unwrap().is_empty(), "checked before any node is asked");
    // A chat model named for transcription: the node's catalog refuses it, and no other node is asked.
    let err = p.transcribe(TranscribeRequest::new(WAV.to_vec(), "wav").with_model("gemma"), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(&err, RemoteError::BadRequest { message } if message.contains("gemma")), "{err:?}");
    assert_eq!((first.lock().unwrap().len(), second.lock().unwrap().len()), (1, 0));
}

#[tokio::test]
async fn the_server_checks_the_request_too() {
    let (mut ours, theirs) = memory_pair();
    let opened = Arc::new(Mutex::new(Vec::new()));
    let host = Host { catalog: catalog(), backend: Some(Echo::new("v", Duration::ZERO)), opened: opened.clone() };
    let server = tokio::spawn(async move { v2::serve(theirs, Caller::new("phone"), &host, &Admission::new(1)).await });
    let mut r = TranscribeRequest::new(vec![], "wav");
    r.id = "x".into();
    ours.send_cbor(&ClientFrame::Transcribe(r)).await.unwrap();
    let f = ours.recv_cbor::<ServerFrame>().await.unwrap().unwrap();
    assert!(matches!(f, ServerFrame::Failed { ref id, error: RemoteError::BadRequest { .. } } if id == "x"), "{f:?}");
    server.await.unwrap().unwrap();
    assert!(opened.lock().unwrap().is_empty());
}

#[tokio::test]
async fn strangers_are_refused_before_the_backend_opens() {
    let dialer = MemoryDialer::new();
    let opened = serving(&dialer, "a", Some(Echo::new("v", Duration::ZERO)), "stranger");
    let err = pool(dialer, &["a"]).transcribe(TranscribeRequest::new(WAV.to_vec(), "wav"), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err, RemoteError::Refused { .. }), "{err:?}");
    assert!(opened.lock().unwrap().is_empty());
}

#[tokio::test]
async fn old_and_chat_only_nodes_fail_over_to_one_that_transcribes() {
    let dialer = MemoryDialer::new();
    let old = old_node(&dialer, "old");
    let admission = Admission::new(1);
    dialer.route_only("chat-only", &[v2::PROTOCOL], move |ch, _| {
        let admission = admission.clone();
        tokio::spawn(async move { v2::serve(ch, Caller::new("phone"), &ChatOnly, &admission).await });
    });
    serving(&dialer, "new", Some(Echo::new("voxtral", Duration::ZERO)), "phone");
    let t = pool(dialer.clone(), &["old", "chat-only", "new"])
        .transcribe(TranscribeRequest::new(WAV.to_vec(), "wav"), &CancelToken::new())
        .await
        .unwrap();
    assert_eq!(t.node, "new");
    assert_eq!(old.load(Ordering::SeqCst), 1);

    // With only those two, the node answer (no transcription model) is reported over the old node's close.
    let err =
        pool(dialer.clone(), &["old", "chat-only"]).transcribe(TranscribeRequest::new(WAV.to_vec(), "wav"), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(&err, RemoteError::Unavailable { message } if message.contains("transcription")), "{err:?}");
    let err = pool(dialer, &["old"]).transcribe(TranscribeRequest::new(WAV.to_vec(), "wav"), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(&err, RemoteError::Unsupported { message } if message.contains("0.5.8")), "{err:?}");
}

#[tokio::test(start_paused = true)]
async fn long_transcriptions_keep_the_stream_alive() {
    // The pool's stall cap is 4 s; the backend takes 10 s and only `Working` frames arrive meanwhile.
    assert!(QUEUE_KEEPALIVE < Duration::from_secs(4));
    let dialer = MemoryDialer::new();
    serving(&dialer, "slow", Some(Echo::new("voxtral", Duration::from_secs(10))), "phone");
    let t = pool(dialer, &["slow"]).transcribe(TranscribeRequest::new(WAV.to_vec(), "wav"), &CancelToken::new()).await.unwrap();
    assert_eq!(t.model, "voxtral");
    assert!(t.meta.timings.total_ms >= 10_000, "{:?}", t.meta.timings);
}

#[tokio::test(start_paused = true)]
async fn working_frames_arrive_while_the_backend_runs() {
    let (mut ours, theirs) = memory_pair();
    let host = Host { catalog: catalog(), backend: Some(Echo::new("voxtral", Duration::from_secs(7))), opened: Default::default() };
    let server = tokio::spawn(async move { v2::serve(theirs, Caller::new("phone"), &host, &Admission::new(1)).await });
    let mut r = TranscribeRequest::new(WAV.to_vec(), "wav");
    r.id = "w".into();
    ours.send_cbor(&ClientFrame::Transcribe(r)).await.unwrap();
    let mut kinds = Vec::new();
    while let Some(f) = ours.recv_cbor::<ServerFrame>().await.unwrap() {
        kinds.push(match f {
            ServerFrame::Accepted { .. } => "accepted",
            ServerFrame::Working { .. } => "working",
            ServerFrame::Transcript { .. } => "transcript",
            ServerFrame::Done { .. } => "done",
            other => panic!("unexpected {other:?}"),
        });
        if kinds.last() == Some(&"done") {
            break;
        }
    }
    server.await.unwrap().unwrap();
    assert_eq!(kinds, ["accepted", "working", "working", "transcript", "done"]);
}

#[tokio::test]
async fn cancelling_stops_the_backend() {
    let dialer = MemoryDialer::new();
    let echo = Echo::new("voxtral", Duration::from_secs(3600));
    serving(&dialer, "a", Some(echo.clone()), "phone");
    let p = pool(dialer, &["a"]);
    let cancel = CancelToken::new();
    let c = cancel.clone();
    let e = echo.clone();
    tokio::spawn(async move {
        while e.seen.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        c.cancel();
    });
    let err = p.transcribe(TranscribeRequest::new(WAV.to_vec(), "wav"), &cancel).await.unwrap_err();
    assert_eq!(err, RemoteError::Cancelled);
    for _ in 0..200 {
        if echo.cancelled.load(Ordering::SeqCst) == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the backend was not stopped");
}
