//! Infer/2 speech out and `Pool::speak` / `Pool::speak_stream` over in-memory channels.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use super::v2::{
    self, AudioHeader, AudioStream, ClientFrame, MAX_AUDIO_CHUNK, MAX_REFERENCE_BYTES, Meta, Priority, QUEUE_KEEPALIVE, ScriptTurn, ServerFrame,
    SpeakBackend, SpeakInput, SpeakRequest, SpeechPiece, Usage, Voice,
};
use super::{Admission, InferHost};
use crate::cancel::CancelToken;
use crate::model::ModelBackend;
use crate::remote::catalog::{Catalog, CatalogModel, ModelState, Purpose};
use crate::remote::pool::{Pool, PoolOptions, Stall};
use crate::remote::{Caller, MemoryDialer, RemoteError, from_cbor, memory_pair, to_cbor};
use crate::speech::{AudioFormat, CloneMode, SpeechNeeds, SpeechProfile};

/// A 16 kHz mono 16-bit WAV of `samples` samples.
fn wav(samples: u32) -> Vec<u8> {
    let data = samples * 2;
    let mut b = b"RIFF".to_vec();
    b.extend((36 + data).to_le_bytes());
    b.extend(b"WAVEfmt ");
    b.extend(16u32.to_le_bytes());
    b.extend(1u16.to_le_bytes());
    b.extend(1u16.to_le_bytes());
    b.extend(16_000u32.to_le_bytes());
    b.extend(32_000u32.to_le_bytes());
    b.extend(2u16.to_le_bytes());
    b.extend(16u16.to_le_bytes());
    b.extend(b"data");
    b.extend(data.to_le_bytes());
    b.extend((0..data).map(|i| i as u8));
    b
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    /// The finished file at once, after `delay`.
    File,
    /// Streams `pieces` pieces, `delay` apart, from a spawned task.
    Stream,
    /// Refuses before any audio with this error.
    Refuse(&'static str),
    /// Starts, sends one piece, then fails.
    FailAfterStart,
    /// Fails in the stream before the start.
    FailBeforeStart,
}

/// A speech backend that "speaks" `bytes` bytes of audio per input character.
struct Mouth {
    name: String,
    mode: Mode,
    delay: Duration,
    seen: Mutex<Vec<SpeakRequest>>,
    stopped: Arc<AtomicUsize>,
}

impl Mouth {
    fn new(name: &str, mode: Mode, delay: Duration) -> Arc<Self> {
        Arc::new(Self { name: name.into(), mode, delay, seen: Mutex::new(Vec::new()), stopped: Arc::new(AtomicUsize::new(0)) })
    }
}

/// The audio a request gets: `chars × 1000` bytes, counting up.
fn audio_for(r: &SpeakRequest) -> Vec<u8> {
    (0..r.input.chars() * 1000).map(|i| (i % 251) as u8).collect()
}

#[async_trait]
impl SpeakBackend for Mouth {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn speak(&self, request: &SpeakRequest, cancel: &CancelToken) -> Result<AudioStream, RemoteError> {
        self.seen.lock().unwrap().push(request.clone());
        let header = AudioHeader::new(request.format.unwrap_or(AudioFormat::Wav)).with_sample_rate(24_000).with_channels(1);
        let audio = audio_for(request);
        match self.mode {
            Mode::Refuse("unsupported") => Err(RemoteError::Unsupported { message: "this engine can't".into() }),
            Mode::Refuse(m) => Err(RemoteError::unavailable(m)),
            Mode::File => {
                // Counts a synthesis that was cancelled or dropped before it finished.
                struct Stopped<'a>(Option<&'a AtomicUsize>);
                impl Drop for Stopped<'_> {
                    fn drop(&mut self) {
                        if let Some(n) = self.0 {
                            n.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
                let mut stopped = Stopped(Some(&self.stopped));
                tokio::select! {
                    _ = tokio::time::sleep(self.delay) => {}
                    _ = cancel.cancelled() => return Err(RemoteError::Cancelled),
                }
                stopped.0 = None;
                let mut usage = Usage::speech(0, 1250);
                usage.completion_tokens = 42;
                let mut pieces = vec![Ok(SpeechPiece::Start(header)), Ok(SpeechPiece::Audio(audio)), Ok(SpeechPiece::Usage(usage))];
                if matches!(request.voice, Some(Voice::Clone { text: Some(_), .. })) {
                    pieces.push(Ok(SpeechPiece::Meta(Meta::default().with_ignored("reference_text"))));
                }
                Ok(AudioStream::from_pieces(pieces))
            }
            Mode::Stream => {
                let (sink, stream) = AudioStream::channel(2);
                let (cancel, delay, stopped) = (cancel.clone(), self.delay, self.stopped.clone());
                tokio::spawn(async move {
                    if sink.start(header).await.is_err() {
                        return;
                    }
                    for piece in audio.chunks(1000) {
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = cancel.cancelled() => {
                                stopped.fetch_add(1, Ordering::SeqCst);
                                return;
                            }
                        }
                        if sink.audio(piece.to_vec()).await.is_err() {
                            stopped.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                    }
                    let _ = sink.usage(Usage::speech(0, 500)).await;
                });
                Ok(stream)
            }
            Mode::FailAfterStart => Ok(AudioStream::from_pieces(vec![
                Ok(SpeechPiece::Start(header)),
                Ok(SpeechPiece::Audio(audio[..10].to_vec())),
                Err(RemoteError::Failed { message: "engine crashed".into() }),
            ])),
            Mode::FailBeforeStart => Ok(AudioStream::from_pieces(vec![Err(RemoteError::unavailable("engine went away"))])),
        }
    }
}

fn catalog() -> Catalog {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    Catalog::new(
        1,
        vec![
            CatalogModel::new("gemma", Purpose::Chat, ModelState::Warm),
            CatalogModel::new("qwen3-tts", Purpose::Speak, ModelState::Warm).with_speech(
                SpeechProfile::new()
                    .with_presets(s(&["vivian", "ryan"]))
                    .with_instructions(true)
                    .with_speed(true)
                    .with_streaming(true)
                    .with_formats(vec![AudioFormat::Wav, AudioFormat::Pcm16, AudioFormat::Mp3]),
            ),
            CatalogModel::new("voxcpm2", Purpose::Speak, ModelState::Loadable)
                .with_speech(SpeechProfile::new().with_design(true).with_clone(CloneMode::Audio).with_formats(vec![AudioFormat::Wav])),
        ],
    )
}

/// Serves speech through a catalog, as an inference node does; guests may not clone.
struct Host {
    catalog: Catalog,
    backend: Option<Arc<Mouth>>,
    opened: Arc<Mutex<Vec<Option<String>>>>,
}

impl InferHost for Host {
    fn authorize(&self, caller: &Caller) -> Result<(), RemoteError> {
        if caller.peer == "stranger" { Err(RemoteError::refused("unknown-device")) } else { Ok(()) }
    }

    fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
        Err(RemoteError::unavailable("speech only"))
    }

    fn open_speak(&self, caller: &Caller, request: &SpeakRequest) -> Result<Arc<dyn SpeakBackend>, RemoteError> {
        self.opened.lock().unwrap().push(request.model.clone());
        if caller.peer == "guest" && request.clones() {
            return Err(RemoteError::refused("guest-clone"));
        }
        self.catalog.route_speak(request.model.as_deref(), &request.needs())?;
        match &self.backend {
            Some(b) => Ok(b.clone()),
            None => Err(RemoteError::unavailable("no speech model on this node")),
        }
    }
}

/// A host before 0.5.11: the default `open_speak`.
struct ChatOnly;

impl InferHost for ChatOnly {
    fn authorize(&self, _caller: &Caller) -> Result<(), RemoteError> {
        Ok(())
    }

    fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
        Err(RemoteError::unavailable("no models"))
    }
}

fn serving(dialer: &MemoryDialer, node: &str, backend: Option<Arc<Mouth>>, peer: &'static str) -> Arc<Mutex<Vec<Option<String>>>> {
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

/// A node before 0.5.11: it can't decode `Speak` and drops the stream.
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

fn clone_voice(consent: bool) -> Voice {
    Voice::clone_from(wav(16_000), "wav", Some("hello there".into()), consent)
}

#[test]
fn requests_and_frames_round_trip_with_audio_as_bytes() {
    let r = SpeakRequest::script(vec![ScriptTurn::new(0, "Hi."), ScriptTurn::new(1, "Hello!")])
        .with_model("fish-audio-drama-3")
        .with_voice(Voice::preset("vivian"))
        .with_speaker_voice(0, clone_voice(true))
        .with_speaker_voice(1, Voice::design("a calm older voice"))
        .with_instructions("warmly")
        .with_language("en")
        .with_speed(1200)
        .with_seed(7)
        .with_format(AudioFormat::Pcm16)
        .with_sample_rate(24_000)
        .with_stream(true)
        .with_priority(Priority::Batch);
    assert!(!r.id.is_empty());
    let frame = ClientFrame::Speak(r.clone());
    let b = to_cbor(&frame).unwrap();
    // The reference clip is one CBOR byte string (major type 2, 2-byte length), not an array.
    let clip = wav(16_000);
    let mut header = vec![0x59];
    header.extend((clip.len() as u16).to_be_bytes());
    header.extend_from_slice(&clip[..16]);
    assert!(b.windows(header.len()).any(|w| w == header));
    assert_eq!(from_cbor::<ClientFrame>(&b).unwrap(), frame);
    assert_eq!(serde_json::from_value::<SpeakRequest>(serde_json::to_value(&r).unwrap()).unwrap(), r);

    // Unset options stay off the wire; the input and voices are tagged by kind.
    let bare = SpeakRequest::text("Hello");
    let v = serde_json::to_value(&bare).unwrap();
    let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    assert_eq!(keys, ["id", "input", "priority"]);
    assert_eq!(v["input"], serde_json::json!({"text": "Hello"}));
    assert_eq!(serde_json::to_value(SpeakInput::Raw("həˈloʊ".into())).unwrap(), serde_json::json!({"raw": "həˈloʊ"}));
    assert_eq!(serde_json::to_value(Voice::preset("ryan")).unwrap(), serde_json::json!({"preset": {"name": "ryan"}}));
    assert_eq!(serde_json::to_value(Voice::design("x")).unwrap(), serde_json::json!({"design": {"description": "x"}}));
    let script = serde_json::to_value(SpeakInput::Script(vec![ScriptTurn::new(2, "a")])).unwrap();
    assert_eq!(script, serde_json::json!({"script": [{"speaker": 2, "text": "a"}]}));
    let v = serde_json::to_value(SpeakRequest::text("x").with_format(AudioFormat::Pcm16)).unwrap();
    assert_eq!(v["format"], "pcm16");

    let frames = [
        ServerFrame::SpeechStart { id: "1".into(), format: AudioFormat::Mp3, sample_rate: Some(24_000), channels: Some(1) },
        ServerFrame::SpeechStart { id: "1".into(), format: AudioFormat::Opus, sample_rate: None, channels: None },
        ServerFrame::Audio { id: "1".into(), offset: 262_144, data: vec![1, 2, 3] },
    ];
    for f in frames {
        assert_eq!(from_cbor::<ServerFrame>(&to_cbor(&f).unwrap()).unwrap(), f);
        assert_eq!(f.id(), "1");
    }
    let audio = to_cbor(&ServerFrame::Audio { id: "1".into(), offset: 0, data: vec![9; 4] }).unwrap();
    assert!(audio.windows(5).any(|w| w == [0x44, 9, 9, 9, 9]), "audio is a byte string");
    let start =
        serde_json::to_value(ServerFrame::SpeechStart { id: "1".into(), format: AudioFormat::Wav, sample_rate: None, channels: None }).unwrap();
    assert_eq!(start, serde_json::json!({"speech_start": {"id": "1", "format": "wav"}}));

    // Usage and Meta: the speech fields are off the wire unless set, so older peers read them.
    assert_eq!(
        serde_json::to_value(Usage::new(1, 2)).unwrap(),
        serde_json::json!({"prompt_tokens": 1, "completion_tokens": 2, "reasoning_tokens": 0, "cached_tokens": 0})
    );
    let u = Usage::speech(12, 3400);
    let v = serde_json::to_value(u).unwrap();
    assert_eq!((v["input_chars"].as_u64(), v["audio_ms"].as_u64()), (Some(12), Some(3400)));
    assert_eq!(from_cbor::<Usage>(&to_cbor(&u).unwrap()).unwrap(), u);
    assert!(serde_json::to_value(Meta::default()).unwrap().get("ignored").is_none());
    let m = Meta::default().with_ignored("reference_text").with_ignored("reference_text");
    assert_eq!(m.ignored, ["reference_text"]);
    assert_eq!(from_cbor::<Meta>(&to_cbor(&m).unwrap()).unwrap(), m);
}

#[test]
fn requests_are_checked_before_any_backend() {
    let bad = |r: SpeakRequest, what: &str| {
        let e = r.check().unwrap_err();
        assert!(matches!(&e, RemoteError::BadRequest { message } if message.contains(what)), "{e:?}");
    };
    assert!(SpeakRequest::text("Hi").check().is_ok());
    assert!(SpeakRequest::text("Hi").with_voice(clone_voice(true)).check().is_ok());
    bad(SpeakRequest::text("  "), "text");
    bad(SpeakRequest::raw(""), "text");
    bad(SpeakRequest::script(vec![]), "turn");
    bad(SpeakRequest::script(vec![ScriptTurn::new(0, "a"), ScriptTurn::new(1, " ")]), "every turn");
    bad(SpeakRequest::text("Hi").with_voice(clone_voice(false)), "consent");
    bad(SpeakRequest::script(vec![ScriptTurn::new(0, "a")]).with_speaker_voice(0, clone_voice(false)), "consent");
    bad(SpeakRequest::text("Hi").with_voice(Voice::clone_from(vec![], "wav", None, true)), "reference clip");
    bad(SpeakRequest::text("Hi").with_voice(Voice::clone_from(vec![0; MAX_REFERENCE_BYTES + 1], "wav", None, true)), "byte limit");
    bad(SpeakRequest::text("Hi").with_voice(Voice::clone_from(vec![1], " ", None, true)), "format");
    bad(SpeakRequest::text("Hi").with_speaker_voice(0, Voice::preset("a")), "script");
    let mut twice = SpeakRequest::script(vec![ScriptTurn::new(0, "a")]);
    twice.voices = vec![v2::SpeakerVoice::new(0, Voice::preset("a")), v2::SpeakerVoice::new(0, Voice::preset("b"))];
    bad(twice, "speaker 0");
    bad(SpeakRequest::text("Hi").with_voice(Voice::preset(" ")), "name");
    bad(SpeakRequest::text("Hi").with_voice(Voice::design("")), "description");
    bad(SpeakRequest::text("Hi").with_speed(0), "speed");
    bad(SpeakRequest::text("Hi").with_sample_rate(0), "sample rate");
}

#[test]
fn needs_follow_the_request() {
    assert_eq!(SpeakRequest::text("héllo").needs(), SpeechNeeds::new().with_input_chars(5));
    let n =
        SpeakRequest::text("Hi").with_voice(Voice::preset("vivian")).with_instructions("sad").with_speed(1100).with_format(AudioFormat::Mp3).needs();
    assert_eq!(n, SpeechNeeds::new().with_input_chars(2).with_preset("vivian").with_instructions().with_speed().with_format(AudioFormat::Mp3));
    assert!(!SpeakRequest::text("Hi").with_speed(1000).needs().speed, "the model's own pace needs nothing");
    let n = SpeakRequest::script(vec![ScriptTurn::new(0, "ab"), ScriptTurn::new(3, "cde")])
        .with_speaker_voice(0, clone_voice(true))
        .with_speaker_voice(3, Voice::clone_from(vec![1, 2, 3], "mp3", None, true))
        .needs();
    assert_eq!((n.speakers, n.voices, n.input_chars, n.clone), (4, 2, 5, Some(false)));
    assert_eq!((n.reference_ms, n.reference_bytes), (Some(1000), Some(wav(16_000).len() as u64)));
    assert!(SpeakRequest::raw("x").needs().raw);
    assert!(SpeakRequest::text("x").with_voice(Voice::design("d")).needs().design);
    // The catalog refuses what the model lacks, by name.
    let e = catalog().route_speak(Some("qwen3-tts"), &SpeakRequest::text("x").with_voice(Voice::preset("alloy")).needs()).unwrap_err();
    assert!(matches!(&e, RemoteError::BadRequest { message } if message.contains("alloy") && message.contains("vivian, ryan")), "{e:?}");
}

#[tokio::test]
async fn the_pool_gets_speech_in_chunks_with_usage_and_metadata() {
    let dialer = MemoryDialer::new();
    let mouth = Mouth::new("qwen3-tts@node-b", Mode::File, Duration::ZERO);
    let opened = serving(&dialer, "inference", Some(mouth.clone()), "phone");
    // 600 characters → 600 000 bytes: three frames (256 KiB, 256 KiB, the rest).
    let text = "a".repeat(600);
    let request = SpeakRequest::text(text.clone()).with_voice(Voice::preset("ryan")).with_format(AudioFormat::Pcm16).with_seed(3);
    let s = pool(dialer, &["inference"]).speak(request.clone(), &CancelToken::new()).await.unwrap();
    assert_eq!(s.audio, audio_for(&request));
    assert_eq!((s.format, s.sample_rate, s.channels), (AudioFormat::Pcm16, Some(24_000), Some(1)));
    assert_eq!((s.node.as_str(), s.model.as_str()), ("inference", "qwen3-tts@node-b"));
    let u = s.usage.unwrap();
    assert_eq!((u.input_chars, u.audio_ms, u.completion_tokens), (600, 1250, 42), "the server fills in the characters");
    assert_eq!(s.meta.served_by.as_deref(), Some("qwen3-tts@node-b"));
    assert!(s.meta.ignored.is_empty());
    let seen = mouth.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!((seen[0].voice.clone(), seen[0].seed, seen[0].cert.clone()), (Some(Voice::preset("ryan")), Some(3), None));
    assert_eq!(*opened.lock().unwrap(), [None]);

    // The frames themselves: SpeechStart once, then audio in order, each ≤ 256 KiB.
    let (mut ours, theirs) = memory_pair();
    let host = Host { catalog: catalog(), backend: Some(mouth.clone()), opened: Default::default() };
    tokio::spawn(async move { v2::serve(theirs, Caller::new("phone"), &host, &Admission::new(1)).await });
    let mut r = SpeakRequest::text(text).with_voice(clone_voice(true)).with_model("voxcpm2");
    r.id = "s".into();
    ours.send_cbor(&ClientFrame::Speak(r)).await.unwrap();
    let (mut starts, mut sizes, mut next) = (0, Vec::new(), 0u64);
    loop {
        match ours.recv_cbor::<ServerFrame>().await.unwrap().unwrap() {
            ServerFrame::Accepted { .. } => {}
            ServerFrame::SpeechStart { .. } => starts += 1,
            ServerFrame::Audio { offset, data, .. } => {
                assert_eq!((starts, offset), (1, next));
                assert!(data.len() <= MAX_AUDIO_CHUNK);
                next += data.len() as u64;
                sizes.push(data.len());
            }
            ServerFrame::Done { finish, .. } => {
                assert_eq!(finish.meta.ignored, ["reference_text"], "a transcript the model doesn't use is named");
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(sizes, [MAX_AUDIO_CHUNK, MAX_AUDIO_CHUNK, 600_000 - 2 * MAX_AUDIO_CHUNK]);
}

#[tokio::test]
async fn speak_stream_hands_over_audio_as_it_comes() {
    let dialer = MemoryDialer::new();
    serving(&dialer, "a", Some(Mouth::new("qwen3-tts", Mode::Stream, Duration::from_millis(5))), "phone");
    let request = SpeakRequest::text("abcd").with_stream(true);
    let mut s = pool(dialer, &["a"]).speak_stream(request.clone(), &CancelToken::new()).await.unwrap();
    assert_eq!((s.header().format, s.header().sample_rate, s.model(), s.node()), (AudioFormat::Wav, Some(24_000), "qwen3-tts", "a"));
    assert!(s.meta().is_none());
    let mut pieces = Vec::new();
    while let Some(p) = s.next().await.unwrap() {
        pieces.push(p);
    }
    assert_eq!(pieces.len(), 4, "one frame per engine piece");
    assert_eq!(pieces.concat(), audio_for(&request));
    assert_eq!(s.received(), 4000);
    assert_eq!(s.usage().map(|u| (u.input_chars, u.audio_ms)), Some((4, 500)));
    assert!(s.next().await.unwrap().is_none(), "done stays done");
}

#[tokio::test]
async fn bad_requests_never_reach_a_node_or_fail_over() {
    let dialer = MemoryDialer::new();
    let first = serving(&dialer, "a", Some(Mouth::new("m", Mode::File, Duration::ZERO)), "phone");
    let second = serving(&dialer, "b", Some(Mouth::new("m", Mode::File, Duration::ZERO)), "phone");
    let p = pool(dialer, &["a", "b"]);
    let err = p.speak(SpeakRequest::text("Hi").with_voice(clone_voice(false)), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(&err, RemoteError::BadRequest { message } if message.contains("consent")), "{err:?}");
    let err = p.speak(SpeakRequest::text(""), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err, RemoteError::BadRequest { .. }), "{err:?}");
    assert!(first.lock().unwrap().is_empty(), "checked before any node is asked");
    // A chat model named for speech, and a preset the model lacks: refused by the first node only.
    let err = p.speak(SpeakRequest::text("Hi").with_model("gemma"), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(&err, RemoteError::BadRequest { message } if message.contains("gemma")), "{err:?}");
    let err = p.speak(SpeakRequest::text("Hi").with_model("qwen3-tts").with_voice(Voice::preset("alloy")), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(&err, RemoteError::BadRequest { message } if message.contains("alloy")), "{err:?}");
    assert_eq!((first.lock().unwrap().len(), second.lock().unwrap().len()), (2, 0));
}

#[tokio::test]
async fn the_server_checks_the_request_too() {
    let (mut ours, theirs) = memory_pair();
    let opened = Arc::new(Mutex::new(Vec::new()));
    let host = Host { catalog: catalog(), backend: Some(Mouth::new("m", Mode::File, Duration::ZERO)), opened: opened.clone() };
    let server = tokio::spawn(async move { v2::serve(theirs, Caller::new("phone"), &host, &Admission::new(1)).await });
    let mut r = SpeakRequest::text("Hi").with_voice(clone_voice(false));
    r.id = "x".into();
    ours.send_cbor(&ClientFrame::Speak(r)).await.unwrap();
    let f = ours.recv_cbor::<ServerFrame>().await.unwrap().unwrap();
    assert!(
        matches!(&f, ServerFrame::Failed { id, error: RemoteError::BadRequest { message } } if id == "x" && message.contains("consent")),
        "{f:?}"
    );
    server.await.unwrap().unwrap();
    assert!(opened.lock().unwrap().is_empty());
}

#[tokio::test]
async fn strangers_are_refused_and_hosts_see_the_whole_request() {
    let dialer = MemoryDialer::new();
    let opened = serving(&dialer, "a", Some(Mouth::new("v", Mode::File, Duration::ZERO)), "stranger");
    let err = pool(dialer, &["a"]).speak(SpeakRequest::text("Hi"), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(err, RemoteError::Refused { .. }), "{err:?}");
    assert!(opened.lock().unwrap().is_empty());
    // A host refuses what this caller may not do (a guest cloning a voice) from the request.
    let dialer = MemoryDialer::new();
    serving(&dialer, "a", Some(Mouth::new("v", Mode::File, Duration::ZERO)), "guest");
    let p = pool(dialer, &["a"]);
    assert!(p.speak(SpeakRequest::text("Hi"), &CancelToken::new()).await.is_ok());
    let err = p.speak(SpeakRequest::text("Hi").with_voice(clone_voice(true)), &CancelToken::new()).await.unwrap_err();
    assert_eq!(err, RemoteError::refused("guest-clone"));
}

#[tokio::test]
async fn old_chat_only_and_refusing_nodes_fail_over_before_the_audio() {
    let dialer = MemoryDialer::new();
    let old = old_node(&dialer, "old");
    let admission = Admission::new(1);
    dialer.route_only("chat-only", &[v2::PROTOCOL], move |ch, _| {
        let admission = admission.clone();
        tokio::spawn(async move { v2::serve(ch, Caller::new("phone"), &ChatOnly, &admission).await });
    });
    let other_engine = Mouth::new("m", Mode::Refuse("unsupported"), Duration::ZERO);
    serving(&dialer, "other-engine", Some(other_engine.clone()), "phone");
    let broken = Mouth::new("m", Mode::FailBeforeStart, Duration::ZERO);
    serving(&dialer, "broken", Some(broken.clone()), "phone");
    serving(&dialer, "new", Some(Mouth::new("qwen3-tts", Mode::File, Duration::ZERO)), "phone");
    let s = pool(dialer.clone(), &["old", "chat-only", "other-engine", "broken", "new"])
        .speak(SpeakRequest::text("Hi"), &CancelToken::new())
        .await
        .unwrap();
    assert_eq!((s.node.as_str(), s.audio.len()), ("new", 2000));
    assert_eq!(old.load(Ordering::SeqCst), 1);
    assert_eq!((other_engine.seen.lock().unwrap().len(), broken.seen.lock().unwrap().len()), (1, 1));

    // With only those, the last node answer is reported over the old node's close.
    let err = pool(dialer.clone(), &["old", "chat-only"]).speak(SpeakRequest::text("Hi"), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(&err, RemoteError::Unavailable { message } if message.contains("speech")), "{err:?}");
    let err = pool(dialer, &["old"]).speak(SpeakRequest::text("Hi"), &CancelToken::new()).await.unwrap_err();
    assert!(matches!(&err, RemoteError::Unsupported { message } if message.contains("0.5.11")), "{err:?}");
}

#[tokio::test]
async fn no_failover_once_the_audio_has_started() {
    let dialer = MemoryDialer::new();
    serving(&dialer, "a", Some(Mouth::new("m", Mode::FailAfterStart, Duration::ZERO)), "phone");
    let second = Mouth::new("m", Mode::File, Duration::ZERO);
    serving(&dialer, "b", Some(second.clone()), "phone");
    let p = pool(dialer, &["a", "b"]);
    let err = p.speak(SpeakRequest::text("Hi"), &CancelToken::new()).await.unwrap_err();
    assert_eq!(err, RemoteError::Failed { message: "engine crashed".into() });
    assert!(second.seen.lock().unwrap().is_empty(), "another node would start over with another voice draw");
    // Streaming: the first piece arrives, then the failure.
    let mut s = p.speak_stream(SpeakRequest::text("Hi"), &CancelToken::new()).await.unwrap();
    assert_eq!(s.node(), "a");
    assert_eq!(s.next().await.unwrap().map(|p| p.len()), Some(10));
    assert_eq!(s.next().await.unwrap_err(), RemoteError::Failed { message: "engine crashed".into() });
    assert!(second.seen.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn long_synthesis_keeps_the_stream_alive() {
    // The pool's stall cap is 4 s; the backend takes 10 s and only `Working` frames arrive meanwhile.
    assert!(QUEUE_KEEPALIVE < Duration::from_secs(4));
    let dialer = MemoryDialer::new();
    serving(&dialer, "slow", Some(Mouth::new("qwen3-tts", Mode::File, Duration::from_secs(10))), "phone");
    let s = pool(dialer, &["slow"]).speak(SpeakRequest::text("Hi"), &CancelToken::new()).await.unwrap();
    assert_eq!(s.model, "qwen3-tts");
    assert!(s.meta.timings.total_ms >= 10_000, "{:?}", s.meta.timings);
    assert!(s.meta.timings.first_token_ms.unwrap() >= 10_000);
}

#[tokio::test(start_paused = true)]
async fn working_frames_arrive_until_the_audio_starts() {
    let (mut ours, theirs) = memory_pair();
    let host = Host { catalog: catalog(), backend: Some(Mouth::new("qwen3-tts", Mode::File, Duration::from_secs(7))), opened: Default::default() };
    let server = tokio::spawn(async move { v2::serve(theirs, Caller::new("phone"), &host, &Admission::new(1)).await });
    let mut r = SpeakRequest::text("Hi");
    r.id = "w".into();
    ours.send_cbor(&ClientFrame::Speak(r)).await.unwrap();
    let mut kinds = Vec::new();
    while let Some(f) = ours.recv_cbor::<ServerFrame>().await.unwrap() {
        kinds.push(match f {
            ServerFrame::Accepted { .. } => "accepted",
            ServerFrame::Working { .. } => "working",
            ServerFrame::SpeechStart { .. } => "start",
            ServerFrame::Audio { .. } => "audio",
            ServerFrame::Done { .. } => "done",
            other => panic!("unexpected {other:?}"),
        });
        if kinds.last() == Some(&"done") {
            break;
        }
    }
    server.await.unwrap().unwrap();
    assert_eq!(kinds, ["accepted", "working", "working", "start", "audio", "done"]);
}

#[tokio::test]
async fn cancelling_stops_the_backend() {
    // Before the audio: the backend's synthesis is cancelled.
    let dialer = MemoryDialer::new();
    let mouth = Mouth::new("qwen3-tts", Mode::File, Duration::from_secs(3600));
    serving(&dialer, "a", Some(mouth.clone()), "phone");
    let p = pool(dialer, &["a"]);
    let cancel = CancelToken::new();
    let (c, m) = (cancel.clone(), mouth.clone());
    tokio::spawn(async move {
        while m.seen.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        c.cancel();
    });
    assert_eq!(p.speak(SpeakRequest::text("Hi"), &cancel).await.unwrap_err(), RemoteError::Cancelled);
    wait_for(&mouth.stopped, 1).await;

    // While streaming: the stream's token stops the node's task.
    let dialer = MemoryDialer::new();
    let mouth = Mouth::new("qwen3-tts", Mode::Stream, Duration::from_millis(20));
    serving(&dialer, "a", Some(mouth.clone()), "phone");
    let cancel = CancelToken::new();
    let mut s = pool(dialer, &["a"]).speak_stream(SpeakRequest::text("a".repeat(500)), &cancel).await.unwrap();
    assert!(s.next().await.unwrap().is_some());
    cancel.cancel();
    assert_eq!(s.next().await.unwrap_err(), RemoteError::Cancelled);
    wait_for(&mouth.stopped, 1).await;

    // Dropping the stream mid-way stops it too.
    let dialer = MemoryDialer::new();
    let mouth = Mouth::new("qwen3-tts", Mode::Stream, Duration::from_millis(20));
    serving(&dialer, "a", Some(mouth.clone()), "phone");
    let mut s = pool(dialer, &["a"]).speak_stream(SpeakRequest::text("a".repeat(500)), &CancelToken::new()).await.unwrap();
    assert!(s.next().await.unwrap().is_some());
    drop(s);
    wait_for(&mouth.stopped, 1).await;
}

async fn wait_for(n: &AtomicUsize, want: usize) {
    for _ in 0..400 {
        if n.load(Ordering::SeqCst) == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the backend was not stopped");
}

#[tokio::test]
async fn backends_that_break_the_order_fail_the_request() {
    struct Odd(Vec<SpeechPiece>);
    #[async_trait]
    impl SpeakBackend for Odd {
        fn name(&self) -> String {
            "odd".into()
        }
        async fn speak(&self, _request: &SpeakRequest, _cancel: &CancelToken) -> Result<AudioStream, RemoteError> {
            Ok(AudioStream::from_pieces(self.0.iter().cloned().map(Ok).collect()))
        }
    }
    struct OddHost(Vec<SpeechPiece>);
    impl InferHost for OddHost {
        fn authorize(&self, _caller: &Caller) -> Result<(), RemoteError> {
            Ok(())
        }
        fn open(&self, _caller: &Caller, _model: Option<&str>) -> Result<Arc<dyn ModelBackend>, RemoteError> {
            Err(RemoteError::unavailable("no"))
        }
        fn open_speak(&self, _caller: &Caller, _request: &SpeakRequest) -> Result<Arc<dyn SpeakBackend>, RemoteError> {
            Ok(Arc::new(Odd(self.0.clone())))
        }
    }
    let h = AudioHeader::new(AudioFormat::Wav);
    for (pieces, what) in [
        (vec![SpeechPiece::Audio(vec![1])], "before its format"),
        (vec![SpeechPiece::Start(h), SpeechPiece::Start(h)], "twice"),
        (vec![SpeechPiece::Usage(Usage::speech(1, 1))], "without audio"),
    ] {
        let (mut ours, theirs) = memory_pair();
        let host = OddHost(pieces);
        tokio::spawn(async move { v2::serve(theirs, Caller::new("phone"), &host, &Admission::new(1)).await });
        let mut r = SpeakRequest::text("Hi");
        r.id = "o".into();
        ours.send_cbor(&ClientFrame::Speak(r)).await.unwrap();
        loop {
            match ours.recv_cbor::<ServerFrame>().await.unwrap().unwrap() {
                ServerFrame::Failed { error, .. } => {
                    assert!(error.to_string().contains(what), "{error}");
                    break;
                }
                ServerFrame::Done { .. } => panic!("done for {what}"),
                _ => {}
            }
        }
    }
}
