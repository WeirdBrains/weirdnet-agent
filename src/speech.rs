//! Speech out (text to speech): what a speaking model does, and what a request needs from it.
//!
//! A model whose catalog purpose is [`Purpose::Speak`](crate::remote::catalog::Purpose::Speak)
//! turns text into audio (infer/2 `Speak`, see
//! [`SpeakRequest`](crate::remote::infer::v2::SpeakRequest)). Each serving copy has a
//! [`SpeechProfile`]: what it does (preset voices, designed voices, style instructions, cloned
//! voices, multi-speaker scripts, raw engine input, speed, streaming), what it takes (languages,
//! output formats, sample rate) and its limits. A request asks for one shape and the profile says
//! whether the model can serve it ([`SpeechProfile::covers`]); nothing is faked: a request for a
//! feature the model lacks is refused with a message that names it.
//!
//! **Profiles are reported or absent.** A bool is true only when the node knows the model does it;
//! a list or limit is `None` (or, for [`formats`](SpeechProfile::formats), empty) when nobody
//! reported it, never defaulted. Every field is left off the wire when unset.
//!
//! - [`SpeechProfile::tightest`] merges the copies of one model across nodes for the catalog: the
//!   intersection of lists, the smallest limit, and a feature only when every copy has it.
//! - [`SpeechProfile::narrowed_by`] applies an operator's overrides: fewer voices, a smaller limit,
//!   a limit the engine doesn't report; anything that would widen what the engine reports is an
//!   error.
//!
//! Since 0.5.11.

use serde::{Deserialize, Serialize};

/// An audio encoding for speech out: `wav`, `pcm16` (raw little-endian 16-bit samples), `mp3`,
/// `opus` or `flac` on the wire.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AudioFormat {
    #[default]
    Wav,
    Pcm16,
    Mp3,
    Opus,
    Flac,
}

impl AudioFormat {
    /// Every format, in wire order.
    pub const ALL: [AudioFormat; 5] = [AudioFormat::Wav, AudioFormat::Pcm16, AudioFormat::Mp3, AudioFormat::Opus, AudioFormat::Flac];

    /// The name on the wire: `wav`, `pcm16`, `mp3`, `opus`, `flac`.
    pub fn as_str(&self) -> &'static str {
        match self {
            AudioFormat::Wav => "wav",
            AudioFormat::Pcm16 => "pcm16",
            AudioFormat::Mp3 => "mp3",
            AudioFormat::Opus => "opus",
            AudioFormat::Flac => "flac",
        }
    }

    /// Parses [`as_str`](Self::as_str)'s names, and `pcm` (OpenAI's name for 16-bit PCM) as
    /// [`Pcm16`](Self::Pcm16); `None` for anything else.
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "wav" => Some(AudioFormat::Wav),
            "pcm16" | "pcm" => Some(AudioFormat::Pcm16),
            "mp3" => Some(AudioFormat::Mp3),
            "opus" => Some(AudioFormat::Opus),
            "flac" => Some(AudioFormat::Flac),
            _ => None,
        }
    }

    /// The MIME type of a file in this format (`audio/pcm` for raw samples; add the rate as
    /// `audio/pcm;rate=24000`).
    pub fn mime(&self) -> &'static str {
        match self {
            AudioFormat::Wav => "audio/wav",
            AudioFormat::Pcm16 => "audio/pcm",
            AudioFormat::Mp3 => "audio/mpeg",
            AudioFormat::Opus => "audio/ogg",
            AudioFormat::Flac => "audio/flac",
        }
    }
}

impl std::fmt::Display for AudioFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a voice-cloning model needs with the reference clip: `audio` (the clip; a transcript is
/// not used), `audio_and_text` (the clip; its transcript helps but may be left out) or
/// `audio_text_required` (the clip and its transcript) on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CloneMode {
    /// The reference clip only; a transcript sent with it is ignored (and named in
    /// [`Meta::ignored`](crate::remote::infer::v2::Meta::ignored)).
    Audio,
    /// The clip, with an optional transcript the model uses when given.
    AudioAndText,
    /// The clip and its transcript, both required.
    AudioTextRequired,
}

impl CloneMode {
    /// A transcript must come with the clip.
    pub fn needs_text(&self) -> bool {
        *self == CloneMode::AudioTextRequired
    }

    /// A transcript sent with the clip is used.
    pub fn uses_text(&self) -> bool {
        matches!(self, CloneMode::AudioAndText | CloneMode::AudioTextRequired)
    }

    /// What every copy of a model accepts when one copy does `self` and another `other`: a
    /// transcript is required if either requires it, and counts as used only if both use it.
    pub fn tightest(self, other: Self) -> Self {
        use CloneMode::*;
        match (self, other) {
            (AudioTextRequired, _) | (_, AudioTextRequired) => AudioTextRequired,
            (Audio, _) | (_, Audio) => Audio,
            _ => AudioAndText,
        }
    }

    fn rank(self) -> u8 {
        // How much the mode asks of a request: narrowing may only ask more.
        match self {
            CloneMode::Audio | CloneMode::AudioAndText => 0,
            CloneMode::AudioTextRequired => 1,
        }
    }
}

/// What one speaking model does, takes and allows. See the [module docs](self).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SpeechProfile {
    /// The named preset voices (`Voice::Preset`). `None`: the model takes no preset by name (or
    /// none was reported); requests without a voice get the model's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presets: Option<Vec<String>>,
    /// Designs a voice from a description (`Voice::Design`).
    #[serde(default, skip_serializing_if = "is_false")]
    pub design: bool,
    /// Takes style or emotion instructions for the chosen voice.
    #[serde(default, skip_serializing_if = "is_false")]
    pub instructions: bool,
    /// Clones a voice from a reference clip (`Voice::Clone`), and what the clip needs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clone: Option<CloneMode>,
    /// Speaks multi-speaker scripts with up to this many speakers (speaker numbers below it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script_speakers: Option<u8>,
    /// Most voices one request may set (the voice plus per-speaker voices).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_voices: Option<u8>,
    /// Takes raw engine input with no template (`SpeakInput::Raw`, such as phonemes).
    #[serde(default, skip_serializing_if = "is_false")]
    pub raw: bool,
    /// Takes a speaking speed.
    #[serde(default, skip_serializing_if = "is_false")]
    pub speed: bool,
    /// Streams audio as it is made (otherwise the finished file comes in chunks).
    #[serde(default, skip_serializing_if = "is_false")]
    pub streaming: bool,
    /// The languages it speaks from text (ISO 639-1 or the engine's codes). `None`: not reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub languages: Option<Vec<String>>,
    /// The output formats it produces, the default first. Empty: not reported.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub formats: Vec<AudioFormat>,
    /// Its output sample rate in Hz.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<u32>,
    /// Longest input in characters one request may carry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_chars: Option<u32>,
    /// Longest reference clip in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_reference_seconds: Option<u32>,
    /// Largest reference clip in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_reference_bytes: Option<u64>,
    /// Longest audio one request produces, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_seconds: Option<u32>,
    /// The model's license (an SPDX id or a named license such as `fish-audio-community`) when it
    /// restricts how speech may be served.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

fn min_known<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn intersect<T: PartialEq + Clone>(a: &[T], b: &[T]) -> Vec<T> {
    a.iter().filter(|x| b.contains(x)).cloned().collect()
}

impl SpeechProfile {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_presets(mut self, presets: Vec<String>) -> Self {
        self.presets = Some(presets);
        self
    }

    pub fn with_design(mut self, design: bool) -> Self {
        self.design = design;
        self
    }

    pub fn with_instructions(mut self, instructions: bool) -> Self {
        self.instructions = instructions;
        self
    }

    pub fn with_clone(mut self, clone: CloneMode) -> Self {
        self.clone = Some(clone);
        self
    }

    pub fn with_script_speakers(mut self, speakers: u8) -> Self {
        self.script_speakers = Some(speakers);
        self
    }

    pub fn with_max_voices(mut self, voices: u8) -> Self {
        self.max_voices = Some(voices);
        self
    }

    pub fn with_raw(mut self, raw: bool) -> Self {
        self.raw = raw;
        self
    }

    pub fn with_speed(mut self, speed: bool) -> Self {
        self.speed = speed;
        self
    }

    pub fn with_streaming(mut self, streaming: bool) -> Self {
        self.streaming = streaming;
        self
    }

    pub fn with_languages(mut self, languages: Vec<String>) -> Self {
        self.languages = Some(languages);
        self
    }

    /// The output formats, the default first.
    pub fn with_formats(mut self, formats: Vec<AudioFormat>) -> Self {
        self.formats = formats;
        self
    }

    pub fn with_sample_rate(mut self, hz: u32) -> Self {
        self.sample_rate = Some(hz);
        self
    }

    pub fn with_max_input_chars(mut self, chars: u32) -> Self {
        self.max_input_chars = Some(chars);
        self
    }

    pub fn with_max_reference_seconds(mut self, seconds: u32) -> Self {
        self.max_reference_seconds = Some(seconds);
        self
    }

    pub fn with_max_reference_bytes(mut self, bytes: u64) -> Self {
        self.max_reference_bytes = Some(bytes);
        self
    }

    pub fn with_max_output_seconds(mut self, seconds: u32) -> Self {
        self.max_output_seconds = Some(seconds);
        self
    }

    pub fn with_license(mut self, license: impl Into<String>) -> Self {
        self.license = Some(license.into());
        self
    }

    /// Nothing is reported (every field unset).
    pub fn is_unknown(&self) -> bool {
        *self == Self::default()
    }

    /// The format a request without one gets: the first reported.
    pub fn default_format(&self) -> Option<AudioFormat> {
        self.formats.first().copied()
    }

    /// What every copy of one model across nodes can do, for the catalog: features
    /// (`design`, `instructions`, `raw`, `speed`, `streaming`, `presets`, `clone`,
    /// `script_speakers`) only when both have them (preset lists intersected, the smaller speaker
    /// count, the stricter clone mode); lists both report intersected and limits both report the
    /// smaller, a list or limit only one reports kept; the sample rate only when they agree (or
    /// one reports none); the license when they agree or one reports none.
    pub fn tightest(a: &Self, b: &Self) -> Self {
        let both = |x: &Option<Vec<String>>, y: &Option<Vec<String>>| match (x, y) {
            (Some(x), Some(y)) => Some(intersect(x, y)),
            _ => None,
        };
        let known = |x: &Option<Vec<String>>, y: &Option<Vec<String>>| match (x, y) {
            (Some(x), Some(y)) => Some(intersect(x, y)),
            (x, y) => x.clone().or_else(|| y.clone()),
        };
        Self {
            presets: both(&a.presets, &b.presets),
            design: a.design && b.design,
            instructions: a.instructions && b.instructions,
            clone: match (a.clone, b.clone) {
                (Some(x), Some(y)) => Some(x.tightest(y)),
                _ => None,
            },
            script_speakers: match (a.script_speakers, b.script_speakers) {
                (Some(x), Some(y)) => Some(x.min(y)),
                _ => None,
            },
            max_voices: min_known(a.max_voices, b.max_voices),
            raw: a.raw && b.raw,
            speed: a.speed && b.speed,
            streaming: a.streaming && b.streaming,
            languages: known(&a.languages, &b.languages),
            formats: match (a.formats.is_empty(), b.formats.is_empty()) {
                (false, false) => intersect(&a.formats, &b.formats),
                (true, _) => b.formats.clone(),
                (false, true) => a.formats.clone(),
            },
            sample_rate: match (a.sample_rate, b.sample_rate) {
                (Some(x), Some(y)) if x != y => None,
                (x, y) => x.or(y),
            },
            max_input_chars: min_known(a.max_input_chars, b.max_input_chars),
            max_reference_seconds: min_known(a.max_reference_seconds, b.max_reference_seconds),
            max_reference_bytes: min_known(a.max_reference_bytes, b.max_reference_bytes),
            max_output_seconds: min_known(a.max_output_seconds, b.max_output_seconds),
            license: match (&a.license, &b.license) {
                (Some(x), Some(y)) if x != y => None,
                (x, y) => x.clone().or_else(|| y.clone()),
            },
        }
    }

    /// This (engine-reported) profile with an operator's overrides applied, field by field: a set
    /// field narrows (a subset of presets, languages or formats; a smaller limit, speaker or voice
    /// count; a stricter clone mode) or fills a list or limit the engine doesn't report. An unset
    /// field (`None`, `false`, empty) changes nothing. `Err` names the first field that would
    /// widen what the engine reports: a preset, language or format it doesn't list, a feature it
    /// lacks (a `true` bool, a clone mode, a speaker count), presets when it reports none, a
    /// larger limit, or another license.
    ///
    /// Bools can't say "off" here; to turn a feature off (cloning on a node, say), clear the
    /// field on the result.
    pub fn narrowed_by(&self, operator: &SpeechProfile) -> Result<Self, String> {
        fn subset(field: &str, engine: &Option<Vec<String>>, op: &Option<Vec<String>>, fill: bool) -> Result<Option<Vec<String>>, String> {
            match (engine, op) {
                (_, None) => Ok(engine.clone()),
                (None, Some(o)) if fill => Ok(Some(o.clone())),
                (None, Some(_)) => Err(format!("{field}: the engine reports none, so the operator can't add any")),
                (Some(e), Some(o)) => match o.iter().find(|x| !e.contains(x)) {
                    Some(x) => Err(format!("{field}: `{x}` is not one the engine reports ({})", e.join(", "))),
                    None => Ok(Some(o.clone())),
                },
            }
        }
        fn feature(field: &str, engine: bool, op: bool) -> Result<bool, String> {
            if op && !engine { Err(format!("{field}: the engine does not report it, so the operator can't turn it on")) } else { Ok(engine) }
        }
        fn limit<T: Ord + Copy + std::fmt::Display>(field: &str, engine: Option<T>, op: Option<T>) -> Result<Option<T>, String> {
            match (engine, op) {
                (Some(e), Some(o)) if o > e => Err(format!("{field}: {o} is over the engine's {e}")),
                (e, o) => Ok(o.or(e)),
            }
        }
        fn count(field: &str, engine: Option<u8>, op: Option<u8>) -> Result<Option<u8>, String> {
            match (engine, op) {
                (None, Some(_)) => Err(format!("{field}: the engine does not report it, so the operator can't set it")),
                (e, o) => limit(field, e, o),
            }
        }
        let formats = if operator.formats.is_empty() {
            self.formats.clone()
        } else if self.formats.is_empty() {
            operator.formats.clone()
        } else if let Some(f) = operator.formats.iter().find(|f| !self.formats.contains(f)) {
            return Err(format!("formats: `{f}` is not one the engine produces ({})", join(&self.formats)));
        } else {
            operator.formats.clone()
        };
        let clone = match (self.clone, operator.clone) {
            (e, None) => e,
            (None, Some(_)) => return Err("clone: the engine does not clone voices, so the operator can't turn it on".into()),
            (Some(e), Some(o)) if o.rank() < e.rank() => return Err(format!("clone: `{o:?}` asks less of a reference than the engine's `{e:?}`")),
            (Some(e), Some(o)) => Some(if o.rank() == e.rank() { e } else { o }),
        };
        let sample_rate = match (self.sample_rate, operator.sample_rate) {
            (Some(e), Some(o)) if e != o => return Err(format!("sample_rate: {o} Hz is not the engine's {e} Hz")),
            (e, o) => e.or(o),
        };
        let license = match (&self.license, &operator.license) {
            (Some(e), Some(o)) if e != o => return Err(format!("license: `{o}` is not the engine's `{e}`")),
            (e, o) => e.clone().or_else(|| o.clone()),
        };
        Ok(Self {
            presets: subset("presets", &self.presets, &operator.presets, false)?,
            design: feature("design", self.design, operator.design)?,
            instructions: feature("instructions", self.instructions, operator.instructions)?,
            clone,
            script_speakers: count("script_speakers", self.script_speakers, operator.script_speakers)?,
            max_voices: limit("max_voices", self.max_voices, operator.max_voices)?,
            raw: feature("raw", self.raw, operator.raw)?,
            speed: feature("speed", self.speed, operator.speed)?,
            streaming: feature("streaming", self.streaming, operator.streaming)?,
            languages: subset("languages", &self.languages, &operator.languages, true)?,
            formats,
            sample_rate,
            max_input_chars: limit("max_input_chars", self.max_input_chars, operator.max_input_chars)?,
            max_reference_seconds: limit("max_reference_seconds", self.max_reference_seconds, operator.max_reference_seconds)?,
            max_reference_bytes: limit("max_reference_bytes", self.max_reference_bytes, operator.max_reference_bytes)?,
            max_output_seconds: limit("max_output_seconds", self.max_output_seconds, operator.max_output_seconds)?,
            license,
        })
    }

    /// Whether a request with these needs can be served by a model with this profile. `Err`
    /// says what the model lacks (for a `BadRequest` that names the model in front of it), such
    /// as `has no preset voice `alloy` (presets: vivian, ryan)` or `does not clone voices`.
    pub fn covers(&self, needs: &SpeechNeeds) -> Result<(), String> {
        for name in &needs.presets {
            match &self.presets {
                None => return Err(format!("has no preset voices (asked for `{name}`)")),
                Some(p) if !p.contains(name) => {
                    return Err(format!("has no preset voice `{name}` (presets: {})", if p.is_empty() { "none".into() } else { p.join(", ") }));
                }
                Some(_) => {}
            }
        }
        if needs.design && !self.design {
            return Err("does not design voices from a description".into());
        }
        if needs.instructions && !self.instructions {
            return Err("does not take style instructions".into());
        }
        if let Some(has_text) = needs.clone {
            match self.clone {
                None => return Err("does not clone voices".into()),
                Some(mode) if mode.needs_text() && !has_text => return Err("needs the reference clip's transcript to clone a voice".into()),
                Some(_) => {}
            }
        }
        if needs.speakers > 0 {
            match self.script_speakers {
                None => return Err("does not speak multi-speaker scripts".into()),
                Some(n) if needs.speakers > n => {
                    return Err(format!(
                        "speaks scripts with at most {n} speakers (speakers 0 to {}); asked for speaker {}",
                        n.saturating_sub(1),
                        needs.speakers - 1
                    ));
                }
                Some(_) => {}
            }
        }
        if let Some(max) = self.max_voices
            && needs.voices > max
        {
            return Err(format!("takes at most {max} voices per request; asked for {}", needs.voices));
        }
        if needs.raw && !self.raw {
            return Err("does not take raw input".into());
        }
        if needs.speed && !self.speed {
            return Err("does not take a speaking speed".into());
        }
        if let Some(f) = needs.format {
            if self.formats.is_empty() {
                return Err(format!("reports no output formats, so `{f}` can't be promised"));
            }
            if !self.formats.contains(&f) {
                return Err(format!("does not produce `{f}` (formats: {})", join(&self.formats)));
            }
        }
        if let Some(max) = self.max_input_chars
            && needs.input_chars > max
        {
            return Err(format!("takes at most {max} characters per request; this one has {}", needs.input_chars));
        }
        if let (Some(max), Some(ms)) = (self.max_reference_seconds, needs.reference_ms)
            && ms > u64::from(max) * 1000
        {
            return Err(format!("takes reference clips of at most {max} s; this one is {:.1} s", ms as f64 / 1000.0));
        }
        if let (Some(max), Some(bytes)) = (self.max_reference_bytes, needs.reference_bytes)
            && bytes > max
        {
            return Err(format!("takes reference clips of at most {max} bytes; this one is {bytes}"));
        }
        Ok(())
    }
}

fn join(formats: &[AudioFormat]) -> String {
    formats.iter().map(AudioFormat::as_str).collect::<Vec<_>>().join(", ")
}

/// What one speech request needs from a model's [`SpeechProfile`]
/// ([`SpeakRequest::needs`](crate::remote::infer::v2::SpeakRequest::needs)), for
/// [`SpeechProfile::covers`] and
/// [`Catalog::route_speak`](crate::remote::catalog::Catalog::route_speak).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct SpeechNeeds {
    /// Preset voices asked for by name (the voice and per-speaker voices), in order, once each.
    pub presets: Vec<String>,
    /// A designed voice.
    pub design: bool,
    /// Style instructions.
    pub instructions: bool,
    /// A cloned voice: `Some(true)` when every reference clip comes with its transcript.
    pub clone: Option<bool>,
    /// For a script: the highest speaker number plus one. 0 for plain or raw text.
    pub speakers: u8,
    /// Voices set (the voice plus per-speaker voices).
    pub voices: u8,
    /// Raw engine input.
    pub raw: bool,
    /// A speaking speed.
    pub speed: bool,
    /// The output format asked for (`None`: the model's default).
    pub format: Option<AudioFormat>,
    /// Characters of input (Unicode scalar values; for a script, the turns' text).
    pub input_chars: u32,
    /// The longest reference clip in milliseconds, when it can be read from the clip (WAV).
    pub reference_ms: Option<u64>,
    /// The largest reference clip in bytes.
    pub reference_bytes: Option<u64>,
}

impl SpeechNeeds {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_preset(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        if !self.presets.contains(&name) {
            self.presets.push(name);
        }
        self.voices = self.voices.saturating_add(1);
        self
    }

    pub fn with_design(mut self) -> Self {
        self.design = true;
        self.voices = self.voices.saturating_add(1);
        self
    }

    pub fn with_instructions(mut self) -> Self {
        self.instructions = true;
        self
    }

    /// A cloned voice; `has_text`: its transcript comes with the clip.
    pub fn with_clone(mut self, has_text: bool) -> Self {
        self.clone = Some(self.clone.map_or(has_text, |t| t && has_text));
        self.voices = self.voices.saturating_add(1);
        self
    }

    /// A script whose highest speaker number is `highest`.
    pub fn with_script(mut self, highest: u8) -> Self {
        self.speakers = highest.saturating_add(1);
        self
    }

    pub fn with_raw(mut self) -> Self {
        self.raw = true;
        self
    }

    pub fn with_speed(mut self) -> Self {
        self.speed = true;
        self
    }

    pub fn with_format(mut self, format: AudioFormat) -> Self {
        self.format = Some(format);
        self
    }

    pub fn with_input_chars(mut self, chars: u32) -> Self {
        self.input_chars = chars;
        self
    }

    pub fn with_reference(mut self, bytes: u64, ms: Option<u64>) -> Self {
        self.reference_bytes = Some(self.reference_bytes.map_or(bytes, |b| b.max(bytes)));
        if let Some(ms) = ms {
            self.reference_ms = Some(self.reference_ms.map_or(ms, |m| m.max(ms)));
        }
        self
    }
}

/// The length in milliseconds of a WAV file (RIFF, any sample format), from its `fmt ` byte
/// rate and `data` size; `None` when the bytes are not a readable WAV. Used for the reference
/// clip's length in [`SpeechNeeds::reference_ms`].
pub fn wav_duration_ms(bytes: &[u8]) -> Option<u64> {
    if bytes.len() < 12 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        return None;
    }
    let mut at = 12usize;
    let mut byte_rate: Option<u32> = None;
    while at.checked_add(8)? <= bytes.len() {
        let id = &bytes[at..at + 4];
        let size = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().ok()?) as usize;
        let body = at + 8;
        if id == b"fmt " {
            if size < 16 || body.checked_add(16)? > bytes.len() {
                return None;
            }
            byte_rate = Some(u32::from_le_bytes(bytes[body + 8..body + 12].try_into().ok()?));
        } else if id == b"data" {
            let rate = byte_rate.filter(|r| *r > 0)?;
            // A streamed WAV may say 0 or 0xFFFFFFFF; count what is there.
            let len = if size == 0 || size == u32::MAX as usize || body.saturating_add(size) > bytes.len() { bytes.len() - body } else { size };
            return Some(len as u64 * 1000 / u64::from(rate));
        }
        at = body.checked_add(size)?.checked_add(size & 1)?;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn qwen_custom() -> SpeechProfile {
        SpeechProfile::new()
            .with_presets(s(&["vivian", "ryan", "serena"]))
            .with_instructions(true)
            .with_speed(true)
            .with_streaming(true)
            .with_languages(s(&["en", "zh", "de"]))
            .with_formats(vec![AudioFormat::Wav, AudioFormat::Pcm16, AudioFormat::Mp3])
            .with_sample_rate(24_000)
            .with_max_input_chars(4000)
            .with_max_output_seconds(300)
    }

    fn drama() -> SpeechProfile {
        SpeechProfile::new()
            .with_clone(CloneMode::AudioTextRequired)
            .with_script_speakers(16)
            .with_max_voices(1)
            .with_formats(vec![AudioFormat::Wav, AudioFormat::Pcm16])
            .with_sample_rate(44_100)
            .with_max_reference_seconds(30)
            .with_license("fish-audio-community")
    }

    #[test]
    fn formats_and_clone_modes_have_wire_names() {
        for f in AudioFormat::ALL {
            assert_eq!(AudioFormat::parse(f.as_str()), Some(f));
            assert_eq!(serde_json::to_value(f).unwrap(), serde_json::json!(f.as_str()));
        }
        assert_eq!(AudioFormat::parse("PCM"), Some(AudioFormat::Pcm16));
        assert_eq!(AudioFormat::parse("aac"), None);
        assert_eq!(AudioFormat::Pcm16.mime(), "audio/pcm");
        let modes = [(CloneMode::Audio, "audio"), (CloneMode::AudioAndText, "audio_and_text"), (CloneMode::AudioTextRequired, "audio_text_required")];
        for (m, name) in modes {
            assert_eq!(serde_json::to_value(m).unwrap(), serde_json::json!(name));
        }
    }

    #[test]
    fn unset_fields_stay_off_the_wire() {
        assert_eq!(serde_json::to_value(SpeechProfile::new()).unwrap(), serde_json::json!({}));
        assert!(SpeechProfile::new().is_unknown());
        let p = drama();
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"clone": "audio_text_required", "script_speakers": 16, "max_voices": 1, "formats": ["wav", "pcm16"],
                "sample_rate": 44100, "max_reference_seconds": 30, "license": "fish-audio-community"})
        );
        assert_eq!(serde_json::from_value::<SpeechProfile>(v).unwrap(), p);
        assert_eq!(p.default_format(), Some(AudioFormat::Wav));
    }

    #[test]
    fn tightest_keeps_what_every_copy_does() {
        let a = qwen_custom();
        let b = SpeechProfile::new()
            .with_presets(s(&["ryan", "vivian", "aiden"]))
            .with_instructions(false)
            .with_speed(true)
            .with_languages(s(&["en", "fr", "zh"]))
            .with_formats(vec![AudioFormat::Pcm16, AudioFormat::Wav])
            .with_sample_rate(24_000)
            .with_max_input_chars(2000)
            .with_max_reference_bytes(1 << 20);
        let t = SpeechProfile::tightest(&a, &b);
        assert_eq!(t.presets, Some(s(&["vivian", "ryan"])));
        assert!(!t.instructions && t.speed && !t.streaming);
        assert_eq!(t.languages, Some(s(&["en", "zh"])));
        assert_eq!(t.formats, [AudioFormat::Wav, AudioFormat::Pcm16]);
        assert_eq!(
            (t.sample_rate, t.max_input_chars, t.max_output_seconds, t.max_reference_bytes),
            (Some(24_000), Some(2000), Some(300), Some(1 << 20))
        );

        // Features need both; lists and limits one reports are kept.
        let bare = SpeechProfile::new();
        let t = SpeechProfile::tightest(&a, &bare);
        assert_eq!((t.presets, t.speed, t.instructions), (None, false, false));
        assert_eq!((t.languages, t.formats, t.max_input_chars), (a.languages.clone(), a.formats.clone(), Some(4000)));
        assert_eq!(SpeechProfile::tightest(&bare, &a).formats, a.formats);

        // Clone modes, speakers, rates and licenses.
        let mut c = drama();
        c.clone = Some(CloneMode::Audio);
        c.script_speakers = Some(4);
        c.sample_rate = Some(48_000);
        let t = SpeechProfile::tightest(&drama(), &c);
        assert_eq!(
            (t.clone, t.script_speakers, t.sample_rate, t.license.as_deref()),
            (Some(CloneMode::AudioTextRequired), Some(4), None, Some("fish-audio-community"))
        );
        assert_eq!(CloneMode::Audio.tightest(CloneMode::AudioAndText), CloneMode::Audio);
        assert_eq!(CloneMode::AudioAndText.tightest(CloneMode::AudioAndText), CloneMode::AudioAndText);
        assert_eq!(SpeechProfile::tightest(&drama(), &SpeechProfile::new()).clone, None);
        c.license = Some("apache-2.0".into());
        assert_eq!(SpeechProfile::tightest(&drama(), &c).license, None);
        assert_eq!(SpeechProfile::tightest(&a, &a), a, "idempotent");
    }

    #[test]
    fn operators_narrow_or_fill_but_never_widen() {
        let engine = qwen_custom().with_max_voices(2);
        let op = SpeechProfile::new()
            .with_presets(s(&["vivian"]))
            .with_languages(s(&["en"]))
            .with_formats(vec![AudioFormat::Wav])
            .with_max_input_chars(2000)
            .with_max_reference_seconds(30)
            .with_max_voices(1);
        let n = engine.narrowed_by(&op).unwrap();
        assert_eq!((n.presets, n.languages, n.formats.clone()), (Some(s(&["vivian"])), Some(s(&["en"])), vec![AudioFormat::Wav]));
        assert_eq!((n.max_input_chars, n.max_reference_seconds, n.max_output_seconds, n.max_voices), (Some(2000), Some(30), Some(300), Some(1)));
        assert!(n.instructions && n.speed && n.streaming, "unset operator bools change nothing");
        assert_eq!(engine.narrowed_by(&SpeechProfile::new()).unwrap(), engine);

        let widen = |op: SpeechProfile, field: &str| {
            let e = engine.narrowed_by(&op).unwrap_err();
            assert!(e.starts_with(field), "{e}");
        };
        widen(SpeechProfile::new().with_presets(s(&["alloy"])), "presets");
        widen(SpeechProfile::new().with_languages(s(&["ja"])), "languages");
        widen(SpeechProfile::new().with_formats(vec![AudioFormat::Opus]), "formats");
        widen(SpeechProfile::new().with_max_input_chars(5000), "max_input_chars");
        widen(SpeechProfile::new().with_design(true), "design");
        widen(SpeechProfile::new().with_raw(true), "raw");
        widen(SpeechProfile::new().with_clone(CloneMode::Audio), "clone");
        widen(SpeechProfile::new().with_script_speakers(2), "script_speakers");
        widen(SpeechProfile::new().with_sample_rate(48_000), "sample_rate");
        widen(SpeechProfile::new().with_max_voices(3), "max_voices");

        // Filling what the engine doesn't report (not presets: those come from the engine).
        let quiet = SpeechProfile::new().with_clone(CloneMode::AudioAndText);
        let n = quiet.narrowed_by(&SpeechProfile::new().with_languages(s(&["en"])).with_max_reference_seconds(30).with_sample_rate(24_000)).unwrap();
        assert_eq!((n.languages, n.max_reference_seconds, n.sample_rate), (Some(s(&["en"])), Some(30), Some(24_000)));
        assert!(quiet.narrowed_by(&SpeechProfile::new().with_presets(s(&["a"]))).unwrap_err().starts_with("presets"));
        // A stricter clone mode narrows; a looser one widens.
        assert_eq!(
            quiet.narrowed_by(&SpeechProfile::new().with_clone(CloneMode::AudioTextRequired)).unwrap().clone,
            Some(CloneMode::AudioTextRequired)
        );
        assert!(drama().narrowed_by(&SpeechProfile::new().with_clone(CloneMode::Audio)).unwrap_err().starts_with("clone"));
        assert!(drama().narrowed_by(&SpeechProfile::new().with_license("mit")).unwrap_err().starts_with("license"));
    }

    #[test]
    fn covers_names_what_a_model_lacks() {
        let q = qwen_custom();
        assert!(q.covers(&SpeechNeeds::new().with_input_chars(10)).is_ok());
        assert!(q.covers(&SpeechNeeds::new().with_preset("vivian").with_instructions().with_speed().with_format(AudioFormat::Mp3)).is_ok());
        let lack = |p: &SpeechProfile, n: SpeechNeeds, what: &str| {
            let e = p.covers(&n).unwrap_err();
            assert!(e.contains(what), "{e}");
        };
        lack(&q, SpeechNeeds::new().with_preset("alloy"), "no preset voice `alloy` (presets: vivian, ryan, serena)");
        lack(&drama(), SpeechNeeds::new().with_preset("alloy"), "no preset voices");
        lack(&q, SpeechNeeds::new().with_design(), "design");
        lack(&drama(), SpeechNeeds::new().with_instructions(), "instructions");
        lack(&q, SpeechNeeds::new().with_clone(true), "does not clone voices");
        lack(&drama(), SpeechNeeds::new().with_clone(false), "transcript");
        assert!(drama().covers(&SpeechNeeds::new().with_clone(true).with_script(15)).is_ok());
        lack(&drama(), SpeechNeeds::new().with_script(16), "at most 16 speakers");
        lack(&q, SpeechNeeds::new().with_script(0), "multi-speaker");
        lack(&drama(), SpeechNeeds::new().with_clone(true).with_clone(true), "at most 1 voices");
        lack(&q, SpeechNeeds::new().with_raw(), "raw");
        lack(&drama(), SpeechNeeds::new().with_speed(), "speed");
        lack(&q, SpeechNeeds::new().with_format(AudioFormat::Flac), "does not produce `flac` (formats: wav, pcm16, mp3)");
        lack(&SpeechProfile::new(), SpeechNeeds::new().with_format(AudioFormat::Wav), "reports no output formats");
        lack(&q, SpeechNeeds::new().with_input_chars(4001), "at most 4000 characters");
        lack(&drama(), SpeechNeeds::new().with_clone(true).with_reference(10, Some(31_000)), "at most 30 s");
        assert!(drama().covers(&SpeechNeeds::new().with_clone(true).with_reference(10, None)).is_ok(), "unknown length is the engine's call");
        lack(&drama().with_max_reference_bytes(100), SpeechNeeds::new().with_clone(true).with_reference(101, None), "at most 100 bytes");
        // A profile reporting nothing serves only the plainest request.
        assert!(SpeechProfile::new().covers(&SpeechNeeds::new().with_input_chars(5)).is_ok());
    }

    #[test]
    fn wav_lengths_come_from_the_header() {
        fn wav(rate: u32, channels: u16, samples: u32) -> Vec<u8> {
            let data = samples * u32::from(channels) * 2;
            let mut b = b"RIFF".to_vec();
            b.extend((36 + data).to_le_bytes());
            b.extend(b"WAVEfmt ");
            b.extend(16u32.to_le_bytes());
            b.extend(1u16.to_le_bytes());
            b.extend(channels.to_le_bytes());
            b.extend(rate.to_le_bytes());
            b.extend((rate * u32::from(channels) * 2).to_le_bytes());
            b.extend((channels * 2).to_le_bytes());
            b.extend(16u16.to_le_bytes());
            b.extend(b"LIST");
            b.extend(3u32.to_le_bytes());
            b.extend(b"abc\0");
            b.extend(b"data");
            b.extend(data.to_le_bytes());
            b.extend(vec![0u8; data as usize]);
            b
        }
        assert_eq!(wav_duration_ms(&wav(16_000, 1, 24_000)), Some(1500));
        assert_eq!(wav_duration_ms(&wav(24_000, 2, 12_000)), Some(500));
        let mut streamed = wav(16_000, 1, 1600);
        let at = streamed.len() - 3200 - 4;
        streamed[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(wav_duration_ms(&streamed), Some(100));
        assert_eq!(wav_duration_ms(b"ID3\x04 not a wav"), None);
        assert_eq!(wav_duration_ms(&wav(16_000, 1, 10)[..30]), None);
        assert_eq!(wav_duration_ms(b"RIFF\x00\x00\x00\x00WAVEdata\x10\x00\x00\x00"), None, "data before fmt");
    }
}
