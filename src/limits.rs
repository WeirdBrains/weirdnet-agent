//! Per-model media and request limits: what a client needs to size images, audio, video and
//! whole requests for one model, such as fitting a video clip's frames and soundtrack to it, or
//! deciding whether the clip can go whole to a model that reads video natively.
//!
//! Every field is absent when the node doesn't know it, never defaulted: a node fills in what it
//! measured when the model loaded, or what its operator set for that hardware, and leaves the
//! rest out. A client that needs a missing limit should say so rather than guess. All fields are
//! left off the wire when absent, so peers before 0.5.7 read and send entries unchanged.
//!
//! Catalog entries carry them as [`CatalogModel::limits`](crate::remote::catalog::CatalogModel::limits);
//! the context window stays in [`CatalogModel::context`](crate::remote::catalog::CatalogModel::context).

use serde::{Deserialize, Serialize};

/// Media and request limits for one model. See the [module docs](self).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MediaLimits {
    /// Most images (`image_url` parts) one request may carry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_images: Option<u32>,
    /// Tokens one image costs. With [`image_patch`](Self::image_patch) set, the most one image
    /// costs (at [`image_edge`](Self::image_edge)); otherwise every image costs this much.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_tokens: Option<u32>,
    /// For models whose image cost grows with size: the edge in pixels of the square one image
    /// token covers (an image costs `ceil(w / patch) * ceil(h / patch)` tokens after scaling to
    /// [`image_edge`](Self::image_edge)). Absent: a fixed cost per image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_patch: Option<u32>,
    /// The longest image edge in pixels the model uses; larger images are scaled down to it, so
    /// sending more only costs bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_edge: Option<u32>,
    /// Longest audio input (`input_audio` part) in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_max_seconds: Option<u32>,
    /// Tokens one minute of audio costs (per minute so fractional per-second rates such as 6.25
    /// stay exact; see [`audio_tokens_per_second`](Self::audio_tokens_per_second)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_tokens_per_minute: Option<u32>,
    /// Largest request body in bytes the serving side accepts (an OpenAI bridge's
    /// `max_request_bytes`, for example), so a client sizes frames to fit the bytes as well as
    /// the tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_request_bytes: Option<u64>,
    /// Longest video clip (`video_url` part) in seconds the model takes whole. Since 0.5.10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_max_seconds: Option<u32>,
    /// Largest video clip in bytes (decoded, not base64) the model takes whole. Since 0.5.10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_max_bytes: Option<u64>,
    /// Most frames the model reads per minute of a clip sent whole (its backend samples down to
    /// this): per minute so rates below one frame a second, such as 0.5, stay exact, as with
    /// [`audio_tokens_per_minute`](Self::audio_tokens_per_minute). See
    /// [`video_frame_rate`](Self::video_frame_rate) for frames per second. Since 0.5.10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_frames_per_minute: Option<u32>,
}

impl MediaLimits {
    /// No limits known.
    pub const UNKNOWN: Self = Self {
        max_images: None,
        image_tokens: None,
        image_patch: None,
        image_edge: None,
        audio_max_seconds: None,
        audio_tokens_per_minute: None,
        max_request_bytes: None,
        video_max_seconds: None,
        video_max_bytes: None,
        video_frames_per_minute: None,
    };

    pub fn new() -> Self {
        Self::UNKNOWN
    }

    /// Whether no limit is known (what is left off the wire).
    pub fn is_unknown(&self) -> bool {
        *self == Self::UNKNOWN
    }

    pub fn with_max_images(mut self, n: u32) -> Self {
        self.max_images = Some(n);
        self
    }

    /// Every image costs `tokens`.
    pub fn with_image_tokens(mut self, tokens: u32) -> Self {
        self.image_tokens = Some(tokens);
        self
    }

    /// The image cost grows with size: one token per `patch` x `patch` pixels.
    pub fn with_image_patch(mut self, patch: u32) -> Self {
        self.image_patch = Some(patch);
        self
    }

    pub fn with_image_edge(mut self, pixels: u32) -> Self {
        self.image_edge = Some(pixels);
        self
    }

    pub fn with_audio_max_seconds(mut self, seconds: u32) -> Self {
        self.audio_max_seconds = Some(seconds);
        self
    }

    pub fn with_audio_tokens_per_minute(mut self, tokens: u32) -> Self {
        self.audio_tokens_per_minute = Some(tokens);
        self
    }

    pub fn with_max_request_bytes(mut self, bytes: u64) -> Self {
        self.max_request_bytes = Some(bytes);
        self
    }

    pub fn with_video_max_seconds(mut self, seconds: u32) -> Self {
        self.video_max_seconds = Some(seconds);
        self
    }

    pub fn with_video_max_bytes(mut self, bytes: u64) -> Self {
        self.video_max_bytes = Some(bytes);
        self
    }

    /// The model reads at most `frames` frames per minute of video (120 for 2 per second).
    pub fn with_video_frames_per_minute(mut self, frames: u32) -> Self {
        self.video_frames_per_minute = Some(frames);
        self
    }

    /// [`video_frames_per_minute`](Self::video_frames_per_minute) per second (the frame rate).
    pub fn video_frame_rate(&self) -> Option<f64> {
        self.video_frames_per_minute.map(|f| f64::from(f) / 60.0)
    }

    /// The most frames the model reads from `millis` of video sent whole (rounded up), if known.
    pub fn video_frames(&self, millis: u64) -> Option<u64> {
        self.video_frames_per_minute.map(|f| (u64::from(f) * millis).div_ceil(60_000))
    }

    /// Whether a clip of `millis` and `bytes` (decoded) fits the known video limits: `Err` names
    /// the first it exceeds (`video_max_seconds` or `video_max_bytes`). An unknown limit is not
    /// checked, so callers that need one should look for it first.
    pub fn check_video(&self, millis: u64, bytes: u64) -> Result<(), &'static str> {
        if self.video_max_seconds.is_some_and(|s| millis > u64::from(s) * 1000) {
            return Err("video_max_seconds");
        }
        if self.video_max_bytes.is_some_and(|b| bytes > b) {
            return Err("video_max_bytes");
        }
        Ok(())
    }

    /// [`audio_tokens_per_minute`](Self::audio_tokens_per_minute) per second.
    pub fn audio_tokens_per_second(&self) -> Option<f64> {
        self.audio_tokens_per_minute.map(|t| f64::from(t) / 60.0)
    }

    /// What one `width` x `height` image costs in tokens, if known: the fixed cost, or with
    /// [`image_patch`](Self::image_patch) the patches after scaling down to
    /// [`image_edge`](Self::image_edge), capped at [`image_tokens`](Self::image_tokens).
    pub fn image_cost(&self, width: u32, height: u32) -> Option<u32> {
        let Some(patch) = self.image_patch.filter(|p| *p > 0) else {
            return self.image_tokens;
        };
        let (w, h) = self.scaled(width, height);
        let tokens = u64::from(w.div_ceil(patch)) * u64::from(h.div_ceil(patch));
        let tokens = u32::try_from(tokens).unwrap_or(u32::MAX);
        Some(self.image_tokens.map_or(tokens, |cap| tokens.min(cap)))
    }

    /// `width` x `height` scaled down (aspect kept) so the longer edge fits
    /// [`image_edge`](Self::image_edge); unchanged when it fits or no edge is known.
    pub fn scaled(&self, width: u32, height: u32) -> (u32, u32) {
        let long = width.max(height);
        match self.image_edge {
            Some(edge) if edge > 0 && long > edge => {
                let s = |v: u32| ((u64::from(v) * u64::from(edge)).div_ceil(u64::from(long)) as u32).max(1);
                (s(width), s(height))
            }
            _ => (width, height),
        }
    }

    /// What `millis` of audio costs in tokens (rounded up), if known.
    pub fn audio_cost(&self, millis: u64) -> Option<u64> {
        self.audio_tokens_per_minute.map(|t| (u64::from(t) * millis).div_ceil(60_000))
    }

    /// Each field from `self` when known, else from `fallback`: operator settings over measured
    /// ones (`operator.or(measured)`).
    pub fn or(self, fallback: Self) -> Self {
        Self {
            max_images: self.max_images.or(fallback.max_images),
            image_tokens: self.image_tokens.or(fallback.image_tokens),
            image_patch: self.image_patch.or(fallback.image_patch),
            image_edge: self.image_edge.or(fallback.image_edge),
            audio_max_seconds: self.audio_max_seconds.or(fallback.audio_max_seconds),
            audio_tokens_per_minute: self.audio_tokens_per_minute.or(fallback.audio_tokens_per_minute),
            max_request_bytes: self.max_request_bytes.or(fallback.max_request_bytes),
            video_max_seconds: self.video_max_seconds.or(fallback.video_max_seconds),
            video_max_bytes: self.video_max_bytes.or(fallback.video_max_bytes),
            video_frames_per_minute: self.video_frames_per_minute.or(fallback.video_frames_per_minute),
        }
    }

    /// The stricter of two copies, field by field: fewer images, a smaller edge, shorter audio and
    /// video, fewer request and video bytes, a lower frame rate, a smaller patch and higher token
    /// costs. A field known on one side
    /// only keeps that side's value, so merging an engine's limits with a model's loses nothing;
    /// a field neither side knows stays absent.
    pub fn tightest(self, other: Self) -> Self {
        fn pick<T: Ord>(a: Option<T>, b: Option<T>, f: fn(T, T) -> T) -> Option<T> {
            match (a, b) {
                (Some(a), Some(b)) => Some(f(a, b)),
                (a, b) => a.or(b),
            }
        }
        Self {
            max_images: pick(self.max_images, other.max_images, Ord::min),
            image_tokens: pick(self.image_tokens, other.image_tokens, Ord::max),
            image_patch: pick(self.image_patch, other.image_patch, Ord::min),
            image_edge: pick(self.image_edge, other.image_edge, Ord::min),
            audio_max_seconds: pick(self.audio_max_seconds, other.audio_max_seconds, Ord::min),
            audio_tokens_per_minute: pick(self.audio_tokens_per_minute, other.audio_tokens_per_minute, Ord::max),
            max_request_bytes: pick(self.max_request_bytes, other.max_request_bytes, Ord::min),
            video_max_seconds: pick(self.video_max_seconds, other.video_max_seconds, Ord::min),
            video_max_bytes: pick(self.video_max_bytes, other.video_max_bytes, Ord::min),
            video_frames_per_minute: pick(self.video_frames_per_minute, other.video_frames_per_minute, Ord::min),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full() -> MediaLimits {
        MediaLimits::new()
            .with_max_images(8)
            .with_image_tokens(1024)
            .with_image_patch(28)
            .with_image_edge(896)
            .with_audio_max_seconds(30)
            .with_audio_tokens_per_minute(375)
            .with_max_request_bytes(32 << 20)
            .with_video_max_seconds(60)
            .with_video_max_bytes(16 << 20)
            .with_video_frames_per_minute(120)
    }

    #[test]
    fn unknown_limits_are_left_off_the_wire() {
        assert!(MediaLimits::default().is_unknown());
        assert_eq!(serde_json::to_string(&MediaLimits::UNKNOWN).unwrap(), "{}");
        let partial = MediaLimits::new().with_max_images(4);
        assert_eq!(serde_json::to_string(&partial).unwrap(), r#"{"max_images":4}"#);
        let back: MediaLimits = serde_json::from_str("{}").unwrap();
        assert!(back.is_unknown());
    }

    #[test]
    fn round_trips_through_json_and_cbor() {
        for l in [MediaLimits::UNKNOWN, MediaLimits::new().with_image_tokens(256), full()] {
            let j: MediaLimits = serde_json::from_str(&serde_json::to_string(&l).unwrap()).unwrap();
            assert_eq!(j, l);
            let c: MediaLimits = cbor4ii::serde::from_slice(&cbor4ii::serde::to_vec(Vec::new(), &l).unwrap()).unwrap();
            assert_eq!(c, l);
        }
        let v = serde_json::to_value(full()).unwrap();
        for k in [
            "max_images",
            "image_tokens",
            "image_patch",
            "image_edge",
            "audio_max_seconds",
            "audio_tokens_per_minute",
            "max_request_bytes",
            "video_max_seconds",
            "video_max_bytes",
            "video_frames_per_minute",
        ] {
            assert!(v.get(k).is_some(), "{k}");
        }
    }

    #[test]
    fn image_and_audio_costs() {
        let fixed = MediaLimits::new().with_image_tokens(256);
        assert_eq!(fixed.image_cost(4000, 3000), Some(256));
        assert_eq!(MediaLimits::UNKNOWN.image_cost(10, 10), None);
        let patched = MediaLimits::new().with_image_patch(28).with_image_edge(896);
        assert_eq!(patched.scaled(1792, 896), (896, 448));
        assert_eq!(patched.scaled(500, 300), (500, 300));
        assert_eq!(patched.image_cost(1792, 896), Some(32 * 16));
        assert_eq!(patched.image_cost(29, 28), Some(2));
        assert_eq!(patched.with_image_tokens(100).image_cost(1792, 896), Some(100), "capped");
        assert_eq!(MediaLimits::new().with_image_patch(14).image_cost(28, 28), Some(4), "no edge: as sent");
        let audio = MediaLimits::new().with_audio_tokens_per_minute(375);
        assert_eq!(audio.audio_tokens_per_second(), Some(6.25));
        assert_eq!(audio.audio_cost(60_000), Some(375));
        assert_eq!(audio.audio_cost(1_000), Some(7), "rounded up");
        assert_eq!(MediaLimits::UNKNOWN.audio_cost(1_000), None);
    }

    #[test]
    fn video_limits() {
        let none = MediaLimits::new().with_max_images(4);
        assert_eq!(serde_json::to_value(none).unwrap(), serde_json::json!({"max_images": 4}), "unknown video fields stay off the wire");
        assert_eq!((none.video_frame_rate(), none.video_frames(10_000)), (None, None));
        assert_eq!(none.check_video(u64::MAX, u64::MAX), Ok(()), "nothing known, nothing checked");
        let v = MediaLimits::new().with_video_max_seconds(30).with_video_max_bytes(8 << 20).with_video_frames_per_minute(30);
        assert_eq!(
            serde_json::to_value(v).unwrap(),
            serde_json::json!({"video_max_seconds": 30, "video_max_bytes": 8388608, "video_frames_per_minute": 30})
        );
        assert_eq!(v.video_frame_rate(), Some(0.5));
        assert_eq!(v.video_frames(10_000), Some(5));
        assert_eq!(v.video_frames(10_001), Some(6), "rounded up");
        assert_eq!(v.check_video(30_000, 8 << 20), Ok(()));
        assert_eq!(v.check_video(30_001, 1), Err("video_max_seconds"));
        assert_eq!(v.check_video(1_000, (8 << 20) + 1), Err("video_max_bytes"));
        // What a 0.5.9 peer sends reads as no video limits; what it can't read it ignores.
        let old: MediaLimits = serde_json::from_str(r#"{"max_images":2,"audio_max_seconds":30}"#).unwrap();
        assert_eq!(old, MediaLimits::new().with_max_images(2).with_audio_max_seconds(30));
        let newer: MediaLimits = serde_json::from_str(r#"{"video_max_seconds":5,"video_codecs":["h264"]}"#).unwrap();
        assert_eq!(newer, MediaLimits::new().with_video_max_seconds(5));
        // Merging: shorter, smaller and slower wins; one side only keeps its value.
        let a = MediaLimits::new().with_video_max_seconds(60).with_video_frames_per_minute(120);
        let b = MediaLimits::new().with_video_max_seconds(20).with_video_max_bytes(4 << 20).with_video_frames_per_minute(60);
        let t = a.tightest(b);
        assert_eq!((t.video_max_seconds, t.video_max_bytes, t.video_frames_per_minute), (Some(20), Some(4 << 20), Some(60)));
        assert_eq!(a.tightest(MediaLimits::UNKNOWN), a);
        let o = MediaLimits::new().with_video_max_bytes(1 << 20).or(b);
        assert_eq!(
            (o.video_max_seconds, o.video_max_bytes, o.video_frames_per_minute),
            (Some(20), Some(1 << 20), Some(60)),
            "operator over measured"
        );
        assert!(!MediaLimits::new().with_video_frames_per_minute(1).is_unknown());
    }

    #[test]
    fn merging() {
        let a = MediaLimits::new().with_max_images(8).with_image_tokens(256).with_max_request_bytes(10);
        let b = MediaLimits::new().with_max_images(2).with_image_tokens(576).with_image_edge(336);
        let t = a.tightest(b);
        assert_eq!((t.max_images, t.image_tokens, t.image_edge, t.max_request_bytes), (Some(2), Some(576), Some(336), Some(10)));
        assert_eq!(t.audio_max_seconds, None);
        assert_eq!(a.tightest(MediaLimits::UNKNOWN), a);
        let operator = MediaLimits::new().with_max_images(1);
        let o = operator.or(a);
        assert_eq!((o.max_images, o.image_tokens), (Some(1), Some(256)));
    }
}
