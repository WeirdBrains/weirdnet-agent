//! Length-prefixed framing for byte streams: a big-endian `u32` length, then the message.

use super::RemoteError;

/// Largest message accepted (8 MiB): a long conversation fits, a runaway one does not.
pub const MAX_FRAME: usize = 8 << 20;

/// One framed message, ready to write.
pub fn encode_frame(message: &[u8]) -> Result<Vec<u8>, RemoteError> {
    if message.len() > MAX_FRAME {
        return Err(RemoteError::protocol(format!("message of {} bytes is over the {MAX_FRAME}-byte limit", message.len())));
    }
    let mut out = Vec::with_capacity(4 + message.len());
    out.extend_from_slice(&(message.len() as u32).to_be_bytes());
    out.extend_from_slice(message);
    Ok(out)
}

/// Collects bytes as they are read and hands out whole messages.
#[derive(Debug, Default)]
pub struct FrameDecoder {
    buf: Vec<u8>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// The next whole message, if one has arrived.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, RemoteError> {
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize;
        if len > MAX_FRAME {
            return Err(RemoteError::protocol(format!("incoming message of {len} bytes is over the {MAX_FRAME}-byte limit")));
        }
        if self.buf.len() < 4 + len {
            return Ok(None);
        }
        let msg = self.buf[4..4 + len].to_vec();
        self.buf.drain(..4 + len);
        Ok(Some(msg))
    }

    /// Bytes held that are not a whole message yet.
    pub fn pending(&self) -> usize {
        self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_survive_any_split() {
        let mut wire = encode_frame(b"hello").unwrap();
        wire.extend(encode_frame(b"").unwrap());
        wire.extend(encode_frame("wörld".as_bytes()).unwrap());
        for split in 0..wire.len() {
            let mut d = FrameDecoder::new();
            let mut got = Vec::new();
            for chunk in [&wire[..split], &wire[split..]] {
                d.push(chunk);
                while let Some(m) = d.next_frame().unwrap() {
                    got.push(m);
                }
            }
            assert_eq!(got, [b"hello".to_vec(), Vec::new(), "wörld".as_bytes().to_vec()], "split at {split}");
            assert_eq!(d.pending(), 0);
        }
    }

    #[test]
    fn oversized_frames_are_refused_both_ways() {
        assert!(encode_frame(&vec![0; MAX_FRAME + 1]).is_err());
        let mut d = FrameDecoder::new();
        d.push(&((MAX_FRAME as u32) + 1).to_be_bytes());
        assert!(matches!(d.next_frame(), Err(RemoteError::Protocol { .. })));
    }
}
