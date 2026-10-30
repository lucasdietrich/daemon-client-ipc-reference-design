//! Wire protocol shared by the daemon and the client library.
//!
//! This crate is "sans-IO": it defines the messages and how they are framed,
//! but it never opens or owns a socket. I/O adapters live behind features:
//!
//! - `sync`  (default): [`sync::read_msg`] / [`sync::write_msg`] over
//!   `std::io::Read` / `std::io::Write`.
//! - `tokio`: [`codec::Codec`], a `tokio_util::codec` Decoder/Encoder.
//!
//! # Frame layout
//!
//! ```text
//! +----------------------+---------------------------+
//! | u32 LE payload len   | postcard-encoded payload  |
//! +----------------------+---------------------------+
//! ```
//!
//! # Conversation
//!
//! ```text
//! client                                  daemon
//!   | -- ClientMsg::Hello { version } ------> |
//!   | <-- ServerMsg::Welcome | Rejected ----- |
//!   |                                         |
//!   | -- ClientMsg::Command { id, cmd } ----> |
//!   | <-- ServerMsg::Event(..) -------------- |   (any time, unsolicited)
//!   | <-- ServerMsg::Response { id, resp } -- |   (same id as the command)
//! ```
//!
//! Responses and events share the same stream, so a client must be ready to
//! receive events while it waits for a response.

use std::path::PathBuf;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Bump on any incompatible change to the message types below.
pub const PROTO_VERSION: u16 = 1;

/// Largest accepted payload. Protects both sides against a misbehaving peer.
pub const MAX_FRAME: usize = 1 << 20;

/// Size of the length prefix.
pub const HDR_LEN: usize = 4;

/// Default socket location on the host. Containers get it via a bind mount
/// of the parent directory (not of the socket file itself).
pub const DEFAULT_SOCKET: &str = "/run/mydaemon/mydaemon.sock";

/// Environment variable that overrides [`DEFAULT_SOCKET`].
pub const SOCKET_ENV: &str = "MYDAEMON_SOCKET";

/// Socket path to use: `$MYDAEMON_SOCKET` if set, else [`DEFAULT_SOCKET`].
pub fn socket_path() -> PathBuf {
    std::env::var_os(SOCKET_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SOCKET))
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// Everything the client sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientMsg {
    /// Must be the first message on a connection.
    Hello { version: u16 },
    /// A command. The daemon answers with [`ServerMsg::Response`] carrying
    /// the same `id`.
    Command { id: u32, cmd: Command },
}

/// Everything the daemon sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerMsg {
    /// Handshake accepted.
    Welcome { version: u16 },
    /// Handshake refused; the daemon closes the connection afterwards.
    Rejected { reason: String },
    /// Answer to a [`ClientMsg::Command`].
    Response { id: u32, resp: Response },
    /// Unsolicited notification. The client never answers it.
    Event(Event),
}

/// Commands: each one gets exactly one [`Response`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Ping,
    Get {
        key: String,
    },
    Set {
        key: String,
        value: Vec<u8>,
    },
    /// Start receiving [`Event`]s on this connection.
    Subscribe,
    /// Stop receiving [`Event`]s on this connection.
    Unsubscribe,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Pong,
    Value(Option<Vec<u8>>),
    Done,
    Error(String),
}

/// Events pushed by the daemon to subscribed clients.
///
/// New variants must be appended at the end. Older clients skip frames they
/// cannot decode instead of dropping the connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Event {
    KeyChanged { key: String },
    Heartbeat { uptime_secs: u64 },
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// A complete frame was received but its payload could not be decoded.
    /// Frame boundaries are intact, so the stream can keep going.
    #[error("cannot decode frame: {0}")]
    Codec(#[from] postcard::Error),

    #[error("frame too large: {0} bytes (max {MAX_FRAME})")]
    TooLarge(usize),
}

impl Error {
    /// True when the peer closed the connection cleanly.
    pub fn is_eof(&self) -> bool {
        matches!(self, Error::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof)
    }

    /// True when the error only affects one frame and the stream is still
    /// usable (see [`Error::Codec`]).
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Error::Codec(_))
    }
}

// ---------------------------------------------------------------------------
// Sans-IO framing
// ---------------------------------------------------------------------------

/// Append one framed message to `out`.
pub fn encode<T: Serialize>(msg: &T, out: &mut Vec<u8>) -> Result<(), Error> {
    let body = postcard::to_allocvec(msg)?;
    if body.len() > MAX_FRAME {
        return Err(Error::TooLarge(body.len()));
    }
    out.reserve(HDR_LEN + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(())
}

/// Validate a length prefix and return the payload length.
pub fn parse_header(hdr: [u8; HDR_LEN]) -> Result<usize, Error> {
    let len = u32::from_le_bytes(hdr) as usize;
    if len > MAX_FRAME {
        Err(Error::TooLarge(len))
    } else {
        Ok(len)
    }
}

/// Total size (header + payload) of the frame at the start of `buf`, or
/// `None` if the header itself is not complete yet.
pub fn frame_len(buf: &[u8]) -> Result<Option<usize>, Error> {
    match buf.first_chunk::<HDR_LEN>() {
        Some(hdr) => Ok(Some(HDR_LEN + parse_header(*hdr)?)),
        None => Ok(None),
    }
}

/// Decode the payload of a complete frame (`frame` includes the header).
pub fn decode_frame<T: DeserializeOwned>(frame: &[u8]) -> Result<T, Error> {
    Ok(postcard::from_bytes(&frame[HDR_LEN..])?)
}

/// Decode one message from the start of `buf`.
///
/// - `Ok(None)`: more bytes are needed.
/// - `Ok(Some((msg, n)))`: a message was decoded from the first `n` bytes.
pub fn decode<T: DeserializeOwned>(buf: &[u8]) -> Result<Option<(T, usize)>, Error> {
    match frame_len(buf)? {
        Some(n) if buf.len() >= n => Ok(Some((decode_frame(&buf[..n])?, n))),
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Blocking adapter
// ---------------------------------------------------------------------------

#[cfg(feature = "sync")]
pub mod sync {
    //! Blocking helpers. Each call reads or writes exactly one frame.

    use std::io::{Read, Write};

    use super::*;

    /// Write one message with a single `write_all`, so that concurrent
    /// writers serialized by a mutex never interleave partial frames.
    pub fn write_msg<W: Write, T: Serialize>(w: &mut W, msg: &T) -> Result<(), Error> {
        let mut buf = Vec::new();
        encode(msg, &mut buf)?;
        w.write_all(&buf)?;
        Ok(())
    }

    /// Read one message. The whole payload is consumed before decoding, so
    /// an [`Error::Codec`] leaves the stream aligned on the next frame.
    pub fn read_msg<R: Read, T: DeserializeOwned>(r: &mut R) -> Result<T, Error> {
        let mut hdr = [0u8; HDR_LEN];
        r.read_exact(&mut hdr)?;
        let mut body = vec![0u8; parse_header(hdr)?];
        r.read_exact(&mut body)?;
        Ok(postcard::from_bytes(&body)?)
    }
}

// ---------------------------------------------------------------------------
// tokio adapter
// ---------------------------------------------------------------------------

#[cfg(feature = "tokio")]
pub mod codec {
    //! `tokio_util::codec` adapter.

    use std::marker::PhantomData;

    use bytes::{Buf, BytesMut};
    use tokio_util::codec::{Decoder, Encoder};

    use super::*;

    /// `In` is the type received, `Out` the type sent.
    ///
    /// Decoded items are `Result<In, Error>`: a frame whose payload cannot be
    /// decoded yields `Some(Err(Error::Codec))` and the stream continues.
    /// Framing and I/O errors are returned as stream errors and are fatal.
    pub struct Codec<In, Out>(PhantomData<fn() -> (In, Out)>);

    impl<In, Out> Default for Codec<In, Out> {
        fn default() -> Self {
            Codec(PhantomData)
        }
    }

    impl<In: DeserializeOwned, Out> Decoder for Codec<In, Out> {
        type Item = Result<In, Error>;
        type Error = Error;

        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Error> {
            let Some(n) = frame_len(src)? else {
                return Ok(None);
            };
            if src.len() < n {
                src.reserve(n - src.len());
                return Ok(None);
            }
            // Consume the frame before decoding so a bad payload cannot
            // wedge the stream.
            let item = decode_frame(&src[..n]);
            src.advance(n);
            Ok(Some(item))
        }
    }

    impl<In, Out: Serialize> Encoder<Out> for Codec<In, Out> {
        type Error = Error;

        fn encode(&mut self, item: Out, dst: &mut BytesMut) -> Result<(), Error> {
            let mut buf = Vec::new();
            super::encode(&item, &mut buf)?;
            dst.extend_from_slice(&buf);
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ServerMsg {
        ServerMsg::Response {
            id: 42,
            resp: Response::Value(Some(b"hello".to_vec())),
        }
    }

    #[test]
    fn roundtrip() {
        let mut buf = Vec::new();
        encode(&sample(), &mut buf).unwrap();
        let (msg, n): (ServerMsg, usize) = decode(&buf).unwrap().unwrap();
        assert_eq!(msg, sample());
        assert_eq!(n, buf.len());
    }

    #[test]
    fn partial_frame_needs_more() {
        let mut buf = Vec::new();
        encode(&sample(), &mut buf).unwrap();
        for cut in 0..buf.len() {
            assert!(
                decode::<ServerMsg>(&buf[..cut]).unwrap().is_none(),
                "cut={cut}"
            );
        }
    }

    #[test]
    fn two_frames_back_to_back() {
        let mut buf = Vec::new();
        encode(&ClientMsg::Hello { version: 1 }, &mut buf).unwrap();
        encode(
            &ClientMsg::Command {
                id: 7,
                cmd: Command::Ping,
            },
            &mut buf,
        )
        .unwrap();
        let (a, n): (ClientMsg, _) = decode(&buf).unwrap().unwrap();
        let (b, _): (ClientMsg, _) = decode(&buf[n..]).unwrap().unwrap();
        assert_eq!(a, ClientMsg::Hello { version: 1 });
        assert_eq!(
            b,
            ClientMsg::Command {
                id: 7,
                cmd: Command::Ping
            }
        );
    }

    #[test]
    fn oversized_header_rejected() {
        let hdr = ((MAX_FRAME + 1) as u32).to_le_bytes();
        assert!(matches!(decode::<ServerMsg>(&hdr), Err(Error::TooLarge(_))));
    }

    #[cfg(feature = "sync")]
    #[test]
    fn sync_bad_payload_keeps_stream_aligned() {
        use std::io::Cursor;
        // A frame with a garbage payload, followed by a valid frame.
        let mut buf = vec![2, 0, 0, 0, 0xff, 0xff];
        encode(&sample(), &mut buf).unwrap();
        let mut cur = Cursor::new(buf);
        let err = sync::read_msg::<_, ServerMsg>(&mut cur).unwrap_err();
        assert!(err.is_recoverable());
        assert_eq!(sync::read_msg::<_, ServerMsg>(&mut cur).unwrap(), sample());
        assert!(
            sync::read_msg::<_, ServerMsg>(&mut cur)
                .unwrap_err()
                .is_eof()
        );
    }
}
