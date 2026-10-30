# Proto crate (`<name>-proto`)

Source of truth: `proto/src/lib.rs` in the reference repo.

## Cargo

```toml
[dependencies]
serde.workspace = true
postcard.workspace = true
thiserror.workspace = true
tokio-util = { workspace = true, optional = true }
bytes = { workspace = true, optional = true }

[features]
default = ["sync"]
sync = []                                 # blocking helpers over std::io
tokio = ["dep:tokio-util", "dep:bytes"]   # tokio_util::codec adapter

[dev-dependencies]
hexdump = "0.1"                           # only for examples/hexdump.rs
```

## Constants and path

```rust
pub const PROTO_VERSION: u16 = 1;          // bump on incompatible message change
pub const MAX_FRAME: usize = 1 << 20;
pub const HDR_LEN: usize = 4;
pub const DEFAULT_SOCKET: &str = "/run/<name>/<name>.sock";
pub const SOCKET_ENV: &str = "<NAME>_SOCKET";

pub fn socket_path() -> PathBuf {
    std::env::var_os(SOCKET_ENV).map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SOCKET))
}
```

## Messages

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientMsg { Hello { version: u16 }, Command { id: u32, cmd: Command } }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerMsg {
    Welcome { version: u16 },
    Rejected { reason: String },          // daemon closes afterwards
    Response { id: u32, resp: Response }, // same id as the command
    Event(Event),                         // unsolicited
}

pub enum Command  { /* domain */ Ping, Subscribe, Unsubscribe }
pub enum Response { /* domain */ Pong, Done, Error(String) }

#[non_exhaustive]
pub enum Event    { /* domain; append new variants at the END only */ }
```

Keep `Ping`, `Subscribe`, `Unsubscribe`, `Response::Done`, `Response::Error(String)`; they are used by the generic daemon and client code. Each `Command` gets exactly one `Response`.

## Error

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("i/o error: {0}")]        Io(#[from] std::io::Error),
    #[error("cannot decode frame: {0}")] Codec(#[from] postcard::Error), // recoverable
    #[error("frame too large: {0} bytes (max {MAX_FRAME})")] TooLarge(usize),
}
impl Error {
    pub fn is_eof(&self) -> bool { matches!(self, Error::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof) }
    pub fn is_recoverable(&self) -> bool { matches!(self, Error::Codec(_)) }
}
```

## Sans-IO framing

```rust
pub fn encode<T: Serialize>(msg: &T, out: &mut Vec<u8>) -> Result<(), Error>; // len check, append hdr+body
pub fn parse_header(hdr: [u8; HDR_LEN]) -> Result<usize, Error>;              // rejects > MAX_FRAME
pub fn frame_len(buf: &[u8]) -> Result<Option<usize>, Error>;                 // header+payload, None if header incomplete
pub fn decode_frame<T: DeserializeOwned>(frame: &[u8]) -> Result<T, Error>;   // frame includes header
pub fn decode<T: DeserializeOwned>(buf: &[u8]) -> Result<Option<(T, usize)>, Error>;
```

Receivers loop: `frame_len` -> if `buf.len() >= n` -> `decode_frame(&buf[..n])` -> **drain `n` bytes before using the result** -> propagate the decode result.

## Adapters

- `sync::write_msg(w, &msg)`: encode into a `Vec`, then a single `write_all` (concurrent writers behind a mutex never interleave partial frames).
- `sync::read_msg(r)`: `read_exact` header, `parse_header`, `read_exact` the full body, then decode (an `Error::Codec` leaves the stream aligned).
- `codec::Codec<In, Out>(PhantomData<fn() -> (In, Out)>)` implementing `tokio_util::codec::{Decoder, Encoder}`. `Decoder::Item = Result<In, Error>`: a bad payload yields `Some(Err(Error::Codec))` and the stream continues; framing/I/O errors are fatal. In `decode`, `src.reserve(n - src.len())` while waiting, and `src.advance(n)` before returning the item.

## Unit tests to carry over

`roundtrip`, `partial_frame_needs_more` (every cut point), `two_frames_back_to_back`, `oversized_header_rejected`, `sync_bad_payload_keeps_stream_aligned` (garbage frame then valid frame then EOF).

## Evolving

- Append an `Event` variant: compatible.
- Any other change to message enums: bump `PROTO_VERSION`.
