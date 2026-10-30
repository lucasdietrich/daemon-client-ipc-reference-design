//! Client library for mydaemond.
//!
//! Flavors, selected with Cargo features (all three are on by default, and
//! any combination can be enabled):
//!
//! - `sync` (default): [`sync::Client`], plain std threads. `Send + Sync`.
//! - `tokio` (default): [`r#async::Client`], for code running in a tokio
//!   runtime. `Send + Sync`.
//! - `mio` (default): [`mio_client::Client`], for callers that own a mio
//!   event loop; no thread, the caller drives the I/O.
//!
//! # Commands and events
//!
//! The daemon sends two kinds of messages on the same socket:
//!
//! - responses, each one answering a command sent by this client,
//! - events, pushed at any time, never answered.
//!
//! The `sync` and `tokio` flavors handle this the same way: `connect`
//! performs the handshake,
//! then starts a background reader (a thread, or a tokio task) that owns the
//! read half of the socket and routes every incoming frame:
//!
//! ```text
//!                       +--> Response { id } --> caller blocked in call(id)
//!  socket --> reader ---+
//!                       +--> Event ------------> Events queue (returned by connect)
//! ```
//!
//! As a consequence a `Client` can be shared (`Arc<Client>`) and used from
//! several threads or tasks at once, and events keep flowing while commands
//! are in flight. Events are only sent after [`Command::Subscribe`].
//!
//! The `mio` flavor has no reader: the caller's poll loop reads frames with
//! `recv` and gets responses and events as one stream.
//!
//! The events queue is bounded ([`EVENT_QUEUE`]). If the application does not
//! drain it, further events are dropped with a warning; responses are never
//! affected.

#[cfg(any(feature = "sync", feature = "tokio", feature = "mio"))]
use mydaemon_proto::ServerMsg;
pub use mydaemon_proto::{
    Command, DEFAULT_SOCKET, Event, PROTO_VERSION, Response, SOCKET_ENV, socket_path,
};

#[cfg(feature = "tokio")]
pub mod r#async;
#[cfg(feature = "mio")]
pub mod mio_client;
#[cfg(any(feature = "sync", feature = "tokio"))]
mod shared;
#[cfg(feature = "sync")]
pub mod sync;

/// Capacity of the events queue returned by `connect`.
pub const EVENT_QUEUE: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Proto(#[from] mydaemon_proto::Error),

    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    #[error("daemon rejected the connection: {0}")]
    Rejected(String),

    #[error("unexpected handshake reply from daemon")]
    Handshake,

    #[error("connection to daemon lost")]
    Disconnected,

    #[error("no response from daemon within the timeout")]
    Timeout,

    #[error("daemon returned an error: {0}")]
    Daemon(String),

    #[error("unexpected response from daemon: {0:?}")]
    Unexpected(Response),
}

pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// Code shared by all flavors
// ---------------------------------------------------------------------------

#[cfg(any(feature = "sync", feature = "tokio", feature = "mio"))]
fn check_welcome(msg: ServerMsg) -> Result<()> {
    match msg {
        ServerMsg::Welcome { version } if version == PROTO_VERSION => Ok(()),
        ServerMsg::Rejected { reason } => Err(Error::Rejected(reason)),
        _ => Err(Error::Handshake),
    }
}
