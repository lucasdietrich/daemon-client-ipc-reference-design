//! Client library for mydaemond.
//!
//! Two flavors, selected with Cargo features (both can be enabled at once):
//!
//! - `blocking` (default): [`blocking::Client`], plain std threads.
//! - `tokio`: [`nonblocking::Client`], for code running in a tokio runtime.
//!
//! # Commands and events
//!
//! The daemon sends two kinds of messages on the same socket:
//!
//! - responses, each one answering a command sent by this client,
//! - events, pushed at any time, never answered.
//!
//! Both flavors handle this the same way: `connect` performs the handshake,
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
//! The events queue is bounded ([`EVENT_QUEUE`]). If the application does not
//! drain it, further events are dropped with a warning; responses are never
//! affected.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use mydaemon_proto::ServerMsg;
pub use mydaemon_proto::{
    Command, DEFAULT_SOCKET, Event, PROTO_VERSION, Response, SOCKET_ENV, socket_path,
};

#[cfg(feature = "blocking")]
pub mod blocking;
#[cfg(feature = "tokio")]
pub mod nonblocking;

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
// Code shared by both flavors
// ---------------------------------------------------------------------------

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn check_welcome(msg: ServerMsg) -> Result<()> {
    match msg {
        ServerMsg::Welcome { version } if version == PROTO_VERSION => Ok(()),
        ServerMsg::Rejected { reason } => Err(Error::Rejected(reason)),
        _ => Err(Error::Handshake),
    }
}

/// Map a raw [`Response`] to the result type of each typed helper.
mod expect {
    use super::*;

    pub fn pong(r: Response) -> Result<()> {
        match r {
            Response::Pong => Ok(()),
            other => fail(other),
        }
    }

    pub fn done(r: Response) -> Result<()> {
        match r {
            Response::Done => Ok(()),
            other => fail(other),
        }
    }

    pub fn value(r: Response) -> Result<Option<Vec<u8>>> {
        match r {
            Response::Value(v) => Ok(v),
            other => fail(other),
        }
    }

    fn fail<T>(r: Response) -> Result<T> {
        match r {
            Response::Error(e) => Err(Error::Daemon(e)),
            other => Err(Error::Unexpected(other)),
        }
    }
}

/// Requests waiting for their response, keyed by request id.
///
/// `T` is the one-shot sender used to wake the caller: a std channel for the
/// blocking client, a tokio oneshot for the async one.
struct Pending<T> {
    inner: Mutex<PendingInner<T>>,
}

struct PendingInner<T> {
    waiters: HashMap<u32, T>,
    next_id: u32,
    /// Set by the reader when the connection is gone, so new calls fail fast
    /// instead of waiting forever.
    closed: bool,
}

impl<T> Pending<T> {
    fn new() -> Self {
        Pending {
            inner: Mutex::new(PendingInner {
                waiters: HashMap::new(),
                next_id: 0,
                closed: false,
            }),
        }
    }

    /// Allocate an id and park `waiter` under it. The returned ticket
    /// removes the entry when dropped (timeout, error, cancelled future).
    fn register(&self, waiter: T) -> Result<Ticket<'_, T>> {
        let mut g = lock(&self.inner);
        if g.closed {
            return Err(Error::Disconnected);
        }
        let mut id = g.next_id;
        while g.waiters.contains_key(&id) {
            id = id.wrapping_add(1); // only after u32 wrap-around
        }
        g.next_id = id.wrapping_add(1);
        g.waiters.insert(id, waiter);
        Ok(Ticket { pending: self, id })
    }

    /// Called by the reader when the response for `id` arrives.
    fn take(&self, id: u32) -> Option<T> {
        lock(&self.inner).waiters.remove(&id)
    }

    /// Called by the reader on disconnect. Dropping the senders wakes every
    /// waiting caller with `Error::Disconnected`.
    fn close(&self) {
        let mut g = lock(&self.inner);
        g.closed = true;
        g.waiters.clear();
    }
}

struct Ticket<'a, T> {
    pending: &'a Pending<T>,
    id: u32,
}

impl<T> Drop for Ticket<'_, T> {
    fn drop(&mut self) {
        self.pending.take(self.id);
    }
}
