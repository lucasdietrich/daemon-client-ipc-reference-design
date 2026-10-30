//! Code shared by the `sync` and `tokio` clients.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::{Error, Response, Result};

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Map a raw [`Response`] to the result type of each typed helper.
pub(crate) mod expect {
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
pub(crate) struct Pending<T> {
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
    pub(crate) fn new() -> Self {
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
    pub(crate) fn register(&self, waiter: T) -> Result<Ticket<'_, T>> {
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
    pub(crate) fn take(&self, id: u32) -> Option<T> {
        lock(&self.inner).waiters.remove(&id)
    }

    /// Called by the reader on disconnect. Dropping the senders wakes every
    /// waiting caller with `Error::Disconnected`.
    pub(crate) fn close(&self) {
        let mut g = lock(&self.inner);
        g.closed = true;
        g.waiters.clear();
    }
}

pub(crate) struct Ticket<'a, T> {
    pending: &'a Pending<T>,
    pub(crate) id: u32,
}

impl<T> Drop for Ticket<'_, T> {
    fn drop(&mut self) {
        self.pending.take(self.id);
    }
}
