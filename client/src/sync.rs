//! Blocking client: std threads and channels, no async runtime.
//!
//! ```no_run
//! use mydaemon_client::sync::Client;
//!
//! let (client, events) = Client::connect()?;
//! client.subscribe()?;
//! std::thread::spawn(move || {
//!     for ev in events.iter() {
//!         println!("event: {ev:?}");
//!     }
//! });
//! client.set("answer", b"42")?;
//! # Ok::<(), mydaemon_client::Error>(())
//! ```

use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::mpsc::{
    self, Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError, TrySendError,
};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use log::{debug, warn};
use mydaemon_proto::sync::{read_msg, write_msg};
use mydaemon_proto::{ClientMsg, ServerMsg};

use crate::shared::{Pending, expect, lock};
use crate::{Command, Error, Event, PROTO_VERSION, Response, Result, check_welcome};

/// Connection to the daemon. `Send + Sync`: share it with `Arc` to issue
/// commands from several threads at once.
pub struct Client {
    writer: Mutex<UnixStream>,
    pending: Arc<Pending<Sender<Response>>>,
    timeout: Option<Duration>,
}

/// Events pushed by the daemon. Every method returns
/// [`Error::Disconnected`] once the connection is gone and the queue empty.
pub struct Events {
    rx: Receiver<Event>,
}

impl Client {
    /// Connect to [`crate::socket_path`] (`$MYDAEMON_SOCKET` or the default).
    pub fn connect() -> Result<(Client, Events)> {
        Self::connect_to(crate::socket_path())
    }

    pub fn connect_to(path: impl AsRef<Path>) -> Result<(Client, Events)> {
        let path = path.as_ref();
        let mut stream = UnixStream::connect(path)?;
        debug!("connected to {}", path.display());

        // Handshake on the bare stream, before the reader thread exists.
        write_msg(
            &mut stream,
            &ClientMsg::Hello {
                version: PROTO_VERSION,
            },
        )?;
        check_welcome(read_msg(&mut stream)?)?;
        debug!("handshake ok, protocol v{PROTO_VERSION}");

        let reader = stream.try_clone()?;
        let pending = Arc::new(Pending::new());
        let (ev_tx, ev_rx) = mpsc::sync_channel(crate::EVENT_QUEUE);
        let p = pending.clone();
        thread::Builder::new()
            .name("mydaemon-client-rx".into())
            .spawn(move || reader_loop(reader, &p, &ev_tx))?;

        let client = Client {
            writer: Mutex::new(stream),
            pending,
            timeout: None,
        };
        Ok((client, Events { rx: ev_rx }))
    }

    /// Fail calls with [`Error::Timeout`] if the daemon does not answer in
    /// time. `None` (the default) waits forever.
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    /// Send a command and wait for its response.
    pub fn call(&self, cmd: Command) -> Result<Response> {
        let (tx, rx) = mpsc::channel();
        let ticket = self.pending.register(tx)?;
        write_msg(
            &mut *lock(&self.writer),
            &ClientMsg::Command { id: ticket.id, cmd },
        )?;
        match self.timeout {
            None => rx.recv().map_err(|_| Error::Disconnected),
            Some(t) => rx.recv_timeout(t).map_err(|e| match e {
                RecvTimeoutError::Timeout => Error::Timeout,
                RecvTimeoutError::Disconnected => Error::Disconnected,
            }),
        }
        // `ticket` dropped here: no-op on success, cleanup on timeout.
    }

    pub fn ping(&self) -> Result<()> {
        expect::pong(self.call(Command::Ping)?)
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        expect::value(self.call(Command::Get { key: key.into() })?)
    }

    pub fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        expect::done(self.call(Command::Set {
            key: key.into(),
            value: value.into(),
        })?)
    }

    pub fn subscribe(&self) -> Result<()> {
        expect::done(self.call(Command::Subscribe)?)
    }

    pub fn unsubscribe(&self) -> Result<()> {
        expect::done(self.call(Command::Unsubscribe)?)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Wakes the reader thread (read returns EOF) so it exits.
        let _ = lock(&self.writer).shutdown(Shutdown::Both);
    }
}

impl Events {
    /// Wait for the next event.
    pub fn recv(&self) -> Result<Event> {
        self.rx.recv().map_err(|_| Error::Disconnected)
    }

    /// Next event if one is queued, without blocking.
    pub fn try_recv(&self) -> Result<Option<Event>> {
        match self.rx.try_recv() {
            Ok(ev) => Ok(Some(ev)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(Error::Disconnected),
        }
    }

    /// Wait at most `timeout` for the next event.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Option<Event>> {
        match self.rx.recv_timeout(timeout) {
            Ok(ev) => Ok(Some(ev)),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => Err(Error::Disconnected),
        }
    }

    /// Iterate over events until the connection is closed.
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.rx.iter()
    }
}

fn reader_loop(
    mut stream: UnixStream,
    pending: &Pending<Sender<Response>>,
    events: &SyncSender<Event>,
) {
    loop {
        match read_msg::<_, ServerMsg>(&mut stream) {
            Ok(ServerMsg::Response { id, resp }) => match pending.take(id) {
                Some(waiter) => {
                    let _ = waiter.send(resp);
                }
                None => debug!("response #{id} has no waiter (timed out?)"),
            },
            Ok(ServerMsg::Event(ev)) => match events.try_send(ev) {
                Ok(()) | Err(TrySendError::Disconnected(_)) => {}
                Err(TrySendError::Full(ev)) => warn!("events queue full, dropping {ev:?}"),
            },
            Ok(other) => warn!("unexpected message from daemon: {other:?}"),
            Err(e) if e.is_recoverable() => warn!("skipping undecodable frame: {e}"),
            Err(e) if e.is_eof() => {
                debug!("daemon closed the connection");
                break;
            }
            Err(e) => {
                warn!("connection error: {e}");
                break;
            }
        }
    }
    pending.close();
    let _ = stream.shutdown(Shutdown::Both);
}
