//! Non-blocking client for callers that own a [`mio::Poll`] loop.
//!
//! No thread and no channel: the caller registers the client as a
//! [`mio::event::Source`] and feeds it readiness events. Commands are
//! queued with [`Client::send`]; their responses come back from
//! [`Client::recv`] with the same id, interleaved with events.
//!
//! ```no_run
//! use mio::{Events, Interest, Poll, Token};
//! use mydaemon_client::mio_client::{Client, Incoming};
//!
//! let mut poll = Poll::new()?;
//! let mut client = Client::connect()?;
//! poll.registry()
//!     .register(&mut client, Token(0), Interest::READABLE | Interest::WRITABLE)?;
//! client.subscribe()?;
//!
//! let mut events = Events::with_capacity(16);
//! loop {
//!     poll.poll(&mut events, None)?;
//!     for ev in events.iter() {
//!         if ev.is_writable() {
//!             client.flush()?;
//!         }
//!         if ev.is_readable() {
//!             // Edge-triggered: drain until `None`.
//!             while let Some(msg) = client.recv()? {
//!                 println!("{msg:?}");
//!             }
//!         }
//!     }
//! }
//! # Ok::<(), mydaemon_client::Error>(())
//! ```

use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::Path;

use log::{debug, warn};
use mio::event::Source;
use mio::net::UnixStream;
use mio::{Interest, Registry, Token};
use mydaemon_proto::sync::{read_msg, write_msg};
use mydaemon_proto::{ClientMsg, ServerMsg, decode_frame, encode, frame_len};

use crate::{Command, Error, Event, PROTO_VERSION, Response, Result, check_welcome};

/// A message from the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Incoming {
    /// Answer to the command sent with the same `id`.
    Response {
        id: u32,
        resp: Response,
    },
    Event(Event),
}

pub struct Client {
    stream: UnixStream,
    rx_buf: Vec<u8>,
    tx_buf: Vec<u8>,
    /// Bytes of `tx_buf` already written.
    tx_sent: usize,
    next_id: u32,
}

impl Client {
    /// Connect to [`crate::socket_path`] (`$MYDAEMON_SOCKET` or the default).
    pub fn connect() -> Result<Client> {
        Self::connect_to(crate::socket_path())
    }

    /// Connect and do the handshake (blocking), then switch to non-blocking.
    pub fn connect_to(path: impl AsRef<Path>) -> Result<Client> {
        let path = path.as_ref();
        let mut stream = StdUnixStream::connect(path)?;
        debug!("connected to {}", path.display());

        write_msg(
            &mut stream,
            &ClientMsg::Hello {
                version: PROTO_VERSION,
            },
        )?;
        check_welcome(read_msg(&mut stream)?)?;
        debug!("handshake ok, protocol v{PROTO_VERSION}");

        stream.set_nonblocking(true)?;
        Ok(Client {
            stream: UnixStream::from_std(stream),
            rx_buf: Vec::new(),
            tx_buf: Vec::new(),
            tx_sent: 0,
            next_id: 0,
        })
    }

    /// Queue a command and try to write it. Returns the id that the matching
    /// [`Incoming::Response`] will carry.
    pub fn send(&mut self, cmd: Command) -> Result<u32> {
        let id = self.next_id;
        self.next_id = id.wrapping_add(1);
        encode(&ClientMsg::Command { id, cmd }, &mut self.tx_buf)?;
        self.flush()?;
        Ok(id)
    }

    pub fn ping(&mut self) -> Result<u32> {
        self.send(Command::Ping)
    }

    pub fn get(&mut self, key: &str) -> Result<u32> {
        self.send(Command::Get { key: key.into() })
    }

    pub fn set(&mut self, key: &str, value: &[u8]) -> Result<u32> {
        self.send(Command::Set {
            key: key.into(),
            value: value.into(),
        })
    }

    pub fn subscribe(&mut self) -> Result<u32> {
        self.send(Command::Subscribe)
    }

    pub fn unsubscribe(&mut self) -> Result<u32> {
        self.send(Command::Unsubscribe)
    }

    /// True while queued bytes are waiting for the socket to become writable.
    pub fn wants_write(&self) -> bool {
        self.tx_sent < self.tx_buf.len()
    }

    /// Write queued bytes until done or the socket would block. Call it when
    /// the token is writable.
    pub fn flush(&mut self) -> Result<()> {
        while self.wants_write() {
            match self.stream.write(&self.tx_buf[self.tx_sent..]) {
                Ok(0) => return Err(Error::Disconnected),
                Ok(n) => self.tx_sent += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(io_error(e)),
            }
        }
        self.tx_buf.clear();
        self.tx_sent = 0;
        Ok(())
    }

    /// Next message from the daemon, or `None` when no complete frame is
    /// available. Call it in a loop until `None` when the token is readable.
    ///
    /// Returns [`Error::Disconnected`] once the daemon is gone and every
    /// buffered frame has been returned.
    pub fn recv(&mut self) -> Result<Option<Incoming>> {
        loop {
            if let Some(n) = frame_len(&self.rx_buf)?
                && self.rx_buf.len() >= n
            {
                let msg = decode_frame::<ServerMsg>(&self.rx_buf[..n]);
                self.rx_buf.drain(..n);
                match msg {
                    Ok(ServerMsg::Response { id, resp }) => {
                        return Ok(Some(Incoming::Response { id, resp }));
                    }
                    Ok(ServerMsg::Event(ev)) => return Ok(Some(Incoming::Event(ev))),
                    Ok(other) => warn!("unexpected message from daemon: {other:?}"),
                    Err(e) if e.is_recoverable() => {
                        warn!("skipping undecodable frame: {e}");
                    }
                    Err(e) => return Err(e.into()),
                }
                continue;
            }

            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk) {
                Ok(0) => {
                    debug!("daemon closed the connection");
                    return Err(Error::Disconnected);
                }
                Ok(n) => self.rx_buf.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(io_error(e)),
            }
        }
    }
}

fn io_error(e: io::Error) -> Error {
    match e.kind() {
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset => Error::Disconnected,
        _ => Error::Io(e),
    }
}

impl AsRawFd for Client {
    /// Socket descriptor, for callers that poll with something other than mio.
    fn as_raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }
}

impl Source for Client {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.stream.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.stream.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        self.stream.deregister(registry)
    }
}
