//! mydaemond: a single-threaded daemon built on mio.
//!
//! A [`Server`] owns the listener, every client connection and the
//! key/value store, so no state is shared and nothing is locked. It does not
//! own the event loop: the caller creates the [`mio::Poll`], gives the server
//! a [`TokenRange`] to register under, and forwards the events whose token
//! belongs to it. Several servers (or other mio components) can therefore
//! share one loop:
//!
//! ```text
//!   Poll --> events --> server.handle_event(ev)   // accept / read + run commands / flush
//!             |   `--> other_server.handle_event(ev)
//!             `-----> server.tick(now)            // heartbeat timer
//! ```
//!
//! The loop itself lives in the caller: see `main.rs` (one server) and
//! `examples/two_servers.rs` (two servers, one loop). Since the caller owns
//! the servers, it pushes events with [`Server::broadcast`] directly.
//!
//! Each connection has an inbound and an outbound byte buffer. Sockets are
//! edge-triggered, so every readiness event drains the socket until
//! `WouldBlock`.
//!
//! Flow control: events are dropped for a client whose outbound buffer is
//! over [`Config::outbound_limit`]. Responses are never dropped; instead the
//! daemon stops reading commands from such a client until it catches up.
//!
//! At most [`Config::max_clients`] connections are served at once; extra
//! ones get `Rejected` and are closed.

mod token_range;

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use mio::event::Event as MioEvent;
use mio::net::{UnixListener, UnixStream};
use mio::{Interest, Registry, Token};
use mydaemon_proto::{
    ClientMsg, Command, Event, PROTO_VERSION, Response, ServerMsg, decode_frame, encode, frame_len,
};

pub use token_range::TokenRange;

/// Default for [`Config::max_clients`].
pub const DEFAULT_MAX_CLIENTS: usize = 16;

/// Positions inside the server's [`TokenRange`]; client slots follow.
const LISTENER_SLOT: usize = 0;
const FIRST_CONN_SLOT: usize = 1;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error(transparent)]
    Proto(#[from] mydaemon_proto::Error),

    #[error("another daemon is already listening on {0}")]
    AlreadyRunning(PathBuf),

    #[error("{0} exists and is not a socket")]
    NotASocket(PathBuf),

    #[error("protocol violation: {0}")]
    Protocol(&'static str),

    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),

    #[error("token range too small: the server needs {needed}, {available} available")]
    TokenRangeTooSmall { needed: usize, available: usize },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Config {
    /// Where to create the listening socket.
    pub socket: PathBuf,
    /// Permission bits applied to the socket file.
    pub mode: u32,
    /// Maximum number of simultaneous clients (at least 1).
    pub max_clients: usize,
    /// Per-connection outbound buffer size (bytes) above which events are
    /// dropped and commands are no longer read.
    pub outbound_limit: usize,
    /// Broadcast `Event::Heartbeat` at this interval, if set.
    pub heartbeat: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            socket: mydaemon_proto::socket_path(),
            mode: 0o660,
            max_clients: DEFAULT_MAX_CLIENTS,
            outbound_limit: 64 * 1024,
            heartbeat: None,
        }
    }
}

pub struct Server {
    registry: Registry,
    tokens: TokenRange,
    listener: UnixListener,
    cfg: Config,
    /// Connections by slot; the slot picks the token inside `tokens`.
    conns: HashMap<usize, Conn>,
    store: HashMap<String, Vec<u8>>,
    next_id: u64,
    started: Instant,
    next_heartbeat: Option<Instant>,
}

impl Server {
    /// Number of tokens a server with this config uses: the listener and
    /// one per client slot.
    pub fn required_tokens(cfg: &Config) -> usize {
        FIRST_CONN_SLOT + cfg.max_clients
    }

    /// Bind the socket and register it on `registry`. The tokens the server
    /// needs are taken from the front of `free`.
    pub fn bind(cfg: Config, registry: &Registry, free: &mut TokenRange) -> Result<Self> {
        if cfg.max_clients == 0 {
            return Err(Error::InvalidConfig("max_clients must be at least 1"));
        }
        let needed = Self::required_tokens(&cfg);
        let available = free.len();
        let tokens = free
            .take(needed)
            .ok_or(Error::TokenRangeTooSmall { needed, available })?;

        let registry = registry.try_clone()?;

        prepare_socket_path(&cfg.socket)?;
        let listener = UnixListener::bind(&cfg.socket)?;
        let started = Instant::now();
        // From here on `Drop` removes the socket file.
        let mut server = Server {
            registry,
            tokens,
            listener,
            next_heartbeat: cfg.heartbeat.map(|period| started + period),
            cfg,
            conns: HashMap::new(),
            store: HashMap::new(),
            next_id: 0,
            started,
        };
        fs::set_permissions(
            &server.cfg.socket,
            fs::Permissions::from_mode(server.cfg.mode),
        )?;
        let listener_token = server.tokens.get(LISTENER_SLOT).expect("in range");
        server
            .registry
            .register(&mut server.listener, listener_token, Interest::READABLE)?;
        info!(
            "listening on {} (mode {:o}, protocol v{PROTO_VERSION}, max {} client(s), tokens {}..{})",
            server.cfg.socket.display(),
            server.cfg.mode,
            server.cfg.max_clients,
            server.tokens.start(),
            server.tokens.end()
        );
        Ok(server)
    }

    /// The tokens this server registers under.
    pub fn tokens(&self) -> TokenRange {
        self.tokens
    }

    /// Handle a readiness event from the caller's poll loop. Returns false,
    /// doing nothing, if the event's token is not in [`Server::tokens`].
    pub fn handle_event(&mut self, event: &MioEvent) -> bool {
        let Some(slot) = self.tokens.index_of(event.token()) else {
            return false;
        };
        match slot {
            LISTENER_SLOT => self.accept(),
            _ => self.service(slot - FIRST_CONN_SLOT),
        }
        true
    }

    /// When [`Server::tick`] next has work to do, for the caller's poll
    /// timeout.
    pub fn deadline(&self) -> Option<Instant> {
        self.next_heartbeat
    }

    /// Run the heartbeat timer. Call it after every poll wake-up.
    pub fn tick(&mut self, now: Instant) {
        let (Some(period), Some(next)) = (self.cfg.heartbeat, self.next_heartbeat) else {
            return;
        };
        if now >= next {
            self.next_heartbeat = Some(now + period);
            self.broadcast(Event::Heartbeat {
                uptime_secs: self.started.elapsed().as_secs(),
            });
        }
    }

    fn token_of(&self, slot: usize) -> Token {
        self.tokens
            .get(FIRST_CONN_SLOT + slot)
            .expect("slot is below max_clients")
    }

    fn accept(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => self.admit(stream),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => {
                    warn!("accept failed: {e}");
                    return;
                }
            }
        }
    }

    fn admit(&mut self, mut stream: UnixStream) {
        let id = self.next_id;
        self.next_id += 1;

        let Some(slot) = (0..self.cfg.max_clients).find(|s| !self.conns.contains_key(s)) else {
            info!(
                "conn {id}: rejected, {} client(s) already connected",
                self.conns.len()
            );
            let reason = format!("too many clients (max {})", self.cfg.max_clients);
            let mut buf = Vec::new();
            // Best effort: a fresh socket always has room for one small frame.
            if encode(&ServerMsg::Rejected { reason }, &mut buf).is_ok() {
                let _ = stream.write(&buf);
            }
            return;
        };

        info!("conn {id}: accepted");
        if let Err(e) = self.registry.register(
            &mut stream,
            self.token_of(slot),
            Interest::READABLE | Interest::WRITABLE,
        ) {
            warn!("conn {id}: cannot register: {e}");
            return;
        }
        self.conns.insert(slot, Conn::new(id, stream));
    }

    /// Run one connection until it would block, then deliver the events it
    /// produced. The connection is taken out of the map while it runs so
    /// that it can borrow the store.
    fn service(&mut self, slot: usize) {
        let Some(mut conn) = self.conns.remove(&slot) else {
            return;
        };
        let mut events = Vec::new();
        match conn.service(&mut self.store, &mut events, self.cfg.outbound_limit) {
            Ok(Outcome::Open) => {
                self.conns.insert(slot, conn);
            }
            Ok(Outcome::Closed) => {}
            Err(e) => log_conn_error(conn.id, &e),
        }
        for event in events {
            self.broadcast(event);
        }
    }

    /// Send `event` to every subscribed connection. Connections that fail
    /// are dropped.
    pub fn broadcast(&mut self, event: Event) {
        let limit = self.cfg.outbound_limit;
        let mut subscribers = 0;
        self.conns.retain(|_, conn| {
            if !conn.subscribed {
                return true;
            }
            subscribers += 1;
            let sent = conn
                .push_event(&event, limit)
                .and_then(|()| conn.flush().map_err(Error::from));
            if let Err(e) = &sent {
                log_conn_error(conn.id, e);
            }
            sent.is_ok()
        });
        debug!("broadcast {event:?} to {subscribers} subscriber(s)");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.cfg.socket);
    }
}

// ---------------------------------------------------------------------------
// Connection state
// ---------------------------------------------------------------------------

enum Outcome {
    Open,
    Closed,
}

enum Step {
    Msg(ClientMsg),
    /// Nothing more to read right now.
    Idle,
    Eof,
}

struct Conn {
    id: u64,
    stream: UnixStream,
    rx: Vec<u8>,
    tx: Vec<u8>,
    /// Handshake done.
    ready: bool,
    /// Close once `tx` is flushed (after a `Rejected`).
    closing: bool,
    subscribed: bool,
}

impl Conn {
    fn new(id: u64, stream: UnixStream) -> Conn {
        Conn {
            id,
            stream,
            rx: Vec::new(),
            tx: Vec::new(),
            ready: false,
            closing: false,
            subscribed: false,
        }
    }

    /// Process everything readable, run the commands, flush the answers.
    /// Events to broadcast (e.g. after a `Set`) are appended to `events`.
    fn service(
        &mut self,
        store: &mut HashMap<String, Vec<u8>>,
        events: &mut Vec<Event>,
        limit: usize,
    ) -> Result<Outcome> {
        while !self.closing {
            // Backpressure: stop reading until the peer drains its socket.
            // If the flush leaves the buffer over the limit it hit
            // WouldBlock, so a writable event will call us again.
            if self.tx.len() > limit {
                self.flush()?;
                if self.tx.len() > limit {
                    return Ok(Outcome::Open);
                }
            }
            match self.next_step()? {
                Step::Msg(msg) => self.handle(msg, store, events)?,
                Step::Idle => break,
                Step::Eof => {
                    if !self.ready {
                        debug!("conn {}: closed before handshake", self.id);
                    }
                    return Ok(Outcome::Closed);
                }
            }
        }
        self.flush()?;
        if self.closing && self.tx.is_empty() {
            Ok(Outcome::Closed)
        } else {
            Ok(Outcome::Open)
        }
    }

    fn next_step(&mut self) -> Result<Step> {
        loop {
            if let Some(n) = frame_len(&self.rx)?
                && self.rx.len() >= n
            {
                let msg = decode_frame::<ClientMsg>(&self.rx[..n]);
                self.rx.drain(..n);
                return Ok(Step::Msg(msg?));
            }
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk) {
                Ok(0) => return Ok(Step::Eof),
                Ok(n) => self.rx.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(Step::Idle),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn handle(
        &mut self,
        msg: ClientMsg,
        store: &mut HashMap<String, Vec<u8>>,
        events: &mut Vec<Event>,
    ) -> Result<()> {
        match (self.ready, msg) {
            (false, ClientMsg::Hello { version }) if version == PROTO_VERSION => {
                self.queue(&ServerMsg::Welcome {
                    version: PROTO_VERSION,
                })?;
                self.ready = true;
            }
            (false, ClientMsg::Hello { version }) => {
                info!("conn {}: rejected, client speaks v{version}", self.id);
                let reason =
                    format!("protocol v{version} unsupported, daemon speaks v{PROTO_VERSION}");
                self.queue(&ServerMsg::Rejected { reason })?;
                self.closing = true;
            }
            (false, ClientMsg::Command { .. }) => return Err(Error::Protocol("expected Hello")),
            (true, ClientMsg::Hello { .. }) => return Err(Error::Protocol("Hello sent twice")),
            (true, ClientMsg::Command { id, cmd }) => {
                debug!("conn {}: #{id} {cmd:?}", self.id);
                let resp = self.run_command(cmd, store, events);
                self.queue(&ServerMsg::Response { id, resp })?;
            }
        }
        Ok(())
    }

    fn run_command(
        &mut self,
        cmd: Command,
        store: &mut HashMap<String, Vec<u8>>,
        events: &mut Vec<Event>,
    ) -> Response {
        match cmd {
            Command::Ping => Response::Pong,
            Command::Get { key } => Response::Value(store.get(&key).cloned()),
            Command::Set { key, value } => {
                if key.is_empty() {
                    return Response::Error("empty key".into());
                }
                store.insert(key.clone(), value);
                events.push(Event::KeyChanged { key });
                Response::Done
            }
            Command::Subscribe => {
                self.subscribed = true;
                Response::Done
            }
            Command::Unsubscribe => {
                self.subscribed = false;
                Response::Done
            }
        }
    }

    fn queue(&mut self, msg: &ServerMsg) -> Result<()> {
        encode(msg, &mut self.tx)?;
        Ok(())
    }

    /// Events are best effort: dropped while the peer is too far behind.
    fn push_event(&mut self, event: &Event, limit: usize) -> Result<()> {
        if self.tx.len() > limit {
            warn!("conn {}: outbound buffer full, dropping event", self.id);
            return Ok(());
        }
        self.queue(&ServerMsg::Event(event.clone()))
    }

    /// Write until `tx` is empty or the socket would block.
    fn flush(&mut self) -> io::Result<()> {
        let mut sent = 0;
        let result = loop {
            if sent == self.tx.len() {
                break Ok(());
            }
            match self.stream.write(&self.tx[sent..]) {
                Ok(0) => break Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => sent += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => break Err(e),
            }
        };
        self.tx.drain(..sent);
        result
    }
}

/// Dropping the stream closes the socket, and the poller forgets it.
impl Drop for Conn {
    fn drop(&mut self) {
        info!("conn {}: closed", self.id);
    }
}

fn log_conn_error(id: u64, e: &Error) {
    match e {
        Error::Io(e)
            if matches!(
                e.kind(),
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
            ) =>
        {
            debug!("conn {id}: peer went away: {e}");
        }
        _ => warn!("conn {id}: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Socket path handling
// ---------------------------------------------------------------------------

/// Create the parent directory and remove a stale socket left by a previous
/// run, but refuse to steal the socket of a daemon that is still alive.
fn prepare_socket_path(path: &Path) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if !meta.file_type().is_socket() {
                return Err(Error::NotASocket(path.into()));
            }
            if StdUnixStream::connect(path).is_ok() {
                return Err(Error::AlreadyRunning(path.into()));
            }
            debug!("removing stale socket {}", path.display());
            fs::remove_file(path)?;
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
