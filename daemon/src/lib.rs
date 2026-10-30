//! mydaemond: a fully synchronous daemon.
//!
//! Threading model, per accepted connection:
//!
//! ```text
//!                 +-------------------+   responses   +-----------------+
//!  socket rd ---> | reader thread     | ------------> |                 |
//!                 | (runs commands)   |               | bounded queue   | ---> writer thread ---> socket wr
//!                 +-------------------+               | (SyncSender)    |
//!  Handle::broadcast(event) ------------------------> |                 |
//!                                                     +-----------------+
//! ```
//!
//! Only the writer thread writes to the socket, so responses and events can
//! never interleave inside a frame. Events are delivered with `try_send`: a
//! client that does not drain its socket loses events instead of stalling
//! the daemon.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;

use log::{debug, info, warn};
use mydaemon_proto::sync::{read_msg, write_msg};
use mydaemon_proto::{ClientMsg, Command, Event, PROTO_VERSION, Response, ServerMsg};

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
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Config {
    /// Where to create the listening socket.
    pub socket: PathBuf,
    /// Permission bits applied to the socket file.
    pub mode: u32,
    /// Per-connection outbound queue length (responses + events).
    pub outbound_queue: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            socket: mydaemon_proto::socket_path(),
            mode: 0o660,
            outbound_queue: 256,
        }
    }
}

/// Cheap, cloneable handle used to push events from anywhere in the daemon.
#[derive(Clone)]
pub struct Handle(Arc<Shared>);

impl Handle {
    /// Send `event` to every subscribed connection.
    pub fn broadcast(&self, event: Event) {
        self.0.broadcast(event);
    }
}

pub struct Server {
    listener: UnixListener,
    shared: Arc<Shared>,
    cfg: Config,
}

impl Server {
    pub fn bind(cfg: Config) -> Result<Self> {
        prepare_socket_path(&cfg.socket)?;
        let listener = UnixListener::bind(&cfg.socket)?;
        fs::set_permissions(&cfg.socket, fs::Permissions::from_mode(cfg.mode))?;
        info!(
            "listening on {} (mode {:o}, protocol v{PROTO_VERSION})",
            cfg.socket.display(),
            cfg.mode
        );
        Ok(Server {
            listener,
            shared: Arc::new(Shared::default()),
            cfg,
        })
    }

    pub fn handle(&self) -> Handle {
        Handle(self.shared.clone())
    }

    /// Accept connections forever, one thread pair per connection.
    pub fn run(&self) -> Result<()> {
        for conn in self.listener.incoming() {
            let stream = match conn {
                Ok(s) => s,
                Err(e) => {
                    warn!("accept failed: {e}");
                    continue;
                }
            };
            let id = self.shared.next_conn.fetch_add(1, Ordering::Relaxed);
            let shared = self.shared.clone();
            let queue = self.cfg.outbound_queue;
            thread::Builder::new()
                .name(format!("conn-{id}"))
                .spawn(move || {
                    if let Err(e) = serve_connection(id, stream, &shared, queue) {
                        warn!("conn {id}: {e}");
                    }
                })?;
        }
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.cfg.socket);
    }
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Shared {
    store: Mutex<HashMap<String, Vec<u8>>>,
    subscribers: Mutex<HashMap<u64, SyncSender<ServerMsg>>>,
    next_conn: AtomicU64,
}

impl Shared {
    fn broadcast(&self, event: Event) {
        let mut subs = lock(&self.subscribers);
        debug!("broadcast {event:?} to {} subscriber(s)", subs.len());
        subs.retain(
            |id, tx| match tx.try_send(ServerMsg::Event(event.clone())) {
                Ok(()) => true,
                Err(TrySendError::Full(_)) => {
                    warn!("conn {id}: outbound queue full, dropping event");
                    true
                }
                Err(TrySendError::Disconnected(_)) => false,
            },
        );
    }

    fn subscribe(&self, conn: u64, tx: &SyncSender<ServerMsg>) {
        lock(&self.subscribers).insert(conn, tx.clone());
    }

    fn unsubscribe(&self, conn: u64) {
        lock(&self.subscribers).remove(&conn);
    }
}

/// A panicking connection thread must not take the whole daemon down.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
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
            if UnixStream::connect(path).is_ok() {
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

// ---------------------------------------------------------------------------
// Connection handling
// ---------------------------------------------------------------------------

fn log_peer(id: u64, stream: &UnixStream) {
    #[cfg(target_os = "linux")]
    match rustix::net::sockopt::socket_peercred(stream) {
        // This is the place to enforce access control on uid/gid.
        Ok(c) => info!(
            "conn {id}: accepted pid={} uid={} gid={}",
            c.pid.as_raw_nonzero(),
            c.uid.as_raw(),
            c.gid.as_raw()
        ),
        Err(e) => warn!("conn {id}: cannot read peer credentials: {e}"),
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = stream;
        info!("conn {id}: accepted");
    }
}

fn serve_connection(id: u64, stream: UnixStream, shared: &Shared, queue: usize) -> Result<()> {
    log_peer(id, &stream);
    let mut reader = stream.try_clone()?;
    let mut writer = stream;

    // Handshake, done inline before the writer thread exists.
    let hello = match read_msg::<_, ClientMsg>(&mut reader) {
        Ok(m) => m,
        // e.g. the liveness probe of a second daemon instance
        Err(e) if e.is_eof() => {
            debug!("conn {id}: closed before handshake");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    match hello {
        ClientMsg::Hello { version } if version == PROTO_VERSION => {
            write_msg(
                &mut writer,
                &ServerMsg::Welcome {
                    version: PROTO_VERSION,
                },
            )?;
        }
        ClientMsg::Hello { version } => {
            info!("conn {id}: rejected, client speaks v{version}");
            let reason = format!("protocol v{version} unsupported, daemon speaks v{PROTO_VERSION}");
            write_msg(&mut writer, &ServerMsg::Rejected { reason })?;
            return Ok(());
        }
        ClientMsg::Command { .. } => return Err(Error::Protocol("expected Hello")),
    }

    let (tx, rx) = mpsc::sync_channel::<ServerMsg>(queue);
    let writer_thread = thread::Builder::new()
        .name(format!("conn-{id}-tx"))
        .spawn(move || writer_loop(id, writer, rx))?;

    let result = reader_loop(id, &mut reader, &tx, shared);

    // Drop every sender so the writer drains its queue and exits.
    shared.unsubscribe(id);
    drop(tx);
    let _ = writer_thread.join();
    info!("conn {id}: closed");
    result
}

fn reader_loop(
    id: u64,
    reader: &mut UnixStream,
    tx: &SyncSender<ServerMsg>,
    shared: &Shared,
) -> Result<()> {
    loop {
        let msg: ClientMsg = match read_msg(reader) {
            Ok(m) => m,
            Err(e) if e.is_eof() => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let ClientMsg::Command { id: req, cmd } = msg else {
            return Err(Error::Protocol("Hello sent twice"));
        };
        debug!("conn {id}: #{req} {cmd:?}");
        let resp = handle_command(id, cmd, tx, shared);
        // Blocking send: a slow client slows down its own commands only.
        if tx.send(ServerMsg::Response { id: req, resp }).is_err() {
            return Ok(()); // writer is gone, peer disconnected
        }
    }
}

fn writer_loop(id: u64, mut writer: UnixStream, rx: Receiver<ServerMsg>) {
    for msg in rx {
        if let Err(e) = write_msg(&mut writer, &msg) {
            debug!("conn {id}: write failed: {e}");
            // Also unblocks the reader thread.
            let _ = writer.shutdown(std::net::Shutdown::Both);
            return;
        }
    }
}

fn handle_command(
    conn: u64,
    cmd: Command,
    tx: &SyncSender<ServerMsg>,
    shared: &Shared,
) -> Response {
    match cmd {
        Command::Ping => Response::Pong,
        Command::Get { key } => Response::Value(lock(&shared.store).get(&key).cloned()),
        Command::Set { key, value } => {
            if key.is_empty() {
                return Response::Error("empty key".into());
            }
            lock(&shared.store).insert(key.clone(), value);
            shared.broadcast(Event::KeyChanged { key });
            Response::Done
        }
        Command::Subscribe => {
            shared.subscribe(conn, tx);
            Response::Done
        }
        Command::Unsubscribe => {
            shared.unsubscribe(conn);
            Response::Done
        }
    }
}
