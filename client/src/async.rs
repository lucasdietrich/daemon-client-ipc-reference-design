//! Async client for tokio. `connect` must be called from inside a tokio
//! runtime, because it spawns the reader task.
//!
//! ```no_run
//! # async fn demo() -> mydaemon_client::Result<()> {
//! use mydaemon_client::r#async::Client;
//!
//! let (client, mut events) = Client::connect().await?;
//! client.subscribe().await?;
//! tokio::spawn(async move {
//!     while let Ok(ev) = events.recv().await {
//!         println!("event: {ev:?}");
//!     }
//! });
//! client.set("answer", b"42").await?;
//! # Ok(())
//! # }
//! ```

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use mydaemon_proto::codec::Codec;
use mydaemon_proto::{ClientMsg, ServerMsg};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::shared::{Pending, expect};
use crate::{Command, Error, Event, PROTO_VERSION, Response, Result, check_welcome};

type Reader = FramedRead<OwnedReadHalf, Codec<ServerMsg, ClientMsg>>;
type Writer = FramedWrite<OwnedWriteHalf, Codec<ServerMsg, ClientMsg>>;

/// Connection to the daemon. `Send + Sync`: share it with `Arc` to issue
/// commands from several tasks at once.
pub struct Client {
    writer: AsyncMutex<Writer>,
    pending: Arc<Pending<oneshot::Sender<Response>>>,
    reader_task: JoinHandle<()>,
    timeout: Option<Duration>,
}

/// Events pushed by the daemon. Every method returns
/// [`Error::Disconnected`] once the connection is gone and the queue empty.
pub struct Events {
    rx: mpsc::Receiver<Event>,
}

impl Client {
    /// Connect to [`crate::socket_path`] (`$MYDAEMON_SOCKET` or the default).
    pub async fn connect() -> Result<(Client, Events)> {
        Self::connect_to(crate::socket_path()).await
    }

    pub async fn connect_to(path: impl AsRef<Path>) -> Result<(Client, Events)> {
        let path = path.as_ref();
        let (rd, wr) = UnixStream::connect(path).await?.into_split();
        debug!("connected to {}", path.display());
        let mut reader: Reader = FramedRead::new(rd, Codec::default());
        let mut writer: Writer = FramedWrite::new(wr, Codec::default());

        // Handshake before the reader task exists.
        writer
            .send(ClientMsg::Hello {
                version: PROTO_VERSION,
            })
            .await?;
        match reader.next().await {
            Some(Ok(Ok(msg))) => check_welcome(msg)?,
            Some(Ok(Err(e)) | Err(e)) => return Err(e.into()),
            None => return Err(Error::Disconnected),
        }
        debug!("handshake ok, protocol v{PROTO_VERSION}");

        let pending = Arc::new(Pending::new());
        let (ev_tx, ev_rx) = mpsc::channel(crate::EVENT_QUEUE);
        let reader_task = tokio::spawn(reader_loop(reader, pending.clone(), ev_tx));

        let client = Client {
            writer: AsyncMutex::new(writer),
            pending,
            reader_task,
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
    ///
    /// Cancel safe: dropping the future removes the pending entry, and
    /// `FramedWrite` keeps any partially written frame in its buffer, so the
    /// stream stays aligned.
    pub async fn call(&self, cmd: Command) -> Result<Response> {
        let (tx, rx) = oneshot::channel();
        let ticket = self.pending.register(tx)?;
        self.writer
            .lock()
            .await
            .send(ClientMsg::Command { id: ticket.id, cmd })
            .await?;
        let wait = async { rx.await.map_err(|_| Error::Disconnected) };
        match self.timeout {
            None => wait.await,
            Some(t) => tokio::time::timeout(t, wait)
                .await
                .map_err(|_| Error::Timeout)?,
        }
    }

    pub async fn ping(&self) -> Result<()> {
        expect::pong(self.call(Command::Ping).await?)
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        expect::value(self.call(Command::Get { key: key.into() }).await?)
    }

    pub async fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        expect::done(
            self.call(Command::Set {
                key: key.into(),
                value: value.into(),
            })
            .await?,
        )
    }

    pub async fn subscribe(&self) -> Result<()> {
        expect::done(self.call(Command::Subscribe).await?)
    }

    pub async fn unsubscribe(&self) -> Result<()> {
        expect::done(self.call(Command::Unsubscribe).await?)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // The write half closes on drop; also stop the reader right away.
        self.reader_task.abort();
    }
}

impl Events {
    /// Wait for the next event.
    pub async fn recv(&mut self) -> Result<Event> {
        self.rx.recv().await.ok_or(Error::Disconnected)
    }

    /// Next event if one is queued, without waiting.
    pub fn try_recv(&mut self) -> Result<Option<Event>> {
        match self.rx.try_recv() {
            Ok(ev) => Ok(Some(ev)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(Error::Disconnected),
        }
    }
}

async fn reader_loop(
    mut reader: Reader,
    pending: Arc<Pending<oneshot::Sender<Response>>>,
    events: mpsc::Sender<Event>,
) {
    while let Some(item) = reader.next().await {
        match item {
            Ok(Ok(ServerMsg::Response { id, resp })) => match pending.take(id) {
                Some(waiter) => {
                    let _ = waiter.send(resp);
                }
                None => debug!("response #{id} has no waiter (timed out or cancelled?)"),
            },
            Ok(Ok(ServerMsg::Event(ev))) => {
                if let Err(mpsc::error::TrySendError::Full(ev)) = events.try_send(ev) {
                    warn!("events queue full, dropping {ev:?}");
                }
            }
            Ok(Ok(other)) => warn!("unexpected message from daemon: {other:?}"),
            Ok(Err(e)) => warn!("skipping undecodable frame: {e}"),
            Err(e) => {
                warn!("connection error: {e}");
                break;
            }
        }
    }
    debug!("daemon connection closed");
    pending.close();
}
