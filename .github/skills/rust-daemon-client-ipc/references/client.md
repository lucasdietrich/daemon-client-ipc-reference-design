# Client crate (`<name>-client`)

Source of truth: `client/src/{lib,shared,sync,async,mio_client}.rs`.

## Cargo

```toml
[dependencies]
<name>-proto = { path = "../proto", default-features = false }
thiserror.workspace = true
log.workspace = true
tokio = { workspace = true, optional = true, features = ["net", "rt", "sync", "time"] }
tokio-util = { workspace = true, optional = true }
futures-util = { workspace = true, optional = true }
mio = { workspace = true, optional = true }

[features]
default = ["sync", "tokio", "mio"]     # trim to what the project needs
sync  = ["<name>-proto/sync"]
tokio = ["dep:tokio", "dep:tokio-util", "dep:futures-util", "<name>-proto/tokio"]
mio   = ["dep:mio", "<name>-proto/sync"]

[dev-dependencies]
<name>d = { path = "../daemon" }       # tests run a real daemon
mio.workspace = true
<name>-proto = { path = "../proto", features = ["sync"] }
simple_logger.workspace = true
tempfile.workspace = true
tokio = { workspace = true, features = ["macros", "rt-multi-thread", "time", "signal"] }

# one [[test]] and [[example]] per flavor, each with required-features = ["<flavor>"]
```

Module/feature names: `sync` -> `sync::Client`, `tokio` -> `r#async::Client` (file `async.rs`), `mio` -> `mio_client::Client`.

## lib.rs

- Re-export from proto: `Command`, `Event`, `Response`, `PROTO_VERSION`, `DEFAULT_SOCKET`, `SOCKET_ENV`, `socket_path`.
- `pub const EVENT_QUEUE: usize = 256;`
- `Error`: `Proto(#[from])`, `Io(#[from])`, `Rejected(String)`, `Handshake`, `Disconnected`, `Timeout`, `Daemon(String)`, `Unexpected(Response)`. `pub type Result<T>`.
- `check_welcome(ServerMsg)`: `Welcome` with matching version -> `Ok`; `Rejected` -> `Error::Rejected`; anything else -> `Error::Handshake`.
- Crate docs carry the reader diagram (socket -> reader -> Response to waiter / Event to queue).

## shared.rs (sync + tokio)

- `lock(&Mutex)` ignoring poisoning.
- `expect::{pong, done, value, ...}`: map a raw `Response` to the typed result of each helper; `Response::Error(e)` -> `Error::Daemon(e)`, other variants -> `Error::Unexpected`. Add one mapper per distinct response shape.
- `Pending<T>`: `Mutex<{ waiters: HashMap<u32, T>, next_id: u32, closed: bool }>`.
  - `register(waiter) -> Result<Ticket>`: `Err(Disconnected)` if closed; skip ids still in use after u32 wrap.
  - `take(id) -> Option<T>` (reader side), `close()` (marks closed and clears waiters, which wakes every caller with `Disconnected`).
  - `Ticket` removes its entry on `Drop` (timeout, error, cancelled future).
  - `T` is a std `mpsc::Sender<Response>` or a tokio `oneshot::Sender<Response>`.

## sync flavor

- `Client { writer: Mutex<UnixStream>, pending: Arc<Pending<Sender<Response>>>, timeout }`, `Events { rx: Receiver<Event> }` (`recv`, `try_recv`, `recv_timeout`, `iter`; all map disconnect to `Error::Disconnected`).
- `connect_to`: connect, blocking handshake on the bare stream (`write_msg Hello`, `check_welcome(read_msg)`), `try_clone` for the reader, `sync_channel(EVENT_QUEUE)`, spawn the named thread running `reader_loop`.
- `call(cmd)`: `mpsc::channel`, `pending.register(tx)`, `write_msg` under the writer lock, `recv` or `recv_timeout` (`Timeout`/`Disconnected` mapping). The ticket drop cleans up.
- `reader_loop`: `read_msg::<ServerMsg>`: `Response` -> `pending.take(id)` and send (log at debug if no waiter); `Event` -> `try_send`, warn on `Full`; other -> warn; `is_recoverable` -> warn and continue; `is_eof` -> break; other errors -> warn and break. On exit: `pending.close()` and `shutdown(Both)`.
- `Drop`: `shutdown(Both)` the writer so the reader gets EOF and exits.

## tokio flavor

- `Reader = FramedRead<OwnedReadHalf, Codec<ServerMsg, ClientMsg>>`, `Writer = FramedWrite<OwnedWriteHalf, Codec<ServerMsg, ClientMsg>>`.
- `Client { writer: tokio::sync::Mutex<Writer>, pending: Arc<Pending<oneshot::Sender<Response>>>, reader_task: JoinHandle<()>, timeout }`, `Events { rx: mpsc::Receiver<Event> }` (`recv(&mut self)`, `try_recv`).
- Handshake: `writer.send(Hello)`, `reader.next()`: `Some(Ok(Ok(msg)))` -> `check_welcome`; `Some(Ok(Err(e)) | Err(e))` -> error; `None` -> `Disconnected`. Then `tokio::spawn(reader_loop)`.
- `call` is cancel safe (see ticket) and uses `tokio::time::timeout` when a timeout is set.
- Reader: `Ok(Ok(msg))` routes, `Ok(Err(e))` (bad payload) skipped with a warning, `Err(e)` breaks; always `pending.close()` at the end.
- `Drop` aborts `reader_task`.

## mio flavor

Single struct, no thread, no mutex: `Client { stream: mio UnixStream, rx_buf, tx_buf, tx_sent, next_id }`.

- `connect_to`: std `UnixStream::connect`, blocking handshake, `set_nonblocking(true)`, `UnixStream::from_std`.
- `send(cmd) -> Result<u32>`: allocate id, `encode` into `tx_buf`, `flush`, return the id. Typed helpers return that id too.
- `wants_write()`, `flush()` (until empty or `WouldBlock`; `Ok(0)` -> `Disconnected`), `recv() -> Result<Option<Incoming>>` (drain until `None`; `Ok(0)` read -> `Disconnected` only after buffered frames were returned).
- `enum Incoming { Response { id, resp }, Event(Event) }`.
- `impl mio::event::Source` delegating to the inner stream.
- `io_error`: `BrokenPipe`/`ConnectionReset` -> `Error::Disconnected`.
- No built-in timeout: use the poll timeout.

## Behaviors every flavor must keep

- Undecodable frames are skipped, not fatal.
- Pending and future calls return `Disconnected` after the daemon goes away; `Events` drains, then returns `Disconnected`.
- `with_timeout(Some(d))` -> `Error::Timeout` (sync/tokio).
- Event queue overflow drops the event with a warning, never a response.
- `Client` is `Send + Sync` (sync/tokio) so it can be shared with `Arc`.
