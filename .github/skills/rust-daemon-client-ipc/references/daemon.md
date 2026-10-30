# Daemon crate (`<name>d`)

Source of truth: `daemon/src/lib.rs`, `daemon/src/token_range.rs`, `daemon/src/main.rs`, `daemon/examples/two_servers.rs`.

## Cargo

```toml
[dependencies]
<name>-proto = { path = "../proto", default-features = false }
mio.workspace = true            # features os-poll, net
clap.workspace = true
thiserror.workspace = true
log.workspace = true
simple_logger.workspace = true

[dev-dependencies]
<name>-proto = { path = "../proto", features = ["sync"] }
tempfile.workspace = true
```

The crate is lib + bin so that tests and other programs can embed the `Server`.

## Public API

```rust
pub const DEFAULT_MAX_CLIENTS: usize = 16;
pub struct Config { socket: PathBuf, mode: u32 /*0o660*/, max_clients: usize,
                    outbound_limit: usize /*64 KiB*/, heartbeat: Option<Duration> }
impl Default for Config { /* socket: proto::socket_path() */ }

impl Server {
    pub fn required_tokens(cfg: &Config) -> usize;          // 1 listener + max_clients
    pub fn bind(cfg, registry: &Registry, free: &mut TokenRange) -> Result<Self>;
    pub fn tokens(&self) -> TokenRange;
    pub fn handle_event(&mut self, ev: &mio::event::Event) -> bool; // false if token is not ours
    pub fn deadline(&self) -> Option<Instant>;              // for the poll timeout
    pub fn tick(&mut self, now: Instant);                   // timers (heartbeat)
    pub fn broadcast(&mut self, event: Event);              // to subscribed connections
}
```

`Error` variants: `Io`, `Proto`, `AlreadyRunning(PathBuf)`, `NotASocket(PathBuf)`, `Protocol(&'static str)`, `InvalidConfig(&'static str)`, `TokenRangeTooSmall { needed, available }`.

## TokenRange

Contiguous block of `mio::Token`s: `new(start, len)` (clamped, never includes `usize::MAX`), `start`, `end`, `len`, `is_empty`, `contains`, `get(i)`, `index_of(token)`, `take(n)` (carve from the front, `None` and unchanged if too few). Copy `token_range.rs` verbatim, including its 3 tests.

Slot layout inside the server's range: slot 0 = listener, slots `1..=max_clients` = connections. `handle_event` maps token -> slot with `index_of`.

## Server internals

- `bind`: validate config, `free.take(required_tokens)`, `registry.try_clone()`, `prepare_socket_path`, `UnixListener::bind`, build the `Server` (so `Drop` removes the socket from here on), `set_permissions(mode)`, register the listener `READABLE`.
- `accept`: loop until `WouldBlock`, ignore `Interrupted`, log other errors.
- `admit`: pick the first free slot; none free -> best-effort write `ServerMsg::Rejected` and drop the stream. Otherwise register `READABLE | WRITABLE` and insert `Conn`.
- `service(slot)`: `conns.remove(&slot)`, run `conn.service(&mut store, &mut events, limit)`, re-insert on `Outcome::Open`, log errors with `log_conn_error`, then `broadcast` the collected events.
- `broadcast`: `conns.retain`, skip non-subscribed, `push_event` + `flush`, drop connections that fail.

## Conn state machine

Fields: `id`, `stream`, `rx: Vec<u8>`, `tx: Vec<u8>`, `ready` (handshake done), `closing` (close after `tx` flushes), `subscribed`.

- `service`: while not closing: if `tx.len() > limit` flush and return `Open` if still over (a writable event resumes); `next_step()` -> `Msg` -> `handle`, `Idle` -> break, `Eof` -> `Closed`. Then `flush`; close when `closing && tx.is_empty()`.
- `next_step`: parse a frame from `rx` (drain before returning the decode result); otherwise read a 4 KiB chunk; `Ok(0)` -> `Eof`, `WouldBlock` -> `Idle`, `Interrupted` -> retry.
- `handle` matches `(ready, msg)`: `Hello` with the right version -> `Welcome`, ready; wrong version -> `Rejected` + `closing`; `Command` before `Hello` -> `Error::Protocol("expected Hello")`; `Hello` twice -> `Error::Protocol`; `Command` when ready -> `run_command` -> queue `Response { id, resp }`.
- `run_command(cmd, store, events)`: **the only domain-specific method.** Mutations push the `Event`s they cause into `events`. Validation failures return `Response::Error(String)`, never a protocol error.
- `push_event`: drop with a warning when `tx.len() > limit`.
- `flush`: write until empty or `WouldBlock`; `Ok(0)` -> `WriteZero`; always `tx.drain(..sent)` even on error.
- `Drop for Conn` logs the close; dropping the stream closes the socket and mio forgets it.
- `log_conn_error`: `BrokenPipe`/`ConnectionReset` at `debug`, everything else `warn`.

## Socket path handling

`prepare_socket_path(path)`: `create_dir_all(parent)`; `symlink_metadata`: not a socket -> `NotASocket`; `UnixStream::connect` succeeds -> `AlreadyRunning`; else remove the stale file; `NotFound` -> fine. `impl Drop for Server` removes the socket file.

## main.rs

clap `Args` (`-n/--max-clients`, validated `>= 1`), `SimpleLogger::new().with_level(Info).env().init()` (`RUST_LOG` overrides), build `Config`, `Poll::new()`, `TokenRange::new(0, Server::required_tokens(&cfg))`, `Server::bind`, loop:

```rust
let timeout = server.deadline().map(|t| t.saturating_duration_since(Instant::now()));
match poll.poll(&mut events, timeout) { Ok(()) => {}, Err(e) if e.kind() == Interrupted => continue, Err(e) => { error!(..); return ExitCode::FAILURE } }
for event in events.iter() { server.handle_event(event); }
server.tick(Instant::now());
```

Return `ExitCode`, never `unwrap` on startup errors; log them.

## Sharing one loop (two_servers example)

`total = a_tokens + b_tokens`, one `free = TokenRange::new(0, total)`, `bind` each server from it, poll timeout = min of the deadlines, dispatch with `a.handle_event(ev) || b.handle_event(ev)`, `tick` both.

## Adaptations

- Access control: add checks in `admit` (e.g. `SO_PEERCRED` via `UnixStream::peer_cred`, currently NOT implemented in the reference).
- Extra mio sources (timers, signalfd, other sockets): give them their own `TokenRange` from the same free pool.
- Signal handling/graceful shutdown: add it to the caller's loop; `Server` drop cleans the socket.
