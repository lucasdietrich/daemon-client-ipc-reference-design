# mydaemon

A single-threaded daemon and its client library, talking over a Unix domain socket.

```
proto/   mydaemon-proto   messages + framing, sans-IO (features: sync, tokio)
daemon/  mydaemond        single-threaded mio daemon (lib + bin)
client/  mydaemon-client  client library (features: sync, tokio, mio; all on by default)
```

## Protocol

Each frame is a `u32` little-endian length followed by a postcard payload
(max 1 MiB). A connection starts with a handshake:

```
client -> Hello { version }
daemon -> Welcome { version } | Rejected { reason }
```

After that:

| Direction        | Message               | Answer                  |
|------------------|-----------------------|-------------------------|
| client -> daemon | `Command { id, cmd }` | `Response { id, resp }` |
| daemon -> client | `Event(..)`           | none                    |

Commands: `Ping`, `Get`, `Set`, `Subscribe`, `Unsubscribe`. Events:
`KeyChanged` (after every `Set`) and `Heartbeat` (every 10 s in `mydaemond`).

Events are only sent to connections that issued `Command::Subscribe`.
Responses and events share the socket, so they can arrive in any order.

To see the bytes of each message: `cargo run -p mydaemon-proto --example hexdump`.

## Daemon (mio)

A `Server` owns the listener, every connection and the key/value store, so
there are no locks. It does not own the event loop: the caller creates the
`mio::Poll`, hands the server a `TokenRange` to register under, forwards the
events with `server.handle_event(ev)` and calls `server.tick(now)` after each
wake-up (heartbeat timer). Several servers can share one loop by carving
disjoint ranges from the same free `TokenRange`. The loop lives in the
caller: `daemon/src/main.rs` runs one server, and
`daemon/examples/two_servers.rs` runs two independent servers (own socket,
store and client limit) in one loop.

Each connection has an inbound and an outbound byte buffer; sockets are
edge-triggered and drained until `WouldBlock`. Since the caller owns the
server, it can push an event to subscribers with `server.broadcast(event)`.

At most `N` clients are served at once (`-n N` / `--max-clients N`, default
16). Extra connections receive `Rejected` and are closed; the slot is freed
when a client disconnects.

Flow control: events for a client whose outbound buffer is over
`Config::outbound_limit` (64 KiB) are dropped, so a stalled client loses
events but never blocks the daemon. Responses are never dropped; the daemon
stops reading commands from that client until it catches up.

On startup the daemon removes a stale socket file, but refuses to start if
another instance is still listening on it. The socket file gets mode `0660`
(`Config::mode`) and is removed on shutdown; there is no other access check.

The socket path is `$MYDAEMON_SOCKET`, else `/run/mydaemon/mydaemon.sock`.

```sh
RUST_LOG=debug MYDAEMON_SOCKET=/tmp/mydaemon/mydaemon.sock cargo run -p mydaemond -- -n 16
```

`just` recipes (see the `justfile`, socket defaults to `/tmp/mydaemon/mydaemon.sock`):
`daemon`, `daemon-two`, `client-sync`, `client-async`, `client-mio`, `hexdump`.

## Client

Both flavors expose the same API:

```rust
let (client, events) = Client::connect()?;      // mydaemon_client::sync
let (client, mut events) = Client::connect().await?; // mydaemon_client::r#async (feature tokio)
client.subscribe()?;
client.set("key", b"value")?;
let ev = events.recv()?;
```

A background reader (thread or tokio task) routes responses to the waiting
caller by id and events to the bounded `Events` queue (256 entries; extra
events are dropped with a warning). `Client` is `Send + Sync`, so one
connection can be shared with `Arc` by many threads or tasks.

- `with_timeout(Some(d))` makes calls fail with `Error::Timeout`.
- When the daemon goes away, pending and future calls return
  `Error::Disconnected`, and `Events` drains then returns it too.
- Frames that fail to decode (e.g. a new event variant from a newer daemon)
  are skipped, not fatal.

```sh
cargo run -p mydaemon-client --example sync
cargo run -p mydaemon-client --features tokio --example async_events
```

### mio

For programs that already run a `mio::Poll` loop, use
`mydaemon_client::mio_client::Client` (feature `mio`). It has no thread and no locks:
register it as a `mio::event::Source`, call `send`/`ping`/`set`/... (each
returns the request id), `flush()` on writable and `recv()` until `None` on
readable. `recv` yields `Incoming::Response { id, resp }` or
`Incoming::Event`. There is no built-in timeout; use the `Poll` timeout.

```sh
cargo run -p mydaemon-client --features mio --example mio_events
```

## Containers

Bind-mount the socket *directory*, not the socket file, so a daemon restart
(new inode) stays visible inside the container:

```sh
docker run -v /run/mydaemon:/run/mydaemon ...
```

Check that the socket mode (default `0660`) and group fit the uid mapping of
the container; add `:z` on SELinux hosts.

## Tests

```sh
cargo test --workspace --all-features
```

Integration tests start a real daemon on a temp socket (plus scripted fake
daemons for rejection, disconnection, timeouts and undecodable frames).

## Evolving the protocol

- Appending a variant to `Event` is backward compatible: old clients skip it.
- Any other change to the message enums: bump `PROTO_VERSION`; mismatched
  clients get `Error::Rejected` at connect time.
