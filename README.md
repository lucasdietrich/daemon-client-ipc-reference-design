# mydaemon

A synchronous daemon and its client library, talking over a Unix domain socket.

```
proto/   mydaemon-proto   messages + framing, sans-IO (features: sync, tokio)
daemon/  mydaemond        fully synchronous daemon (lib + bin)
client/  mydaemon-client  client library (features: blocking [default], tokio)
```

## Protocol

Each frame is a `u32` little-endian length followed by a postcard payload
(max 1 MiB). A connection starts with a handshake:

```
client -> Hello { version }
daemon -> Welcome { version } | Rejected { reason }
```

After that:

| Direction       | Message                     | Answer                         |
|-----------------|-----------------------------|--------------------------------|
| client -> daemon | `Command { id, cmd }`       | `Response { id, resp }`        |
| daemon -> client | `Event(..)`                 | none                           |

Events are only sent to connections that issued `Command::Subscribe`.
Responses and events share the socket, so they can arrive in any order.

## Daemon (sync)

One reader thread and one writer thread per connection. The reader runs
commands and queues responses; `Handle::broadcast` queues events. Only the
writer touches the socket. Events go through `try_send`: a client that stops
reading loses events but never blocks the daemon.

On startup the daemon removes a stale socket file, but refuses to start if
another instance is still listening on it. Peer credentials (`SO_PEERCRED`)
are logged on Linux; `log_peer` is where to add uid/gid access checks.

```sh
RUST_LOG=debug MYDAEMON_SOCKET=/tmp/mydaemon/mydaemon.sock cargo run -p mydaemond
```

## Client

Both flavors expose the same API:

```rust
let (client, events) = Client::connect()?;      // blocking
let (client, mut events) = Client::connect().await?; // tokio
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
cargo run -p mydaemon-client --example blocking
cargo run -p mydaemon-client --features tokio --example async_events
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
