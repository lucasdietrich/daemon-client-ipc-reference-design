---
name: rust-daemon-client-ipc
description: 'Scaffold or extend a Rust daemon + client library + IPC protocol workspace (proto/daemon/client crates): single-threaded mio daemon over a Unix domain socket, sans-IO length-prefixed postcard framing, handshake with version check, request/response ids plus unsolicited events, and sync (threads), tokio and mio client flavors. Use when asked to create a new daemon, a client library for a daemon, IPC over Unix sockets, a wire protocol crate, or to reapply the mydaemon reference architecture to a new project.'
argument-hint: 'Project name, commands, events, and which client flavors are needed'
---

# Rust daemon + client + IPC (reference architecture)

Reference implementation: `/home/lucas/Projects/Embedded/lwnet-poc-daemon-lib` (crates `mydaemon-proto`, `mydaemond`, `mydaemon-client`). When it exists on disk, copy files from it and rename; otherwise rebuild from the references below.

## When to use

- New daemon that serves local clients over a Unix socket.
- New client library for such a daemon (blocking, tokio, or inside a caller-owned mio loop).
- Adding commands, events or a client flavor to a project that already follows this layout.

Not for: network (TCP) services, multi-threaded request handlers, or protocols that must be human readable.

## Architecture

```
proto/   <name>-proto    messages + framing, sans-IO. features: sync (default), tokio
daemon/  <name>d         single-threaded mio server (lib + bin). Caller owns the poll loop.
client/  <name>-client   features: sync, tokio, mio. One flavor per module.
```

Design rules (do not drift from them without a reason):

1. **Sans-IO protocol crate.** It defines messages, `encode`/`decode`/`frame_len`/`decode_frame`, and never touches a socket. I/O adapters sit behind features.
2. **Frame = `u32` LE length + postcard payload**, capped by `MAX_FRAME` (1 MiB). Consume the whole frame before decoding, so a bad payload (`Error::Codec`, recoverable) never desyncs the stream.
3. **Handshake first**: `Hello { version }` -> `Welcome { version } | Rejected { reason }`. Any incompatible enum change bumps `PROTO_VERSION`.
4. **Two server-to-client message kinds on one socket**: `Response { id, resp }` (answers exactly one `Command { id, cmd }`) and `Event(..)` (unsolicited, only after `Subscribe`, never answered).
5. **`Event` is `#[non_exhaustive]` and append-only.** Clients skip frames they cannot decode.
6. **Daemon: no locks, no threads.** `Server` owns listener, connections and the state. It does not own the event loop: the caller creates `mio::Poll`, passes a `TokenRange`, forwards events with `handle_event`, and calls `tick(now)` after each wake-up. Several servers can share one loop.
7. **Backpressure without blocking**: events are dropped for a client whose outbound buffer exceeds `outbound_limit`; responses are never dropped, the daemon just stops reading that client's commands until it drains.
8. **Bounded everything**: `max_clients` (extra connections get `Rejected`), `MAX_FRAME`, client `EVENT_QUEUE` (256, overflow dropped with a warning).
9. **Socket hygiene**: remove a stale socket, refuse if another daemon answers on it, refuse a non-socket file, apply `mode` (0660), remove the file in `Drop`.
10. **Client flavors share the same API shape** (`connect`, `connect_to(path)`, `call`, typed helpers, `with_timeout`) and the same error enum. `sync`/`tokio` use a background reader plus a `Pending` id->waiter table; `mio` has no reader, the caller drives I/O.
11. **Socket path**: `DEFAULT_SOCKET` constant + `<NAME>_SOCKET` env override, via `socket_path()` in the proto crate. Containers bind-mount the socket directory, not the file.

## What is generic vs domain specific

| Keep as is (rename only) | Replace per project |
|---|---|
| framing, `Error`, `PROTO_VERSION`, handshake | `Command`, `Response`, `Event` variants |
| `TokenRange`, accept/admit/service/flush/backpressure | `Server.store` and `Conn::run_command` |
| `prepare_socket_path`, `Drop` cleanup | `Config` fields beyond socket/mode/max_clients/outbound_limit/heartbeat |
| `Pending`/`Ticket`, `check_welcome`, reader loops | typed helpers (`ping`, `get`, `set`, ...) and `expect::*` mappers |
| test harness `common/mod.rs` (`start_daemon`, `fake_daemon`) | domain tests |

## Procedure

Track these steps with a todo list.

1. **Collect requirements** (ask if missing): project name (`<name>`, crate names `<name>-proto`, `<name>d`, `<name>-client`, env var `<NAME>_SOCKET`, socket path), commands and their responses, events and what triggers them, daemon state, which client flavors are needed (drop unused ones, including their features, tests and examples).
2. **Workspace skeleton**: see [tooling](./references/tooling.md) (root `Cargo.toml` with `[workspace.dependencies]`, edition 2024, `justfile`, `.gitignore`).
3. **Proto crate**: see [protocol](./references/protocol.md). Write messages, framing, adapters and unit tests first. Add a `hexdump` example if the user wants to inspect bytes.
4. **Daemon crate**: see [daemon](./references/daemon.md). Library with `Server`, `Config`, `TokenRange`; thin `main.rs` (clap + simple_logger + poll loop); optional multi-server example.
5. **Client crate**: see [client](./references/client.md). `lib.rs` (errors, re-exports, `check_welcome`), `shared.rs`, then each enabled flavor, with `required-features` on tests and examples.
6. **Tests**: see [testing](./references/testing.md). Real daemon on a temp socket for the happy paths, scripted fake daemons for the failure paths.
7. **Docs**: README with the layout, protocol, daemon, client, container, tests and protocol-evolution sections (outline in [tooling](./references/tooling.md)). Crate-level `//!` docs carry the diagrams.
8. **Verify**: `cargo test --workspace --all-features`, `cargo clippy --workspace --all-features --all-targets`, `cargo fmt --check`, and run each example through the `justfile`. Check that every README claim (defaults, features, flags, function names) exists in the code.

## Pitfalls seen in the reference

- README claimed behavior that the code did not have (peer credentials logging, a `log_peer` hook) and wrong feature defaults. Re-grep the code for every symbol and default the README names.
- `Server::service` removes the connection from the map while it runs (to borrow the store) and re-inserts it only on `Outcome::Open`; do not early-return between those two points.
- Edge-triggered mio: always drain reads until `WouldBlock`, and flush writes until `WouldBlock`; register connections for `READABLE | WRITABLE` once.
- Broadcast after the connection borrow ends: commands push events into a `Vec<Event>`, the server broadcasts them afterwards.
- `Token(usize::MAX)` is reserved by mio; `TokenRange::new` clamps for that.
- Sync client: shut the socket down in `Drop` or the reader thread never exits. Tokio client: abort the reader task in `Drop`.
- Async `call` must stay cancel safe: register a `Ticket` that removes the waiter on drop; `FramedWrite` keeps partial frames buffered.
- Client handshake runs on the bare blocking stream before the reader exists (the mio client then switches to non-blocking).
- The `let` chains used (`if let ... && cond`) need edition 2024 (Rust 1.88+).
