# Workspace, justfile and README

## Root Cargo.toml

```toml
[workspace]
resolver = "3"
members = ["proto", "daemon", "client"]

[workspace.package]
version = "0.1.0"
edition = "2024"
license = "MIT OR Apache-2.0"

[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
postcard = { version = "1", default-features = false, features = ["use-std"] }
thiserror = "2"
log = "0.4"
simple_logger = "5"
tokio = { version = "1", default-features = false }
tokio-util = { version = "0.7", default-features = false, features = ["codec"] }
futures-util = { version = "0.3", default-features = false, features = ["std", "sink"] }
bytes = "1"
mio = { version = "1", features = ["os-poll", "net"] }
clap = { version = "4", features = ["derive"] }
tempfile = "3"
```

Member crates use `version.workspace = true`, `edition.workspace = true`, `license.workspace = true`, and `dep.workspace = true` for shared dependencies. `.gitignore`: `/target`.

## justfile

```just
export <NAME>_SOCKET := env("<NAME>_SOCKET", "/tmp/<name>/<name>.sock")   # unprivileged path for dev
export RUST_LOG := env("RUST_LOG", "info")

default:
    @just --list

# Run the daemon; extra arguments are passed through, e.g. `just daemon -n 4`.
daemon *args:
    cargo run -p <name>d -- {{args}}

daemon-two:
    cargo run -p <name>d --example two_servers

client-sync:
    cargo run -p <name>-client --example sync

client-async:
    cargo run -p <name>-client --features tokio --example async_events

client-mio:
    cargo run -p <name>-client --features mio --example mio_events

hexdump:
    cargo run -q -p <name>-proto --example hexdump
```

One recipe per binary/example, each with a one-line comment. Keep `--features` flags in sync with the client `default` features.

## Examples to provide

- `client/examples/sync.rs`, `async_events.rs`, `mio_events.rs`: connect, subscribe, issue a few commands, print events.
- `daemon/examples/two_servers.rs`: two servers in one loop.
- `proto/examples/hexdump.rs`: serialize sample messages and print the bytes.

## README outline

1. Title + one-line description, crate layout block with features.
2. **Protocol**: frame layout, handshake, request/response vs event table, ordering note, hexdump command.
3. **Daemon**: ownership model (caller owns the loop, `TokenRange`, `handle_event`, `tick`, `broadcast`), buffers and edge triggering, `max_clients`, flow control, stale socket handling, run command with env vars, default socket path.
4. **Client**: shared API snippet per flavor, reader routing, timeout, disconnect behavior, skipped frames, example commands; a **mio** subsection.
5. **Containers**: bind-mount the socket directory, check mode and group, `:z` on SELinux.
6. **Tests**: command and what the integration tests cover.
7. **Evolving the protocol**: append `Event` variants, otherwise bump `PROTO_VERSION`.
8. Optionally a `just` recipes table.

## README accuracy checklist

Before finishing, verify with `grep` that every item the README mentions exists:

- feature names and which are in `default`,
- CLI flags and env var names, default socket path, default numbers (`max_clients`, `outbound_limit`, `EVENT_QUEUE`, heartbeat interval, socket mode),
- function and type names (`handle_event`, `tick`, `broadcast`, `Incoming`, ...),
- security claims (peer credentials, access checks): only document what is implemented,
- every command in the README runs (`cargo run ...`, `just ...`).
