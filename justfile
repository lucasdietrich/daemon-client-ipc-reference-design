# Unprivileged socket path shared by the daemon and the clients.
export MYDAEMON_SOCKET := env("MYDAEMON_SOCKET", "/tmp/mydaemon/mydaemon.sock")
export RUST_LOG := env("RUST_LOG", "info")

default:
    @just --list

# Run the daemon; extra arguments are passed through, e.g. `just daemon -n 4`.
daemon *args:
    cargo run -p mydaemond -- {{args}}

# Run two daemons in one poll loop (sockets /tmp/mydaemon/a.sock and b.sock).
daemon-two:
    cargo run -p mydaemond --example two_servers

# Run the sync (std threads) client example.
client-sync:
    cargo run -p mydaemon-client --example sync

# Run the async (tokio) client example.
client-async:
    cargo run -p mydaemon-client --features tokio --example async_events

# Run the mio client example.
client-mio:
    cargo run -p mydaemon-client --features mio --example mio_events

# Print the serialized bytes of sample commands and responses.
hexdump:
    cargo run -q -p mydaemon-proto --example hexdump
