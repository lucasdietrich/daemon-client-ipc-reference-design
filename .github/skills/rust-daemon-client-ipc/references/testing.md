# Testing

Source of truth: `client/tests/common/mod.rs`, `client/tests/{sync,async,mio}.rs`, `daemon/tests/shared_loop.rs`.

Run: `cargo test --workspace --all-features` (feature-gated tests are skipped otherwise).

## Harness (`client/tests/common/mod.rs`, `#![allow(dead_code)]`)

- `start_daemon()` / `start_daemon_with(|cfg| ..)`: init `simple_logger` at `Warn` (ignore the already-initialized error), temp dir socket, `Config { socket, ..Default }` tweaked by the closure, `Poll::new()`, `TokenRange::new(0, Server::required_tokens(&cfg))`, `Server::bind`, run the loop on a spawned thread. Returns `TestDaemon { socket, _dir: TempDir }`; the test keeps it alive.
- `fake_daemon(welcome: bool, script)`: std `UnixListener` in a temp dir, accepts one connection, asserts the `Hello` carries `PROTO_VERSION`, replies `Welcome` or `Rejected`, then runs `script(stream)` to produce scripted misbehavior.

## Per-flavor test matrix

| Test | Real daemon | Fake daemon |
|---|---|---|
| `ping_set_get` (happy path) | x | |
| `daemon_error_is_mapped` (`Response::Error` -> `Error::Daemon`) | x | |
| `events_only_after_subscribe` | x | |
| `heartbeat_event` (`heartbeat = Some(50ms)`) | x | |
| `concurrent_calls_with_interleaved_events` (threads/tasks sharing `Arc<Client>`) | x | |
| `max_clients_is_enforced` (extra connection -> `Error::Rejected`) | x | |
| `version_rejected` | | x (`welcome = false`) |
| `disconnect_wakes_caller_and_events` | | x (script drops the stream) |
| `undecodable_frame_is_skipped` (garbage frame then a valid one) | | x |
| `timeout` | | x (script never answers) |
| `cancelled_call_does_not_break_the_next_one` (tokio) | x | |
| `pipelined_commands_and_events` (mio, 50 commands) | x | |
| `disconnect_after_buffered_frames` (mio: buffered event returned before `Disconnected`) | | x |

The mio tests use a `Harness { poll, events, client }` with `next(timeout)` (flush, `recv`, poll) and `response(id)` (skip unrelated messages).

## Daemon integration test

`daemon/tests/shared_loop.rs`: two servers (own sockets, stores, limits) in one poll loop; raw `UnixStream` helpers `connect` (Hello/Welcome) and `call`; second test asserts `bind` fails with `TokenRangeTooSmall` when the pool is too small.

## Unit tests

- proto: see [protocol](./protocol.md).
- daemon `token_range.rs`: `get_and_index_of_are_inverse`, `take_carves_disjoint_ranges`, `length_is_clamped`.

## Rules

- Never use a fixed socket path in tests; always a `tempfile` dir.
- Use bounded waits (`Duration::from_secs(5)`) so a bug fails instead of hanging.
- Tests of failure paths use the fake daemon, not sleeps or killing processes.
