#![allow(dead_code)]

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use mio::{Events, Poll};
use mydaemon_proto::sync::{read_msg, write_msg};
use mydaemon_proto::{ClientMsg, PROTO_VERSION, ServerMsg};
use mydaemond::{Config, Server, TokenRange};
use tempfile::TempDir;

/// A real daemon on a private socket. Keep the value alive for the test.
pub struct TestDaemon {
    pub socket: PathBuf,
    _dir: TempDir,
}

pub fn start_daemon() -> TestDaemon {
    start_daemon_with(|_| {})
}

pub fn start_daemon_with(tweak: impl FnOnce(&mut Config)) -> TestDaemon {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Warn)
        .env()
        .init();
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("mydaemon.sock");
    let mut cfg = Config {
        socket: socket.clone(),
        ..Config::default()
    };
    tweak(&mut cfg);
    let poll = Poll::new().unwrap();
    let mut free = TokenRange::new(0, Server::required_tokens(&cfg));
    let server = Server::bind(cfg, poll.registry(), &mut free).unwrap();
    thread::spawn(move || run_loop(server, poll));
    TestDaemon { socket, _dir: dir }
}

fn run_loop(mut server: Server, mut poll: Poll) {
    let mut events = Events::with_capacity(128);
    loop {
        let timeout = server
            .deadline()
            .map(|t| t.saturating_duration_since(Instant::now()));
        match poll.poll(&mut events, timeout) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("poll failed: {e}"),
        }
        for event in events.iter() {
            server.handle_event(event);
        }
        server.tick(Instant::now());
    }
}

/// A fake daemon that runs `script` on the first connection, after an
/// optional successful handshake.
pub fn fake_daemon(
    welcome: bool,
    script: impl FnOnce(UnixStream) + Send + 'static,
) -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("fake.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let hello: ClientMsg = read_msg(&mut s).unwrap();
        assert_eq!(
            hello,
            ClientMsg::Hello {
                version: PROTO_VERSION
            }
        );
        let reply = if welcome {
            ServerMsg::Welcome {
                version: PROTO_VERSION,
            }
        } else {
            ServerMsg::Rejected {
                reason: "go away".into(),
            }
        };
        write_msg(&mut s, &reply).unwrap();
        script(s);
    });
    (dir, socket)
}
