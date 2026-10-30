#![allow(dead_code)]

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::thread;

use mydaemon_proto::sync::{read_msg, write_msg};
use mydaemon_proto::{ClientMsg, PROTO_VERSION, ServerMsg};
use mydaemond::{Config, Handle, Server};
use tempfile::TempDir;

/// A real daemon on a private socket. Keep the value alive for the test.
pub struct TestDaemon {
    pub socket: PathBuf,
    pub handle: Handle,
    _dir: TempDir,
}

pub fn start_daemon() -> TestDaemon {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Warn)
        .env()
        .init();
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("mydaemon.sock");
    let server = Server::bind(Config {
        socket: socket.clone(),
        ..Config::default()
    })
    .unwrap();
    let handle = server.handle();
    thread::spawn(move || server.run());
    TestDaemon {
        socket,
        handle,
        _dir: dir,
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
