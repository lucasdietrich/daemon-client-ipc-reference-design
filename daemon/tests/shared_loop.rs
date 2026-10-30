//! Two servers driven by one caller-owned poll loop.

use std::os::unix::net::UnixStream;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use mio::Poll;
use mydaemon_proto::sync::{read_msg, write_msg};
use mydaemon_proto::{ClientMsg, Command, Event, PROTO_VERSION, Response, ServerMsg};
use mydaemond::{Config, Error, Server, TokenRange};

fn connect(socket: &Path) -> UnixStream {
    let mut s = UnixStream::connect(socket).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    write_msg(
        &mut s,
        &ClientMsg::Hello {
            version: PROTO_VERSION,
        },
    )
    .unwrap();
    let welcome: ServerMsg = read_msg(&mut s).unwrap();
    assert_eq!(
        welcome,
        ServerMsg::Welcome {
            version: PROTO_VERSION
        }
    );
    s
}

fn call(s: &mut UnixStream, id: u32, cmd: Command) -> Response {
    write_msg(s, &ClientMsg::Command { id, cmd }).unwrap();
    match read_msg(s).unwrap() {
        ServerMsg::Response { id: rid, resp } if rid == id => resp,
        other => panic!("unexpected message: {other:?}"),
    }
}

fn config(dir: &Path, name: &str, max_clients: usize) -> Config {
    Config {
        socket: dir.join(name),
        max_clients,
        ..Config::default()
    }
}

#[test]
fn two_servers_share_one_poll_loop() {
    let dir = tempfile::tempdir().unwrap();
    let cfg_a = Config {
        heartbeat: Some(Duration::from_millis(50)),
        ..config(dir.path(), "a.sock", 2)
    };
    let cfg_b = config(dir.path(), "b.sock", 3);

    let mut poll = Poll::new().unwrap();
    let mut free = TokenRange::new(0, 100);
    let mut a = Server::bind(cfg_a.clone(), poll.registry(), &mut free).unwrap();
    let mut b = Server::bind(cfg_b.clone(), poll.registry(), &mut free).unwrap();

    // Ranges are disjoint and sized for each config.
    assert_eq!(a.tokens().len(), Server::required_tokens(&cfg_a));
    assert_eq!(b.tokens().len(), Server::required_tokens(&cfg_b));
    assert_eq!(a.tokens().end(), b.tokens().start());

    thread::spawn(move || {
        let mut events = mio::Events::with_capacity(32);
        loop {
            let timeout = [a.deadline(), b.deadline()]
                .into_iter()
                .flatten()
                .min()
                .map(|t| t.saturating_duration_since(Instant::now()));
            poll.poll(&mut events, timeout).unwrap();
            for ev in events.iter() {
                let _ = a.handle_event(ev) || b.handle_event(ev);
            }
            let now = Instant::now();
            a.tick(now);
            b.tick(now);
        }
    });

    let mut ca = connect(&cfg_a.socket);
    let mut cb = connect(&cfg_b.socket);
    assert_eq!(call(&mut ca, 0, Command::Ping), Response::Pong);
    assert_eq!(call(&mut cb, 0, Command::Ping), Response::Pong);

    // Stores are independent.
    let set = Command::Set {
        key: "k".into(),
        value: b"v".to_vec(),
    };
    assert_eq!(call(&mut ca, 1, set), Response::Done);
    assert_eq!(
        call(&mut ca, 2, Command::Get { key: "k".into() }),
        Response::Value(Some(b"v".to_vec()))
    );
    assert_eq!(
        call(&mut cb, 1, Command::Get { key: "k".into() }),
        Response::Value(None)
    );

    // Timers of one server fire inside the shared loop.
    assert_eq!(call(&mut ca, 3, Command::Subscribe), Response::Done);
    assert!(matches!(
        read_msg::<_, ServerMsg>(&mut ca).unwrap(),
        ServerMsg::Event(Event::Heartbeat { .. })
    ));
}

#[test]
fn bind_fails_when_token_range_is_too_small() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), "a.sock", 4);
    let needed = Server::required_tokens(&cfg);
    let poll = Poll::new().unwrap();
    let mut free = TokenRange::new(0, needed - 1);
    match Server::bind(cfg, poll.registry(), &mut free) {
        Err(Error::TokenRangeTooSmall {
            needed: n,
            available,
        }) => {
            assert_eq!((n, available), (needed, needed - 1));
        }
        Err(e) => panic!("unexpected error: {e}"),
        Ok(_) => panic!("bind should fail"),
    }
    assert_eq!(free.len(), needed - 1);
}
