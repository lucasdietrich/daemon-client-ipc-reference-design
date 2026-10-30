mod common;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use common::{fake_daemon, start_daemon, start_daemon_with};
use mydaemon_client::sync::Client;
use mydaemon_client::{Error, Event};
use mydaemon_proto::sync::{read_msg, write_msg};
use mydaemon_proto::{ClientMsg, ServerMsg};

const WAIT: Duration = Duration::from_secs(5);

#[test]
fn ping_set_get() {
    let d = start_daemon();
    let (c, _events) = Client::connect_to(&d.socket).unwrap();
    c.ping().unwrap();
    assert_eq!(c.get("k").unwrap(), None);
    c.set("k", b"v").unwrap();
    assert_eq!(c.get("k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn daemon_error_is_mapped() {
    let d = start_daemon();
    let (c, _events) = Client::connect_to(&d.socket).unwrap();
    assert!(matches!(c.set("", b"v"), Err(Error::Daemon(_))));
}

#[test]
fn events_only_after_subscribe() {
    let d = start_daemon();
    let (a, a_events) = Client::connect_to(&d.socket).unwrap();
    let (b, b_events) = Client::connect_to(&d.socket).unwrap();
    a.subscribe().unwrap();

    b.set("x", b"1").unwrap();
    assert_eq!(
        a_events.recv_timeout(WAIT).unwrap(),
        Some(Event::KeyChanged { key: "x".into() })
    );
    assert_eq!(
        b_events.recv_timeout(Duration::from_millis(200)).unwrap(),
        None
    );

    a.unsubscribe().unwrap();
    b.set("x", b"2").unwrap();
    assert_eq!(
        a_events.recv_timeout(Duration::from_millis(200)).unwrap(),
        None
    );
}

#[test]
fn heartbeat_event() {
    let d = start_daemon_with(|cfg| cfg.heartbeat = Some(Duration::from_millis(50)));
    let (c, events) = Client::connect_to(&d.socket).unwrap();
    c.subscribe().unwrap();
    assert!(matches!(
        events.recv_timeout(WAIT).unwrap(),
        Some(Event::Heartbeat { .. })
    ));
}

/// Many threads share one client while it receives a flood of events:
/// every response must reach the right caller.
#[test]
fn concurrent_calls_with_interleaved_events() {
    let d = start_daemon();
    let (c, events) = Client::connect_to(&d.socket).unwrap();
    let c = Arc::new(c.with_timeout(Some(WAIT)));
    c.subscribe().unwrap();

    let workers: Vec<_> = (0..8)
        .map(|t| {
            let c = c.clone();
            thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("t{t}-{i}");
                    c.set(&key, key.as_bytes()).unwrap();
                    assert_eq!(c.get(&key).unwrap().as_deref(), Some(key.as_bytes()));
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }

    let mut n = 0;
    while events.try_recv().unwrap().is_some() {
        n += 1;
    }
    assert!(n > 0, "expected some events");
}

#[test]
fn max_clients_is_enforced() {
    let d = start_daemon_with(|cfg| cfg.max_clients = 2);
    let (a, _a_events) = Client::connect_to(&d.socket).unwrap();
    let (b, _b_events) = Client::connect_to(&d.socket).unwrap();
    a.ping().unwrap();
    b.ping().unwrap();

    assert!(matches!(
        Client::connect_to(&d.socket),
        Err(Error::Rejected(_))
    ));
    a.ping().unwrap();

    // The slot is freed once the daemon notices the disconnect.
    drop(a);
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        match Client::connect_to(&d.socket) {
            Ok((c, _events)) => {
                c.ping().unwrap();
                break;
            }
            Err(Error::Rejected(_)) if std::time::Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
}

#[test]
fn version_rejected() {
    let (_dir, sock) = fake_daemon(false, |_| {});
    match Client::connect_to(&sock) {
        Err(Error::Rejected(reason)) => assert_eq!(reason, "go away"),
        Err(e) => panic!("unexpected error: {e}"),
        Ok(_) => panic!("connect should fail"),
    }
}

#[test]
fn disconnect_wakes_caller_and_events() {
    let (_dir, sock) = fake_daemon(true, |mut s| {
        // Read one command, send an event, then hang up without answering.
        let _: ClientMsg = read_msg(&mut s).unwrap();
        write_msg(
            &mut s,
            &ServerMsg::Event(Event::KeyChanged { key: "bye".into() }),
        )
        .unwrap();
    });
    let (c, events) = Client::connect_to(&sock).unwrap();
    assert!(matches!(c.ping(), Err(Error::Disconnected)));
    assert_eq!(
        events.recv().unwrap(),
        Event::KeyChanged { key: "bye".into() }
    );
    assert!(matches!(events.recv(), Err(Error::Disconnected)));
    assert!(matches!(c.ping(), Err(Error::Disconnected)));
}

#[test]
fn undecodable_frame_is_skipped() {
    use std::io::Write;
    let (_dir, sock) = fake_daemon(true, |mut s| {
        let cmd: ClientMsg = read_msg(&mut s).unwrap();
        let ClientMsg::Command { id, .. } = cmd else {
            panic!()
        };
        // Garbage payload, as a newer daemon could send, then the response.
        s.write_all(&[2, 0, 0, 0, 0xff, 0xff]).unwrap();
        write_msg(
            &mut s,
            &ServerMsg::Response {
                id,
                resp: mydaemon_client::Response::Pong,
            },
        )
        .unwrap();
        thread::sleep(Duration::from_millis(200));
    });
    let (c, _events) = Client::connect_to(&sock).unwrap();
    c.ping().unwrap();
}

#[test]
fn timeout() {
    let (_dir, sock) = fake_daemon(true, |mut s| {
        let _: ClientMsg = read_msg(&mut s).unwrap();
        thread::sleep(Duration::from_secs(2));
    });
    let (c, _events) = Client::connect_to(&sock).unwrap();
    let c = c.with_timeout(Some(Duration::from_millis(100)));
    assert!(matches!(c.ping(), Err(Error::Timeout)));
}
