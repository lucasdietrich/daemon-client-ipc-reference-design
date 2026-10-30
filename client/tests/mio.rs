mod common;

use std::io::Write;
use std::time::Duration;

use common::{fake_daemon, start_daemon, start_daemon_with};
use mio::{Events, Interest, Poll, Token};
use mydaemon_client::mio_client::{Client, Incoming};
use mydaemon_client::{Command, Error, Event, Response};
use mydaemon_proto::sync::{read_msg, write_msg};
use mydaemon_proto::{ClientMsg, ServerMsg};

const WAIT: Duration = Duration::from_secs(5);

struct Harness {
    poll: Poll,
    events: Events,
    client: Client,
}

impl Harness {
    fn new(socket: &std::path::Path) -> Harness {
        let poll = Poll::new().unwrap();
        let mut client = Client::connect_to(socket).unwrap();
        poll.registry()
            .register(
                &mut client,
                Token(0),
                Interest::READABLE | Interest::WRITABLE,
            )
            .unwrap();
        Harness {
            poll,
            events: Events::with_capacity(16),
            client,
        }
    }

    /// Next message, driving the poll loop; `Ok(None)` on timeout.
    fn next(&mut self, timeout: Duration) -> Result<Option<Incoming>, Error> {
        loop {
            self.client.flush()?;
            if let Some(msg) = self.client.recv()? {
                return Ok(Some(msg));
            }
            self.poll.poll(&mut self.events, Some(timeout)).unwrap();
            if self.events.is_empty() {
                return Ok(None);
            }
        }
    }

    fn response(&mut self, id: u32) -> Response {
        loop {
            match self.next(WAIT).unwrap().expect("timed out") {
                Incoming::Response { id: rid, resp } if rid == id => return resp,
                _ => {}
            }
        }
    }
}

#[test]
fn ping_set_get() {
    let d = start_daemon();
    let mut h = Harness::new(&d.socket);

    let id = h.client.ping().unwrap();
    assert_eq!(h.response(id), Response::Pong);

    let id = h.client.set("k", b"v").unwrap();
    assert_eq!(h.response(id), Response::Done);

    let id = h.client.get("k").unwrap();
    assert_eq!(h.response(id), Response::Value(Some(b"v".to_vec())));
}

#[test]
fn pipelined_commands_and_events() {
    let d = start_daemon();
    let mut h = Harness::new(&d.socket);
    let sub = h.client.subscribe().unwrap();
    assert_eq!(h.response(sub), Response::Done);

    let ids: Vec<u32> = (0..50)
        .map(|i| h.client.set(&format!("k{i}"), b"x").unwrap())
        .collect();
    let mut answered = Vec::new();
    let mut changed = 0;
    while answered.len() < ids.len() || changed < ids.len() {
        match h.next(WAIT).unwrap().expect("timed out") {
            Incoming::Response { id, resp } => {
                assert_eq!(resp, Response::Done);
                answered.push(id);
            }
            Incoming::Event(Event::KeyChanged { .. }) => changed += 1,
            Incoming::Event(_) => {}
        }
    }
    answered.sort_unstable();
    assert_eq!(answered, ids);
}

#[test]
fn heartbeat_event() {
    let d = start_daemon_with(|cfg| cfg.heartbeat = Some(Duration::from_millis(50)));
    let mut h = Harness::new(&d.socket);
    let sub = h.client.subscribe().unwrap();
    h.response(sub);
    assert!(matches!(
        h.next(WAIT).unwrap(),
        Some(Incoming::Event(Event::Heartbeat { .. }))
    ));
}

#[test]
fn disconnect_after_buffered_frames() {
    let (_dir, sock) = fake_daemon(true, |mut s| {
        let _: ClientMsg = read_msg(&mut s).unwrap();
        write_msg(
            &mut s,
            &ServerMsg::Event(Event::KeyChanged { key: "bye".into() }),
        )
        .unwrap();
    });
    let mut h = Harness::new(&sock);
    h.client.ping().unwrap();
    assert_eq!(
        h.next(WAIT).unwrap(),
        Some(Incoming::Event(Event::KeyChanged { key: "bye".into() }))
    );
    assert!(matches!(h.next(WAIT), Err(Error::Disconnected)));
}

#[test]
fn undecodable_frame_is_skipped() {
    let (_dir, sock) = fake_daemon(true, |mut s| {
        let ClientMsg::Command { id, cmd } = read_msg(&mut s).unwrap() else {
            panic!()
        };
        assert_eq!(cmd, Command::Ping);
        // Garbage payload, then the response split across two writes.
        s.write_all(&[2, 0, 0, 0, 0xff, 0xff]).unwrap();
        let mut buf = Vec::new();
        mydaemon_proto::encode(
            &ServerMsg::Response {
                id,
                resp: Response::Pong,
            },
            &mut buf,
        )
        .unwrap();
        s.write_all(&buf[..3]).unwrap();
        s.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        s.write_all(&buf[3..]).unwrap();
        std::thread::sleep(Duration::from_millis(200));
    });
    let mut h = Harness::new(&sock);
    let id = h.client.ping().unwrap();
    assert_eq!(h.response(id), Response::Pong);
}
