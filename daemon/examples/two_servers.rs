//! Two independent servers (own socket, store and client limit) driven by
//! one poll loop.
//!
//!   cargo run -p mydaemond --example two_servers [SOCKET_A SOCKET_B]
//!
//! Then talk to either one, e.g.
//!   MYDAEMON_SOCKET=/tmp/mydaemon/a.sock cargo run -p mydaemon-client --example sync

use std::io;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use log::{LevelFilter, error};
use mio::{Events, Poll};
use mydaemond::{Config, Server, TokenRange};

fn run(socket_a: PathBuf, socket_b: PathBuf) -> mydaemond::Result<()> {
    let cfg_a = Config {
        socket: socket_a,
        max_clients: 4,
        heartbeat: Some(Duration::from_secs(5)),
        ..Config::default()
    };
    let cfg_b = Config {
        socket: socket_b,
        max_clients: 2,
        heartbeat: Some(Duration::from_secs(7)),
        ..Config::default()
    };

    let mut poll = Poll::new()?;

    // One pool of tokens; each server carves its share off the front.
    let total = Server::required_tokens(&cfg_a) + Server::required_tokens(&cfg_b);
    let mut free = TokenRange::new(0, total);
    let mut a = Server::bind(cfg_a, poll.registry(), &mut free)?;
    let mut b = Server::bind(cfg_b, poll.registry(), &mut free)?;

    let mut events = Events::with_capacity(128);
    loop {
        let timeout = [a.deadline(), b.deadline()]
            .into_iter()
            .flatten()
            .min()
            .map(|t| t.saturating_duration_since(Instant::now()));
        match poll.poll(&mut events, timeout) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
        for event in events.iter() {
            // Each token belongs to exactly one server.
            let _ = a.handle_event(event) || b.handle_event(event);
        }
        let now = Instant::now();
        a.tick(now);
        b.tick(now);
    }
}

fn main() -> ExitCode {
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .expect("logger already initialized");

    let mut args = std::env::args_os().skip(1);
    let socket_a = args
        .next()
        .map_or_else(|| "/tmp/mydaemon/a.sock".into(), PathBuf::from);
    let socket_b = args
        .next()
        .map_or_else(|| "/tmp/mydaemon/b.sock".into(), PathBuf::from);

    match run(socket_a, socket_b) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            ExitCode::FAILURE
        }
    }
}
