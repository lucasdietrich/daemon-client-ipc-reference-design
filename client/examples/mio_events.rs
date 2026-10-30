//! cargo run -p mydaemon-client --features mio --example mio_events
//!
//! Needs a running daemon (cargo run -p mydaemond). Set MYDAEMON_SOCKET to
//! use another socket path.

use std::time::Duration;

use log::{LevelFilter, info};
use mio::{Events, Interest, Poll, Token};
use mydaemon_client::Error;
use mydaemon_client::mio_client::Client;

const DAEMON: Token = Token(0);

fn main() -> Result<(), Error> {
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .expect("logger already initialized");

    let mut poll = Poll::new()?;
    let mut client = Client::connect()?;
    poll.registry()
        .register(&mut client, DAEMON, Interest::READABLE | Interest::WRITABLE)?;

    client.subscribe()?;
    client.set("key", b"value")?;

    let mut events = Events::with_capacity(16);
    loop {
        poll.poll(&mut events, Some(Duration::from_secs(30)))?;
        if events.is_empty() {
            return Ok(());
        }
        for ev in events.iter() {
            if ev.is_writable() {
                client.flush()?;
            }
            if ev.is_readable() {
                while let Some(msg) = client.recv()? {
                    info!("{msg:?}");
                }
            }
        }
    }
}
