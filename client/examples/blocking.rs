//! cargo run -p mydaemon-client --example blocking
//!
//! Needs a running daemon (cargo run -p mydaemond). Set MYDAEMON_SOCKET to
//! use another socket path, e.g. for an unprivileged test run.

use std::thread;
use std::time::Duration;

use log::{LevelFilter, info, warn};
use mydaemon_client::blocking::Client;

fn main() -> Result<(), mydaemon_client::Error> {
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .expect("logger already initialized");

    let (client, events) = Client::connect()?;
    let client = client.with_timeout(Some(Duration::from_secs(5)));
    client.subscribe()?;

    // Events are consumed on their own thread, commands on this one.
    let listener = thread::spawn(move || {
        for ev in events.iter() {
            info!("event: {ev:?}");
        }
        warn!("event stream closed");
    });

    client.ping()?;
    for i in 0..3 {
        client.set(&format!("key{i}"), format!("value{i}").as_bytes())?;
    }
    info!("key1 = {:?}", client.get("key1")?.map(String::from_utf8));

    thread::sleep(Duration::from_millis(200));
    drop(client); // closes the connection, which ends the events iterator
    let _ = listener.join();
    Ok(())
}
