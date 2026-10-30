//! cargo run -p mydaemon-client --features tokio --example async_events
//!
//! Needs a running daemon (cargo run -p mydaemond). Prints every event
//! until Ctrl-C, while pinging the daemon every few seconds.

use std::time::Duration;

use log::{LevelFilter, info, warn};
use mydaemon_client::nonblocking::Client;

#[tokio::main]
async fn main() -> Result<(), mydaemon_client::Error> {
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .expect("logger already initialized");

    let (client, mut events) = Client::connect().await?;
    let client = client.with_timeout(Some(Duration::from_secs(5)));
    client.subscribe().await?;
    client.set("hello", b"from async client").await?;

    let mut tick = tokio::time::interval(Duration::from_secs(3));
    loop {
        tokio::select! {
            ev = events.recv() => match ev {
                Ok(ev) => info!("event: {ev:?}"),
                Err(e) => { warn!("{e}"); break; }
            },
            _ = tick.tick() => {
                client.ping().await?;
                info!("ping ok");
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    Ok(())
}
