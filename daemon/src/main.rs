use std::io;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::Parser;
use log::{LevelFilter, error};
use mio::{Events, Poll};
use mydaemond::{Config, DEFAULT_MAX_CLIENTS, Server, TokenRange};

const HEARTBEAT: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Serve at most this many clients at once.
    #[arg(
        short = 'n',
        long,
        default_value_t = DEFAULT_MAX_CLIENTS as u32,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    max_clients: u32,
}

fn main() -> ExitCode {
    let args = Args::parse();

    // Level defaults to Info, RUST_LOG overrides it (e.g. RUST_LOG=debug).
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .expect("logger already initialized");

    let cfg = Config {
        max_clients: args.max_clients as usize,
        heartbeat: Some(HEARTBEAT),
        ..Config::default()
    };

    let mut poll = match Poll::new() {
        Ok(p) => p,
        Err(e) => {
            error!("cannot create poll: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut free = TokenRange::new(0, Server::required_tokens(&cfg));
    let mut server = match Server::bind(cfg, poll.registry(), &mut free) {
        Ok(s) => s,
        Err(e) => {
            error!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let mut events = Events::with_capacity(128);
    loop {
        let timeout = server
            .deadline()
            .map(|t| t.saturating_duration_since(Instant::now()));
        match poll.poll(&mut events, timeout) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                error!("poll failed: {e}");
                return ExitCode::FAILURE;
            }
        }
        for event in events.iter() {
            server.handle_event(event);
        }
        server.tick(Instant::now());
    }
}
