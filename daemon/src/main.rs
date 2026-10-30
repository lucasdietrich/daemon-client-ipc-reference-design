use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant};

use log::{LevelFilter, error};
use mydaemon_proto::Event;
use mydaemond::{Config, Server};

const HEARTBEAT: Duration = Duration::from_secs(10);

fn main() -> ExitCode {
    // Level defaults to Info, RUST_LOG overrides it (e.g. RUST_LOG=debug).
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .expect("logger already initialized");

    let server = match Server::bind(Config::default()) {
        Ok(s) => s,
        Err(e) => {
            error!("{e}");
            return ExitCode::FAILURE;
        }
    };

    // Example of an event source that is not triggered by a command.
    let handle = server.handle();
    let started = Instant::now();
    thread::Builder::new()
        .name("heartbeat".into())
        .spawn(move || {
            loop {
                thread::sleep(HEARTBEAT);
                handle.broadcast(Event::Heartbeat {
                    uptime_secs: started.elapsed().as_secs(),
                });
            }
        })
        .expect("spawn heartbeat thread");

    match server.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            ExitCode::FAILURE
        }
    }
}
