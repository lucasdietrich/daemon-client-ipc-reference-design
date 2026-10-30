//! Shows how messages look on the wire.
//!
//!   cargo run -p mydaemon-proto --example hexdump
//!
//! Each frame is a `u32` little-endian payload length followed by the
//! postcard payload: the enum variant index first, then the fields.

use mydaemon_proto::{ClientMsg, Command, Event, PROTO_VERSION, Response, ServerMsg, encode};
use serde::Serialize;

fn show<T: Serialize + std::fmt::Debug>(msg: &T) {
    let mut frame = Vec::new();
    encode(msg, &mut frame).expect("encode");
    println!("{msg:?}  ({} bytes)", frame.len());
    hexdump::hexdump(&frame);
    println!();
}

fn main() {
    println!("== client -> daemon ==\n");
    show(&ClientMsg::Hello {
        version: PROTO_VERSION,
    });
    for (id, cmd) in [
        Command::Ping,
        Command::Get {
            key: "answer".into(),
        },
        Command::Set {
            key: "answer".into(),
            value: b"42".to_vec(),
        },
        Command::Subscribe,
        Command::Unsubscribe,
    ]
    .into_iter()
    .enumerate()
    {
        show(&ClientMsg::Command { id: id as u32, cmd });
    }

    println!("== daemon -> client ==\n");
    show(&ServerMsg::Welcome {
        version: PROTO_VERSION,
    });
    show(&ServerMsg::Rejected {
        reason: "too many clients (max 16)".into(),
    });
    for (id, resp) in [
        Response::Pong,
        Response::Value(Some(b"42".to_vec())),
        Response::Value(None),
        Response::Done,
        Response::Error("empty key".into()),
    ]
    .into_iter()
    .enumerate()
    {
        show(&ServerMsg::Response {
            id: id as u32,
            resp,
        });
    }
    show(&ServerMsg::Event(Event::KeyChanged {
        key: "answer".into(),
    }));
    show(&ServerMsg::Event(Event::Heartbeat { uptime_secs: 300 }));
}
