mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{fake_daemon, start_daemon};
use mydaemon_client::r#async::Client;
use mydaemon_client::{Error, Event};
use mydaemon_proto::sync::{read_msg, write_msg};
use mydaemon_proto::{ClientMsg, ServerMsg};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn ping_set_get() {
    let d = start_daemon();
    let (c, _events) = Client::connect_to(&d.socket).await.unwrap();
    c.ping().await.unwrap();
    c.set("k", b"v").await.unwrap();
    assert_eq!(c.get("k").await.unwrap(), Some(b"v".to_vec()));
}

#[tokio::test]
async fn events_only_after_subscribe() {
    let d = start_daemon();
    let (a, mut a_events) = Client::connect_to(&d.socket).await.unwrap();
    let (b, mut b_events) = Client::connect_to(&d.socket).await.unwrap();
    a.subscribe().await.unwrap();

    b.set("x", b"1").await.unwrap();
    let ev = timeout(WAIT, a_events.recv()).await.unwrap().unwrap();
    assert_eq!(ev, Event::KeyChanged { key: "x".into() });
    assert!(
        timeout(Duration::from_millis(200), b_events.recv())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_calls_with_interleaved_events() {
    let d = start_daemon();
    let (c, mut events) = Client::connect_to(&d.socket).await.unwrap();
    let c = Arc::new(c.with_timeout(Some(WAIT)));
    c.subscribe().await.unwrap();

    let mut tasks = Vec::new();
    for t in 0..8 {
        let c = c.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..100 {
                let key = format!("t{t}-{i}");
                c.set(&key, key.as_bytes()).await.unwrap();
                assert_eq!(c.get(&key).await.unwrap().as_deref(), Some(key.as_bytes()));
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert!(events.try_recv().unwrap().is_some());
}

#[tokio::test]
async fn disconnect_wakes_caller_and_events() {
    let (_dir, sock) = fake_daemon(true, |mut s| {
        let _: ClientMsg = read_msg(&mut s).unwrap();
        write_msg(
            &mut s,
            &ServerMsg::Event(Event::KeyChanged { key: "bye".into() }),
        )
        .unwrap();
    });
    let (c, mut events) = Client::connect_to(&sock).await.unwrap();
    assert!(matches!(c.ping().await, Err(Error::Disconnected)));
    assert_eq!(
        events.recv().await.unwrap(),
        Event::KeyChanged { key: "bye".into() }
    );
    assert!(matches!(events.recv().await, Err(Error::Disconnected)));
    assert!(matches!(c.ping().await, Err(Error::Disconnected)));
}

#[tokio::test]
async fn cancelled_call_does_not_break_the_next_one() {
    let d = start_daemon();
    let (c, _events) = Client::connect_to(&d.socket).await.unwrap();
    // Cancel a call as early as possible, then make sure the stream is fine.
    let _ = timeout(Duration::from_nanos(1), c.ping()).await;
    for _ in 0..10 {
        c.ping().await.unwrap();
    }
}

#[tokio::test]
async fn version_rejected() {
    let (_dir, sock) = fake_daemon(false, |_| {});
    assert!(matches!(
        Client::connect_to(&sock).await,
        Err(Error::Rejected(_))
    ));
}
