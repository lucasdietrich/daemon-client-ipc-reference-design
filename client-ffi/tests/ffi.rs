use std::ffi::{CStr, CString, c_char};
use std::ptr;
use std::thread;
use std::time::{Duration, Instant};

use mio::{Events, Poll};
use mydaemon_ffi::*;
use mydaemond::{Config, Server, TokenRange};

fn start_daemon() -> (tempfile::TempDir, CString) {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("mydaemon.sock");
    let cfg = Config {
        socket: socket.clone(),
        ..Config::default()
    };
    let poll = Poll::new().unwrap();
    let mut free = TokenRange::new(0, Server::required_tokens(&cfg));
    let server = Server::bind(cfg, poll.registry(), &mut free).unwrap();
    thread::spawn(move || run_loop(server, poll));
    let path = CString::new(socket.to_str().unwrap()).unwrap();
    (dir, path)
}

fn run_loop(mut server: Server, mut poll: Poll) {
    let mut events = Events::with_capacity(128);
    loop {
        let timeout = server
            .deadline()
            .map(|t| t.saturating_duration_since(Instant::now()));
        match poll.poll(&mut events, timeout) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("poll failed: {e}"),
        }
        for event in events.iter() {
            server.handle_event(event);
        }
        server.tick(Instant::now());
    }
}

fn connect(path: &CStr) -> *mut mydaemon_client_t {
    let mut c = ptr::null_mut();
    assert_eq!(unsafe { mydaemon_client_connect(path.as_ptr(), &mut c) }, 0);
    assert!(!c.is_null());
    c
}

fn next_msg(c: *mut mydaemon_client_t) -> mydaemon_msg_t {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert_eq!(unsafe { mydaemon_client_flush(c) }, 0);
        let mut msg = std::mem::MaybeUninit::<mydaemon_msg_t>::uninit();
        match unsafe { mydaemon_client_recv(c, msg.as_mut_ptr()) } {
            1 => return unsafe { msg.assume_init() },
            0 => {
                assert!(Instant::now() < deadline, "timed out");
                assert!(unsafe { mydaemon_client_fd(c) } >= 0);
                thread::sleep(Duration::from_millis(5));
            }
            rc => panic!("recv failed: {rc}"),
        }
    }
}

fn payload(m: &mydaemon_msg_t) -> &[u8] {
    unsafe { std::slice::from_raw_parts(m.data, m.len) }
}

#[test]
fn ping_set_get() {
    let (_dir, path) = start_daemon();
    let c = connect(&path);
    let key = CString::new("k").unwrap();

    let (mut ping, mut set, mut get) = (0, 0, 0);
    unsafe {
        assert_eq!(mydaemon_client_ping(c, &mut ping), 0);
        assert_eq!(
            mydaemon_client_set(c, key.as_ptr(), b"v1".as_ptr(), 2, &mut set),
            0
        );
        assert_eq!(mydaemon_client_get(c, key.as_ptr(), &mut get), 0);
    }

    let m = next_msg(c);
    assert_eq!((m.kind, m.id), (mydaemon_msg_kind_t::Pong, ping));
    let m = next_msg(c);
    assert_eq!((m.kind, m.id), (mydaemon_msg_kind_t::Done, set));
    let m = next_msg(c);
    assert_eq!((m.kind, m.id), (mydaemon_msg_kind_t::Value, get));
    assert!(m.has_value);
    assert_eq!(payload(&m), b"v1");
    assert_eq!(
        unsafe { *m.data.add(m.len) },
        0,
        "payload is NUL terminated"
    );

    unsafe { mydaemon_client_free(c) };
}

#[test]
fn missing_key_has_no_value() {
    let (_dir, path) = start_daemon();
    let c = connect(&path);
    let key = CString::new("absent").unwrap();
    unsafe { assert_eq!(mydaemon_client_get(c, key.as_ptr(), ptr::null_mut()), 0) };
    let m = next_msg(c);
    assert_eq!(m.kind, mydaemon_msg_kind_t::Value);
    assert!(!m.has_value);
    unsafe { mydaemon_client_free(c) };
}

#[test]
fn subscriber_gets_key_changed_event() {
    let (_dir, path) = start_daemon();
    let c = connect(&path);
    let key = CString::new("watched").unwrap();
    unsafe {
        assert_eq!(mydaemon_client_subscribe(c, ptr::null_mut()), 0);
        assert_eq!(
            mydaemon_client_set(c, key.as_ptr(), b"x".as_ptr(), 1, ptr::null_mut()),
            0
        );
    }
    loop {
        let m = next_msg(c);
        if m.kind == mydaemon_msg_kind_t::KeyChanged {
            assert_eq!(payload(&m), b"watched");
            break;
        }
    }
    unsafe { mydaemon_client_free(c) };
}

#[test]
fn connect_failure_sets_last_error() {
    let path = CString::new("/nonexistent/mydaemon.sock").unwrap();
    let mut c = ptr::null_mut();
    let rc = unsafe { mydaemon_client_connect(path.as_ptr(), &mut c) };
    assert_eq!(rc, MYDAEMON_E_IO);
    assert!(c.is_null());
    let err = mydaemon_last_error();
    assert!(!err.is_null());
    assert!(
        !unsafe { CStr::from_ptr(err as *const c_char) }
            .to_bytes()
            .is_empty()
    );
}

#[test]
fn null_arguments_are_rejected() {
    unsafe {
        assert_eq!(
            mydaemon_client_connect(ptr::null(), ptr::null_mut()),
            MYDAEMON_E_INVALID_ARG
        );
        assert_eq!(
            mydaemon_client_ping(ptr::null_mut(), ptr::null_mut()),
            MYDAEMON_E_INVALID_ARG
        );
        assert_eq!(
            mydaemon_client_flush(ptr::null_mut()),
            MYDAEMON_E_INVALID_ARG
        );
        mydaemon_client_free(ptr::null_mut());
    }
}
