//! C ABI over [`mydaemon_client::mio_client::Client`].
//!
//! The matching header is `include/mydaemon.h`; keep both in sync.
//! Every function that takes a handle requires a pointer obtained from
//! [`mydaemon_client_connect`] that was not yet freed.

#![allow(non_camel_case_types)]

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_int};
use std::os::fd::AsRawFd;
use std::ptr;

use mydaemon_client::mio_client::{Client, Incoming};
use mydaemon_client::{Error, Event, Response, Result};

pub const MYDAEMON_OK: c_int = 0;
pub const MYDAEMON_E_INVALID_ARG: c_int = -1;
pub const MYDAEMON_E_IO: c_int = -2;
pub const MYDAEMON_E_PROTO: c_int = -3;
pub const MYDAEMON_E_REJECTED: c_int = -4;
pub const MYDAEMON_E_HANDSHAKE: c_int = -5;
pub const MYDAEMON_E_DISCONNECTED: c_int = -6;
pub const MYDAEMON_E_DAEMON: c_int = -7;
pub const MYDAEMON_E_UNEXPECTED: c_int = -8;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(msg: &str) {
    // Interior NULs cannot be represented; drop them rather than lose the message.
    let msg = CString::new(msg.replace('\0', "")).unwrap_or_default();
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(msg));
}

fn fail(code: c_int, msg: &str) -> c_int {
    set_last_error(msg);
    code
}

fn error_code(e: &Error) -> c_int {
    match e {
        Error::Io(_) => MYDAEMON_E_IO,
        Error::Proto(_) => MYDAEMON_E_PROTO,
        Error::Rejected(_) => MYDAEMON_E_REJECTED,
        Error::Handshake => MYDAEMON_E_HANDSHAKE,
        Error::Disconnected => MYDAEMON_E_DISCONNECTED,
        Error::Daemon(_) => MYDAEMON_E_DAEMON,
        Error::Unexpected(_) => MYDAEMON_E_UNEXPECTED,
        // Timeout is never produced by the mio flavor.
        _ => MYDAEMON_E_IO,
    }
}

fn fail_with(e: &Error) -> c_int {
    fail(error_code(e), &e.to_string())
}

/// Opaque handle.
pub struct mydaemon_client_t {
    client: Client,
    /// Payload of the last message handed out by `recv`, NUL terminated.
    payload: Vec<u8>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum mydaemon_msg_kind_t {
    Pong = 0,
    Value = 1,
    Done = 2,
    Error = 3,
    KeyChanged = 4,
    Heartbeat = 5,
    UnknownEvent = 6,
}

#[repr(C)]
pub struct mydaemon_msg_t {
    pub kind: mydaemon_msg_kind_t,
    pub id: u32,
    pub has_value: bool,
    pub uptime_secs: u64,
    pub data: *const u8,
    pub len: usize,
}

unsafe fn handle<'a>(client: *mut mydaemon_client_t) -> Option<&'a mut mydaemon_client_t> {
    unsafe { client.as_mut() }
}

unsafe fn str_arg<'a>(s: *const c_char) -> Option<&'a str> {
    if s.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(s) }.to_str().ok()
}

/// Run a command call: validate the handle, store the id, map the error.
unsafe fn command(
    client: *mut mydaemon_client_t,
    id: *mut u32,
    f: impl FnOnce(&mut Client) -> Result<u32>,
) -> c_int {
    let Some(h) = (unsafe { handle(client) }) else {
        return fail(MYDAEMON_E_INVALID_ARG, "null client handle");
    };
    match f(&mut h.client) {
        Ok(new_id) => {
            if !id.is_null() {
                unsafe { *id = new_id };
            }
            MYDAEMON_OK
        }
        Err(e) => fail_with(&e),
    }
}

/// # Safety
/// `path` is NULL or a NUL terminated string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_connect(
    path: *const c_char,
    out: *mut *mut mydaemon_client_t,
) -> c_int {
    if out.is_null() {
        return fail(MYDAEMON_E_INVALID_ARG, "null out pointer");
    }
    unsafe { *out = ptr::null_mut() };

    let result = if path.is_null() {
        Client::connect()
    } else {
        match unsafe { str_arg(path) } {
            Some(p) => Client::connect_to(p),
            None => return fail(MYDAEMON_E_INVALID_ARG, "path is not valid UTF-8"),
        }
    };
    match result {
        Ok(client) => {
            let boxed = Box::new(mydaemon_client_t {
                client,
                payload: Vec::new(),
            });
            unsafe { *out = Box::into_raw(boxed) };
            MYDAEMON_OK
        }
        Err(e) => fail_with(&e),
    }
}

/// # Safety
/// `client` is NULL or a live handle; it must not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_free(client: *mut mydaemon_client_t) {
    if !client.is_null() {
        drop(unsafe { Box::from_raw(client) });
    }
}

/// # Safety
/// `client` is a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_fd(client: *const mydaemon_client_t) -> c_int {
    match unsafe { client.as_ref() } {
        Some(h) => h.client.as_raw_fd(),
        None => fail(MYDAEMON_E_INVALID_ARG, "null client handle"),
    }
}

/// # Safety
/// `client` is a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_wants_write(client: *const mydaemon_client_t) -> bool {
    unsafe { client.as_ref() }.is_some_and(|h| h.client.wants_write())
}

/// # Safety
/// `client` is a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_flush(client: *mut mydaemon_client_t) -> c_int {
    let Some(h) = (unsafe { handle(client) }) else {
        return fail(MYDAEMON_E_INVALID_ARG, "null client handle");
    };
    match h.client.flush() {
        Ok(()) => MYDAEMON_OK,
        Err(e) => fail_with(&e),
    }
}

/// # Safety
/// `client` is a live handle; `id` is NULL or valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_ping(
    client: *mut mydaemon_client_t,
    id: *mut u32,
) -> c_int {
    unsafe { command(client, id, |c| c.ping()) }
}

/// # Safety
/// `client` is a live handle; `key` is a NUL terminated UTF-8 string; `id` is
/// NULL or valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_get(
    client: *mut mydaemon_client_t,
    key: *const c_char,
    id: *mut u32,
) -> c_int {
    let Some(key) = (unsafe { str_arg(key) }) else {
        return fail(MYDAEMON_E_INVALID_ARG, "key is null or not valid UTF-8");
    };
    unsafe { command(client, id, |c| c.get(key)) }
}

/// # Safety
/// `client` is a live handle; `key` is a NUL terminated UTF-8 string; `value`
/// points to `value_len` readable bytes (it may be NULL when `value_len` is 0);
/// `id` is NULL or valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_set(
    client: *mut mydaemon_client_t,
    key: *const c_char,
    value: *const u8,
    value_len: usize,
    id: *mut u32,
) -> c_int {
    let Some(key) = (unsafe { str_arg(key) }) else {
        return fail(MYDAEMON_E_INVALID_ARG, "key is null or not valid UTF-8");
    };
    if value.is_null() && value_len != 0 {
        return fail(MYDAEMON_E_INVALID_ARG, "null value with non-zero length");
    }
    let value: &[u8] = if value_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(value, value_len) }
    };
    unsafe { command(client, id, |c| c.set(key, value)) }
}

/// # Safety
/// `client` is a live handle; `id` is NULL or valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_subscribe(
    client: *mut mydaemon_client_t,
    id: *mut u32,
) -> c_int {
    unsafe { command(client, id, |c| c.subscribe()) }
}

/// # Safety
/// `client` is a live handle; `id` is NULL or valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_unsubscribe(
    client: *mut mydaemon_client_t,
    id: *mut u32,
) -> c_int {
    unsafe { command(client, id, |c| c.unsubscribe()) }
}

fn fill(h: &mut mydaemon_client_t, msg: Incoming) -> mydaemon_msg_t {
    let mut out = mydaemon_msg_t {
        kind: mydaemon_msg_kind_t::UnknownEvent,
        id: 0,
        has_value: false,
        uptime_secs: 0,
        data: ptr::null(),
        len: 0,
    };
    let mut payload: Option<Vec<u8>> = None;
    match msg {
        Incoming::Response { id, resp } => {
            out.id = id;
            match resp {
                Response::Pong => out.kind = mydaemon_msg_kind_t::Pong,
                Response::Done => out.kind = mydaemon_msg_kind_t::Done,
                Response::Value(v) => {
                    out.kind = mydaemon_msg_kind_t::Value;
                    out.has_value = v.is_some();
                    payload = v;
                }
                Response::Error(text) => {
                    out.kind = mydaemon_msg_kind_t::Error;
                    payload = Some(text.into_bytes());
                }
            }
        }
        Incoming::Event(Event::KeyChanged { key }) => {
            out.kind = mydaemon_msg_kind_t::KeyChanged;
            payload = Some(key.into_bytes());
        }
        Incoming::Event(Event::Heartbeat { uptime_secs }) => {
            out.kind = mydaemon_msg_kind_t::Heartbeat;
            out.uptime_secs = uptime_secs;
        }
        Incoming::Event(_) => {}
    }
    if let Some(mut bytes) = payload {
        out.len = bytes.len();
        bytes.push(0);
        h.payload = bytes;
        out.data = h.payload.as_ptr();
    }
    out
}

/// # Safety
/// `client` is a live handle; `msg` is valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mydaemon_client_recv(
    client: *mut mydaemon_client_t,
    msg: *mut mydaemon_msg_t,
) -> c_int {
    let Some(h) = (unsafe { handle(client) }) else {
        return fail(MYDAEMON_E_INVALID_ARG, "null client handle");
    };
    if msg.is_null() {
        return fail(MYDAEMON_E_INVALID_ARG, "null msg pointer");
    }
    match h.client.recv() {
        Ok(Some(incoming)) => {
            unsafe { msg.write(fill(h, incoming)) };
            1
        }
        Ok(None) => 0,
        Err(e) => fail_with(&e),
    }
}

/// Pointer is valid until the next failing call on the same thread.
#[unsafe(no_mangle)]
pub extern "C" fn mydaemon_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ref().map_or(ptr::null(), |s| s.as_ptr()))
}
