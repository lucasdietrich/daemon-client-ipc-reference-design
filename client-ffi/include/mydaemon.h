/* C API of libmydaemon_ffi (mio flavor of mydaemon-client).
 *
 * The client never blocks after the handshake and owns no thread: poll
 * mydaemon_client_fd() with poll/epoll/select, call mydaemon_client_flush()
 * when it is writable and mydaemon_client_recv() until it returns 0 when it
 * is readable. Always drain recv() until it returns 0.
 *
 * Return values: 0 or a positive count on success, a negative MYDAEMON_E_*
 * on failure. mydaemon_last_error() describes the last failure of the
 * calling thread. Handles are not thread safe; use one per thread or lock.
 */
#ifndef MYDAEMON_H
#define MYDAEMON_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MYDAEMON_OK 0
#define MYDAEMON_E_INVALID_ARG (-1)
#define MYDAEMON_E_IO (-2)
#define MYDAEMON_E_PROTO (-3)
#define MYDAEMON_E_REJECTED (-4)
#define MYDAEMON_E_HANDSHAKE (-5)
#define MYDAEMON_E_DISCONNECTED (-6)
#define MYDAEMON_E_DAEMON (-7)
#define MYDAEMON_E_UNEXPECTED (-8)

typedef struct mydaemon_client mydaemon_client_t;

typedef enum mydaemon_msg_kind {
    /* Responses: `id` matches the id returned by the command call. */
    MYDAEMON_MSG_PONG = 0,
    /* `has_value` tells whether the key exists; `data`/`len` hold the value. */
    MYDAEMON_MSG_VALUE = 1,
    MYDAEMON_MSG_DONE = 2,
    /* `data`/`len` hold the daemon's error text. */
    MYDAEMON_MSG_ERROR = 3,
    /* Events (only after mydaemon_client_subscribe()); `id` is unused. */
    /* `data`/`len` hold the key name. */
    MYDAEMON_MSG_KEY_CHANGED = 4,
    /* `uptime_secs` is set. */
    MYDAEMON_MSG_HEARTBEAT = 5,
    /* Event added by a newer daemon; ignore it. */
    MYDAEMON_MSG_UNKNOWN_EVENT = 6
} mydaemon_msg_kind_t;

typedef struct mydaemon_msg {
    mydaemon_msg_kind_t kind;
    uint32_t id;
    bool has_value;
    uint64_t uptime_secs;
    /* Always NUL terminated (len excludes the terminator); NULL when unused.
     * Valid until the next mydaemon_client_recv() or mydaemon_client_free()
     * on the same handle. */
    const uint8_t *data;
    size_t len;
} mydaemon_msg_t;

/* Connect and do the handshake (blocking). `path` may be NULL: then
 * $MYDAEMON_SOCKET or the built-in default is used. */
int mydaemon_client_connect(const char *path, mydaemon_client_t **out);

/* Accepts NULL. */
void mydaemon_client_free(mydaemon_client_t *client);

/* Socket descriptor to poll. Owned by the handle: do not close it. */
int mydaemon_client_fd(const mydaemon_client_t *client);

/* True while queued bytes wait for the socket to become writable. */
bool mydaemon_client_wants_write(const mydaemon_client_t *client);

/* Write queued bytes until done or the socket would block. */
int mydaemon_client_flush(mydaemon_client_t *client);

/* Command calls queue a request and try to send it. On success the request
 * id is stored in `*id` (may be NULL if not needed). */
int mydaemon_client_ping(mydaemon_client_t *client, uint32_t *id);
int mydaemon_client_get(mydaemon_client_t *client, const char *key, uint32_t *id);
int mydaemon_client_set(mydaemon_client_t *client, const char *key,
                        const uint8_t *value, size_t value_len, uint32_t *id);
int mydaemon_client_subscribe(mydaemon_client_t *client, uint32_t *id);
int mydaemon_client_unsubscribe(mydaemon_client_t *client, uint32_t *id);

/* Next message: 1 and `*msg` filled, 0 when nothing is available (would
 * block), negative on error. MYDAEMON_E_DISCONNECTED is returned once the
 * daemon is gone and all buffered messages were returned. */
int mydaemon_client_recv(mydaemon_client_t *client, mydaemon_msg_t *msg);

/* Description of the last error on this thread, or NULL. Valid until the next
 * failing call on the same thread. */
const char *mydaemon_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* MYDAEMON_H */
