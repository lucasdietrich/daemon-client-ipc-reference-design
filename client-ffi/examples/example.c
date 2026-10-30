/* Build and run: just client-c
 * Needs a running daemon (just daemon); honors $MYDAEMON_SOCKET.
 */
#include <poll.h>
#include <stdio.h>
#include <string.h>

#include "mydaemon.h"

static int check(int rc, const char *what)
{
    if (rc < 0) {
        const char *err = mydaemon_last_error();
        fprintf(stderr, "%s: %s\n", what, err ? err : "unknown error");
    }
    return rc;
}

static void print_msg(const mydaemon_msg_t *m)
{
    switch (m->kind) {
    case MYDAEMON_MSG_PONG:
        printf("[%u] pong\n", m->id);
        break;
    case MYDAEMON_MSG_VALUE:
        if (m->has_value)
            printf("[%u] value: %.*s\n", m->id, (int)m->len, (const char *)m->data);
        else
            printf("[%u] value: <none>\n", m->id);
        break;
    case MYDAEMON_MSG_DONE:
        printf("[%u] done\n", m->id);
        break;
    case MYDAEMON_MSG_ERROR:
        printf("[%u] error: %s\n", m->id, (const char *)m->data);
        break;
    case MYDAEMON_MSG_KEY_CHANGED:
        printf("event: key changed: %s\n", (const char *)m->data);
        break;
    case MYDAEMON_MSG_HEARTBEAT:
        printf("event: heartbeat, uptime %llus\n", (unsigned long long)m->uptime_secs);
        break;
    default:
        printf("event: unknown\n");
    }
}

int main(void)
{
    mydaemon_client_t *c;
    if (check(mydaemon_client_connect(NULL, &c), "connect") < 0)
        return 1;

    const char value[] = "value";
    if (check(mydaemon_client_subscribe(c, NULL), "subscribe") < 0 ||
        check(mydaemon_client_set(c, "key", (const uint8_t *)value, strlen(value), NULL), "set") < 0 ||
        check(mydaemon_client_get(c, "key", NULL), "get") < 0)
        goto fail;

    for (;;) {
        struct pollfd pfd = {
            .fd = mydaemon_client_fd(c),
            .events = POLLIN | (mydaemon_client_wants_write(c) ? POLLOUT : 0),
        };
        int n = poll(&pfd, 1, 30000);
        if (n <= 0)
            break; /* timeout or error */

        if ((pfd.revents & POLLOUT) && check(mydaemon_client_flush(c), "flush") < 0)
            goto fail;
        if (pfd.revents & (POLLIN | POLLHUP)) {
            mydaemon_msg_t msg;
            int rc;
            while ((rc = mydaemon_client_recv(c, &msg)) > 0)
                print_msg(&msg);
            if (check(rc, "recv") < 0)
                goto fail;
        }
    }

    mydaemon_client_free(c);
    return 0;
fail:
    mydaemon_client_free(c);
    return 1;
}
