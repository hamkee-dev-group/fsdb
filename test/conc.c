/*
 * test/conc.c — Same-key concurrency regression tests.
 *
 * Validates mutation contracts under concurrent access:
 *   1. UPDATE+UPDATE: last-writer-wins, no corruption, no stale reads
 *   2. UPDATE+DELETE: key is updated or deleted, never stale original
 *
 * Uses pthread barriers for tight thread synchronization to maximize
 * the chance of hitting the race window in write_file().
 */
#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/un.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#define ITERS 50

static const char *sock_path;
static const char *db = "concdb";
static pthread_barrier_t bar;

static int send_cmd(const char *cmd, char *resp, size_t rsz)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    struct timeval tv = {5, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", sock_path);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    size_t len = strlen(cmd);
    char buf[512];
    memcpy(buf, cmd, len);
    buf[len] = '\n';
    send(fd, buf, len + 1, 0);
    ssize_t n, tot = 0;
    while ((n = recv(fd, resp + tot, rsz - (size_t)tot - 1, 0)) > 0)
        tot += n;
    resp[tot] = '\0';
    while (tot > 0 && (resp[tot - 1] == '\n' || resp[tot - 1] == '\r'))
        resp[--tot] = '\0';
    close(fd);
    return 0;
}

struct targ { char cmd[256]; };

static void *racer(void *arg)
{
    struct targ *t = arg;
    char resp[256];
    pthread_barrier_wait(&bar);
    send_cmd(t->cmd, resp, sizeof(resp));
    return NULL;
}

/*
 * Contract: concurrent UPDATEs on the same key are last-writer-wins.
 * After two concurrent UPDATEs complete, GET must return one of the
 * two written values — never the original, never corrupted data.
 */
static int test_update_update(void)
{
    char cmd[256], resp[256];
    int fail = 0;

    printf("[CONC] UPDATE+UPDATE same key\n");
    for (int i = 0; i < ITERS; i++) {
        char key[32];
        snprintf(key, sizeof(key), "uu%d", i);

        snprintf(cmd, sizeof(cmd), "INSERT %s %s original", db, key);
        send_cmd(cmd, resp, sizeof(resp));

        pthread_barrier_init(&bar, NULL, 2);
        struct targ a = {{0}}, b = {{0}};
        snprintf(a.cmd, sizeof(a.cmd), "UPDATE %s %s valueA", db, key);
        snprintf(b.cmd, sizeof(b.cmd), "UPDATE %s %s valueB", db, key);

        pthread_t t1, t2;
        pthread_create(&t1, NULL, racer, &a);
        pthread_create(&t2, NULL, racer, &b);
        pthread_join(t1, NULL);
        pthread_join(t2, NULL);
        pthread_barrier_destroy(&bar);

        snprintf(cmd, sizeof(cmd), "GET %s %s", db, key);
        send_cmd(cmd, resp, sizeof(resp));
        if (strcmp(resp, "valueA") != 0 && strcmp(resp, "valueB") != 0) {
            printf("  FAIL iter %d: expected valueA|valueB, got '%s'\n",
                   i, resp);
            fail++;
        }

        snprintf(cmd, sizeof(cmd), "DELETE %s %s", db, key);
        send_cmd(cmd, resp, sizeof(resp));
    }
    if (!fail)
        printf("  PASS: %d iterations — last-writer-wins, no corruption\n",
               ITERS);
    return fail;
}

/*
 * Contract: concurrent GET during UPDATE must return either the
 * complete old value or the complete new value — never empty,
 * truncated, or mixed data.  The temp+rename write path should
 * guarantee this atomicity.
 */

#define VLEN 200
#define NREADERS 4

struct ar_reader {
    char cmd[256];
    char resp[VLEN + 64];
    int bad;         /* set by thread if response is invalid */
};

static char val_a[VLEN + 1];
static char val_b[VLEN + 1];

static void *ar_read_racer(void *arg)
{
    struct ar_reader *r = arg;
    pthread_barrier_wait(&bar);
    send_cmd(r->cmd, r->resp, sizeof(r->resp));
    /* Validate: must be all-A or all-B, exact length */
    r->bad = (strcmp(r->resp, val_a) != 0 && strcmp(r->resp, val_b) != 0);
    return NULL;
}

static int test_atomic_read(void)
{
    char cmd[VLEN + 64], resp[VLEN + 64];
    int fail = 0;

    memset(val_a, 'A', VLEN); val_a[VLEN] = '\0';
    memset(val_b, 'B', VLEN); val_b[VLEN] = '\0';

    printf("[CONC] UPDATE+GET atomic read\n");
    for (int i = 0; i < ITERS; i++) {
        char key[32];
        snprintf(key, sizeof(key), "ar%d", i);

        snprintf(cmd, sizeof(cmd), "INSERT %s %s %s", db, key, val_a);
        send_cmd(cmd, resp, sizeof(resp));

        pthread_barrier_init(&bar, NULL, NREADERS + 1);
        struct targ updater = {{0}};
        snprintf(updater.cmd, sizeof(updater.cmd),
                 "UPDATE %s %s %s", db, key, val_b);

        struct ar_reader readers[NREADERS];
        for (int r = 0; r < NREADERS; r++) {
            memset(&readers[r], 0, sizeof(readers[r]));
            snprintf(readers[r].cmd, sizeof(readers[r].cmd),
                     "GET %s %s", db, key);
        }

        pthread_t tu, tr[NREADERS];
        pthread_create(&tu, NULL, racer, &updater);
        for (int r = 0; r < NREADERS; r++)
            pthread_create(&tr[r], NULL, ar_read_racer, &readers[r]);
        pthread_join(tu, NULL);
        for (int r = 0; r < NREADERS; r++)
            pthread_join(tr[r], NULL);
        pthread_barrier_destroy(&bar);

        for (int r = 0; r < NREADERS; r++) {
            if (readers[r].bad) {
                printf("  FAIL iter %d reader %d: got len=%zu '%.*s...'\n",
                       i, r, strlen(readers[r].resp), 20, readers[r].resp);
                fail++;
            }
        }

        snprintf(cmd, sizeof(cmd), "DELETE %s %s", db, key);
        send_cmd(cmd, resp, sizeof(resp));
    }
    if (!fail)
        printf("  PASS: %d iterations — readers see only complete values\n",
               ITERS);
    return fail;
}

/*
 * Contract: concurrent UPDATE + DELETE on the same key must not
 * resurrect stale data.  After both complete, GET must return either
 * the updated value or "not found" — never the original value.
 * (The TOCTOU in write_file() means UPDATE can win after DELETE;
 * that is acceptable — resurrection of the *new* value is fine.)
 */
static int test_update_delete(void)
{
    char cmd[256], resp[256];
    int fail = 0;

    printf("[CONC] UPDATE+DELETE same key\n");
    for (int i = 0; i < ITERS; i++) {
        char key[32];
        snprintf(key, sizeof(key), "ud%d", i);

        snprintf(cmd, sizeof(cmd), "INSERT %s %s original", db, key);
        send_cmd(cmd, resp, sizeof(resp));

        pthread_barrier_init(&bar, NULL, 2);
        struct targ a = {{0}}, b = {{0}};
        snprintf(a.cmd, sizeof(a.cmd), "UPDATE %s %s updated", db, key);
        snprintf(b.cmd, sizeof(b.cmd), "DELETE %s %s", db, key);

        pthread_t t1, t2;
        pthread_create(&t1, NULL, racer, &a);
        pthread_create(&t2, NULL, racer, &b);
        pthread_join(t1, NULL);
        pthread_join(t2, NULL);
        pthread_barrier_destroy(&bar);

        snprintf(cmd, sizeof(cmd), "GET %s %s", db, key);
        send_cmd(cmd, resp, sizeof(resp));
        if (strcmp(resp, "updated") != 0 && strstr(resp, "ERR") == NULL) {
            printf("  FAIL iter %d: expected updated|ERR, got '%s'\n",
                   i, resp);
            fail++;
        }

        snprintf(cmd, sizeof(cmd), "DELETE %s %s", db, key);
        send_cmd(cmd, resp, sizeof(resp));
    }
    if (!fail)
        printf("  PASS: %d iterations — no stale resurrection\n", ITERS);
    return fail;
}

int main(void)
{
    sock_path = getenv("FSDB_SOCKET");
    if (!sock_path)
        sock_path = "/var/run/fsdb.sock";

    char resp[256];
    send_cmd("CREATE concdb", resp, sizeof(resp));

    int fail = 0;
    fail += test_update_update();
    fail += test_update_delete();
    fail += test_atomic_read();

    if (fail) {
        printf("FAILED: %d contract violations\n", fail);
        return 1;
    }
    printf("All concurrency contracts verified\n");
    return 0;
}
