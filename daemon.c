#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <ctype.h>
#include <stdint.h>
#include <dirent.h>
#include <time.h>
#include <poll.h>
#include <syslog.h>

#define QUEUE_SIZE 1024
#define MAX_EVENTS 64
#define MAX_ID 256
#define MAX_DATA 1024
#define MAX_CMD (64 + MAX_ID + MAX_ID + MAX_DATA)
#define MAX_PATH_LEN 1024

#define SOCKET_PATH "/tmp/fsdb.sock"
#define DB_FOLDER "/var/lib/fsdb"
#define LOG_FOLDER "/var/log"
#define PIDFILE_PATH "/var/run/fsdb.pid"
static int server_fd;
static int epoll_fd;
static int audit_fd;
static int pid_fd = -1;

static int client_queue[QUEUE_SIZE];
static int queue_head = 0, queue_tail = 0;
static pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;

volatile sig_atomic_t running = 1;

void handle_signal(int sig)
{
    (void)sig;
    running = 0;
    pthread_cond_broadcast(&queue_cond);
}

void cleanup_socket(void)
{
    unlink(SOCKET_PATH);
    if (server_fd >= 0)
        close(server_fd);
    if (epoll_fd >= 0)
        close(epoll_fd);
    if (audit_fd >= 0)
        close(audit_fd);
    if (pid_fd >= 0)
    {
        close(pid_fd);
        unlink(PIDFILE_PATH);
    }
}

void log_sys(const char *msg)
{
    syslog(LOG_ERR, "%s: %s", msg, strerror(errno));
}
int lock_file(int fd, short type)
{
    struct flock fl = {.l_type = type, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};
    return fcntl(fd, F_SETLKW, &fl);
}

int unlock_file(int fd)
{
    struct flock fl = {.l_type = F_UNLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};
    return fcntl(fd, F_SETLK, &fl);
}

void build_data_path(char *path, size_t size, const char *db, const char *id)
{
    if (id[1])
    {
        snprintf(path, size, "%s/%s/%c/%c/%s", DB_FOLDER, db, id[0], id[1], id);
    }
    else
    {
        snprintf(path, size, "%s/%s/_/%s", DB_FOLDER, db, id);
    }
}

int write_file(const char *db, const char *id, const char *data, int update)
{
    char path[MAX_PATH_LEN];
    build_data_path(path, sizeof(path), db, id);

    if (!update)
    {
        struct stat st;
        if (stat(path, &st) == 0)
        {
            errno = EEXIST;
            return -1;
        }
    }

    int flags = O_WRONLY | O_CREAT | (update ? O_TRUNC : O_EXCL);
    int fd = open(path, flags, 0600);
    if (fd < 0)
        return -1;

    if (lock_file(fd, F_WRLCK) != 0)
    {
        close(fd);
        return -1;
    }

    size_t len = strlen(data);
    ssize_t written = write(fd, data, len);
    if (written < 0 || (size_t)written < len)
    {
        unlock_file(fd);
        close(fd);
        return -1;
    }
    unlock_file(fd);
    fdatasync(fd);
    close(fd);
    return 0;
}

int read_file(const char *db, const char *id, char *out)
{
    char path[MAX_PATH_LEN];
    build_data_path(path, sizeof(path), db, id);
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;

    if (lock_file(fd, F_RDLCK) != 0)
    {
        close(fd);
        return -1;
    }

    ssize_t n = read(fd, out, MAX_DATA);
    if (n < 0)
    {
        unlock_file(fd);
        close(fd);
        return -1;
    }
    out[n] = '\0';
    unlock_file(fd);
    close(fd);
    return 0;
}

int delete_file(const char *db, const char *id)
{
    char path[MAX_PATH_LEN];
    build_data_path(path, sizeof(path), db, id);
    return unlink(path);
}

int check_file(const char *db, const char *id)
{
    char path[MAX_PATH_LEN];
    build_data_path(path, sizeof(path), db, id);
    return access(path, F_OK);
}

int touch_file(const char *db, const char *id)
{
    char path[MAX_PATH_LEN];
    build_data_path(path, sizeof(path), db, id);
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0)
        return -1;
    close(fd);
    return 0;
}

void audit_log(const char *cmd, const char *id, const char *status)
{
    time_t now = time(NULL);
    struct tm tm;
    gmtime_r(&now, &tm);

    char timestr[32];
    strftime(timestr, sizeof(timestr), "%Y-%m-%dT%H:%M:%SZ", &tm);

    char log_entry[1024];
    snprintf(log_entry, sizeof(log_entry), "%s %s %s %s\n", timestr, cmd, id, status);

    struct flock fl = {.l_type = F_WRLCK, .l_whence = SEEK_SET};
    if (fcntl(audit_fd, F_SETLKW, &fl) == -1)
    {
        log_sys("audit_log: lock failed");
        return;
    }

    size_t len = strlen(log_entry);
    ssize_t written = write(audit_fd, log_entry, len);
    if (written < 0)
    {
        log_sys("audit_log: write failed");
    }
    else if ((size_t)written < len)
    {
        syslog(LOG_WARNING, "audit_log: partial write (%zd/%zu)", written, len);
    }

    fl.l_type = F_UNLCK;
    if (fcntl(audit_fd, F_SETLK, &fl) == -1)
    {
        log_sys("audit_log: unlock failed");
    }

    fdatasync(audit_fd);
}

int sanitize_id(char *id)
{
    size_t i = 0;
    for (; i < MAX_ID - 1 && id[i]; ++i)
    {
        if (!isalnum((unsigned char)id[i]))
        {
            return 0;
        }
    }
    id[i] = '\0';
    return 1;
}

int enqueue_client(int fd)
{
    int err = pthread_mutex_lock(&queue_lock);
    if (err != 0)
    {
        errno = err;
        log_sys("pthread_mutex_lock (enqueue)");
        close(fd);
        return -1;
    }

    int next = (queue_tail + 1) % QUEUE_SIZE;
    if (next == queue_head)
    {
        pthread_mutex_unlock(&queue_lock);
        return -1;
    }

    client_queue[queue_tail] = fd;
    queue_tail = next;

    err = pthread_cond_signal(&queue_cond);
    if (err != 0)
    {
        errno = err;
        log_sys("pthread_cond_signal");
    }

    err = pthread_mutex_unlock(&queue_lock);
    if (err != 0)
    {
        errno = err;
        log_sys("pthread_mutex_unlock (enqueue)");
        running = 0;
        pthread_cond_broadcast(&queue_cond);
        return -1;
    }

    return 0;
}

int dequeue_client(void)
{
    int err = pthread_mutex_lock(&queue_lock);
    if (err != 0)
    {
        errno = err;
        log_sys("pthread_mutex_lock (dequeue)");
        return -1;
    }

    while (queue_head == queue_tail && running)
    {
        err = pthread_cond_wait(&queue_cond, &queue_lock);
        if (err != 0)
        {
            errno = err;
            log_sys("pthread_cond_wait");
            running = 0;
            pthread_mutex_unlock(&queue_lock);
            return -1;
        }
    }

    if (!running)
    {
        pthread_mutex_unlock(&queue_lock);
        return -1;
    }

    int fd = client_queue[queue_head];
    queue_head = (queue_head + 1) % QUEUE_SIZE;

    err = pthread_mutex_unlock(&queue_lock);
    if (err != 0)
    {
        errno = err;
        log_sys("pthread_mutex_unlock (dequeue)");
        running = 0;
        pthread_cond_broadcast(&queue_cond);
        return -1;
    }

    return fd;
}
int generate_structure(const char *db)
{
    char base_path[MAX_PATH_LEN];
    snprintf(base_path, sizeof(base_path), "%s/%s", DB_FOLDER, db);
    mkdir(base_path, 0700);
    char special_path[MAX_PATH_LEN];
    snprintf(special_path, sizeof(special_path), "%s/_", base_path);
    if(mkdir(special_path, 0700) == -1 && errno != EEXIST)
    {
        log_sys("mkdir special path");
        return 0;
    }
    for (char a = '0'; a <= 'z'; ++a)
    {
        if (!isalnum((unsigned char)a))
            continue;

        char level1[MAX_PATH_LEN];
        snprintf(level1, sizeof(level1), "%s/%c", base_path, a);
        struct stat st1;
        if (stat(level1, &st1) == -1)
        {
            if (mkdir(level1, 0700) == -1 && errno != EEXIST)
            {
                log_sys("mkdir level1");
                return 0;
            }
        }

        for (char b = '0'; b <= 'z'; ++b)
        {
            if (!isalnum((unsigned char)b))
                continue;

            char level2[MAX_PATH_LEN];
            snprintf(level2, sizeof(level2), "%s/%c", level1, b);
            struct stat st2;
            if (stat(level2, &st2) == -1)
            {
                if (mkdir(level2, 0700) == -1 && errno != EEXIST)
                {
                    log_sys("mkdir level2");
                    return 0;
                }
            }
        }
    }
    return 1;
}

void *worker_thread(void *arg)
{
    (void)arg;
    while (running)
    {
        int client_fd = dequeue_client();
        if (client_fd < 0)
            break;

        struct pollfd pfd = {.fd = client_fd, .events = POLLIN};
        if (poll(&pfd, 1, 1000) <= 0)
        {
            shutdown(client_fd, SHUT_RDWR);
            close(client_fd);
            continue;
        }

        char buf[MAX_CMD + 1] = {0};
        ssize_t len = recv(client_fd, buf, MAX_CMD, 0);
        if (len <= 0)
        {
            shutdown(client_fd, SHUT_RDWR);
            close(client_fd);
            continue;
        }

        char *saveptr = NULL;
        const char *cmd = strtok_r(buf, " ", &saveptr);
        char *db = strtok_r(NULL, " ", &saveptr);
        char *id = strtok_r(NULL, " ", &saveptr);
        const char *data = strtok_r(NULL, "", &saveptr);

        if (!cmd || !db)
        {
            send(client_fd, "ERR invalid", 11, 0);
            shutdown(client_fd, SHUT_RDWR);
            close(client_fd);
            continue;
        }

        if (strcmp(cmd, "CREATE") == 0)
        {
            if (!sanitize_id(db) || !generate_structure(db))
            {
                send(client_fd, "ERR CREATE", 10, 0);
            }
            else
            {
                send(client_fd, "OK", 2, 0);
            }
            close(client_fd);
            continue;
        }

        if (!id || !sanitize_id(id))
        {
            send(client_fd, "ERR invalid ID", 14, 0);
            shutdown(client_fd, SHUT_RDWR);
            close(client_fd);
            continue;
        }
        if (strcmp(cmd, "INSERT") == 0)
        {
            if (!data)
            {
                send(client_fd, "ERR data missing", 17, 0);
                audit_log("INSERT", id, "ERROR_MISSING");
            }
            else
            {
                int res = write_file(db, id, data, 0);
                if (res == 0)
                    send(client_fd, "OK", 2, 0);
                else
                {
                    int err = errno;
                    if (err == EEXIST)
                        send(client_fd, "ERR already exists", 19, 0);
                    else
                    {
                        send(client_fd, "ERR write error", 16, 0);
                        audit_log("INSERT", id, "ERROR_WRITE_FAIL");
                    }
                }
            }
        }
        else if (strcmp(cmd, "TOUCH") == 0)
        {
            if (touch_file(db, id) == 0)
                send(client_fd, "OK", 2, 0);
            else if (errno == EEXIST)
                send(client_fd, "ERR already exists", 19, 0);
            else
                send(client_fd, "ERR touch failed", 17, 0);
        }
        else if (strcmp(cmd, "EXISTS") == 0)
        {
            if (check_file(db, id) == 0)
                send(client_fd, "Y", 1, 0);
            else
                send(client_fd, "N", 1, 0);
        }
        else if (strcmp(cmd, "UPDATE") == 0)
        {
            if (!data)
            {
                send(client_fd, "ERR data missing", 16, 0);
            }
            else if (write_file(db, id, data, 1) == 0)
            {
                send(client_fd, "OK", 2, 0);
            }
            else
            {
                send(client_fd, "ERR update failed", 17, 0);
                audit_log("UPDATE", id, "ERROR_FAIL");
            }
        }
        else if (strcmp(cmd, "DELETE") == 0)
        {
            if (delete_file(db, id) == 0)
            {
                send(client_fd, "OK", 2, 0);
            }
            else
            {
                send(client_fd, "ERR delete failed", 18, 0);
                audit_log("DELETE", id, "ERROR_FAIL");
            }
        }
        else if (strcmp(cmd, "GET") == 0)
        {
            char out[MAX_DATA + 1] = {0};
            if (read_file(db, id, out) == 0)
            {
                send(client_fd, out, strlen(out), 0);
            }
            else
            {
                send(client_fd, "ERR not found", 13, 0);
            }
        }
        else
        {
            send(client_fd, "ERR unknown command", 20, 0);
            audit_log("UNKNOWN", id, "ERROR_CMD");
        }

        shutdown(client_fd, SHUT_RDWR);
        close(client_fd);
    }
    return NULL;
}
void check_running(const char *pidfile_path)
{
    pid_fd = open(pidfile_path, O_RDWR | O_CREAT, 0644);
    if (pid_fd < 0)
    {
        log_sys("open pidfile");
        exit(EXIT_FAILURE);
    }

    if (flock(pid_fd, LOCK_EX | LOCK_NB) < 0)
    {
        if (errno == EWOULDBLOCK)
        {
            syslog(LOG_ERR, "Another fsdb instance is already running");
            exit(EXIT_FAILURE);
        }
        log_sys("flock");
        exit(EXIT_FAILURE);
    }

    if (ftruncate(pid_fd, 0) == -1)
    {
        log_sys("ftruncate pidfile");
        exit(EXIT_FAILURE);
    }

    char pid_str[32];
    snprintf(pid_str, sizeof(pid_str), "%d\n", getpid());
    if (write(pid_fd, pid_str, strlen(pid_str)) < 0)
    {
        log_sys("write pidfile");
        close(pid_fd);
        unlink(pidfile_path);
        exit(EXIT_FAILURE);
    }
}

int main(void)
{
    openlog("fsdb", LOG_PID | LOG_CONS, LOG_DAEMON);

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    mkdir(DB_FOLDER, 0700);
    unlink(SOCKET_PATH);

    check_running("/var/run/fsdb.pid");
    server_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (server_fd < 0)
    {
        log_sys("socket");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        log_sys("bind");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 128) < 0)
    {
        log_sys("listen");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

    char audit_path[512];
    snprintf(audit_path, sizeof(audit_path), "%s/fsdb.log", LOG_FOLDER);
    audit_fd = open(audit_path, O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (audit_fd < 0)
    {
        log_sys("open audit log");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0)
    {
        log_sys("epoll_create1");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

    struct epoll_event ev = {.events = EPOLLIN, .data.fd = server_fd};
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1)
    {
        log_sys("epoll_ctl ADD");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    int thread_count = (nproc > 0 && nproc < 256) ? (int)nproc : 4;

    for (int i = 0; i < thread_count; ++i)
    {
        pthread_t tid;
        if (pthread_create(&tid, NULL, worker_thread, NULL) != 0)
        {
            log_sys("pthread_create");
            cleanup_socket();
            exit(EXIT_FAILURE);
        }

        if (pthread_detach(tid) != 0)
        {
            log_sys("pthread_detach");
            cleanup_socket();
            exit(EXIT_FAILURE);
        }
    }

    struct epoll_event events[MAX_EVENTS];
    syslog(LOG_INFO, "fsdb daemon started");

    while (running)
    {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (nfds == -1)
        {
            if (errno == EINTR)
                continue;
            log_sys("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; ++i)
        {
            if (events[i].data.fd == server_fd)
            {
                int client_fd = accept4(server_fd, NULL, NULL, SOCK_NONBLOCK);
                if (client_fd < 0)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                        continue;
                    log_sys("accept4");
                    continue;
                }

                if (enqueue_client(client_fd) != 0)
                {
                    shutdown(client_fd, SHUT_RDWR);
                    close(client_fd);
                }
            }
        }
    }

    syslog(LOG_INFO, "fsdb daemon shutting down");
    cleanup_socket();
    closelog();
    return 0;
}
