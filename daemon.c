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
#include <stdatomic.h>
#include <limits.h>

 
#define QUEUE_SIZE      1024
#define MAX_EVENTS      64
#define MAX_ID          256
#define MAX_DATA_HARD   65536
#define MAX_CMD_OVERHEAD (256 + MAX_ID + MAX_ID)
#define MAX_CMD         (MAX_CMD_OVERHEAD + MAX_DATA_HARD)
#define MAX_RECV_BUF    (MAX_CMD + 4096)
#define MAX_PATH_LEN    1024
#define MAX_WORKERS     256
#define MAX_KEYS_BUF    (64 * 1024)

 
#define DEFAULT_SOCKET_PATH  "/var/run/fsdb.sock"
#define DEFAULT_DB_FOLDER    "/var/lib/fsdb"
#define DEFAULT_LOG_FOLDER   "/var/log"
#define DEFAULT_PIDFILE_PATH "/var/run/fsdb.pid"
#define DEFAULT_AUTH_PATH    "/etc/fsdb/token"

static const char *socket_path;
static const char *db_folder;
static const char *log_folder;
static const char *pidfile_path;
static const char *auth_token_path;

 
static int server_fd = -1;
static int epoll_fd  = -1;
static int audit_fd  = -1;
static int pid_fd    = -1;

 
static int signal_pipe[2] = {-1, -1};

 
static int client_queue[QUEUE_SIZE];
static int queue_head = 0, queue_tail = 0;
static pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  queue_cond = PTHREAD_COND_INITIALIZER;

 
static pthread_t worker_tids[MAX_WORKERS];
static int       worker_count = 0;

 
static time_t               start_time;
static atomic_uint_fast64_t stat_total_requests;
static atomic_uint_fast64_t stat_active_workers;

 
static char auth_token[256];
static int  auth_enabled = 0;

 
static pthread_mutex_t audit_lock = PTHREAD_MUTEX_INITIALIZER;

 
static size_t max_data_size = 1024;     
static int    rate_limit    = 0;        
static long   default_ttl   = 0;       
static mode_t socket_mode   = 0660;    

 
static int    rate_count;
static time_t rate_window;

static _Atomic int running = 1;




static void handle_signal(int sig)
{
    int saved = errno;
    char c = (sig == SIGHUP) ? 'H' : 'T';  
    (void)write(signal_pipe[1], &c, 1);     
    errno = saved;
}




static int constant_time_eq(const void *a, const void *b, size_t len)  
{
    const volatile unsigned char *x = a;
    const volatile unsigned char *y = b;
    volatile unsigned char acc = 0;
    for (size_t i = 0; i < len; i++)
        acc |= x[i] ^ y[i];
    return acc == 0;
}

static void log_sys(const char *msg)
{
    syslog(LOG_ERR, "%s: %s", msg, strerror(errno));
}




static void extract_value(const char *body, const char *key, char *dest, size_t maxlen)
{
    size_t keylen = strlen(key);
    dest[0] = '\0';
    while (*body)
    {
        const char *amp = strchr(body, '&');
        const char *field_end = amp ? amp : body + strlen(body);
        const char *eq = memchr(body, '=', (size_t)(field_end - body));
        if (eq && (size_t)(eq - body) == keylen && memcmp(body, key, keylen) == 0)
        {
            size_t vlen = (size_t)(field_end - eq - 1);
            if (vlen > maxlen - 1)
                vlen = maxlen - 1;
            memcpy(dest, eq + 1, vlen);
            dest[vlen] = '\0';
            return;
        }
        if (!amp)
            break;
        body = amp + 1;
    }
}

static int url_decode_str(const char *src, char *dst, size_t dst_size)
{
    size_t di = 0;
    for (size_t si = 0; src[si] && di + 1 < dst_size; ++si)
    {
        if (src[si] == '%')
        {
            if (isxdigit((unsigned char)src[si + 1]) && isxdigit((unsigned char)src[si + 2]))
            {
                char hex[3] = {src[si + 1], src[si + 2], 0};
                char *endptr;
                long val = strtol(hex, &endptr, 16);
                if (endptr != hex + 2 || val < 0 || val > 255)
                    return -1;
                if (val == 0)  
                    return -1;
                dst[di++] = (char)val;
                si += 2;
            }
            else
                return -1;
        }
        else if (src[si] == '+')
            dst[di++] = ' ';
        else
            dst[di++] = src[si];
    }
    dst[di] = '\0';
    return 0;
}

static int http_to_legacy_command(char *buf, size_t buflen)
{
    char action[32], db[256], id[256];
    char *data = malloc(MAX_DATA_HARD);
    char *body = malloc(MAX_DATA_HARD + 1024);
    if (!data || !body)
    {
        free(data);
        free(body);
        return -1;
    }

    int rc = -1;
    size_t body_size = (size_t)MAX_DATA_HARD + 1024;

    const char *p = strstr(buf, "\r\n\r\n");
    if (!p)
        goto out;
    p += 4;
    if (url_decode_str(p, body, body_size) == -1)
        goto out;

    extract_value(body, "ACTION", action, sizeof(action));
    extract_value(body, "db", db, sizeof(db));
    extract_value(body, "id", id, sizeof(id));
    extract_value(body, "data", data, (size_t)MAX_DATA_HARD);

    if (strcmp(action, "INSERT") == 0 && db[0] && id[0] && data[0])
        snprintf(buf, buflen, "INSERT %s %s %s", db, id, data);
    else if (strcmp(action, "GET") == 0 && db[0] && id[0])
        snprintf(buf, buflen, "GET %s %s", db, id);
    else if (strcmp(action, "TOUCH") == 0 && db[0] && id[0])
        snprintf(buf, buflen, "TOUCH %s %s", db, id);
    else if (strcmp(action, "UPDATE") == 0 && db[0] && id[0] && data[0])
        snprintf(buf, buflen, "UPDATE %s %s %s", db, id, data);
    else if (strcmp(action, "EXISTS") == 0 && db[0] && id[0])
        snprintf(buf, buflen, "EXISTS %s %s", db, id);
    else if (strcmp(action, "CREATE") == 0 && db[0])
        snprintf(buf, buflen, "CREATE %s", db);
    else if (strcmp(action, "DELETE") == 0 && db[0] && id[0])
        snprintf(buf, buflen, "DELETE %s %s", db, id);
    else if (strcmp(action, "KEYS") == 0 && db[0])
        snprintf(buf, buflen, "KEYS %s %s", db, id[0] ? id : "1000");
    else if (strcmp(action, "COUNT") == 0 && db[0])
        snprintf(buf, buflen, "COUNT %s", db);
    else if (strcmp(action, "STATS") == 0)
        snprintf(buf, buflen, "STATS");
    else
        goto out;
    rc = 0;
out:
    free(data);
    free(body);
    return rc;
}

static int check_http_auth(const char *buf)  
{
    if (!auth_enabled)
        return 1;
    const char *auth = strcasestr(buf, "\r\nAuthorization:");
    if (!auth)
        return 0;
    auth += 16;
    while (*auth == ' ')
        auth++;
    if (strncmp(auth, "Bearer ", 7) != 0)
        return 0;
    auth += 7;

    size_t tlen = strlen(auth_token);
     
    const char *end = auth;
    while (*end && *end != '\r' && *end != '\n')
        end++;
    size_t vlen = (size_t)(end - auth);

     
    size_t cmplen = tlen < vlen ? tlen : vlen;
    int cmp_ok = constant_time_eq(auth, auth_token, cmplen);
    int len_ok = (vlen == tlen);
    return len_ok & cmp_ok;
}




static void cleanup_socket(void)
{
    if (socket_path)
        unlink(socket_path);
    if (server_fd >= 0)
        close(server_fd);
    if (epoll_fd >= 0)
        close(epoll_fd);
    if (audit_fd >= 0)
        close(audit_fd);
    if (pid_fd >= 0)
    {
        close(pid_fd);
        if (pidfile_path)
            unlink(pidfile_path);
    }
    if (signal_pipe[0] >= 0)
        close(signal_pipe[0]);
    if (signal_pipe[1] >= 0)
        close(signal_pipe[1]);
}

static void reopen_audit_log(void)  
{
    pthread_mutex_lock(&audit_lock);
    char path[512];
    snprintf(path, sizeof(path), "%s/fsdb.log", log_folder);
    int new_fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (new_fd >= 0)
    {
        int old = audit_fd;
        audit_fd = new_fd;
        close(old);
        syslog(LOG_INFO, "audit log reopened (SIGHUP)");
    }
    else
        log_sys("reopen audit log");
    pthread_mutex_unlock(&audit_lock);
}




static ssize_t send_all(int fd, const void *buf, size_t len, int flags)
{
    const char *p = buf;
    size_t remaining = len;
    while (remaining > 0)
    {
        ssize_t n = send(fd, p, remaining, flags);
        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                struct pollfd pfd = {.fd = fd, .events = POLLOUT};
                if (poll(&pfd, 1, 1000) <= 0)
                    return -1;
                continue;
            }
            return -1;
        }
        p += n;
        remaining -= (size_t)n;
    }
    return (ssize_t)len;
}

static ssize_t sosend(int fd, const void *buf, size_t len, int post_request)
{
    ssize_t sent;
    if (post_request)
    {
        char response[MAX_DATA_HARD + 512];
        int n = snprintf(response, sizeof(response),
                         "HTTP/1.1 200 OK\r\n"
                         "Content-Type: text/plain\r\n"
                         "Content-Length: %zu\r\n"
                         "Connection: close\r\n\r\n",
                         len);
        if (n > 0 && (size_t)n < sizeof(response) - len)
        {
            memcpy(response + n, buf, len);
            sent = send_all(fd, response, (size_t)n + len, MSG_NOSIGNAL);
        }
        else
            sent = -1;
    }
    else
        sent = send_all(fd, buf, len, MSG_NOSIGNAL);

    if (sent < 0)
        syslog(LOG_WARNING, "send failed: %s", strerror(errno));
    return sent;
}




static ssize_t recv_full(int fd, char *buf, size_t bufsize, int timeout_ms)
{
    size_t total = 0;
    int proto = -1;  

    while (total < bufsize - 1)
    {
        int wait_ms;
        if (total == 0)
            wait_ms = timeout_ms;
        else if (proto == 1)
            wait_ms = 200;
        else
            wait_ms = 50;  

        struct pollfd pfd = {.fd = fd, .events = POLLIN};
        if (poll(&pfd, 1, wait_ms) <= 0)
            break;

        ssize_t n = recv(fd, buf + total, bufsize - 1 - total, 0);
        if (n <= 0)
            break;
        total += (size_t)n;
        buf[total] = '\0';

        if (proto == -1)
        {
            if (total >= 5 && strncmp(buf, "POST ", 5) == 0)
                proto = 1;
            else if (total >= 4 && strncmp(buf, "GET ", 4) == 0)
                proto = 1;
            else
                proto = 0;
        }

        if (proto == 1)
        {
            if (strncmp(buf, "POST ", 5) == 0)
            {
                char *hdr_end = strstr(buf, "\r\n\r\n");
                if (hdr_end)
                {
                    size_t hdr_len = (size_t)(hdr_end - buf) + 4;
                    const char *cl = strcasestr(buf, "\r\nContent-Length:");
                    if (cl && cl < hdr_end)
                    {
                        char *endp;
                        long clen = strtol(cl + 17, &endp, 10);
                         
                        if (clen < 0 || clen > (long)(bufsize - hdr_len))
                            break;
                        if (endp == cl + 17)  
                            break;
                        if (hdr_len + (size_t)clen <= total)
                            break;  
                    }
                    else
                        break;  
                }
            }
            else  
            {
                if (strstr(buf, "\r\n\r\n"))
                    break;
            }
        }
        else
        {
             
            if (memchr(buf, '\n', total))
                break;
        }
    }

     
    if (total > 0 && proto != 1 && buf[total - 1] == '\n')
        buf[--total] = '\0';
    if (total > 0 && proto != 1 && buf[total - 1] == '\r')
        buf[--total] = '\0';

    return (ssize_t)total;
}




static int build_data_path(char *path, size_t size, const char *db, const char *id)
{
    int n;
    if (id[1])
        n = snprintf(path, size, "%s/%s/%c/%c/%s", db_folder, db, id[0], id[1], id);
    else
        n = snprintf(path, size, "%s/%s/_/%s", db_folder, db, id);
    if (n < 0 || (size_t)n >= size)
        return -1;
    return 0;
}

static void ensure_parent_dir(const char *db, const char *id)
{
    char dir[MAX_PATH_LEN];
     
    snprintf(dir, sizeof(dir), "%s/%s", db_folder, db);
    mkdir(dir, 0700);
    if (id[1])
    {
        snprintf(dir, sizeof(dir), "%s/%s/%c", db_folder, db, id[0]);
        mkdir(dir, 0700);
        snprintf(dir, sizeof(dir), "%s/%s/%c/%c", db_folder, db, id[0], id[1]);
        mkdir(dir, 0700);
    }
    else
    {
        snprintf(dir, sizeof(dir), "%s/%s/_", db_folder, db);
        mkdir(dir, 0700);
    }
}

static int is_expired(const char *path)  
{
    if (default_ttl <= 0)
        return 0;
    struct stat st;
    if (stat(path, &st) != 0)
        return 0;
    return (time(NULL) - st.st_mtime) >= default_ttl;
}




static int write_file(const char *db, const char *id, const char *data, int update)
{
    char path[MAX_PATH_LEN];
    if (build_data_path(path, sizeof(path), db, id) != 0)
        return -1;

    ensure_parent_dir(db, id);

    char tmp_path[MAX_PATH_LEN];
    snprintf(tmp_path, sizeof(tmp_path), "%s.XXXXXX", path);
    int fd = mkstemp(tmp_path);
    if (fd < 0)
        return -1;

    

    fcntl(fd, F_SETFD, FD_CLOEXEC);

    size_t len = strlen(data);
    size_t total_written = 0;
    while (total_written < len)
    {
        ssize_t w = write(fd, data + total_written, len - total_written);
        if (w < 0)
        {
            if (errno == EINTR)
                continue;
            close(fd);
            unlink(tmp_path);
            return -1;
        }
        total_written += (size_t)w;
    }
    fdatasync(fd);
    close(fd);

    if (update)
    {
         
        if (access(path, F_OK) != 0)
        {
            int saved = errno;
            unlink(tmp_path);
            errno = saved;
            return -1;
        }
        if (rename(tmp_path, path) != 0)
        {
            unlink(tmp_path);
            return -1;
        }
    }
    else
    {
        if (link(tmp_path, path) != 0)
        {
            int saved = errno;
            unlink(tmp_path);
            errno = saved;
            return -1;
        }
        unlink(tmp_path);
    }
    return 0;
}

static int read_file(const char *db, const char *id, char *out, size_t out_size)
{
    char path[MAX_PATH_LEN];
    if (build_data_path(path, sizeof(path), db, id) != 0)
        return -1;

    if (is_expired(path))
    {
        unlink(path);
        errno = ENOENT;
        return -1;
    }

    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;

    ssize_t n = read(fd, out, out_size - 1);
    close(fd);
    if (n < 0)
        return -1;
    out[n] = '\0';
    return 0;
}

static int delete_file(const char *db, const char *id)
{
    char path[MAX_PATH_LEN];
    if (build_data_path(path, sizeof(path), db, id) != 0)
        return -1;
    return unlink(path);
}

static int check_file(const char *db, const char *id)
{
    char path[MAX_PATH_LEN];
    if (build_data_path(path, sizeof(path), db, id) != 0)
        return -1;
    if (is_expired(path))
    {
        unlink(path);
        return -1;
    }
    return access(path, F_OK);
}

static int touch_file(const char *db, const char *id)
{
    char path[MAX_PATH_LEN];
    if (build_data_path(path, sizeof(path), db, id) != 0)
        return -1;
    ensure_parent_dir(db, id);
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0)
        return -1;
    close(fd);
    return 0;
}




static void handle_keys(int fd, const char *db, int limit, int post_request)
{
    char *out = malloc(MAX_KEYS_BUF);
    if (!out)
    {
        sosend(fd, "ERR out of memory", 17, post_request);
        return;
    }

    size_t pos = 0;
    int count = 0;
    char base[MAX_PATH_LEN];
    snprintf(base, sizeof(base), "%s/%s", db_folder, db);

    DIR *d1 = opendir(base);
    if (!d1)
    {
        sosend(fd, "ERR database not found", 22, post_request);
        free(out);
        return;
    }

    struct dirent *e1;
    while ((e1 = readdir(d1)) != NULL && count < limit)
    {
        if (e1->d_name[0] == '.')
            continue;

        char l1[MAX_PATH_LEN];
        snprintf(l1, sizeof(l1), "%s/%s", base, e1->d_name);

        if (e1->d_name[0] == '_' && e1->d_name[1] == '\0')
        {
            DIR *ds = opendir(l1);
            if (!ds)
                continue;
            struct dirent *es;
            while ((es = readdir(ds)) != NULL && count < limit)
            {
                if (es->d_name[0] == '.')
                    continue;
                if (default_ttl > 0)
                {
                    char fp[MAX_PATH_LEN];
                    snprintf(fp, sizeof(fp), "%s/%s", l1, es->d_name);
                    if (is_expired(fp))
                    {
                        unlink(fp);
                        continue;
                    }
                }
                int n = snprintf(out + pos, MAX_KEYS_BUF - pos, "%s\n", es->d_name);
                if (n < 0 || pos + (size_t)n >= MAX_KEYS_BUF)
                {
                    closedir(ds);
                    goto keys_done;
                }
                pos += (size_t)n;
                count++;
            }
            closedir(ds);
        }
        else
        {
            DIR *d2 = opendir(l1);
            if (!d2)
                continue;
            struct dirent *e2;
            while ((e2 = readdir(d2)) != NULL && count < limit)
            {
                if (e2->d_name[0] == '.')
                    continue;
                char l2[MAX_PATH_LEN];
                snprintf(l2, sizeof(l2), "%s/%s", l1, e2->d_name);
                DIR *d3 = opendir(l2);
                if (!d3)
                    continue;
                struct dirent *e3;
                while ((e3 = readdir(d3)) != NULL && count < limit)
                {
                    if (e3->d_name[0] == '.')
                        continue;
                    if (strchr(e3->d_name, '.'))
                        continue;  
                    if (default_ttl > 0)
                    {
                        char fp[MAX_PATH_LEN];
                        snprintf(fp, sizeof(fp), "%s/%s", l2, e3->d_name);
                        if (is_expired(fp))
                        {
                            unlink(fp);
                            continue;
                        }
                    }
                    int n = snprintf(out + pos, MAX_KEYS_BUF - pos, "%s\n", e3->d_name);
                    if (n < 0 || pos + (size_t)n >= MAX_KEYS_BUF)
                    {
                        closedir(d3);
                        closedir(d2);
                        goto keys_done;
                    }
                    pos += (size_t)n;
                    count++;
                }
                closedir(d3);
            }
            closedir(d2);
        }
    }
keys_done:
    closedir(d1);

    if (pos == 0)
        sosend(fd, "EMPTY", 5, post_request);
    else
    {
        if (pos > 0 && out[pos - 1] == '\n')
            pos--;
        sosend(fd, out, pos, post_request);
    }
    free(out);
}

static void handle_count(int fd, const char *db, int post_request)
{
    char base[MAX_PATH_LEN];
    snprintf(base, sizeof(base), "%s/%s", db_folder, db);

    DIR *d1 = opendir(base);
    if (!d1)
    {
        sosend(fd, "ERR database not found", 22, post_request);
        return;
    }

    long count = 0;
    struct dirent *e1;
    while ((e1 = readdir(d1)) != NULL)
    {
        if (e1->d_name[0] == '.')
            continue;
        char l1[MAX_PATH_LEN];
        snprintf(l1, sizeof(l1), "%s/%s", base, e1->d_name);

        if (e1->d_name[0] == '_' && e1->d_name[1] == '\0')
        {
            DIR *ds = opendir(l1);
            if (!ds)
                continue;
            struct dirent *es;
            while ((es = readdir(ds)) != NULL)
            {
                if (es->d_name[0] == '.')
                    continue;
                if (default_ttl > 0)
                {
                    char fp[MAX_PATH_LEN];
                    snprintf(fp, sizeof(fp), "%s/%s", l1, es->d_name);
                    if (is_expired(fp))
                    {
                        unlink(fp);
                        continue;
                    }
                }
                count++;
            }
            closedir(ds);
        }
        else
        {
            DIR *d2 = opendir(l1);
            if (!d2)
                continue;
            struct dirent *e2;
            while ((e2 = readdir(d2)) != NULL)
            {
                if (e2->d_name[0] == '.')
                    continue;
                char l2[MAX_PATH_LEN];
                snprintf(l2, sizeof(l2), "%s/%s", l1, e2->d_name);
                DIR *d3 = opendir(l2);
                if (!d3)
                    continue;
                struct dirent *e3;
                while ((e3 = readdir(d3)) != NULL)
                {
                    if (e3->d_name[0] == '.' || strchr(e3->d_name, '.'))
                        continue;
                    if (default_ttl > 0)
                    {
                        char fp[MAX_PATH_LEN];
                        snprintf(fp, sizeof(fp), "%s/%s", l2, e3->d_name);
                        if (is_expired(fp))
                        {
                            unlink(fp);
                            continue;
                        }
                    }
                    count++;
                }
                closedir(d3);
            }
            closedir(d2);
        }
    }
    closedir(d1);

    char buf[32];
    snprintf(buf, sizeof(buf), "%ld", count);
    sosend(fd, buf, strlen(buf), post_request);
}




static void audit_log(const char *cmd, const char *id, const char *status)
{
    time_t now = time(NULL);
    struct tm tm;
    gmtime_r(&now, &tm);

    char timestr[32];
    strftime(timestr, sizeof(timestr), "%Y-%m-%dT%H:%M:%SZ", &tm);

    char entry[1024];
    snprintf(entry, sizeof(entry), "%s %s %s %s\n", timestr, cmd, id, status);

    pthread_mutex_lock(&audit_lock);

    size_t len = strlen(entry);
    size_t total_written = 0;
    while (total_written < len)
    {
        ssize_t w = write(audit_fd, entry + total_written, len - total_written);
        if (w < 0)
        {
            if (errno == EINTR)
                continue;
            log_sys("audit_log: write");
            break;
        }
        total_written += (size_t)w;
    }

    fdatasync(audit_fd);  

    pthread_mutex_unlock(&audit_lock);
}




static int sanitize_id(char *id)
{
    if (!id[0])
        return 0;
    size_t i = 0;
    for (; i < MAX_ID - 1 && id[i]; ++i)
    {
        if (!isalnum((unsigned char)id[i]))
            return 0;
    }
    if (id[i] != '\0')  
        return 0;
    return 1;
}




static int enqueue_client(int fd)
{
    int err = pthread_mutex_lock(&queue_lock);
    if (err != 0)
    {
        errno = err;
        log_sys("pthread_mutex_lock (enqueue)");
         
        return -1;
    }

    int next = (queue_tail + 1) % QUEUE_SIZE;
    if (next == queue_head)
    {
        pthread_mutex_unlock(&queue_lock);
        syslog(LOG_WARNING, "queue full, rejecting connection");
        const char *msg = "ERR server busy\n";
        send(fd, msg, strlen(msg), MSG_NOSIGNAL);
        return -1;
    }

    client_queue[queue_tail] = fd;
    queue_tail = next;

    pthread_cond_signal(&queue_cond);
    pthread_mutex_unlock(&queue_lock);
    return 0;
}

static int dequeue_client(void)
{
    pthread_mutex_lock(&queue_lock);

    while (queue_head == queue_tail)
    {
        if (!atomic_load(&running))
        {
             
            pthread_mutex_unlock(&queue_lock);
            return -1;
        }
        pthread_cond_wait(&queue_cond, &queue_lock);
    }

     
    int fd = client_queue[queue_head];
    queue_head = (queue_head + 1) % QUEUE_SIZE;
    pthread_mutex_unlock(&queue_lock);
    return fd;
}




static int generate_structure(const char *db)
{
    char base_path[MAX_PATH_LEN];
    snprintf(base_path, sizeof(base_path), "%s/%s", db_folder, db);
    if (mkdir(base_path, 0700) == -1 && errno != EEXIST)
    {
        log_sys("mkdir base");
        return 0;
    }

    char special[MAX_PATH_LEN];
    snprintf(special, sizeof(special), "%s/_", base_path);
    if (mkdir(special, 0700) == -1 && errno != EEXIST)
    {
        log_sys("mkdir _");
        return 0;
    }

    for (char a = '0'; a <= 'z'; ++a)
    {
        if (!isalnum((unsigned char)a))
            continue;
        char l1[MAX_PATH_LEN];
        snprintf(l1, sizeof(l1), "%s/%c", base_path, a);
        if (mkdir(l1, 0700) == -1 && errno != EEXIST)
        {
            log_sys("mkdir level1");
            return 0;
        }
        for (char b = '0'; b <= 'z'; ++b)
        {
            if (!isalnum((unsigned char)b))
                continue;
            char l2[MAX_PATH_LEN];
            snprintf(l2, sizeof(l2), "%s/%c", l1, b);
            if (mkdir(l2, 0700) == -1 && errno != EEXIST)
            {
                log_sys("mkdir level2");
                return 0;
            }
        }
    }
    return 1;
}




static void handle_stats(int fd, int http)
{
    time_t now = time(NULL);
    long uptime = (long)(now - start_time);
    uint_fast64_t reqs = atomic_load(&stat_total_requests);
    uint_fast64_t active = atomic_load(&stat_active_workers);

    int depth;
    pthread_mutex_lock(&queue_lock);
    depth = (queue_tail - queue_head + QUEUE_SIZE) % QUEUE_SIZE;
    pthread_mutex_unlock(&queue_lock);

    char body[512];
    snprintf(body, sizeof(body),
             "{\"uptime\":%ld,\"requests\":%lu,\"active_workers\":%lu,"
             "\"queue_depth\":%d,\"max_queue\":%d,\"workers\":%d,"
             "\"max_data\":%zu,\"ttl\":%ld,\"rate_limit\":%d,\"auth\":%s}",
             uptime, (unsigned long)reqs, (unsigned long)active,
             depth, QUEUE_SIZE - 1, worker_count,
             max_data_size, default_ttl, rate_limit,
             auth_enabled ? "true" : "false");

    sosend(fd, body, strlen(body), http);
}




static void *worker_thread(void *arg)
{
    (void)arg;
    pthread_setname_np(pthread_self(), "fsdb-worker");

    while (1)  
    {
        int client_fd = dequeue_client();
        if (client_fd < 0)
            break;

        atomic_fetch_add(&stat_active_workers, 1);
        atomic_fetch_add(&stat_total_requests, 1);

        char buf[MAX_RECV_BUF + 1];
        memset(buf, 0, 16);  
        ssize_t len = recv_full(client_fd, buf, sizeof(buf), 1000);
        if (len <= 0)
            goto done;

        int post_request = 0;

         
        if (len >= 4 && strncmp(buf, "GET ", 4) == 0)
        {
             
            const char *path_start = buf + 4;
            const char *path_end = strchr(path_start, ' ');
            size_t plen = path_end ? (size_t)(path_end - path_start) : strlen(path_start);
            if ((plen == 7 && memcmp(path_start, "/health", 7) == 0) ||
                (plen == 6 && memcmp(path_start, "/stats", 6) == 0))
                handle_stats(client_fd, 1);
            else
            {
                const char *r = "HTTP/1.1 404 Not Found\r\n"
                                "Content-Length: 9\r\n"
                                "Connection: close\r\n\r\nNot Found";
                send_all(client_fd, r, strlen(r), MSG_NOSIGNAL);
            }
            goto done;
        }

         
        if (len >= 5 && strncmp(buf, "POST ", 5) == 0)
        {
            post_request = 1;
            if (!check_http_auth(buf))
            {
                const char *r = "HTTP/1.1 401 Unauthorized\r\n"
                                "Content-Length: 12\r\n"
                                "Connection: close\r\n\r\nUnauthorized";
                send_all(client_fd, r, strlen(r), MSG_NOSIGNAL);
                goto done;
            }
            if (http_to_legacy_command(buf, sizeof(buf)) == -1)
            {
                sosend(client_fd, "ERR invalid", 11, post_request);
                goto done;
            }
        }

         
        char *saveptr = NULL;
        const char *cmd = strtok_r(buf, " ", &saveptr);
        char *db = strtok_r(NULL, " ", &saveptr);
        char *id = strtok_r(NULL, " ", &saveptr);
         
        const char *data = (saveptr && *saveptr) ? saveptr : NULL;

        if (!cmd)
        {
            sosend(client_fd, "ERR invalid", 11, post_request);
            goto done;
        }

         
        if (strcmp(cmd, "STATS") == 0)
        {
            handle_stats(client_fd, post_request);
            goto done;
        }

        if (!db)
        {
            sosend(client_fd, "ERR invalid", 11, post_request);
            goto done;
        }

        if (!sanitize_id(db))
        {
            sosend(client_fd, "ERR invalid DB", 14, post_request);
            goto done;
        }

         
        if (strcmp(cmd, "CREATE") == 0)
        {
            if (!generate_structure(db))
                sosend(client_fd, "ERR CREATE", 10, post_request);
            else
                sosend(client_fd, "OK", 2, post_request);
            goto done;
        }

         
        if (strcmp(cmd, "KEYS") == 0)
        {
            int limit = 1000;
            if (id)
            {
                limit = atoi(id);
                if (limit <= 0 || limit > 100000)
                    limit = 1000;
            }
            handle_keys(client_fd, db, limit, post_request);
            goto done;
        }

         
        if (strcmp(cmd, "COUNT") == 0)
        {
            handle_count(client_fd, db, post_request);
            goto done;
        }

        if (!id || !sanitize_id(id))
        {
            sosend(client_fd, "ERR invalid ID", 14, post_request);
            goto done;
        }

         
        if (strcmp(cmd, "INSERT") == 0)
        {
            if (!data)
            {
                sosend(client_fd, "ERR data missing", 16, post_request);
                audit_log("INSERT", id, "ERROR_MISSING");
            }
            else if (strlen(data) > max_data_size)  
            {
                sosend(client_fd, "ERR data too large", 18, post_request);
                audit_log("INSERT", id, "ERROR_TOO_LARGE");
            }
            else
            {
                int res = write_file(db, id, data, 0);
                if (res == 0)
                    sosend(client_fd, "OK", 2, post_request);
                else if (errno == EEXIST)
                    sosend(client_fd, "ERR already exists", 18, post_request);
                else
                {
                    sosend(client_fd, "ERR write error", 15, post_request);
                    audit_log("INSERT", id, "ERROR_WRITE_FAIL");
                }
            }
        }
        else if (strcmp(cmd, "TOUCH") == 0)
        {
            if (touch_file(db, id) == 0)
                sosend(client_fd, "OK", 2, post_request);
            else if (errno == EEXIST)
                sosend(client_fd, "ERR already exists", 18, post_request);
            else
                sosend(client_fd, "ERR touch failed", 16, post_request);
        }
        else if (strcmp(cmd, "EXISTS") == 0)
        {
            if (check_file(db, id) == 0)
                sosend(client_fd, "Y", 1, post_request);
            else
                sosend(client_fd, "N", 1, post_request);
        }
        else if (strcmp(cmd, "UPDATE") == 0)
        {
            if (!data)
                sosend(client_fd, "ERR data missing", 16, post_request);
            else if (strlen(data) > max_data_size)
            {
                sosend(client_fd, "ERR data too large", 18, post_request);
                audit_log("UPDATE", id, "ERROR_TOO_LARGE");
            }
            else if (write_file(db, id, data, 1) == 0)
                sosend(client_fd, "OK", 2, post_request);
            else
            {
                sosend(client_fd, "ERR update failed", 17, post_request);
                audit_log("UPDATE", id, "ERROR_FAIL");
            }
        }
        else if (strcmp(cmd, "DELETE") == 0)
        {
            if (delete_file(db, id) == 0)
                sosend(client_fd, "OK", 2, post_request);
            else
            {
                sosend(client_fd, "ERR delete failed", 17, post_request);
                audit_log("DELETE", id, "ERROR_FAIL");
            }
        }
        else if (strcmp(cmd, "GET") == 0)
        {
            char *out = malloc(max_data_size + 1);
            if (!out)
            {
                sosend(client_fd, "ERR out of memory", 17, post_request);
            }
            else
            {
                if (read_file(db, id, out, max_data_size + 1) == 0)
                    sosend(client_fd, out, strlen(out), post_request);
                else
                    sosend(client_fd, "ERR not found", 13, post_request);
                free(out);
            }
        }
        else
        {
            sosend(client_fd, "ERR unknown command", 19, post_request);
            audit_log("UNKNOWN", cmd, "ERROR_CMD");
        }

    done:
        atomic_fetch_sub(&stat_active_workers, 1);
        shutdown(client_fd, SHUT_RDWR);
        close(client_fd);
    }
    return NULL;
}




static void check_running(void)
{
    pid_fd = open(pidfile_path, O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (pid_fd < 0)
    {
        log_sys("open pidfile");
        exit(EXIT_FAILURE);
    }
    if (flock(pid_fd, LOCK_EX | LOCK_NB) < 0)
    {
        if (errno == EWOULDBLOCK)
            syslog(LOG_ERR, "Another fsdb instance is already running");
        else
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




static void load_auth_token(void)
{
    int fd = open(auth_token_path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
    {
        syslog(LOG_INFO, "no auth token (%s), HTTP auth disabled", auth_token_path);
        auth_enabled = 0;
        return;
    }
    ssize_t n = read(fd, auth_token, sizeof(auth_token) - 1);
    close(fd);
    if (n <= 0)
    {
        auth_enabled = 0;
        return;
    }
    auth_token[n] = '\0';
    while (n > 0 && (auth_token[n - 1] == '\n' || auth_token[n - 1] == '\r' || auth_token[n - 1] == ' '))
        auth_token[--n] = '\0';
    if (n == 0)
    {
        auth_enabled = 0;
        return;
    }
    auth_enabled = 1;
    syslog(LOG_INFO, "HTTP auth enabled (%s)", auth_token_path);
}




static void daemonize(void)
{
    pid_t pid = fork();
    if (pid < 0)
    {
        perror("fork");
        exit(EXIT_FAILURE);
    }
    if (pid > 0)
        exit(0);
    if (setsid() < 0)
    {
        perror("setsid");
        exit(EXIT_FAILURE);
    }
    pid = fork();
    if (pid < 0)
    {
        perror("fork");
        exit(EXIT_FAILURE);
    }
    if (pid > 0)
        exit(0);

    int devnull = open("/dev/null", O_RDWR);
    if (devnull >= 0)
    {
        dup2(devnull, STDIN_FILENO);
        dup2(devnull, STDOUT_FILENO);
        dup2(devnull, STDERR_FILENO);
        if (devnull > STDERR_FILENO)
            close(devnull);
    }
    umask(0077);
    if (chdir("/") != 0)
        perror("chdir");
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s [-d] [-h]\n"
            "  -d    daemonize (fork to background)\n"
            "  -h    show this help\n\n"
            "Environment variables:\n"
            "  FSDB_SOCKET       socket path       (default: %s)\n"
            "  FSDB_DBDIR        data directory     (default: %s)\n"
            "  FSDB_LOGDIR       log directory      (default: %s)\n"
            "  FSDB_PIDFILE      PID file path      (default: %s)\n"
            "  FSDB_AUTH         auth token file    (default: %s)\n"
            "  FSDB_SOCKET_MODE  socket permissions (default: 0660)\n"
            "  FSDB_MAX_DATA     max data bytes     (default: 1024, max: 65536)\n"
            "  FSDB_RATE_LIMIT   max accepts/sec    (default: 0=unlimited)\n"
            "  FSDB_TTL          key TTL in seconds (default: 0=no expiry)\n",
            prog,
            DEFAULT_SOCKET_PATH, DEFAULT_DB_FOLDER, DEFAULT_LOG_FOLDER,
            DEFAULT_PIDFILE_PATH, DEFAULT_AUTH_PATH);
}




int main(int argc, char *argv[])
{
    int daemon_flag = 0;
    int opt;
    while ((opt = getopt(argc, argv, "dh")) != -1)
    {
        switch (opt)
        {
        case 'd':
            daemon_flag = 1;
            break;
        case 'h':
            usage(argv[0]);
            return 0;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    if (daemon_flag)
        daemonize();

    openlog("fsdb", LOG_PID | LOG_CONS, LOG_DAEMON);
    start_time = time(NULL);
    atomic_init(&stat_total_requests, 0);
    atomic_init(&stat_active_workers, 0);
    rate_count = 0;

     
    socket_path     = getenv("FSDB_SOCKET")  ? getenv("FSDB_SOCKET")  : DEFAULT_SOCKET_PATH;
    db_folder       = getenv("FSDB_DBDIR")   ? getenv("FSDB_DBDIR")   : DEFAULT_DB_FOLDER;
    log_folder      = getenv("FSDB_LOGDIR")  ? getenv("FSDB_LOGDIR")  : DEFAULT_LOG_FOLDER;
    pidfile_path    = getenv("FSDB_PIDFILE") ? getenv("FSDB_PIDFILE") : DEFAULT_PIDFILE_PATH;
    auth_token_path = getenv("FSDB_AUTH")    ? getenv("FSDB_AUTH")    : DEFAULT_AUTH_PATH;

     
    const char *v;
    if ((v = getenv("FSDB_MAX_DATA")))
    {
        long x = strtol(v, NULL, 10);
        if (x > 0 && (size_t)x <= MAX_DATA_HARD)
            max_data_size = (size_t)x;
    }
    if ((v = getenv("FSDB_RATE_LIMIT")))
    {
        long x = strtol(v, NULL, 10);
        if (x > 0 && x <= INT_MAX)
            rate_limit = (int)x;
    }
    if ((v = getenv("FSDB_TTL")))
    {
        long x = strtol(v, NULL, 10);
        if (x > 0)
            default_ttl = x;
    }
    if ((v = getenv("FSDB_SOCKET_MODE")))
    {
        long x = strtol(v, NULL, 8);  
        if (x > 0 && x <= 0777)
            socket_mode = (mode_t)x;
    }

     
    check_running();

    mkdir(db_folder, 0700);
    unlink(socket_path);

    load_auth_token();

     
    if (pipe2(signal_pipe, O_CLOEXEC | O_NONBLOCK) != 0)
    {
        log_sys("pipe2");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

     
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_signal;
    sa.sa_flags = SA_RESTART;
    sigfillset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGQUIT, &sa, NULL);    
    sigaction(SIGHUP, &sa, NULL);     

    struct sigaction sa_ign;
    memset(&sa_ign, 0, sizeof(sa_ign));
    sa_ign.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa_ign, NULL);

     
    server_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (server_fd < 0)
    {
        log_sys("socket");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    if (strlen(socket_path) >= sizeof(addr.sun_path))
    {
        syslog(LOG_ERR, "socket path too long (%zu >= %zu)",
               strlen(socket_path), sizeof(addr.sun_path));
        cleanup_socket();
        exit(EXIT_FAILURE);
    }
    strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path) - 1);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        log_sys("bind");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }
    if (chmod(socket_path, socket_mode) != 0)  
    {
        log_sys("chmod");
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
    snprintf(audit_path, sizeof(audit_path), "%s/fsdb.log", log_folder);
    audit_fd = open(audit_path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (audit_fd < 0)
    {
        log_sys("open audit log");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

     
    epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0)
    {
        log_sys("epoll_create1");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

    struct epoll_event ev;

    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1)
    {
        log_sys("epoll_ctl server");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

     
    ev.events = EPOLLIN;
    ev.data.fd = signal_pipe[0];
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, signal_pipe[0], &ev) == -1)
    {
        log_sys("epoll_ctl signal_pipe");
        cleanup_socket();
        exit(EXIT_FAILURE);
    }

    

    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    worker_count = (nproc > 0 && nproc < MAX_WORKERS) ? (int)nproc : 4;

    pthread_attr_t tattr;
    pthread_attr_init(&tattr);
    pthread_attr_setstacksize(&tattr, 1024 * 1024);

    for (int i = 0; i < worker_count; ++i)
    {
        if (pthread_create(&worker_tids[i], &tattr, worker_thread, NULL) != 0)
        {
            log_sys("pthread_create");
            pthread_attr_destroy(&tattr);
            cleanup_socket();
            exit(EXIT_FAILURE);
        }
    }
    pthread_attr_destroy(&tattr);

    syslog(LOG_INFO, "fsdb started (workers=%d, auth=%s, socket=%s, db=%s, "
                     "max_data=%zu, ttl=%ld, rate_limit=%d)",
           worker_count, auth_enabled ? "on" : "off",
           socket_path, db_folder, max_data_size, default_ttl, rate_limit);

    struct epoll_event events[MAX_EVENTS];

    while (atomic_load(&running))
    {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
        if (nfds == -1)
        {
            if (errno == EINTR)
                continue;
            log_sys("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; ++i)
        {
             
            if (events[i].data.fd == signal_pipe[0])
            {
                char c;
                while (read(signal_pipe[0], &c, 1) > 0)
                {
                    if (c == 'T')
                    {
                        atomic_store(&running, 0);
                    }
                    else if (c == 'H')
                    {
                        reopen_audit_log();
                    }
                }
                if (!atomic_load(&running))
                    pthread_cond_broadcast(&queue_cond);  
                continue;
            }

             
            if (events[i].data.fd == server_fd)
            {
                int client_fd = accept4(server_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (client_fd < 0)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                        continue;
                    log_sys("accept4");
                    continue;
                }

                 
                if (rate_limit > 0)
                {
                    time_t now = time(NULL);
                    if (now != rate_window)
                    {
                        rate_count = 0;
                        rate_window = now;
                    }
                    if (rate_count++ >= rate_limit)
                    {
                        syslog(LOG_WARNING, "rate limit exceeded (%d/s)", rate_limit);
                        const char *msg = "ERR rate limited\n";
                        send(client_fd, msg, strlen(msg), MSG_NOSIGNAL);
                        close(client_fd);
                        continue;
                    }
                }

                if (enqueue_client(client_fd) != 0)
                {
                    shutdown(client_fd, SHUT_RDWR);
                    close(client_fd);
                }
            }
        }
    }

     
    syslog(LOG_INFO, "fsdb shutting down, draining %d workers...", worker_count);
    pthread_cond_broadcast(&queue_cond);

    for (int i = 0; i < worker_count; ++i)
        pthread_join(worker_tids[i], NULL);

    syslog(LOG_INFO, "fsdb stopped (served %lu requests)",
           (unsigned long)atomic_load(&stat_total_requests));
    cleanup_socket();
    closelog();
    return 0;
}
