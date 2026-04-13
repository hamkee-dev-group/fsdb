fsdb is a lightweight and secure Unix daemon that provides basic key-value storage functionality over a UNIX domain socket. 
A local, atomic, multi-threaded, syslog-audited, filesystem-backed object store with consistent performance under load.

- Fast local IPC via UNIX domain sockets
- HTTP/1.1 POST support: speak to `fsdb` from any language or frontend via HTTP, using nginx proxy or curl
- Extremely scalable 2-level folder sharding for 100M+ files
- Safe concurrent access via atomic temp+rename writes (no partial files on crash)
- Audit logging with timestamped entries (SIGHUP for log rotation)
- Process-safe with `flock`-based PID file locking
- Graceful shutdown: in-flight requests drain before exit
- Hardened with `-fPIE`, `-D_FORTIFY_SOURCE=2`, stack protector, `O_CLOEXEC`, and secure input sanitization
- Memory-safe: passes Valgrind and Clang analyzer with zero leaks
- Can run on `tmpfs`, `xfs`, or any Linux filesystem
- Secure and concurrent: uses `epoll`, `pthread`, self-pipe signal handling, and atomic file operations
- Optional HTTP bearer-token authentication
- Configurable rate limiting, TTL, data size limits
- KEYS/COUNT for key enumeration
- Daemonize with `-d`, or run under systemd

## Commands

| Command | Description |
|---------|-------------|
| `CREATE <db>` | Initialize DB directory structure (optional — dirs are created lazily) |
| `INSERT <db> <id> <data>` | Insert new record (fails if exists) |
| `UPDATE <db> <id> <data>` | Overwrite existing record |
| `GET <db> <id>` | Fetch contents |
| `DELETE <db> <id>` | Remove entry |
| `EXISTS <db> <id>` | Test existence (returns `Y` or `N`) |
| `TOUCH <db> <id>` | Create an empty file (fails if exists) |
| `KEYS <db> [limit]` | List keys (default limit: 1000, order is filesystem-dependent) |
| `COUNT <db>` | Count keys in database |
| `STATS` | Return JSON metrics (uptime, requests, queue depth, etc.) |

## HTTP Interface

**POST parameters** (form-encoded body):
- `ACTION` — one of `INSERT`, `UPDATE`, `GET`, `DELETE`, `EXISTS`, `TOUCH`, `CREATE`, `KEYS`, `COUNT`, `STATS`
- `db` — database name
- `id` — key
- `data` — value (if needed by action)

**GET endpoints:**
- `GET /health` or `GET /stats` — JSON health/metrics (unauthenticated)

**Authentication:** If `/etc/fsdb/token` (or `$FSDB_AUTH`) contains a token, all HTTP POST requests require `Authorization: Bearer <token>` header. GET /health is always unauthenticated.

## Usage

```bash
# Build
make

# Run (foreground)
./daemon

# Run (background daemon)
./daemon -d

# Install as systemd service
sudo make install
sudo systemctl enable --now fsdb

# Log rotation
kill -HUP $(cat /var/run/fsdb.pid)

# Run tests
make test

# Run benchmark (self-contained, no pre-started daemon needed)
make bench-roundtrip

# Run benchmark with custom op count
BENCH_OPS=10000 make bench-roundtrip
```

### Benchmark

`make bench-roundtrip` starts a temporary daemon, runs the stress test, prints
machine-readable metrics, and cleans up. Output includes:

- `ops_per_sec` — throughput (INSERT roundtrips per second)
- `p50_us` / `p95_us` — per-operation latency percentiles in microseconds
- `elapsed_s` — total wall-clock time

Compare `ops_per_sec` and `p95_us` before and after a change to detect
performance regressions. Protocol behavior and crash-safe atomic file writes
(`temp+rename`) are verified separately by `make test`.

## Configuration (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `FSDB_SOCKET` | `/var/run/fsdb.sock` | Socket path |
| `FSDB_DBDIR` | `/var/lib/fsdb` | Data directory |
| `FSDB_LOGDIR` | `/var/log` | Log directory |
| `FSDB_PIDFILE` | `/var/run/fsdb.pid` | PID file path |
| `FSDB_AUTH` | `/etc/fsdb/token` | Auth token file |
| `FSDB_SOCKET_MODE` | `0660` | Socket file permissions (octal) |
| `FSDB_MAX_DATA` | `1024` | Max data bytes per record (max: 65536) |
| `FSDB_RATE_LIMIT` | `0` | Max connections/sec (0 = unlimited) |
| `FSDB_TTL` | `0` | Key TTL in seconds (0 = no expiry, mtime-based) |

## Security & Hardening

- All IDs and DB names sanitized (alphanumeric only, null bytes rejected)
- Atomic writes via temp file + rename/link (crash-safe)
- Constant-time token comparison (timing-attack resistant)
- `O_CLOEXEC` on all file descriptors
- Async-signal-safe signal handling via self-pipe
- Socket permissions configurable (default `0660`)
- Uses `sigaction()` for reliable signal handling
- Validated with valgrind, clang --analyze, and sanitizers

## Architecture

- `main()` initializes socket, threads, signal pipe, and epoll event loop
- Clients accepted via epoll, queued using a mutex-guarded ring buffer
- Worker threads (one per CPU core) process requests from the queue
- Each database is a subdirectory; each key is a file
- Graceful shutdown: workers drain remaining queue before exiting

## Recommended Filesystem Tuning

For best performance:
- Use XFS with directory indexing and large inode allocation
- Mount with `noatime`, `nodiratime`, `inode64` (XFS-specific)
- For high-performance volatility: mount `tmpfs` on specific DBs
