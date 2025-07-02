fsdb is a lightweight and secure Unix daemon that provides basic key-value storage functionality over a UNIX domain socket. 
A local, atomic, multi-threaded, syslog-audited, filesystem-backed object store with consistent performance under load.

- ✅ Fast local IPC via UNIX domain sockets
- ✅ HTTP/1.1 POST support: speak to `fsdb` from any language or frontend via HTTP, using nginx proxy or curl!
- ✅ Extremely scalable 2-level folder sharding for 100M+ files
- ✅ Safe concurrent access via POSIX file locks
- ✅ Audit logging with timestamped entries
- ✅ Process-safe with `flock`-based PID file locking
- ✅ Signal handling for graceful shutdown
- ✅ Hardened with `-fPIE`, `-D_FORTIFY_SOURCE=2`, stack protector, and secure file sanitization
- ✅ Memory-safe: passes Valgrind and Clang analyzer with zero leaks
- ✅ Can run on `tmpfs`, `xfs`, or any Linux filesystem
- 🔒 Secure and concurrent: uses `epoll`, `pthread`, and safe file locking (`fcntl`)
- 🔌 Communication via Unix socket
 
 🧪 Commands Supported
 - `CREATE <db>`: initialize DB structure
- `INSERT <db> <id> <data>`: insert new record
- `UPDATE <db> <id> <data>`: update existing
- `GET <db> <id>`: fetch contents
- `DELETE <db> <id>`: remove entry
- `CHECK <db> <id>`: test existence
- `TOUCH <db> <id>`: create an empty file

- **HTTP POST Parameters:**
    - `ACTION` — one of `INSERT`, `UPDATE`, `GET`, `DELETE`, `EXISTS`, `TOUCH`, `CREATE`
    - `db` — database name
    - `id` — key
    - `data` — value (if needed by action)

🔐 Security & Hardening

Uses flock() and fcntl() locks
Sanitizes all id and db inputs
Resists TOCTOU vulnerabilities
Can run under chroot or as an unprivileged user
Validated with valgrind, clang --analyze, and sanitizers

🧱 Architecture

main() initializes socket, threads, and event loop
Clients accepted via epoll, queued using a ring buffer
Worker threads process requests from the queue
Each database is a subdirectory; each key is a file

🧠 Recommended Filesystem Tuning

For best performance:

Use XFS with directory indexing and large inode allocation
Mount with noatime, nodiratime, inode64 (XFS-specific)
For high-performance volatility: mount tmpfs on specific DBs
