fsdb is a lightweight and secure Unix daemon that provides basic key-value storage functionality over a UNIX domain socket. 
A local, atomic, multi-threaded, syslog-audited, filesystem-backed object store with consistent performance under load.
It supports simple database-like operations (`INSERT`, `UPDATE`, `GET`, `DELETE`) backed by a directory-based flat file structure.

- 🔒 Secure and concurrent: uses `epoll`, `pthread`, and safe file locking (`fcntl`)
- 🚦 Multi-threaded worker model with job queue
- 📁 Persistent flat-file storage under customizable directory
- 🧼 Input sanitization and audit logging
- 🧠 Memory-safe and race-free (validated via Valgrind, Clang Static Analyzer, and sanitizers)
- 🔐 Single-instance locking via PID file and `flock`
- 🔌 Communication via Unix socket
- 
🧱 Architecture

main() initializes socket, threads, and event loop
Clients accepted via epoll, queued using a ring buffer
Worker threads process requests from the queue
Each database is a subdirectory; each key is a file

