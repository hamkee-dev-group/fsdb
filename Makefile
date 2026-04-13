CC      := clang
CFLAGS  := -std=c11 -O2 -Wall -Wextra -Wpedantic -Wconversion \
           -fstack-protector-strong -D_FORTIFY_SOURCE=2 -fPIE
LDFLAGS := -Wl,-z,relro,-z,now -pie -lpthread

PREFIX  := /usr/local
BINDIR  := $(PREFIX)/bin
UNITDIR := /etc/systemd/system
CONFDIR := /etc/fsdb

all: daemon test/client test/stress

daemon: daemon.o
	$(CC) daemon.o $(LDFLAGS) -o $@

daemon.o: daemon.c
	$(CC) $(CFLAGS) -c daemon.c -o daemon.o

test/client: test/client.c
	$(CC) $(CFLAGS) test/client.c -o test/client $(LDFLAGS)

test/stress: test/stress.c
	$(CC) $(CFLAGS) test/stress.c -o test/stress $(LDFLAGS)

clean:
	rm -f daemon daemon.o test/client test/stress

test: all
	@if [ -n "$$FSDB_SOCKET" ]; then exec ./test/test_suite.sh; fi; \
	TD=$$(mktemp -d); \
	trap 'kill $$DPID 2>/dev/null; wait $$DPID 2>/dev/null; rm -rf "$$TD"' EXIT INT TERM; \
	export FSDB_SOCKET="$$TD/fsdb.sock"; \
	export FSDB_DBDIR="$$TD/db"; \
	export FSDB_LOGDIR="$$TD/log"; \
	export FSDB_PIDFILE="$$TD/fsdb.pid"; \
	mkdir -p "$$FSDB_DBDIR" "$$FSDB_LOGDIR"; \
	./daemon & DPID=$$!; \
	for i in 1 2 3 4 5 6 7 8 9 10; do \
		[ -S "$$FSDB_SOCKET" ] && break; \
		kill -0 $$DPID 2>/dev/null || { echo "daemon failed to start"; exit 1; }; \
		sleep 0.5; \
	done; \
	[ -S "$$FSDB_SOCKET" ] || { echo "daemon socket not ready"; exit 1; }; \
	./test/test_suite.sh

install: daemon fsdb.service
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 daemon $(DESTDIR)$(BINDIR)/fsdb
	install -d $(DESTDIR)$(UNITDIR)
	install -m 644 fsdb.service $(DESTDIR)$(UNITDIR)/fsdb.service
	install -d $(DESTDIR)$(CONFDIR)
	@id -u fsdb >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin -d /var/lib/fsdb fsdb
	@echo ""
	@echo "Installed. To enable:"
	@echo "  systemctl daemon-reload"
	@echo "  systemctl enable --now fsdb"
	@echo ""
	@echo "Directories /run/fsdb, /var/lib/fsdb, /var/log/fsdb are"
	@echo "managed by systemd (RuntimeDirectory, StateDirectory, LogsDirectory)."
	@echo ""
	@echo "Optional: create /etc/fsdb/token to enable HTTP auth"

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/fsdb
	rm -f $(DESTDIR)$(UNITDIR)/fsdb.service
	@echo "Uninstalled. Run 'systemctl daemon-reload' to refresh systemd."

.PHONY: all clean test install uninstall
