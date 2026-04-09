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
	./test/test_suite.sh

install: daemon fsdb.service
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 daemon $(DESTDIR)$(BINDIR)/fsdb
	install -d $(DESTDIR)$(UNITDIR)
	install -m 644 fsdb.service $(DESTDIR)$(UNITDIR)/fsdb.service
	install -d $(DESTDIR)$(CONFDIR)
	install -d $(DESTDIR)/var/lib/fsdb
	install -d $(DESTDIR)/var/run
	@echo ""
	@echo "Installed. To enable:"
	@echo "  systemctl daemon-reload"
	@echo "  systemctl enable --now fsdb"
	@echo ""
	@echo "Optional: create /etc/fsdb/token to enable HTTP auth"

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/fsdb
	rm -f $(DESTDIR)$(UNITDIR)/fsdb.service
	@echo "Uninstalled. Run 'systemctl daemon-reload' to refresh systemd."

.PHONY: all clean test install uninstall
