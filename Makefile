CC      := clang
CFLAGS  := -std=c11 -O2 -Wall -Wextra -Wpedantic -Wconversion \
           -fstack-protector-strong -D_FORTIFY_SOURCE=2 -fPIE
LDFLAGS := -Wl,-z,relro,-z,now -pie -lpthread

all: daemon

daemon: daemon.o
	$(CC) daemon.o $(LDFLAGS) -o $@

daemon.o: daemon.c
	$(CC) $(CFLAGS) -c daemon.c -o daemon.o

clean:
	rm -f daemon daemon.o
