CC = clang
CFLAGS = -Wall -Wextra -Wpedantic -std=c11 -Wconversion -fsanitize=undefined -fsanitize=address -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O2 -Wformat -Wshadow -Wcast-align	

all: daemon client

daemon: daemon.c
	$(CC) $(CFLAGS) -o daemon daemon.c -lpthread

client: client.c
	$(CC) $(CFLAGS) -o client client.c

clean:
	rm -f daemon client db.socket
