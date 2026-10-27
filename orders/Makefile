CC ?= cc
CFLAGS ?= -O2 -g -std=gnu11 -Wall -Wextra -Wno-unused-parameter -pthread -I/usr/include/postgresql
LDLIBS = -lmicrohttpd -lpq -lcurl -lcjson -lpthread

SRCS = main.c db.c client.c worker.c
OBJS = $(SRCS:.c=.o)

.PHONY: all clean

all: orders

orders: $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDLIBS)

%.o: %.c orders.h
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f orders $(OBJS)
