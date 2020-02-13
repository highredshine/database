CFLAGS = -D_GNU_SOURCE -g -g3 -pthread -std=gnu99 -Wall -Wcast-qual -Wcast-align -Wextra -Wfloat-equal -Winline -Wnested-externs

PROMPT = -DPROMPT

EXECS = server client

.PHONY: all clean

all: $(EXECS)

server: server.c db.c comm.c
	gcc $(CFLAGS) $(PROMPT) $^ -o $@

client: client.c
	gcc $(CFLAGS) $< -o $@

clean:
	rm -f $(EXECS)