CFLAGS+=-g3 -Wall -pedantic -std=c99

default: main

index.c: index.h

main: main.c index.c crc32.c

clean:
	rm -f *.o main
