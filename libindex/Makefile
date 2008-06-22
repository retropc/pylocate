CFLAGS+=-g3 -Wall -pedantic -std=c99

default: main

index.c: index.h

main: main.o index.o crc32.o

clean:
	rm -f *.o main
