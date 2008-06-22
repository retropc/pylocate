CFLAGS+=-g3 -Wall -pedantic -std=c99

all: main

main: main.o index.o crc32.o

index.c: index.h

clean:
	rm -f *.o main
