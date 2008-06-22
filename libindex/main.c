#include <stdio.h>
#include <string.h>
#include "index.h"

u_int32_t chksum_crc32 (unsigned char *block, unsigned int length);

int main(void) {
  struct findex *f = findex_load("/home/chris/.pyindex2");
  struct findex_ctx c;
  char *a, *b;
  char bufa[1024], bufb[1024];
  size_t lena, lenb;

  if(!f) {
    puts("error opening");
    return 1;
  }

  if(!findex_lookup(&c, f, "a", 1)) { 
    puts("error looking up");
    findex_close(f);
    return 2;
  }

  while(findex_next(&c, &a, &lena, &b, &lenb)) {
    memcpy(bufa, a, lena);
    bufa[lena] = '\0';
    memcpy(bufb, b, lenb);
    bufb[lenb] = '\0';
    printf("%s/%s\n", bufa, bufb);
  }

  findex_close(f);
  return 0;
}

