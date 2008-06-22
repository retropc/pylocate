#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

#include <stdio.h>
#include <unistd.h>

#include "index.h"

#define DEBUG

#define Error(x) printf("%s\n", x)
#define MCpy(dst, m, pos, size) memcpy(dst, m + pos, size)
#define MIntCpy(dst, m, pos) { findex_e __t; MCpy(&__t, m, pos, sizeof(findex_e)); dst = htonl(__t); }

#define TYPE_STRING 0
#define TYPE_INTEGER 1
#define FINDEX_VERSION 0

uint32_t crc32(const char *s, const size_t len);

struct fdict_element {
  char *key;
  union {
    char *s;
    findex_e i;
  } value;
  int type;
};

struct fdict {
  findex_e size;
  struct fdict_element elements[];
};

struct ftable {
  findex_e size;
  findex_e *table;
  findex_e *chains;
};

struct fstringlist {
  findex_e size;
  findex_e *index;
  char *values;
};

struct fdatalist {
  findex_e *values;
};

struct findex {
  int fd;
  char *map;
  size_t len;

  struct fdict *metadata;
  struct ftable index;

  struct fdatalist datalist;
  struct fstringlist pathlist;
  struct fstringlist filelist;
};

static struct findex *findex_open(const char *filename) {
  struct findex *f, fo;
  struct stat sb;

  memset(&fo, 0, sizeof(fo));

  fo.fd = open(filename, O_RDONLY);
  if(fo.fd == -1)
    return NULL;

  if(fstat(fo.fd, &sb) == -1) {
    close(fo.fd);
    return NULL;
  }

  fo.len = sb.st_size;

  fo.map = mmap(0, fo.len, PROT_READ, MAP_SHARED, fo.fd, 0);
  if(fo.map == MAP_FAILED) {
    close(fo.fd);
    return NULL;
  }

  f = (struct findex *)malloc(sizeof(struct findex));
  if(!f) {
    munmap(fo.map, fo.len);
    close(fo.fd);
    return NULL;
  }

  memcpy(f, &fo, sizeof(struct findex));

  return f;
}

static void fdict_free(struct fdict *d) {
  int i;

  for(i=0;i<d->size;i++)
    if(d->elements[i].key)
      free(d->elements[i].key);

  free(d);
}

static struct fdict *fdict_load(const char *m) {
  findex_e size, i;
  size_t pos, ssize;
  struct fdict *d;

  MIntCpy(size, m, 0);
  ssize = sizeof(struct fdict_element) * size + sizeof(struct fdict);

  d = (struct fdict *)malloc(ssize);
  if(!d)
    return NULL;
  memset(d, 0, ssize);
  d->size = size;

  for(pos=sizeof(findex_e),i=0;i<size;i++) {
    findex_e keylen, alloclen, vlen = 0;
    MIntCpy(keylen, m, pos);
    pos+=sizeof(findex_e);

    MIntCpy(d->elements[i].type, m, pos + keylen);

    alloclen = keylen + 1;
    if(d->elements[i].type == TYPE_STRING) {
      MIntCpy(vlen, m, pos + keylen + sizeof(findex_e));
      alloclen+=vlen + 1;
    }

    d->elements[i].key = (char *)malloc(alloclen);
    if(!d->elements[i].key) {
      fdict_free(d);
      return NULL;
    }

    MCpy(d->elements[i].key, m, pos, keylen);
    d->elements[i].key[keylen] = '\0';
    d->elements[i].value.s = d->elements[i].key + keylen + 1;

    pos+=keylen + sizeof(findex_e);
    switch(d->elements[i].type) {
      case TYPE_STRING:
        MCpy(d->elements[i].value.s, m, pos + sizeof(findex_e), vlen);
        pos+=vlen + sizeof(findex_e);
        break;
      case TYPE_INTEGER:
        MIntCpy(d->elements[i].value.i, m, pos);
        pos+=sizeof(findex_e);
        break;
      default:
        Error("Bad type");
        return NULL;
    }
  }

  return d;
}

void findex_close(struct findex *f) {
  if(f->metadata)
    fdict_free(f->metadata);
  munmap(f->map, f->len);
  close(f->fd);
  free(f);
}

static struct fdict_element *fdict_get(struct fdict *d, const char *key) {
  int i;

  for(i=0;i<d->size;i++) {
    if(strcmp(key, d->elements[i].key))
      continue;

    return &d->elements[i];
  }
  return NULL;
}

static char *fdict_getstring(struct fdict *d, const char *key) {
  struct fdict_element *e = fdict_get(d, key);
  if(!e || (e->type != TYPE_STRING))
    return NULL;

  return e->value.s;
}

static int fdict_getint(struct fdict *d, const char *key, findex_e *v) {
  struct fdict_element *e = fdict_get(d, key);
  if(!e || (e->type != TYPE_INTEGER))
    return 0;

  *v = e->value.i;
  return 1;
}

static void ftable_load(struct ftable *t, void *m, const findex_e size) {
  t->table = m;
  t->size = size;
  t->chains = (findex_e *)m + size;
}

static fchain *ftable_get(struct ftable *t, const findex_e index) {
  findex_e ret;

  MIntCpy(ret, t->table, index);

  return &t->chains[ret];
}

static fchain *ftable_lookup(struct ftable *t, const char *data, const size_t len) {
  uint32_t h = crc32(data, len) % t->size;

  return ftable_get(t, h);
}

findex_e fchain_get(fchain *c, const findex_e index) {
  findex_e ret;

  MIntCpy(ret, c, index);

  return ret;
}

static void fstringlist_load(struct fstringlist *s, char *m) {
  MIntCpy(s->size, m, 0);
  s->index = (findex_e *)m + 1;
  s->values = m + (2 + s->size) * sizeof(findex_e);
}

static char *fstringlist_get(struct fstringlist *s, const findex_e index, size_t *len) {
  findex_e startpos, endpos;

  MIntCpy(endpos, s->index, index);
  if(index == 0) {
    startpos = 0;
  } else {
    MIntCpy(startpos, s->index, index - 1);
  }

  *len = endpos - startpos;

  return &s->values[startpos];
}

static void fdatalist_load(struct fdatalist *d, void *m) {
  d->values = m;
}

static int fdatalist_get(struct fdatalist *d, const findex_e index, findex_e *a, findex_e *b) {
  findex_e i = index * 2, f;
  
  MIntCpy(f, d->values, i);
  if(f == NOT_PRESENT)
    return 0;

  *a = f;
  MIntCpy(*b, d->values, i + 1);

  return 1;
}

#ifdef DEBUG
void findex_printdict(struct fdict *d) {
  int i;
  for(i=0;i<d->size;i++) {
    printf("%d: %s(%d) == ", i, d->elements[i].key, d->elements[i].type);
    switch(d->elements[i].type) {
      case TYPE_STRING:
        printf("|%s|\n", d->elements[i].value.s);
        break;
      case TYPE_INTEGER:
        printf("%d\n", d->elements[i].value.i);
        break;
      default:
        printf("??\n");
    }
  }
}

void fstringlist_print(struct fstringlist *s) {
  int i;

  for(i=0;i<s->size;i++) {
    char buf[8192], *p;
    size_t len;

    p = fstringlist_get(s, i, &len);
    memcpy(buf, p, len);
    buf[len] = '\0';

    printf("%s\n", buf);
  }
}

void fdatalist_print(struct fdatalist *d) {
  findex_e a, b;
  int i = 0;

  while(fdatalist_get(d, i++, &a, &b))
    printf("(%d, %d)\n", a, b);
}
#endif /* DEBUG */

struct findex *findex_load(const char *filename) {
  struct findex *f = findex_open(filename);
  findex_e version, indexoffset, dataoffset, pathoffset, fileoffset, tablesize;
  if(!f)
    return NULL;

  if(memcmp(f->map, "PIDX", 4)) {
    findex_close(f);
    return NULL;
  }

  f->metadata = fdict_load(f->map + sizeof(findex_e));
  if(!f->metadata || !fdict_getint(f->metadata, "version", &version) || (version != FINDEX_VERSION)) {
    findex_close(f);
    return NULL;
  }

  if(!fdict_getint(f->metadata, "indexoffset", &indexoffset) ||
     !fdict_getint(f->metadata, "dataoffset", &dataoffset) ||
     !fdict_getint(f->metadata, "pathoffset", &pathoffset) ||
     !fdict_getint(f->metadata, "fileoffset", &fileoffset) || 
     !fdict_getint(f->metadata, "tablesize", &tablesize)
    ) {
    findex_close(f);
    return NULL;
  }

  ftable_load(&f->index, f->map + indexoffset, tablesize);
  fstringlist_load(&f->pathlist, f->map + pathoffset);
  fstringlist_load(&f->filelist, f->map + fileoffset);
  fdatalist_load(&f->datalist, f->map + dataoffset);

  return f;
}

int findex_lookup(struct findex_ctx *c, struct findex *f, const char *key, const size_t len) {
  fchain *cc = ftable_lookup(&f->index, key, len);
  if(!cc)
    return 0;

  c->chain = cc;
  c->findex = f;
  c->index = 0;

  return 1;
}

int findex_next(struct findex_ctx *c, char **path, size_t *pathlen, char **file, size_t *filelen) {
  findex_e pathindex, fileindex;
  findex_e dataindex = fchain_get(c->chain, c->index);
  if(dataindex == NOT_PRESENT)
    return 0;

  if(!fdatalist_get(&c->findex->datalist, dataindex, &pathindex, &fileindex))
    return 0;

  *path = fstringlist_get(&c->findex->pathlist, pathindex, pathlen);
  *file = fstringlist_get(&c->findex->filelist, fileindex, filelen);

  c->index++;
  return 1;
}

char *findex_metadata_getstring(struct findex *f, const char *key) {
  return fdict_getstring(f->metadata, key);
}

int findex_metadata_getint(struct findex *f, const char *key, findex_e *out) {
  return fdict_getint(f->metadata, key, out);
}
