#include <netinet/in.h>

#define NOT_PRESENT -1

typedef uint32_t findex_e;
typedef findex_e fchain;
struct findex;

struct findex_ctx {
  struct findex *findex;
  findex_e index;
  fchain *chain;
};

struct findex *findex_load(const char *);
void findex_close(struct findex *);
int findex_lookup(struct findex_ctx *c, struct findex *f, const char *key, size_t len);
int findex_next(struct findex_ctx *c, char **path, size_t *pathlen, char **file, size_t *filelen);
char *findex_metadata_getstring(struct findex *f, const char *key);
int findex_metadata_getint(struct findex *f, const char *key, findex_e *out);
