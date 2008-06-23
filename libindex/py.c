#include <Python.h>
#include <structmember.h>

#include "index.h"

typedef struct {
  PyObject_HEAD
  struct findex *index;
  PyObject *metadict;
} Index;

typedef struct {
  PyObject_HEAD
  struct findex_ctx ctx;
  Index *index;
} IndexContext;

static PyObject *IndexContext_NEW(Index *parent, struct findex_ctx *c);
static void IndexContext_dealloc(IndexContext *self);
static PyObject *IndexContext_next(IndexContext *self);
static PyObject *Index_lookup(Index *self, PyObject *args);

static PyObject *get_metadict(Index *self);

PyTypeObject IndexContext_Type = {
  PyObject_HEAD_INIT(&PyType_Type)
  0,
  "IndexContext",                       /* tp_name */
  sizeof(IndexContext),                 /* tp_basicsize */
  0,                                    /* tp_itemsize */
  /* methods */
  (destructor)IndexContext_dealloc,     /* tp_dealloc */
  0,                                    /* tp_print */
  0,                                    /* tp_getattr */
  0,                                    /* tp_setattr */
  0,                                    /* tp_compare */
  0,                                    /* tp_repr */
  0,                                    /* tp_as_number */
  0,                                    /* tp_as_sequence */
  0,                                    /* tp_as_mapping */
  0,                                    /* tp_hash */
  0,                                    /* tp_call */
  0,                                    /* tp_str */
  PyObject_GenericGetAttr,              /* tp_getattro */
  0,                                    /* tp_setattro */
  0,                                    /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT,                   /* tp_flags */
  0,                                    /* tp_doc */
  0,                                    /* tp_traverse */
  0,                                    /* tp_clear */
  0,                                    /* tp_richcompare */
  0,                                    /* tp_weaklistoffset */
  PyObject_SelfIter,                    /* tp_iter */
  (iternextfunc)IndexContext_next,      /* tp_iternext */
  0,                                    /* tp_methods */
};

static PyObject *Index_new(PyObject *self, PyObject *args);
static void Index_dealloc(Index *self);

static PyMappingMethods Index_Mapping = {
  0,
  (binaryfunc)Index_lookup,
  0,
};

static PyMemberDef Index_Members[] = {
  {"metadata", T_OBJECT_EX, offsetof(Index, metadict), 0, "metadata"},
  {NULL},
};

PyTypeObject Index_Type = {
  PyObject_HEAD_INIT(&PyType_Type)
  0,
  "Index",                              /* tp_name */
  sizeof(Index),                        /* tp_basicsize */
  0,                                    /* tp_itemsize */
  /* methods */
  (destructor)Index_dealloc,            /* tp_dealloc */
  0,                                    /* tp_print */
  0,                                    /* tp_getattr */
  0,                                    /* tp_setattr */
  0,                                    /* tp_compare */
  0,                                    /* tp_repr */
  0,                                    /* tp_as_number */
  0,                                    /* tp_as_sequence */
  &Index_Mapping,                       /* tp_as_mapping */
  0,                                    /* tp_hash */
  0,                                    /* tp_call */
  0,                                    /* tp_str */
  PyObject_GenericGetAttr,              /* tp_getattro */
  0,                                    /* tp_setattro */
  0,                                    /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT,                   /* tp_flags */
  0,                                    /* tp_doc */
  0,                                    /* tp_traverse */
  0,                                    /* tp_clear */
  0,                                    /* tp_richcompare */
  0,                                    /* tp_weaklistoffset */
  0,                                    /* tp_iter */
  0,                                    /* tp_iternext */
//  Index_Methods,                        /* tp_methods */
  0,
  Index_Members,                        /* tp_members */
};

static PyObject *Index_new(PyObject *self, PyObject *args) {
  Index *object;
  char *filename;
  struct findex *f;

  if(!PyArg_ParseTuple(args, "s", &filename))
    return NULL;

  f = findex_load(filename);
  if(!f)
    return NULL;

  object = PyObject_NEW(Index, &Index_Type);
  if(object) {
    object->index = f;
    object->metadict = get_metadict(object);
  } else {
    findex_close(f);
  }

  return (PyObject *)object;
}

static void Index_dealloc(Index *self) {
  findex_close(self->index);

  Py_DECREF(self->metadict);

  PyObject_DEL(self);
}

static PyObject *IndexContext_NEW(Index *parent, struct findex_ctx *c) {
  IndexContext *ctx = NULL;

  ctx = PyObject_NEW(IndexContext, &IndexContext_Type);
  if(!ctx)
    return NULL;

  ctx->index = parent;
  memcpy(&ctx->ctx, c, sizeof(struct findex_ctx));
  Py_INCREF(parent);

  return (PyObject *)ctx;
}

static void IndexContext_dealloc(IndexContext *self) {
  Py_DECREF(self->index);

  PyObject_DEL(self);
}

static PyObject *IndexContext_next(IndexContext *self) {
  size_t lena, lenb;
  char *a, *b;

  if(!findex_next(&self->ctx, &a, &lena, &b, &lenb))
    return NULL;
  
  return Py_BuildValue("(s#s#)", a, lena, b, lenb);
}

static PyObject *Index_lookup(Index *self, PyObject *key) {
  struct findex_ctx c;
  int keylen;
  char *skey;

  if(!PyString_CheckExact(key))
    return NULL;

  if(PyString_AsStringAndSize(key, &skey, &keylen) == -1)
    return NULL;

  if(!findex_lookup(&c, self->index, skey, keylen))
    return NULL;

  return (PyObject *)IndexContext_NEW(self, &c);
}

static PyObject *get_metadict(Index *self) {
  int i = 0, type;
  findex_e ii;
  char *key;
  PyObject *metadict = PyDict_New();
  PyObject *foo;

  while((type=findex_metadata_gettype(self->index, i, &key)) != -1) {
    switch(type) {
      case TYPE_STRING:
        foo = PyString_FromString(findex_metadata_getistring(self->index, i));
        if(foo) {
          PyDict_SetItemString(metadict, key, foo);
          Py_DECREF(foo);
        }
        break;
      case TYPE_INTEGER:
        if(findex_metadata_getiint(self->index, i, &ii)) {
          foo = PyInt_FromLong(ii);
          if(foo) {
            PyDict_SetItemString(metadict, key, foo);
            Py_DECREF(foo);
          }
        }
        break;
    }
    i++;
  }

  return metadict;
}

static PyMethodDef methods[] = {
  {"Index", Index_new, METH_VARARGS},
  {NULL, NULL},
};

PyMODINIT_FUNC initcIndex(void) {
  (void)Py_InitModule("cIndex", methods);
}
