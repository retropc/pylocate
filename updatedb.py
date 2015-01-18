#!/usr/bin/env python

import sys
import os
import scandir
import trie
import multiprocessing

def index(base, root):
  l = len(base)  
  batch = []
  for root, dirs, files in scandir.walk(root):
    root = root[l:]
    if len(root) > 0 and root[0] == os.path.sep:
      root = root[1:]

    for filename in files:
      batch+=[(root, filename)]
      if len(batch) > 50:
        yield batch
        batch = []
        
  if batch:
    yield batch

def worker_parallel_fn(base, path, q):
  for batch in index(base, path):
    q.put(batch)
  q.put(None)
  
def worker_parallel(base, paths):
  q = multiprocessing.Queue(maxsize=10000)
  
  remaining = len(paths)
  for path in paths:
    indexer = multiprocessing.Process(target=worker_parallel_fn, args=(base, path, q))
    indexer.start()
  
  while True:
    batch = q.get()
    if batch is None:
      remaining-=1
      if not remaining:
        break
      continue
      
    for x in batch:
      yield x

def worker_serial(base, paths):
  for path in paths:
    for x in index(base, path):
      for y in x:
        yield y

WORKER = worker_serial
def main(indexfile, paths):
  paths = [unicode(os.path.realpath(x)) for x in paths]
  
  if len(paths) == 1:
    base = paths[0]
  else:
    base = ""
  
  newfile = "%s.new" % indexfile
  i = trie.FIndexWriteTrie(newfile, {"base": base})
  try:
    for root, filename in WORKER(base, paths):
      i.add(root, filename)
  except:
    print repr(root), repr(filename)
    raise
  finally:
    i.close()

  if os.path.exists(indexfile):
    os.unlink(indexfile)
  os.rename(newfile, indexfile)
  
if __name__ == "__main__":
  if len(sys.argv) < 3:
    print "usage: %s [index file] [path 1] ?path 2? ... ?path n?" % sys.argv[0]
  else:
    main(sys.argv[1], sys.argv[2:])
