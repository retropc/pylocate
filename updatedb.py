#!/usr/bin/env python

import sys
import os
import scandir
import trie
import indexer

def main(indexfile, paths):
  paths = [unicode(os.path.realpath(x)) for x in paths]
  
  if len(paths) == 1:
    base = paths[0]
  else:
    base = ""
  
  newfile = "%s.new" % indexfile
  i = trie.FIndexWriteTrie(newfile, {"base": base})
  w = indexer.ParallelIndexer(base, paths)
  w.start()
  try:
    for root, filename in w:
      i.add(root, filename)
  finally:
    i.close()
    w.terminate()
    
  if os.path.exists(indexfile):
    os.unlink(indexfile)
  os.rename(newfile, indexfile)
  
if __name__ == "__main__":
  args = sys.argv[1:]
    
  if len(args) < 2:
    print "usage: %s [index file] [path 1] ?path 2? ... ?path n?" % sys.argv[0]
    sys.exit(1)
    
  try:
    main(args[0], args[1:])
  except KeyboardInterrupt:
    pass
