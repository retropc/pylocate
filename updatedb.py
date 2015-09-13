#!/usr/bin/env python

import sys
import os
import scandir
import trie
import indexer

def main(indexfile, paths, parallel=True, exclude_path=set(), exclude_name=set()):
  ic = indexer.ParallelIndexer if parallel else indexer.SerialIndexer
  paths = [unicode(os.path.realpath(x)) for x in paths]
  
  if len(paths) == 1:
    base = paths[0]
  else:
    if sys.platform == "win32":
      base = ""
    else:
      base = "/"
  
  newfile = "%s.new" % indexfile
  i = trie.FIndexWriteTrie(newfile, {"base": base})
  
  w = ic(base, paths, exclude_path, exclude_name)
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
  
  parallel, exclude_path, exclude_name = True, set(), set()
  while args:
    if args[0] == "-s":
      args.pop(0)
      parallel = False
    elif args[0] == "-x":
      args.pop(0)
      exclude_path.add(args.pop(0))
    elif args[0] == "-X":
      args.pop(0)
      exclude_name.add(args.pop(0))
    else:
      break
    
  if len(args) < 2:
    print "usage: %s ?-s? ?-x [path exclusion]? ?-X [name exclusion]? [index file] [path 1] ?path 2? ... ?path n?" % sys.argv[0]
    sys.exit(1)
    
  try:
    main(args[0], args[1:], parallel=parallel, exclude_path=exclude_path, exclude_name=exclude_name)
  except KeyboardInterrupt:
    pass
