#!/usr/bin/env python

import sys, os, trie

def main(indexfile, paths):
  paths = [unicode(os.path.realpath(x)) for x in paths]
  
  newfile = "%s.new" % indexfile
  if len(paths) == 1:
    d = dict(base=paths[0])
    l = len(paths[0])
  else:
    d = dict(base="")
    l = 0

  i = trie.FIndexWriteTrie(newfile, d)

  for path in paths:
    for root, dirs, files in os.walk(path):
      xroot = root[l:]
      if len(xroot) > 0 and xroot[0] == os.path.sep:
        xroot = xroot[1:]

      for x in files:
        i.add(xroot, x)
    
  i.close()

  if os.path.exists(indexfile):
    os.unlink(indexfile)
  os.rename(newfile, indexfile)
  
if __name__ == "__main__":
  if len(sys.argv) < 3:
    print "usage: %s [index file] [path 1] ?path 2? ... ?path n?" % sys.argv[0]
  else:
    main(sys.argv[1], sys.argv[2:])
