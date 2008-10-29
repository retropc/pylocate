#!/usr/bin/env python

import sys, os, trie

def main(indexfile, basepath):
  basepath = unicode(os.path.realpath(basepath))
  
  newfile = "%s.new" % indexfile
  i = trie.FIndexWriteTrie(newfile, dict(base=basepath))

  l = len(basepath)

  for root, dirs, files in os.walk(basepath):
    xroot = root[l:]
    if len(xroot) > 0 and xroot[0] == os.path.sep:
      xroot = xroot[1:]

    for x in files:
#      print xroot, x
      i.add(xroot, x)
    
  i.close()

  if os.path.exists(indexfile):
    os.unlink(indexfile)
  os.rename(newfile, indexfile)
  
if __name__ == "__main__":
  if len(sys.argv) < 3:
    print "usage: %s [index file] [base path]" % sys.argv[0]
  else:
    main(sys.argv[1], sys.argv[2])
