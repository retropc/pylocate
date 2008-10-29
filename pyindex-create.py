#!/usr/bin/env python

import sys, os, trie

def main(indexfile, basepath):
  basepath = os.path.realpath(basepath)
  i = trie.FIndexWriteTrie(indexfile, dict(base=basepath))

  l = len(basepath) + 1

  for root, dirs, files in os.walk(basepath):
    xroot = root[l:]
    for x in files:
      i.add(xroot, x)

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print "usage: %s [index file] [base path]" % sys.argv[0]
  else:
    main(sys.argv[1], sys.argv[2])
