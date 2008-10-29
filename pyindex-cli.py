#!/usr/bin/env python

import sys, os, trie

def main(indexfile, terms):
  i = trie.FIndexReadTrie(indexfile)

  try:
    base = i.metadata["base"]
    for x in i[terms]:
      print os.path.join(base, *x)
  finally:
    i.close()

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print "usage: %s [index file] [search term]" % sys.argv[0]
  else:
    main(sys.argv[1], sys.argv[2])
