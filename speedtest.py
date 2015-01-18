import indexer
import trie
import time

def produce():
  f = open("dump", "w")
  i = 0
  idx = indexer.SerialIndexer("", ["e:\\"])
  idx.start()
  for x in idx:
    f.write(x[0].decode("iso8859-1", "replace").encode("utf8") + "\n")
    f.write(x[1].decode("iso8859-1", "replace").encode("utf8") + "\n")
    i+=1
    if i > 55000:
      break
    
def run():
  f = open("dump", "r")
  a = []
  try:
    while True:
      b = f.next()[:-1].decode("utf8")
      c = f.next()[:-1].decode("utf8")
      a.append((b, c))
  except StopIteration:
    pass

  newfile = "foo.new"
  i = trie.FIndexWriteTrie(newfile, {"base": ""})
  try:
    for root, filename in a:
      i.add(root, filename)
  finally:
    i.close()

def main():    
  t = time.time()
  run()
  t2 = time.time()
  print t2 - t

if __name__ == "__main__":
  #produce()
  main()