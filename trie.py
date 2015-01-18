from util import *
import datalist
import mmap
import platform
import codec

DEFAULT_CODEC = codec.ZlibCodec(level=3)
#DEFAULT_CODEC = codec.PlainCodec()

def dpermutate(x, i=(1,2,3)):
  return permutations(x, 1) + permutations(x, 2) + permutations(x, 3)

def permutations(x, l):
  o = []
  for i in range(0, len(x) - l + 1):
    o.append(x[i:i + l])
  return o

class TrieException(Exception):
  pass

TRIE_KEY = {}
def trie_key(key):
  try:
    return TRIE_KEY[key]
  except KeyError:
    pass
    
  l = len(key)
  if l == 3:
    k = INT_LEN*(ord(key[0]) + ord(key[1]) * 256 + ord(key[2])*65536 + 65536)
  elif l == 2:
    k = INT_LEN*(ord(key[0]) + ord(key[1]) * 256 + 256)
  else:
    k = INT_LEN*ord(key[0])
  TRIE_KEY[key] = k
  return k
  
class SearchTrie(object):
  VERSION = 1
  MAGIC = "FITRIE" + U32(VERSION)
  TRIE_LEN = (256 + 256*256 + 256*256*256) * INT_LEN

  def __init__(self, f, m, l, offset=0):
    self._f, self._m, self._l, self.__offset = f, m, l, offset
    
  def close(self):
    self._m.close()
    self._f.close()
    
  def _read(self, pos):
    pos+=self.__offset
    return self._m[pos:pos+INT_LEN]

  def _write(self, pos, data):
    pos+=self.__offset
    self._m[pos:pos+INT_LEN] = U32(data)
    
class HeaderTrie(SearchTrie):
  HEADER_LEN = 1024
  
class WriteTrie(HeaderTrie):
  def __init__(self, filename, metadata={}, codec=DEFAULT_CODEC):
    self.__last_path = None
    self.__counts = bytearray(256 + 256*256)
    open(filename, "w").close()
    
    f = MMapOpen(filename, "w")
    length = self.TRIE_LEN + self.HEADER_LEN
    m = MMap(f, "w", length)
    l = datalist.WriteDataList(f, codec, offset=length)
    
    super(WriteTrie, self).__init__(f, m, l, offset=self.HEADER_LEN)
    self.__write_header(metadata)
    
  def __write_header(self, metadata):
    self._m.seek(0)
    self._m.write(self.MAGIC)
    
    metadata_w = {}
    metadata_w.update(metadata)
    metadata_w["__platform"] = platform.system()
    self._m.write(U32(self._l.write_dict(metadata_w)))

  def __intern_path(self, path):
    if self.__last_path is None or self.__last_path[0] != path:
      path_pos = self._l.write_string(path)
      self.__last_path = path, path_pos
    else:
      path_pos = self.__last_path[1]
    return path_pos
    
  def _add(self, keys, path, fn):
    path_pos = self.__intern_path(path)
    data_pos = U32(self._l.write_string("%s%s" % (U32(path_pos), fn)))
    
    counts = self.__counts
    for key in keys:
      trie_pos = trie_key(key)
      
      if len(key) < 3:
        trie_pos_d = trie_pos / INT_LEN
        c = counts[trie_pos_d]
        if c == 255:
          continue
        counts[trie_pos_d]+=1
      
      next_pos = self._read(trie_pos)
      list_pos = self._l.write("%s%s" % (next_pos, data_pos))
      self._write(trie_pos, list_pos)
      
  def close(self):
    self._l.flush()
    super(WriteTrie, self).close()
    
class ReadTrie(HeaderTrie):
  def __init__(self, filename, codec=DEFAULT_CODEC):
    f = MMapOpen(filename, "r")
    f.seek(0, 2)
    
    m = MMap(f, "r")
    l = datalist.ReadDataList(f, m, codec, offset=self.TRIE_LEN + self.HEADER_LEN)
    super(ReadTrie, self).__init__(f, m, l, offset=self.HEADER_LEN)

    self.__read_header()

  def __read_header(self):
    if self._m[:len(self.MAGIC)] != self.MAGIC:
      raise TrieException("Bad header")
    
    p = len(self.MAGIC)
    metadata_pos = gU32(self._m[p:p+INT_LEN])
    
    self.metadata = dict(self._l.read_dict(metadata_pos))
    self.platform = self.metadata["__platform"]
    del self.metadata["__platform"]
    
  def _get(self, key):
    trie_pos = trie_key(key)
    
    current_pos = gU32(self._read(trie_pos))
    while current_pos != 0:
      current_pos, data_pos = gU32x2(self._l.read(current_pos, INT_LEN*2))
      data = self._l.read_string(data_pos)

      yield self._l.read_string(gU32(data[:INT_LEN])), data[INT_LEN:]

class FIndexWriteTrie(WriteTrie):
  def add(self, path, fn):
    keys = set(dpermutate(fn.lower().encode("iso-8859-1", "replace")))
    self._add(keys, path.encode("utf8"), fn.encode("utf8"))
      
class FIndexReadTrie(ReadTrie):
  def __getitem__(self, key):
    key = key.lower().encode("iso-8859-1", "replace")

    xlen = len(key)
    if xlen > 3:
      xlen = 3

    p = permutations(key, xlen)
    if not p:
      return

    for path, file in self._get(p[0]):
      df = file.lower().encode("iso-8859-1", "replace")
      if df.find(key) != -1:
        yield path.decode("utf8"), file.decode("utf8")

def main():
  a = FIndexWriteTrie("test", dict(fish="mooooo", zz="moaefawefawef"))
  try:
    a.add("mooxyz", "fff")
  finally:
    a.close()

  b = FIndexReadTrie("test")
  try:
    print b.metadata
    print list(b["f"])
  finally:
    b.close()
    
if __name__ == "__main__":
  main()
