from util import *
import datalist
import platform
import codec
import struct
import array

DEFAULT_CODEC = codec.ZlibCodec(level=3)
#DEFAULT_CODEC = codec.PlainCodec()
            
               #01234567890123456789012345678901234567890
               #          1         2         3         4
KEY_CHARS = "\x000123456789abcdefghijklmnopqrstuvwxyz ,-."
KEY_CHARS_LEN = len(KEY_CHARS)            
KEY_CHARS_LEN_SQ = KEY_CHARS_LEN*KEY_CHARS_LEN
KEY_MAP = [0] * 256
for i, x in enumerate(KEY_CHARS):
  KEY_MAP[ord(x)] = i
  
def permutate(x):
  o = set()
  for i in range(0, len(x) - 1):
    o.add(x[i:i + 2])
    o.add(x[i:i + 3])
  o.update(x)
  return o

class TrieException(Exception):
  pass

KEY_UNPACK_2, KEY_UNPACK_3 = struct.Struct("bb").unpack, struct.Struct("bbb").unpack
TRIE_KEY = {}
def trie_key(key):
  try:
    return TRIE_KEY[key]
  except KeyError:
    pass
    
  l = len(key)
  if l == 1:
    k = KEY_MAP[ord(key[0])]
  elif l == 2:
    kv = KEY_UNPACK_2(key[:2])
    k = KEY_MAP[kv[0]] + KEY_CHARS_LEN * KEY_MAP[kv[1]]
  else:
    kv = KEY_UNPACK_3(key[:3])
    k = KEY_MAP[kv[0]] + KEY_CHARS_LEN * (KEY_MAP[kv[1]] + KEY_CHARS_LEN * KEY_MAP[kv[2]])

  TRIE_KEY[key] = k
  return k

class SearchTrie(object):
  VERSION = 1
  MAGIC = "FITRIE" + U32(VERSION)
  TRIE_LEN = KEY_CHARS_LEN**3 * INT_LEN

  def __init__(self, f, l, offset=0):
    self._f, self._l, self.__offset = f, l, offset
    self._trie = array.array(INT_TYPE)
  
  def close(self):
    self._f.close()
    
class HeaderTrie(SearchTrie):
  HEADER_LEN = 1024
  
class WriteTrie(HeaderTrie):
  def __init__(self, filename, metadata={}, codec=DEFAULT_CODEC):
    f = open(filename, "wb")

    l = datalist.WriteDataList(f, codec, offset=self.HEADER_LEN + self.TRIE_LEN)
    super(WriteTrie, self).__init__(f, l, offset=self.HEADER_LEN)

    self.__last_path = None
    self.__counts = bytearray(KEY_CHARS_LEN**2)
    self._trie.extend(bytearray(KEY_CHARS_LEN**3))
    
    self.__write_header(metadata)
    
  def __write_header(self, metadata):
    self._f.seek(0)
    self._f.write(self.MAGIC)
    
    metadata_w = {}
    metadata_w.update(metadata)
    metadata_w["__platform"] = platform.system()
    self._f.write(U32(self._l.write_dict(metadata_w)))

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
        c = counts[trie_pos]
        if c == 255:
          continue
        counts[trie_pos]+=1
      
      next_pos = self._trie[trie_pos]
      list_pos = self._l.write("%s%s" % (U32(next_pos), data_pos))
      self._trie[trie_pos] = list_pos
      
  def close(self):
    self._l.flush()
    self._f.seek(self.HEADER_LEN)
    self._trie.tofile(self._f)
    super(WriteTrie, self).close()
    
class ReadTrie(HeaderTrie):
  def __init__(self, filename, codec=DEFAULT_CODEC):
    f = MMapOpen(filename, "r")
    self._m = MMap(f, "r")
    l = datalist.ReadDataList(f, self._m, codec, offset=self.TRIE_LEN + self.HEADER_LEN)
    super(ReadTrie, self).__init__(f, l, offset=self.HEADER_LEN)

    self.__read_header()
    f.seek(self.HEADER_LEN)
    self._trie.fromfile(f, KEY_CHARS_LEN**3)
    
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
    current_pos = self._trie[trie_pos]
    while current_pos != 0:
      current_pos, data_pos = gU32x2(self._l.read(current_pos, INT_LEN*2))
      data = self._l.read_string(data_pos)

      yield self._l.read_string(gU32(data[:INT_LEN])), data[INT_LEN:]

  def close(self):
    self._m.close()
    super(ReadTrie, self).close()
    
class FIndexWriteTrie(WriteTrie):
  def add(self, path, fn):
    keys = permutate(fn.lower().encode("iso-8859-1", "replace"))
    self._add(keys, path.encode("utf8"), fn.encode("utf8"))
      
class FIndexReadTrie(ReadTrie):
  def __getitem__(self, key):
    key = key.lower().encode("iso-8859-1", "replace")
    if not key:
      return
      
    xlen = min(3, len(key))
    for path, file in self._get(key[:xlen]):
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
