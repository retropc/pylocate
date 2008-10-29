import struct, mmap, os, platform, copy

INT_LEN = 4

def U32(x):
  q =  struct.pack("!L", x)
  assert(len(q) == INT_LEN)
  return q

def gU32(x):
  return struct.unpack("!L", x)[0]
  
def dpermutate(x, i=[1,2,3]):
  o = []
  for y in i:
    o.extend(permutations(x, y))
  return o

def permutations(x, l):
  o = []
  for i in range(0, len(x) - l + 1):
    o.append(x[i:i + l])
  return o

class fobject(object):
  def __new__(type, *args, **kwargs):
    obj = object.__new__(type)
    obj._setupfields()
    return obj
    
class DataListException(Exception):
  pass
  
class DataList(fobject):
  def __init__(self, handle):
    super(DataList, self).__init__()
    self._handle = handle
    
  def _setupfields(self):
    self._handle = None
    
class WriteDataList(DataList):
  def __init__(self, fn, startpos):
    super(WriteDataList, self).__init__(open(fn, "w+b"))
    self._handle.seek(startpos)
    self._pos = startpos
  
  def add(self, str):
    return self.write("%s%s" % (U32(len(str)), str))

  def write(self, str):
    self._handle.write(str)
    p = self._pos
    self._pos+=len(str)
    return p
    
  def reset(self):
    self._handle.seek(0, 0)
    
  def read(self, length):
    return self._handle.read(length)

  def __del__(self):
    self.close()
    
  def close(self):
    if self._handle:
      self._handle.close()
      self._handle = None
      
  def writedict(self, d):
    previtem = 0
    for k, v in d.iteritems():
      keypos, valuepos = self.add(k), self.add(v)
      previtem = self.write("%s%s%s" % (U32(previtem), U32(keypos), U32(valuepos)))
      
    return previtem
    
class ReadDataList(DataList):
  def __init__(self, m, startpos):
    super(ReadDataList, self).__init__(m)
    self._offset = startpos

  def read(self, pos, length):
    self._handle.seek(pos+self._offset, 0)
    x = self._handle.read(length)
    if len(x) != length:
      raise DataListException("Bad length")
    return x

  def get(self, pos):
    l = gU32(self.read(pos, INT_LEN))
    return self.read(pos + INT_LEN, l)

  def readdict(self, pos):
    while pos != 0:
      x = self.read(pos, INT_LEN*3)
      pos, keypos, valuepos = gU32(x[:INT_LEN]), gU32(x[INT_LEN:INT_LEN*2]), gU32(x[INT_LEN*2:INT_LEN*3])
      yield self.get(keypos), self.get(valuepos)
      
  def close(self):
    pass

class DeletingWriteDataList(WriteDataList):
  def __init__(self, fn, startpos):
    self.__fn = fn
    super(DeletingWriteDataList, self).__init__(fn, startpos)
    
  def close(self):
    super(DeletingWriteDataList, self).close()
    if self.__fn:
      fn = self.__fn
      self.__fn = None
      os.unlink(fn)
    
class MMapReadDataList(ReadDataList):
  def read(self, pos, length):
    pos = pos + self._offset
    x = self._handle[pos:pos+length]
    if len(x) != length:
      raise DataListException("Bad length")
    return x
    
class TrieException(Exception):
  pass

class MMapException(Exception):
  pass

def MMapOpen(fn, mode=""):
  if mode == "w":
    fmode = "r+b"
  elif mode == "r":
    fmode = "rb"
  else:
    raise MMapException("Bad mode")

  return open(fn, fmode)

def MMap(handle, length, mode="w"):
  if mode != "w" and mode != "r":
    raise MMapException("Bad mode")

  if platform.system() != "Windows":
    pos = handle.tell()

    handle.seek(0, 2)
    totallength = handle.tell()
    if totallength < length:
      handle.seek(length-1, 0)
      handle.write("\x00")
    handle.seek(pos, 0)

    if mode == "w":
      prot = mmap.PROT_WRITE|mmap.PROT_READ
    else:
      prot = mmap.PROT_READ

  if mode == "w":
    access = mmap.ACCESS_WRITE
  else:
    access = mmap.ACCESS_READ

  return mmap.mmap(handle.fileno(), length, access=access)
  
TRIE_SEPERATE = 0
TRIE_MERGED = 1

MODE_WRITE = 0
MODE_READ = 1

PLATFORM_WINDOWS = 0
PLATFORM_UNIX = 1

class SearchTrie(fobject):
  HEADERLEN = 1024
  VERSION = 0
  TYPE = TRIE_SEPERATE
  MAGIC = "FITRIE" + U32(VERSION)
  
  LEN = (256 + 256*256 + 256*256*256) * INT_LEN + HEADERLEN
  def __init__(self, handle, list, m):
    super(SearchTrie, self).__init__()
    self._handle = handle
    self._list = list
    self._m = m

  def _setupfields(self):
    self._handle, self._list, self._m = None, None, None
    
  def __del__(self):
    self.close()
    
  def close(self):
    if self._list:
      self._list.close()
      self._list = None
    if self._m:
      self._m.close()
      self._m = None
    if self._handle:
      self._handle.close()
      self._handle = None

  def _triepos(self, key):
    l = len(key)
    if l == 0 or l > 3:
      raise TrieException("Bad key")
    if l == 1:
      return INT_LEN*ord(key[0])
    elif l == 2:
      return INT_LEN*(ord(key[0]) + ord(key[1]) * 256 + 256)
    else:
      return INT_LEN*(ord(key[0]) + ord(key[1]) * 256 + ord(key[2])*256*256 + 256*256)
      
  def _write(self, pos, data):
    self._m[pos+self.HEADERLEN:pos+INT_LEN+self.HEADERLEN] = U32(data)
    
  def _read(self, pos, raw=False):
    d = self._m[pos+self.HEADERLEN:pos+INT_LEN+self.HEADERLEN]
    if raw:
      return d
    
    if len(d) != INT_LEN:
      raise TrieException("Bad read")
    return gU32(d)
    
class WriteTrie(SearchTrie):
  MODE = MODE_WRITE
  def __init__(self, fn, metadata, sclass=WriteDataList, suffix="sl"):
    self.__lastpath = None
  
    f = open(fn, "wb")
    f.close()
    
    h = MMapOpen(fn, mode="w")
    m = MMap(h, self.LEN, mode="w")

    super(WriteTrie, self).__init__(h, sclass("%s.%s" % (fn, suffix), 0), m)
    self.__writeheader(metadata)
    
  def __writeheader(self, metadata):
    self._m.seek(0, 0)
    self._m.write(self.MAGIC)
    self._m.write(U32(self.TYPE))
    
    metadata = copy.deepcopy(metadata)
    if platform.system() == "Windows":
      p = PLATFORM_WINDOWS
    else:
      p = PLATFORM_UNIX
    self.platform = metadata["__platform"] = chr(p)
      
    self._m.write(U32(self._list.writedict(metadata)))
    
  def __addpath(self, path):
    self.__lastpath = (path, self._list.add(path))
    return self.__lastpath[1]
    
  def add(self, keys, path, fn):
    if self.__lastpath is None or self.__lastpath[0] != path:
      pathpos = self.__addpath(path)
    else:
      pathpos = self.__lastpath[1]
      
    filepos = self._list.add(fn)
    datapos = U32(self._list.write("%s%s" % (U32(pathpos), U32(filepos))))
    
    for key in keys:
      triepos = self._triepos(key)
      
      existing = self._read(triepos, raw=True)
      if len(existing) != INT_LEN:
        raise TrieException("Bad lookup")

      listpos = self._list.write("%s%s" % (existing, datapos))
      self._write(triepos, listpos)
      
class ReadTrie(SearchTrie):
  MODE = MODE_READ
  def __init__(self, fn):
    handle = MMapOpen(fn, mode="r")
    handle.seek(0, 2)
    
    m = MMap(handle, handle.tell(), mode="r")
    self.__verifyheader(m)
    
    if self.__ismerged(m):
      l = MMapReadDataList(m, self.LEN)
    else:
      l = ReadDataList(open("%s.sl" % fn, "rb"), 0)
    
    self.__readmetadata(l)
    
    super(ReadTrie, self).__init__(handle, l, m)

  def __ismerged(self, m):
    return gU32(m[len(self.MAGIC):len(self.MAGIC)+INT_LEN]) == TRIE_MERGED
    
  def __verifyheader(self, m):
    if m[0:len(self.MAGIC)] != self.MAGIC:
      raise TrieException("Bad header")
    p = len(self.MAGIC)+INT_LEN
    self.metadatapos = gU32(m[p:p+INT_LEN])
    p+=INT_LEN
    
  def __readmetadata(self, l):
    self.metadata = dict(l.readdict(self.metadatapos))
    self.platform = ord(self.metadata["__platform"])
    del self.metadata["__platform"]
    
  def _get(self, key):
    triepos = self._triepos(key)
    d = self._read(triepos)
    
    while d != 0:
      d2 = self._list.read(d, INT_LEN*2)
      if len(d2) != INT_LEN*2:
        raise TrieException("Bad read")
      
      d = gU32(d2[:INT_LEN])
      datapos = gU32(d2[INT_LEN:])

      d2 = self._list.read(datapos, INT_LEN*2)
      if len(d2) != INT_LEN*2:
        raise TrieException("Bad read")
      
      pathpos = gU32(d2[:INT_LEN])
      filepos = gU32(d2[INT_LEN:])
      
      yield self._list.get(pathpos), self._list.get(filepos)

class MergedWriteTrie(WriteTrie):
  TYPE = TRIE_MERGED
  def __init__(self, fn, metadata={}):
    super(MergedWriteTrie, self).__init__(fn, metadata, sclass=DeletingWriteDataList, suffix="tmp")
    
  def merge(self):
    if self._m is None or self._list is None:
      return
      
    self._m.close()
    self._m = None
    l = self._list
    self._list = None

    try:
      m = MMap(self._handle, self.LEN+l._pos, mode="w")
      try:
        pos = 0
        pstart = self.LEN
    
        l.reset()
        m.seek(self.LEN)
        while True:
          d = l.read(8192 * 2)
          le = len(d)
          if le == 0:
            break
          pos = pos + le
          m.write(d)
          
        assert(pos == l._pos)
      finally:
          m.close()
    finally:
      l.close()
      
  def close(self):
    self.merge()
    super(MergedWriteTrie, self).close()

class BaseFIndexWriteTrie:
  def add(self, path, fn):
    keys = list(set(dpermutate(fn.lower().encode("iso-8859-1", "replace"))))
    keys.sort()
    WriteTrie.add(self, keys, path.encode("utf-8"), fn.encode("utf-8"))
      
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
      df = file.lower().decode("utf-8").encode("iso-8859-1", "replace")
      if df.find(key) != -1:
        yield path.decode("utf-8"), file.decode("utf-8")

class FIndexMergedWriteTrie(BaseFIndexWriteTrie, MergedWriteTrie):
  pass

class FIndexSeperateWriteTrie(BaseFIndexWriteTrie, WriteTrie):
  pass

FIndexWriteTrie = FIndexMergedWriteTrie

def main():
  a = FIndexMergedWriteTrie("test", dict(fish="mooooo", zz="moaefawefawef"))
#  a = FIndexSeperateWriteTrie("test")
  try:
    a.add("mooxyz", "fff")
  finally:
    a.close()

  b = FIndexReadTrie("test")
  try:
    #print list(b["moo"])
    print b.metadata
    print list(b["f"])
  finally:
    b.close()
    
if __name__ == "__main__":
  main()

