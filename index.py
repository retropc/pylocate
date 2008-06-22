import os, util, marshal, zlib, struct

NOT_PRESENT = 2**32-1
TABLE_SIZE = 1024

def hash(x):
  return zlib.crc32(x)

class MDataStructure:
  def __init__(self, m, offset=0):
    self._m = m
    self._offset = offset

  def _ri(self, x):
    s = self._offset + x * 4
    return struct.unpack("!L", self._m[s:s+4])

class MHashTable:
  def __init__(self, m, offset, size):
    MDataStructure.__init__(self, m, offset)
    self.__size = size

  def __getitem__(self, key):
    h = hash(key) % self.__size
    return self._ri(h)

class MFixedList:
  def __init__(self, m, offset):
    MDataStructure.__init__(self, m)
    self.__m = m
    self.__offset = offset

  def __getitem__(self, index):
    return self._ri(index)

class MChainedHashTable:
  def __init__(self, m, (hoffset, hsize), (coffset, )):
    MDataStructure.__init__(self, m)
    self.__table = MHashTable(m, hoffset, hsize)
    self.__chains = MFixedList(m, coffset)

  def __getitem__(self, key):
    x = self.__table[key]
    if x == NOT_PRESENT:
      return None

    while True:
      x2 = self.__chains[x]
      x = x + 1 
      if x2 == NOT_PRESENT:
        break
      return x2
#      yield x2

class MStringList(MDataStructure):
  def __init__(self, m, offset):
    MDataStructure.__init__(self, m, offset)
    self.__length = self._ri(0)
    self.__totallength = self._ri(1)
    self.__index = MFixedList(m, offset + 8)
    self.__stringoffset = (2 + self.__length) * 4

  def __getitem__(self, index):
    stringpos = self.__index[index]
    if index == self.__length - 1:
      nextstringpos = self.__totallength
    else:
      nextstringpos = self.__index[index + 1]

    stringlen = nextstringpos - stringpos
    spos = self._offset + self.__stringoffset
    return self.__m[spos:spos+stringlen]

class Index:
  def __init__(self):
    pass

class WriteIndex(Index):
  def __init__(self, tablesize=TABLE_SIZE):
    Index.__init__(self)
    self.__pathdict = {}
    self.__filedict = {}
    self.__pathlist = []
    self.__filelist = []
    self.__index = [None] * tablesize
    self.__datalist = []
    self.metadata = {}
    self.__tablesize = tablesize
    self.metadata["tablesize"] = tablesize

  def dump(self, f):
    marshal.dump([self.metadata, self.__index, self.__pathlist, self.__filelist, self.__datalist], f)

  def add(self, filename):
    path, file = os.path.split(filename)
    lcpath, lcfile = path.lower(), file.lower()

    pathindex = self.__pathdict.get(path, -1)
    if pathindex == -1:
      self.__pathlist.append(path)
      self.__pathdict[path] = pathindex = len(self.__pathlist) - 1

    fileindex = self.__filedict.get(file, -1)
    if fileindex == -1:
      self.__filelist.append(file)
      self.__filedict[file] = fileindex = len(self.__filelist) - 1

    i = (pathindex, fileindex)
    self.__datalist.append(i)
    l = len(self.__datalist) - 1

    for p in util.dpermutate(lcfile):
      h = hash(p) % self.__tablesize
      x = self.__index[h]
      if x is None:
        self.__index[h] = [l]
      else:
        x.append(l)

class ReadIndex(Index):
  def __init__(self, f):
    Index.__init__(self)
    self.metadata, self.__index, self.__pathlist, self.__filelist, self.__datalist = marshal.load(f)
    self.__open = True
    self.__tablesize = self.metadata["tablesize"]

  def __lookup(self, key):
    xlen = len(key)
    if xlen > 3:
      xlen = 3
      
    p = util.permutations(key, xlen)
    if not p:
      return []
    x = self.__index[hash(p[0]) % self.__tablesize]
    if x is None:
      return []
    return x

  def __getitem__(self, key):
    key = key.lower()
    for pathindex, fileindex in (self.__datalist[x] for x in self.__lookup(key)):
      path = self.__pathlist[pathindex]
      file = self.__filelist[fileindex]
      if file.lower().find(key) != -1:
        yield path, file

  def close(self):
    if not self.__open:
      return
    self.__open = False

  def __del__(self):
    self.close()
