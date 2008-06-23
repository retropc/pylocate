import os, util, marshal, zlib, struct, mmap

NOT_PRESENT = 2**32-1
TABLE_SIZE = 128 * 128 * 128

def hash(x):
  return zlib.crc32(x)

class MDataStructure:
  def __init__(self, m, offset=0):
    self._m = m
    self._offset = offset

  def _ri(self, x):
    s = self._offset + x*4
    return struct.unpack("!L", self._m[s:s+4])[0]

  def _di(self, x):
    s = self._offset + x
    return struct.unpack("!L", self._m[s:s+4])[0]

class MFixedList(MDataStructure):
  def __getitem__(self, x):
    return self._ri(x)

  def __iter__(self):
    i = 0
    while True:
      x = self._ri(i)
      if x == NOT_PRESENT:
        break
      i = i + 1
      yield x

class MDataList(MFixedList):
  def __getitem__(self, x):
    y = x * 2
    return MFixedList.__getitem__(self, y), MFixedList.__getitem__(self, y + 1)

  def __iter__(self):
    x = MFixedList.__iter__(self)
    while True:
      try:
        l = x.next()
      except StopIteration:
        return
      yield l, x.next()

class MHashTable(MDataStructure):
  def __init__(self, m, offset, size):
    MDataStructure.__init__(self, m, offset)
    self.__size = size

  def get(self, x):
    return self._ri(x)

  def __getitem__(self, key):
    h = hash(key) % self.__size
    return self._ri(h)

class MChainedHashTable(MDataStructure):
  def __init__(self, m, offset, size):
    MDataStructure.__init__(self, m, offset)
    self.__table = MHashTable(m, offset, size)
    self.__chainstart = self._offset + size * 4

  def __getitem__(self, key):
    x = self.__table[key]
    if x == NOT_PRESENT:
      return

    return MFixedList(self._m, self.__chainstart + x * 4)

  def get(self, x):
    x = self.__table.get(x)
    if x == NOT_PRESENT:
      return
    return MFixedList(self._m, self.__chainstart + x * 4)

class MStringList(MDataStructure):
  def __init__(self, m, offset):
    MDataStructure.__init__(self, m, offset)
    self.__length = self._ri(0)
    self.__index = MFixedList(m, offset + 4)
    self.__stringoffset = offset + (2 + self.__length) * 4

  def __getitem__(self, index):
    stringendpos = self.__index[index]
    if index == 0:
      stringstartpos = 0
    else:
      stringstartpos = self.__index[index - 1]

    return self._m[self.__stringoffset+stringstartpos:self.__stringoffset+stringendpos]

  def __iter__(self):
    for x in xrange(self.__length):
      yield self[x]

TYPE_STRING = 0
TYPE_INT = 1

class InternalDict(MDataStructure):
  def __init__(self, m, offset):
    MDataStructure.__init__(self, m, offset)
    self.__data = {}

    size = self._ri(0)
    pos = 4
    for i in xrange(size):
      keylen = self._di(pos)
      s = self._offset + pos + 4
      key = self._m[s:s + keylen]
      pos = pos + keylen + 4

      type_ = self._di(pos)
      pos = pos + 4
      if type_ == TYPE_STRING:
        valuelen = self._di(pos)
        pos = pos + 4
        s = self._offset + pos
        value = self._m[s:s + valuelen]
        pos = pos + valuelen
      elif type_ == TYPE_INT:
        value = self._di(pos)
        pos = pos + 4
      else:
        assert False, "Bad type."

      self.__data[key] = value

  def get(self):
    return self.__data

def MDictionary(m, offset):
  return InternalDict(m, offset).get()

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
    self.metadata["version"] = 0

  def __writedict(self, f, iw, d):
    iw(len(d))
    p = 4
    for k, v in d.iteritems():
      iw(len(k))
      f.write(k)
      p = p + len(k) + 4 + 4
      if isinstance(v, str):
        iw(TYPE_STRING)
        iw(len(v))
        f.write(v)
        p = p + 4 + len(v)
      elif isinstance(v, int):
        iw(TYPE_INT)
        iw(v)
        p = p + 4
      else:
        assert False, "Bad type"
    return p

  def __writehashtable(self, iw, d):
    i = 0
    for x in d:
      if x is None:
        iw(NOT_PRESENT)
      else:
        iw(x)
      i = i + 1
    return i * 4

  def __writechainedhashtable(self, iw, d):
    xpos = [0]
    def adapt(x):
      for x in d:
        if x is None:
          yield None
        else:
          yield xpos[0]
          xpos[0] = xpos[0] + len(x) + 1

    self.__writehashtable(iw, adapt(d))
    for x in d:
      if not x is None:
        self.__writefixedlist(iw, x)
    return (xpos[0] + len(d)) * 4

  def __writefixedlist(self, iw, d):
    i = 1
    for x in d:
      iw(x)
      i = i + 1
    iw(NOT_PRESENT)
    return i * 4

  def __writestringlist(self, f, iw, d):
    iw(len(d))
    xpos = [0]

    def adapt(x):
      for x in d:
        xpos[0] = xpos[0] + len(x)
        yield xpos[0]

    pos = self.__writefixedlist(iw, adapt(d)) + 4
    pos = pos + xpos[0]
    for x in d:
      f.write(x)
    return pos

  def dump(self, f):
    iw = lambda x: f.write(struct.pack("!L", x))
    f.write("PIDX")
    pos = 4

    metapos = f.tell()
    self.metadata["indexoffset"] = 0
    self.metadata["dataoffset"] = 0
    self.metadata["pathoffset"] = 0
    self.metadata["fileoffset"] = 0

    pos = pos + self.__writedict(f, iw, self.metadata)
    self.metadata["indexoffset"] = pos
    pos = pos + self.__writechainedhashtable(iw, self.__index)
    def adapt(x):
      for a, b in x:
        yield a
        yield b

    self.metadata["dataoffset"] = pos
    pos = pos + self.__writefixedlist(iw, adapt(self.__datalist))
    self.metadata["pathoffset"] = pos
    pos = pos + self.__writestringlist(f, iw, self.__pathlist)
    self.metadata["fileoffset"] = pos
    pos = pos + self.__writestringlist(f, iw, self.__filelist)
    endpos = f.tell()
    f.seek(metapos)
    self.__writedict(f, iw, self.metadata)
    f.seek(endpos)

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

class IndexProvider(Index):
  def __getitem__(self, key):
    key = key.lower()

    xlen = len(key)
    if xlen > 3:
      xlen = 3
      
    p = util.permutations(key, xlen)
    if not p:
      return

    for path, file in self._fetch(p[0]):
      if file.lower().find(key) != -1:
        yield path, file

  def close(self):
    pass

class SlowIndexProvider(IndexProvider):
  def __init__(self, fn):
    IndexProvider.__init__(self)

    self.__fn = open(fn, "rb")
    try:
      self.open = False
      self.__m = mmap.mmap(self.__fn.fileno(), 0, mmap.MAP_SHARED, mmap.PROT_READ)
      try:
        assert self.__m[0:4] == "PIDX"
        self.metadata = MDictionary(self.__m, 4)
        self.__tablesize = self.metadata["tablesize"]
        self.__index = MChainedHashTable(self.__m, self.metadata["indexoffset"], self.__tablesize)
        self.__pathlist = MStringList(self.__m, self.metadata["pathoffset"])
        self.__filelist = MStringList(self.__m, self.metadata["fileoffset"])
        self.__datalist = MDataList(self.__m, self.metadata["dataoffset"])

        self.__open = True
      except:
        self.__m.close()
        raise
    except:
      self.__fn.close()
      raise

  def _fetch(self, key):  
    for pathindex, fileindex in (self.__datalist[x] for x in self.__index[key]):
      path = self.__pathlist[pathindex]
      file = self.__filelist[fileindex]
      yield path, file

  def __close(self):
    if not self.__open:
      return
    self.__open = False

    self.__m.close()
    self.__fn.close()

  # hmm
  def __del__(self):
    self.__close()

try:
  from cIndex import FastIndex
except ImportError:
  FastIndex = None

class FastIndexProvider(IndexProvider):
  def __init__(self, fn):
    self.__index = FastIndex(fn)
    self.metadata = self.__index.metadata

  def _fetch(self, key):
    return self.__index[key]

if FastIndex:
  ReadIndex = FastIndexProvider
  print "FAST"
else:
  ReadIndex = SlowIndexProvider
