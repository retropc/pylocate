import os, util, marshal

class Index:
  def __init__(self):
    pass

class WriteIndex(Index):
  def __init__(self):
    Index.__init__(self)
    self.__pathdict = {}
    self.__filedict = {}
    self.__pathlist = []
    self.__filelist = []
    self.__index = {}
    self.__datalist = []
    self.metadata = {}

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
      self.__index.setdefault(p, []).append(l)

class ReadIndex(Index):
  def __init__(self, f):
    Index.__init__(self)
    self.metadata, self.__index, self.__pathlist, self.__filelist, self.__datalist = marshal.load(f)
    self.__open = True

  def __lookup(self, key):
    xlen = len(key)
    if xlen > 3:
      xlen = 3
      
    p = util.permutations(key, xlen)
    if not p:
      return []
    return self.__index.get(p[0], [])

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
