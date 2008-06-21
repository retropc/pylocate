import os, util, marshal

class Index:
  def __init__(self):
    self.__pathdict = {}
    self.__filedict = {}
    self.__pathlist = []
    self.__filelist = []
    self.__index = {}
    self.metadata = {}
    self.__datalist = []

  def load(self, f):
    self.metadata, self.__index, self.__pathlist, self.__filelist, self.__datalist = marshal.load(f)

  def gc(self):
    self.__pathdict = {}
    self.__filedict = {}

  def dump(self, f):
    marshal.dump([self.metadata, self.__index, self.__pathlist, self.__filelist, self.__datalist], f)

  def __createdicts(self):
    if not self.__pathdict and self.__pathlist:
      for index, value in enumerate(self.__pathlist):
        self.__pathdict[value] = index

    if not self.__filedict and self.__filelist:
      for index, value in enumerate(self.__filelist):
        self.__filedict[value] = index

  def add(self, filename):
    self.__createdicts()

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

  def __lookup(self, key):
    p = util.permutations(key, 3)
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
