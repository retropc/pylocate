import multiprocessing
import scandir
import os
import platform
import traceback
import codecs

if platform.system() == "Windows":
  trans = lambda x: x.lower()
else:
  trans = lambda x: x

def utf8_iso8859_1(data, table=dict((x, x.decode("iso-8859-1")) for x in map(chr, range(0, 256)))):
  return (table.get(data.object[data.start]), data.start+1)

codecs.register_error("mixed-iso-8859-1", utf8_iso8859_1)

def decode(x):
  try:
    return x.decode("utf-8", "mixed-iso-8859-1")
  except UnicodeDecodeError:
    return x.decode("iso-8859-1", "ignore")

class Indexer(object):
  def __init__(self, base, paths, exclude_path, exclude_name):
    self.base, self.paths = base, paths
    self.exclude_path, self.exclude_name = set(trans(x) for x in exclude_path), set(trans(x) for x in exclude_name)
  
  def start(self):
    pass
    
  def terminate(self):
    pass

  def _index(self, root):
    l = len(self.base)  
    batch = []
    for root, dirs, files in scandir.walk(bytes(root)):
      rootl = trans(root)

      root = decode(root)[l:]
      if len(root) > 0 and root[0] == os.path.sep:
        root = root[1:]
      
      i, l_dirs = 0, len(dirs)
      while i < l_dirs:
        d = trans(dirs[i])
        if (d in self.exclude_name) or (os.path.join(rootl, d) in self.exclude_path):
          dirs.pop(i)
          l_dirs-=1
        else:
          yield [(root, decode(dirs[i]) + "/")]
          i+=1
      
      for filename in files:
        batch+=[(root, decode(filename))]
        if len(batch) > 50:
          yield batch
          batch = []
          
    if batch:
      yield batch
  
class SerialIndexer(Indexer):
  def __iter__(self):
    for path in self.paths:
      for x in self._index(path):
        for y in x:
          yield y
  
class ParallelIndexer(Indexer):
  def start(self):
    self.queue = multiprocessing.Queue(maxsize=10000)
    indexers = []
    for path in self.paths:
      indexer = multiprocessing.Process(target=self.parallel_fn, args=(path,))
      indexers.append(indexer)
      indexer.start()
    self.indexers = indexers
    
  def terminate(self):
    for indexer in self.indexers:
      indexer.terminate()
    
  def __iter__(self):
    remaining = len(self.paths)
    while True:
      complete, data = self.queue.get()
      if complete:
        if data is not None:
          raise Exception(data)
        remaining-=1
        if not remaining:
          break
        continue
      
      for x in data:
        yield x
      
  def parallel_fn(self, path):
    try:
      for batch in self._index(path):
        self.queue.put((False, batch))
      self.queue.put((True, None))
    except Exception, e:
      self.queue.put((True, traceback.format_exc()))
