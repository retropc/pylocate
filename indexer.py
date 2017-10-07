import multiprocessing
import scandir
import os
import platform

if platform.system() == "Windows":
  trans = lambda x: x.lower()
else:
  trans = lambda x: x

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
    for root, dirs, files in scandir.walk(root):
      rootl = trans(root)

      root = root[l:]
      if len(root) > 0 and root[0] == os.path.sep:
        root = root[1:]
      
      i, l_dirs = 0, len(dirs)
      while i < l_dirs:
        d = trans(dirs[i])
        if (d in self.exclude_name) or (os.path.join(rootl, d) in self.exclude_path):
          dirs.pop(i)
          l_dirs-=1
        else:
          yield [(root, dirs[i] + "/")]
          i+=1
      
      for filename in files:
        batch+=[(root, filename)]
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
      batch = self.queue.get()
      if batch is None:
        remaining-=1
        if not remaining:
          break
        continue
      
      for x in batch:
        yield x
      
  def parallel_fn(self, path):
    for batch in self._index(path):
      self.queue.put(batch)
    self.queue.put(None)
