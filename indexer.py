import multiprocessing
import scandir
import os

def index(base, root):
  l = len(base)  
  batch = []
  for root, dirs, files in scandir.walk(root):
    root = root[l:]
    if len(root) > 0 and root[0] == os.path.sep:
      root = root[1:]

    for filename in files:
      batch+=[(root, filename)]
      if len(batch) > 50:
        yield batch
        batch = []
        
  if batch:
    yield batch

class Indexer(object):
  def __init__(self, base, paths):
    self.base, self.paths = base, paths
  
  def start(self):
    pass  
    
  def terminate(self):
    pass
  
class SerialIndexer(Indexer):
  def __iter__(self):
    for path in self.paths:
      for x in index(self.base, path):
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
    for batch in index(self.base, path):
      self.queue.put(batch)
    self.queue.put(None)
