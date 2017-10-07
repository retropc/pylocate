import threading

class Queue(object):
  def __init__(self):
    self.__value = None
    self.__cv = threading.Condition()

  def set(self, value):
    self.__cv.acquire()
    try:
      self.__value = value
      self.__cv.notify()
    finally:
      self.__cv.release()

  def remove(self):
    self.__cv.acquire()
    try:
      while self.__value is None:
        self.__cv.wait()

      item, self.__value = self.__value, None
      return item
    finally:
      self.__cv.release()

  def poll(self):
    self.__cv.acquire()
    try:
      return self.__value
    finally:
      self.__cv.release()

class IndexWorkerGeneric(threading.Thread):
  def __init__(self, index, callback, max_results):
    super(IndexWorkerGeneric, self).__init__()
    self.setDaemon(True)

    self.index, self.__callback, self.max_results = index, callback, max_results
    self.__queue = Queue()
    self.__active_worker = None

  def __check(self):
    return self.__queue.poll() is not None

  def run(self):
    callback = self.__callback
    while True:
      item = self.__queue.remove()
      if item == False:
        break

      term, tag = item
      for i, x in enumerate(self.index.get(term, self.__check)):
        if i > self.max_results:
          break

        callback([x], tag)
      callback(True, tag)

  def search(self, term, tag):
    self.__queue.set((term, tag))

  def terminate(self):
    self.__queue.set(False)
    self.join()
