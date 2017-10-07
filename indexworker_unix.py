import threading
import os
import trie
import indexworker_generic
import select
import errno
import sys
import fcntl
import subprocess
import traceback

PYPY = "pypy"
TYPES = trie.FIndexReadTrie,

TYPES = {x.__name__: x for x in TYPES}

def set_nonblock(fd):
  flags = fcntl.fcntl(fd, fcntl.F_GETFL)
  fcntl.fcntl(fd, fcntl.F_SETFL, flags|os.O_NONBLOCK)

class ThreadWorker(threading.Thread):
  def __init__(self, i, o, callback):
    super(ThreadWorker, self).__init__()
    self.setDaemon(True)

    self.__i, self.__o, self.__callback = i, o, callback
    self.__t_r, self.__t_w = os.pipe()

    self.__buf = ""
    self.__in_buf = []

  def __write(self):
    try:
      while self.__buf:
        written = os.write(self.__o, self.__buf[:8192])
        self.__buf = self.__buf[written:]
    except OSError, e:
      if e.errno == errno.EAGAIN:
        return
      raise

  def __input(self, l):
    i = self.__in_buf
    i.append(l)
    if len(i) == 3:
      self.__in_buf = []
      if i[1] == "" and i[2] == "":
        t = True
      else:
        t = [(i[1].decode("utf8"), i[2].decode("utf8"))]
      self.__callback(t, int(i[0]))

  def run(self):
    i, o, t = self.__i, self.__o, self.__t_r
    set_nonblock(i)
    set_nonblock(o)
    reader = line_reader(i)
    while True:
      r, w, _ = select.select([i, t], [o] if self.__buf else [], [])
      if i in r:
        try:
          while True:
            l = reader.next()
            if l is None:
              break
            self.__input(l)
        except StopIteration:
          raise Exception("child died")

      if t in r:
        b = os.read(t, 8192)
        if not b:
          break

        self.__buf+=b
        self.__write()

      if w:
        self.__write()

  def search(self, term, tag):
    os.write(self.__t_w, "%d %s\n" % (tag, term))

  def terminate(self):
    os.close(self.__t_w)
    self.join()
    os.close(self.__t_r)
    os.close(self.__i)
    os.close(self.__o)

class IndexWorkerUnix(object):
  def __init__(self, index, callback, max_results):
    # parent:
    #   i = c2p_r
    #   o = p2c_w
    # child:
    #   stdin = p2c_r
    #   stdout = c2p_w
    #   weird_fd = index_fd
    (p2c_r, p2c_w), (c2p_r, c2p_w) = os.pipe(), os.pipe()

    self.pid = os.fork()
    if self.pid == 0: # child
      os.close(p2c_w)
      os.close(c2p_r)

      sys.stdin.flush()
      os.dup2(p2c_r, 0)
      os.close(p2c_r)

      sys.stdout.flush()
      os.dup2(c2p_w, 1)
      os.close(c2p_w)
      try:
        f2 = os.dup(index.fileno())
        index.close()
        os.closerange(2+1, f2)
        os.closerange(f2+1, subprocess.MAXFD+1)

        args = [sys.argv[0], "--child", index.__class__.__name__, str(f2), str(max_results)]

        try:
          os.execvp(PYPY, [PYPY] + args)
        except OSError, e:
          if e.errno == errno.ENOENT:
            print >>sys.stderr, "not using pypy -- slow mode"
            sys.stderr.flush()
            os.execvp(sys.executable, [sys.executable] + args)
          else:
            raise
      except Exception, e:
        traceback.print_exc()
        raise
      finally:
        try:
          sys.stderr.flush()
        finally:
          os._exit(1)

    os.close(c2p_w)
    os.close(p2c_r)
    i, o = c2p_r, p2c_w
    self.t = ThreadWorker(i, o, callback)

  def start(self):
    self.t.start()

  def search(self, term, tag):
    self.t.search(term, tag)

  def terminate(self):
    self.t.terminate()

def line_reader(fd):
  buf = ""
  while True:
    try:
      data = os.read(fd, 8192)
      if not data:
        break
    except OSError, e:
      if e.errno == errno.EAGAIN:
        yield None
        continue
      raise
    tokens = (buf + data).split("\n")
    buf = tokens.pop(-1)
    for x in tokens:
      yield x

def callback(result, tag):
  if result == True:
    sys.stdout.write("%s\n\n\n" % tag)
  else:
    for x in result:
      y = x[0].encode("utf8"), x[1].encode("utf8")
      if "\n" in y[0] or "\n" in y[1]:
        y = y[0].replace("\n", "NL"), y[1].replace("\n", "NL")
      sys.stdout.write("%s\n%s\n%s\n" % (tag, y[0], y[1]))
  sys.stdout.flush()

def main(index_class, index_fd, max_results):
  f = os.fdopen(index_fd, "rb")
  i = TYPES[index_class](filename=None, f=f)
  try:
    worker = indexworker_generic.IndexWorkerGeneric(i, callback, max_results)
    try:
      worker.start()

      for x in line_reader(0):
        tag, term = x.split(" ", 1)
        tag_len = len(tag)

        worker.search(term, tag)
    finally:
      worker.terminate()
  finally:
    i.close()

def child(args):
  main(args[0], int(args[1]), int(args[2]))

if __name__ == "__main__":
  import sys
  child(sys.argv[1:])
