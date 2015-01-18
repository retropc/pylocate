from util import *
import json
import array
import codec

class DataListException(Exception):
  pass

PAGE_SIZE = 65536
MAX_PAGES = 65536
PAGE_TABLE_LEN = MAX_PAGES * INT_LEN
  
class DataList(object):
  def __init__(self, handle):
    super(DataList, self).__init__()
    self._handle = handle
  
class WriteDataList(DataList):
  def __init__(self, f, codec, offset=0):
    self.__f, self.__offset, self.__codec = f, offset, codec
    self.__pos, self.__page, self.__pages = 0, 0, array.array(INT_TYPE)
    self.__buf, self.__buf_pos = bytearray(), 0
    self.__write_pos, self.__write_buf, self.__write_buf_len = 0, bytearray(), 0
    
  def write(self, value):
    s_len = len(value)
    if s_len > PAGE_SIZE:
      raise DataListException("too large a value")

    pos = self.__buf_pos
    if pos + s_len > PAGE_SIZE:
      self.__flush_page()
      pos = self.__buf_pos
    
    self.__buf += value
    self.__buf_pos+=s_len
    return (self.__page << 16) + pos
  
  def __seek(self, pos):
    self.__f.seek(self.__offset+pos)

  def __tell(self):
    return self.__f.tell() - self.__offset

  def __write_data(self, data):
    self.__write_buf+=data
    self.__write_buf_len+=len(data)
    if self.__write_buf_len > 1024000:
      self.__flush_data()
      
  def __flush_data(self):
    if not self.__write_buf:
      return

    self.__seek(PAGE_TABLE_LEN + self.__write_pos)
    self.__f.write(self.__write_buf)
    self.__write_pos+=self.__write_buf_len
    self.__write_buf, self.__write_buf_len = bytearray(), 0

  def __flush_page(self):
    if not self.__buf:
      return
    if self.__page >= MAX_PAGES:
      raise DataListException("too many pages")
    
    d = self.__codec.encode(buffer(self.__buf))
    self.__write_data(d)
    self.__pos+=len(d)
    self.__pages.append(self.__pos)

    self.__buf = bytearray()
    self.__page+=1
    self.__buf_pos = 0
          
  def flush(self):
    self.__flush_page()
    self.__seek(0)
    self.__pages.tofile(self.__f)
    self.__flush_data()
    
  def write_string(self, value):
    return self.write(U16(len(value)) + value)
    
  def write_dict(self, value):
    return self.write_string(json.dumps(value))
    
class ReadDataList(DataList):
  def __init__(self, f, m, codec, offset=0, cache_max=128):
    self.__m, self.__offset, self.__codec, self.__cache_max = m, offset, codec, cache_max
    self.__pages, self.__cache = array.array(INT_TYPE), {}
    f.seek(self.__offset)
    self.__pages.fromfile(f, MAX_PAGES)
    
  def __read(self, pos, length):
    pos = pos + self.__offset
    x = self.__m[pos:pos+length]
    assert len(x) == length
    return x

  def __read_page(self, page):
    data = self.__cache.get(page)
    if data is not None:
      return data
    
    if page == 0:
      page_offset, page_len = 0, self.__pages[page]
    else:
      page_offset = self.__pages[page - 1]
      page_len = self.__pages[page] - page_offset
      
    while len(self.__cache) > self.__cache_max:
      del self.__cache[self.__cache.iterkeys().next()]

    u = self.__read(PAGE_TABLE_LEN + page_offset, page_len)
    self.__cache[page] = data = self.__codec.decode(u)
    return data
    
  def read(self, pos, length):
    page = pos >> 16
    pos = pos & 65535
    
    data = self.__read_page(page)
    return data[pos:pos+length]

  def read_string(self, pos):
    return self.read(pos + SHORT_LEN, gU16(self.read(pos, SHORT_LEN)))
      
  def read_dict(self, pos):
    return json.loads(self.read_string(pos))

def main():
  with open("wibble", "w") as f:
    l = WriteDataList(f, codec=codec.ZlibCodec(), offset=5)
    l2 = []
    for x in range(10000):
      l2.append(l.write_string("hi there"))
    l.flush()
    
  with MMapOpen("wibble", "r") as f:
    m = MMap(f, "r")
    try:
      l = ReadDataList(f, m, codec=codec.ZlibCodec(), offset=5)
      for x in l2:
        if l.read_string(x) != "hi there":
          raise Exception()
    finally:
      m.close()
    
if __name__ == "__main__":
  main()

