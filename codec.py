import zlib

class PlainCodec(object):
  def encode(self, data):
    return data
    
  def decode(self, data):
    return data
    
class ZlibCodec(object):
  def __init__(self, level=6):
    self.level = level
    
  def encode(self, data):
    return zlib.compress(data, self.level)
  
  def decode(self, data):
    return zlib.decompress(data)
