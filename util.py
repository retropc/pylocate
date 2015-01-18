import struct
import mmap
import platform

INT_TYPE = "L"
INT_S = struct.Struct(INT_TYPE)
INT_S2 = struct.Struct(INT_TYPE + INT_TYPE)
INT_LEN = INT_S.size
if INT_LEN != 4:
  raise Exception("Bad length.")
U32 = INT_S.pack
def gU32(x): return INT_S.unpack(x)[0]
gU32x2 = INT_S2.unpack

SHORT_TYPE = "H"
SHORT_S = struct.Struct(SHORT_TYPE)
SHORT_LEN = SHORT_S.size
if SHORT_LEN != 2:
  raise Exception("Bad length.")
U16 = SHORT_S.pack
def gU16(x): return SHORT_S.unpack(x)[0]

def MMapOpen(fn, mode=""):
  if mode == "w":
    fmode = "r+b"
  elif mode == "r":
    fmode = "rb"
  else:
    raise Exception("Bad mode")

  return open(fn, fmode)

def MMap(handle, mode, length=0):
  if mode != "w" and mode != "r":
    raise Exception("Bad mode")

  if platform.system() != "Windows":
    pos = handle.tell()
    handle.seek(0, 2)
    total_length = handle.tell()
    if totallength < length:
      handle.seek(length-1, 0)
      handle.write("\x00")
    handle.seek(pos, 0)

  if mode == "w":
    access = mmap.ACCESS_WRITE
  else:
    access = mmap.ACCESS_READ

  return mmap.mmap(handle.fileno(), length, access=access)

__all__ = "INT_TYPE", "INT_LEN", "gU32", "gU32x2", "U32", "SHORT_TYPE", "SHORT_LEN", "U16", "gU16", "MMapOpen", "MMap"

