#!/usr/bin/env python
from distutils.core import setup, Extension

module1 = Extension('cIndex',
                sources = ['index.c', 'crc32.c', 'py.c'])

setup(name = 'cIndex',
      version = '1.0',
      description = 'Python interface to libindex',
      ext_modules = [module1])

