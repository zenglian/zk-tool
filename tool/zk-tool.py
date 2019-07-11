#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os,sys
from util.style import yellow

print
print yellow("Working dir: " + sys.path[0])
os.chdir(sys.path[0])

from util.menu import Menu

if __name__ == "__main__":
  menu = Menu()
  menu.main()
