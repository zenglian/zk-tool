#!/usr/bin/python2

import os
cwd = os.path.dirname(__file__)
os.chdir(cwd)
print "cwd:", os.getcwd()

from util.menu import TestMenu

if __name__ == "__main__":
    menu = TestMenu()
    menu.main()
