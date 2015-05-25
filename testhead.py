import os
import sys
import inspect

cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(
  os.path.split(inspect.getfile(inspect.currentframe()))[0], "../../util")))
if cmd_subfolder not in sys.path:
  sys.path.insert(0, cmd_subfolder)

from headparser import *

testdata = ["DOUBLE", "VARCHAR", "INT", "INT"]

print(testdata)
testparser = Headparser(testdata)

data = testparser.parserdata_list()

print(data)
