import yaml
from loader import *

import os, sys, inspect
# Import the util path, this method even works if the path contains
# symlinks to modules.
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(
  os.path.split(inspect.getfile(inspect.currentframe()))[0], '../../../util')))
if cmd_subfolder not in sys.path:
  sys.path.insert(0, cmd_subfolder)

from log import *

configfile = 'config.yaml'

# open the file and load data
try:
  config = open(configfile, 'r')
except:
  Log.Fatal("Fail to open file {0}".format(configfile))

try:
  stream = yaml.load(config)
except:
  Log.Fatal("Fail to load yaml file {0}".format(configfile))

# get list of files
try:
  dataset_with_id = stream['withid']
  dataset_without_id = stream['withoutid']
except:
  Log.Fatal("Fail to get file list")

# start to load the data
Log.Info("Load dataset with id")
for dataset in dataset_with_id:
  Log.Info("Load dataset: {0}".format(getdataname(dataset)))
  load = Loader(dataset)
  try:
    load.loadinput_id()
  except:
    Log.Fatal("Could not load dataset: {0}".format(dataset))
    pass

Log.Info("Load dataset without id")
for dataset in dataset_without_id:
  Log.Info("Load dataset: {0}".format(getdataname(dataset)))
  load = Loader(dataset)
  try:
    load.loadinput_noid()
  except:
    Log.Fatal("Could not load dataset: {0}".format(dataset))
    pass
