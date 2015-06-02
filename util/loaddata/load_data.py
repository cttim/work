import yaml
from loader import *

configfile = 'config.yaml'

config = open(configfile, 'r')
stream = yaml.load(config)

dataset_with_id = stream['withid']
dataset_without_id = stream['withoutid']

print('load dataset with id')
for dataset in dataset_with_id:
  print('load {0}'.format(dataset))
  load = Loader(dataset)
  try:
    load.loadinput_id()
  except:
    print("can not load dataset: {0}".format(dataset))
    pass

print('load dataset without id')
for dataset in dataset_without_id:
  print('load {0}'.format(dataset))
  load = Loader(dataset)
  try:
    load.loadinput_noid()
  except:
    print("can not load dataset: {0}".format(dataset))
    pass
