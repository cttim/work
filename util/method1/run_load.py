from loader import *

# put all path of dataset into datasets
datasets = ['test.csv', 'test1.csv', 'test2.csv']

for dataset in datasets:
  data_loader = Loader(dataset)
  data_loader.loadinput() 
