#load input

from headparser import *

import csv
import pyodbc

class Loader(object):
  def __init__(self,dataset_name):
    self.dataset = dataset_name
  
  def loadinput():
    
