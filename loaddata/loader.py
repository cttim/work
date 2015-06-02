
from headparser import *

import csv
import pyodbc

constr = 'DSN=hana;UID=SYSTEM;PWD=manager'

def getdataname(datapath):
  datalist = datapath.split("/")
  dataname = datalist[len(datalist) - 1]
  datalist = dataname.split(".")
  dataname = datalist[0]
  return dataname

class Loader(object):
  def __init__(self,dataset_name):
    self.dataset = dataset_name
  
  def loadinput_id(self):
    # connect to hanadb
    con = pyodbc.connect(constr)
    cur = con.cursor()
    # set the scheme
    cur.execute("SET SCHEMA DM_PAL")
    
    # open csv file
    with open(self.dataset) as csvfile:
      reader = csv.reader(csvfile, delimiter=',')
      row = next(reader)
      
      # use headparser get all column name and type
      parser = Headparser(row)
      colstring = "ID INTEGER, " + parser.parserdata_string()
      
      # get tablename and type name
      tablename = getdataname(self.dataset)
      tabletypename = tablename + "_type"
      tablename = tablename.upper()
      tabletypename = tabletypename.upper()
      # create data type table
      try:
        cur.execute("DROP TYPE " + tabletypename)
      except:
        pass

      datatype_execute = "CREATE TYPE " + tabletypename + " AS TABLE(" + colstring + ")"
      cur.execute(datatype_execute)
      
      # create input table and insert data into the input table
	#create input table
      try:
        cur.execute("DROP TABLE " + tablename)
      except:
        pass

      datainput_execute = "CREATE COLUMN TABLE " + tablename + " LIKE " + tabletypename
      cur.execute(datainput_execute)

      	#insert data
      
      tablelen = len(row) + 1
      
      query = "INSERT INTO " + tablename + " VALUES({0})"
      query = query.format(','.join('?' * tablelen))
      IDCOUNTER = 0
      #row = [str(IDCOUNTER)] + row
      #cur.execute(query, row)   
      for row in reader:
        IDCOUNTER += 1
        row = [str(IDCOUNTER)] + row
        cur.execute(query, row)
      cur.commit()

  def loadinput_noid(self):
    # connect to hanadb
    con = pyodbc.connect(constr)
    cur = con.cursor()
    # set the scheme
    cur.execute("SET SCHEMA DM_PAL")
    
    # open csv file
    with open(self.dataset) as csvfile:
      reader = csv.reader(csvfile, delimiter=',')
      row = next(reader)
     
      # use headparser get all column name and type
      parser = Headparser(row)
      colstring = parser.parserdata_string()
      
      # get tablename and type name
      tablename = getdataname(self.dataset)
      tabletypename = tablename + "_type"
      tablename = tablename.upper()
      tabletypename = tabletypename.upper()
      # create data type table
      try:
        cur.execute("DROP TYPE " + tabletypename)
      except:
        pass

      datatype_execute = "CREATE TYPE " + tabletypename + " AS TABLE(" + colstring + ")"
      cur.execute(datatype_execute)
      
      # create input table and insert data into the input table
	#create input table
      try:
        cur.execute("DROP TABLE " + tablename)
      except:
        pass

      datainput_execute = "CREATE COLUMN TABLE " + tablename + " LIKE " + tabletypename
      cur.execute(datainput_execute)

      	#insert data
      
      tablelen = len(row)
      query = 'INSERT INTO ' + tablename + ' VALUES({0})'
      query = query.format(','.join('?' * tablelen))
      for row in reader:
        cur.execute(query, row)
      cur.commit()
       
       
      
    
    
