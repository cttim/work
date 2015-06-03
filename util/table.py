# Table class
# Used as a helper class for sql table creation

# There are only 3 tabletypes: TYPE, COLNUM TABLE and TABLE

import pyodbc

class Table(object):
  def __init__(self, cur):
    self.cursor = cur
  
  # create table func, including colnum table and table
  def create_table(self, name, tabletype, attribute):
    cur = self.cursor
    try:
      cur.execute("DROP TABLE {0}".format(name))
    except:
      pass
    executestring = "CREATE {0} {1}({2})".format(tabletype, name, ','.join(attribute))
    cur.execute(executestring)
    cur.commit()

  # create type table
  def create_type(self, name, attribute):
    cur = self.cursor
    try:
      cur.execute("DROP TYPE {0}".format(name))
    except:
      pass

    executestring = "CREATE TYPE {0} AS TABLE({1})".format(name, ','.join(attribute))
    cur.execute(executestring)
    cur.commit()
    
  # create table like a type
  def create_table_like(self, name, tabletype, liketype):
    cur = self.cursor
    try:
      cur.execute("DROP TABLE {0}".format(name))
    except:
      pass
    executestring = "CREATE {0} {1} LIKE {2}".format(tabletype, name, liketype)
    cur.execute(executestring)
    cur.commit()






