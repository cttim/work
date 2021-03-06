# This class is used to create control table according to options
# Pass cursor and options into the constructor of the class

import pyodbc

class Optionparser():
  def __init__(self, option, cur):
    self.options = 'dict(' + option + ')'
    self.cursor=cur
    
  def executeoption(self):
    cur = self.cursor

    # create control table
    try:
      cur.execute("DROP TABLE #PAL_CONTROL_TBL")
    except:
      pass
    cur.execute("""
		CREATE LOCAL TEMPORARY COLUMN TABLE #PAL_CONTROL_TBL(
		"NAME" VARCHAR (100), 
		"INTARGS" INTEGER, 
		"DOUBLEARGS" DOUBLE, 
		"STRINGARGS" VARCHAR (100)
		)
    """)

    # parse the options
    test = eval(self.options)
    executestring=""
    for key in test:
      value = test[key]
      if(isinstance(value, float)):
        executestring = "INSERT INTO #PAL_CONTROL_TBL VALUES ('{0}',NULL,{1},NULL)".format(key, value)
        cur.execute(executestring)
      elif(isinstance(value, int)):
        executestring = "INSERT INTO #PAL_CONTROL_TBL VALUES ('{0}',{1},NULL,NULL)".format(key, value)
        cur.execute(executestring)
      elif(isinstance(value, str)):
        executestring = "INSERT INTO #PAL_CONTROL_TBL VALUES ('{0}',NULL,NULL,'{1}')".format(key, value)
        cur.execute(executestring)
      else:
        raise Exception('incorrect option, check option format')
    cur.commit()
      






     
