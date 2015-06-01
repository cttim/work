import pyodbc

constr = 'DSN=hana;UID=SYSTEM;PWD=manager'

class Optionparser():
  def __init__(self, option):
    self.options = 'dict(' + option + ')'
    
  def executeoption(self):
    # connect to hanadb
    con = pyodbc.connect(constr)
    cur = con.cursor()

    # set the scheme
    cur.execute("SET SCHEMA DM_PAL")
    
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

    test = eval(self.options)
    executestring=""
    for key in test:
      value = test[key]
      if(isinstance(value, float)):
        executestring = "INSERT INTO #PAL_CONTROL_TBL VALUES ({0},NULL,{1},NULL)".format("'"+key+"'", value)
        print(executestring)
        cur.execute(executestring)
      elif(isinstance(value, int)):
        executestring = "INSERT INTO #PAL_CONTROL_TBL VALUES ({0},{1},NULL,NULL)".format("'"+key+"'", value)
        print(executestring)
        cur.execute(executestring)
      elif(isinstance(value, str)):
        executestring = "INSERT INTO #PAL_CONTROL_TBL VALUES ({0},NULL,NULL,{1})".format("'"+key+"'", value)
        print(executestring)
        cur.execute(executestring)
      else:
        print("error option")
    cur.commit()
      






     
