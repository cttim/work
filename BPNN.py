'''
  @file BPNN.py
'''

import os
import sys
import inspect
import csv
import pyodbc
import time

constr = 'DSN=hana;UID=SYSTEM;PWD=manager'

# Import the util path, this method even works if the path contains symlinks to
# modules.
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(
  os.path.split(inspect.getfile(inspect.currentframe()))[0], "../../util")))
if cmd_subfolder not in sys.path:
  sys.path.insert(0, cmd_subfolder)

from log import *
from profiler import *
from optionparser import *

import shlex
import subprocess
import re
import collections

'''
This class implements the K-Means clustering benchmark.
'''
class KMEANS(object):

  '''
  Create the K-Means Clustering benchmark instance, show some informations and
  return the instance.
  @param dataset - Input dataset to perform K-Means clustering on.
  @param timeout - The time until the timeout. Default no timeout.
  @param path - Path to the mlpack executable.
  @param verbose - Display informational messages.
  '''
  def __init__(self, dataset, timeout=0, path=os.environ["MLPACK_BIN"],
      verbose=True, debug=os.environ["MLPACK_BIN_DEBUG"]):
    self.verbose = verbose
    self.dataset = dataset
    self.path = path
    self.timeout = timeout
    self.debug = debug

    # Get description from executable.
    cmd = shlex.split(self.path + "kmeans -h")
    try:
      s = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=False)
    except Exception as e:
      Log.Fatal("Could not execute command: " + str(cmd))
    else:
      # Use regular expression pattern to get the description.
      pattern = re.compile(br"""(.*?)Required.*?options:""",
          re.VERBOSE|re.MULTILINE|re.DOTALL)

      match = pattern.match(s)
      
      if not match:
        Log.Warn("Can't parse description", self.verbose)
        description = ""
      else:
        description = match.group(1)

      self.description = description

  '''
  Destructor to clean up at the end. Use this method to remove created files.
  '''
  def __del__(self):
    Log.Info("Clean up.", self.verbose)
    filelist = ["gmon.out", "output.csv"]
    for f in filelist:
      if os.path.isfile(f):
        os.remove(f)

  '''
  Run valgrind massif profiler on the K-Means clustering method. If the method
  has been successfully completed the report is saved in the specified file.
  @param options - Extra options for the method.
  @param fileName - The name of the massif output file.
  @param massifOptions - Extra massif options.
  @return Returns False if the method was not successful, if the method was
  successful save the report file in the specified file.
  '''
  def RunMemory(self, options, fileName, massifOptions="--depth=2"):
    print("output file name" + fileName)
    Log.Info("Perform K-Means Memory Profiling.", self.verbose)

    # If the dataset contains two files then the second file is the centroids
    # file.
    if len(self.dataset) == 2:
      cmd = shlex.split(self.debug + "kmeans -i " + self.dataset[0] + " -I "
          + self.dataset[1] + " -o output.csv -v " + options)
    else:
      cmd = shlex.split(self.debug + "kmeans -i " + self.dataset[0] +
          " -o output.csv -v " + options)

    return Profiler.MassifMemoryUsage(cmd, fileName, self.timeout, massifOptions)

  '''
  Perform K-Means Clustering. If the method has been successfully completed
  return the elapsed time in seconds.
  @param options - Extra options for the method.
  @return - Elapsed time in seconds or a negative value if the method was not
  successful.
  '''
  def RunTiming(self, options):

    # get data set
    data = self.dataset[0]

    # connect to hana
    con = pyodbc.connect(constr)
    cur = con.cursor()

    # set the schema
    cur.execute("SET SCHEMA DM_PAL")
    
    try:
      cur.execute("DROP TYPE PAL_CONTROL_T")
    except:
      pass
    cur.execute("""
    CREATE TYPE PAL_CONTROL_T AS TABLE(
      "NAME" VARCHAR(100),
      "INTARGS" INTEGER,
      "DOUBLEARGS" DOUBLE,
      "STRINGARGS" VARCHAR(100)
    )
    """)
    
    try:
      cur.execute("DROP TYPE PAL_TRAIN_NN_RESULT_T")
    except:
      pass
    cur.execute("""
    CREATE TYPE PAL_TRAIN_NN_RESULT_T AS TABLE(
      "NAME" VARCHAR(100),
      "VALUE" DOUBLE
    )
    """)

    try:
      cur.execute("DROP TYPE PAL_NN_MODEL_T")
    except:
      pass
    cur.execute("""
    CREATE TYPE PAL_NN_MODEL_T AS TABLE(
      "NAME" VARCHAR(100),
      "MODEL" CLOB
    )
    """)
    
    try:
      cur.execute("DROP TABLE PAL_NN_PDATA_TBL")
    except:
      pass
    cur.execute("""
    CREATE COLUMN TABLE PAL_NN_PDATA_TBL(
      "POSITION" INTEGER,
      "SCHEMA_NAME" VARCHAR(100),
      "TYPE_NAME" VARCHAR(100),
      "PARAMETER_TYPE" VARCHAR(100)
    )
    """)
    cur.execute("INSERT INTO PAL_NN_PDATA_TBL VALUES (1, 'DM_PAL', 'ADULT_TYPE', 'IN')")
    cur.execute("INSERT INTO PAL_NN_PDATA_TBL VALUES (2, 'DM_PAL', 'PAL_CONTROL_T', 'IN')")
    cur.execute("INSERT INTO PAL_NN_PDATA_TBL VALUES (3, 'DM_PAL', 'PAL_TRAIN_NN_RESULT_T', 'OUT')")
    cur.execute("INSERT INTO PAL_NN_PDATA_TBL VALUES (4, 'DM_PAL', 'PAL_NN_MODEL_T', 'OUT')")

    # create the proc
    cur.execute("""call "SYS".AFLLANG_WRAPPER_PROCEDURE_DROP('DM_PAL', 'PAL_NN_TRAIN')""")
    cur.execute("""call "SYS".AFLLANG_WRAPPER_PROCEDURE_CREATE('AFLPAL', 'CREATEBPNN', 'DM_PAL', 'PAL_NN_TRAIN', PAL_NN_PDATA_TBL)""")

    # create table
    opt = Optionparser(options,cur)
    opt.executeoption()
    try:
      cur.execute("DROP TABLE PAL_TRAIN_NN_RESULT_TBL")
    except:
      pass
    cur.execute("CREATE COLUMN TABLE PAL_TRAIN_NN_RESULT_TBL LIKE PAL_TRAIN_NN_RESULT_T")

    try:
      cur.execute("DROP TABLE PAL_TRAIN_NN_MODEL_TBL")
    except:
      pass
    cur.execute("CREATE COLUMN TABLE PAL_TRAIN_NN_MODEL_TBL LIKE PAL_NN_MODEL_T")

    # run the proc and count time
    starttime=time.time()
      
    BPNN_call = """CALL "DM_PAL".PAL_NN_TRAIN(ADULT, #PAL_CONTROL_TBL, PAL_TRAIN_NN_RESULT_TBL, PAL_TRAIN_NN_MODEL_TBL) with OVERVIEW"""
    cur.execute(BPNN_call)
      
    elapsedtime=time.time() - starttime

    return elapsedtime
