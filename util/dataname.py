# return two values, first is data table name, second is datatype table name
def getdataname(datapath):
  datalist = datapath.split("/")
  dataname = datalist[len(datalist) - 1]
  datalist = dataname.split(".")
  dataname = datalist[0]
  datatypename = dataname + "_type"
  dataname = dataname.upper()
  datatypename = datatypename.upper()
  return {'DATANAME': dataname, 'DATATYPENAME': datatypename}
