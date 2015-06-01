# head should only contain 4 types: DOUBLE, INTEGER, VARCHAR(...) and CLOB
# pass in list to create a headparser

class Headparser(object):
  # pass in head data, a head list should be passed into init
  def __init__(self, headdata):
    self.data = headdata

  # return a list of string 
  def parserdata_list(self):
    data = self.data
    count = 0
    for element in data:
      data[count] = "v" + str(count+1) + " " + data[count]
      count += 1
    return data

  # return a string
  def parserdata_string(self):
    data = self.data
    datastring = ""
    count = 0
    for element in data:
      if(count == 0):
        datastring = datastring + "v" + str(count+1) + " " + data[count]
      else:
        datastring = datastring +", v" + str(count+1) + " " + data[count]
      count += 1
    return datastring

