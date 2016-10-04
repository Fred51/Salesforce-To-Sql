
# coding: utf-8

# <b><p style="font-size:21px">Jet Dataload</p>
# <p>Author: Steven Henkel</p>
# <p>Purpose: To load data from the 3 salesforce instances into one unified location for marketing pipeline reporting</p></b>

# <b><p style="font-size:21px">Helper Functions</p>
# <p>Purpose: To load all of the functions that will be used for the purpose of pulling data out of salesforce into a consolidated sql table.</p></b>

# In[50]:

import datetime
import pandas as pd
from simple_salesforce import Salesforce
import requests
import cx_Oracle
import sqlalchemy
import collections

#Date Created: 2016-05-06
#Last Editted: 2016-05-06
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to import a text file and split it into a single list
#    file <String>: The name of the file to insert
#    delimiter <String>: The delimiter used to seperate elements in the list
def txtToList(file, delimiter = "\n"):
    if(file[-4:] != ".txt"):
        fileName = file+".txt"
    else:
        fileName = file
    try:
        with open (fileName, "r") as userFile:
            return userFile.read().split(delimiter)
    except:
        return "File does not exist!"

#Date Created: 2015-11-04
#Last Editted: 2015-11-04
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Change Notes:
#Purpose: The purpose of this function is to establish a db connection in the package
#    name <string>: The name of the schema
#    password <string>: The pw for the schema.
#    connection <string>: The connection string for the schema.
def cxOracleConnection(name, password, connection):
    global db, cursor, engineSqlAlchemy, conSqlAlchemy
    db = cx_Oracle.connect(name, password, connection)
    cursor = db.cursor()
    engineSqlAlchemy = sqlalchemy.create_engine("oracle://" + name + ":" + password + "@" + connection)
    conSqlAlchemy = engineSqlAlchemy.connect()
    
#Date Created: 2015-09-15
#Last Editted: 2015-09-15
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Change Notes:
#Purpose: The purpose of this function is to convert a string into a typical SQL column name
#    columnEntry <string>: the name to convert to a sql name. 
def convertSQLNames (columnEntry):
    columnEntry=columnEntry.replace(" ","_").replace("<","_").replace(">","_").replace(":","_").replace(";","_").replace("'","").replace(",","_").replace("?","_").replace("\\","_").replace("(","_").replace(")","_").replace("/","_").replace("[","_").replace("]","_").replace("#","_").replace(".","_").replace("$","_").replace("%","_").replace("-","_").replace("!","_").replace("&","_")
    columnEntry=columnEntry.replace("__________","_").replace("_________","_").replace("________","_").replace("_______","_").replace("______","_").replace("_____","_").replace("____","_").replace("___","_").replace("__","_")
    if(isNumber(columnEntry[0])):
        columnEntry = "C_" + columnEntry
    columnEntry=columnEntry[:30]
    return columnEntry.upper()

#Date Created: 2016-03-16
#Last Editted: 2016-03-16
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to clean code with if = type if statements by turning it into a function
#     ifValue <value>: If this value
#     equalsValue <value>: Equals this value
#     thenValue <value>: then this value
#     elseValue <value>: else this value
#     equals <boolean>: If set to false, the function is equivalent to ifNotEquals. (default = True)
def ifEquals (ifValue, equalsValue, thenValue, elseValue, equals = True):
    if (equals and ifValue == equalsValue or not equals and ifValue != equalsValue):
        return thenValue
    else:
        return elseValue

#Date Created: 2016-03-11
#Last Editted: 2016-04-05
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 2
#Change Notes:
#     2016-04-05: Made the code simpler with list comprehension as well as allowing the value to be returned if the function crashes (V1 - V2)
#Purpose: The purpose of this function is to take a list, and return a list with a function applied to it's elements
#    baseList <String>: The original list
#    function <function>: The function to apply to the elements
#    skipFlag: If False, return the parameter to the function as a value without applying the function (default: False)
def listFunction (baseList, function, tableType = False, parameters = [], tableTypeIndex = -1):
    if(tableType):
        return [[ifEquals(k,tableTypeIndex,tryFunction(function,listify(j) + parameters,j),j) for k,j in enumerate(i)] for i in baseList]
    else:
        return [tryFunction(function,listify(i) + parameters,i) for i in baseList]

#Date Created: 2016-04-05
#Last Editted: 2016-04-05
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is is to try to apply a function with parameters, and if it fails, return a value
#    function <function>: The function to apply
#    parameterList <string or list of strings>: The parameters for a function
#    exceptReturnValue: The value to return if the function fails to execute
def tryFunction (function, parameterList, exceptReturnValue):
    parameters = listify(parameterList)
    try:
        return function(*parameters)
    except:
        return exceptReturnValue
    
#Date Created: 2016-03-07
#Last Editted: 2016-03-07
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to allow a user to import data from Salesforce into Oracle.
#    user <String>: the name of the table to create and/or insert to.
#    password <String>: The identifier string for the report in the link to the salesforce page (eg 00OG0000006h6nl)
#    token <String>: The string before salesforce.com that the instance refers to.
def salesforceConnection(user,password,token):
    global salesforceConnectionVariable
    salesforceConnectionVariable = Salesforce(user, password, token) 
    
#Date Created: 2016-03-07
#Last Editted: 2016-03-07
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to split a string into a 2 dimensional list based on 2 delimiters
#    baseString <String>: a string to split into a list
#    splitDelimiterFirst <String>: The delimiter to split the string into "rows"
#    splitDelimiterSecond <String>: The delimiter to split the string into "columns"
def split2D(baseString, splitDelimiterFirst, splitDelimiterSecond):
    newList = []
    for i in baseString.split(splitDelimiterFirst):
        newList += [i.split(splitDelimiterSecond)]
    return newList

#Date Created: 2016-03-07
#Last Editted: 2016-03-07
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to remove a string from a 2 dimensional list
#    baseString <String>: a string to split into a list
#    removeString <String>: the string to remove
def removeStringList2D(baseList,removeString):
    newList = []
    for i in range(0,len(baseList)):
        rowList = []
        for j in range(0,len(baseList[0])):
            try:
                rowList += [baseList[i][j].replace(removeString,"")]
            except:
                continue
        newList += [rowList]
    return newList
       
#Date Created: 2016-03-07
#Last Editted: 2016-06-01
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1.3
#Purpose: The purpose of this function is to allow a user to import data from Salesforce into Oracle.
#    tableName <String>: the name of the table to create and/or insert to.
#    reportIdentifier <String>: The identifier string for the report in the link to the salesforce page (eg 00OG0000006h6nl)
#    instance <String>: The string before salesforce.com that the instance refers to.
#    instanceName <String>: The content to add into an instance column, to specify the instance. If = "" then do not add the instance column. (default: "")
#    rebuildFlag <Boolean>: If true, the table will be rebuilt, otherwise the table will just be inserted to. (default: True)
#    printLog <Boolean>: If true, the table will be rebuilt, otherwise the table will just be inserted to. (default: False)
def importSalesForceToSql (tableName, reportIdentifier, instance, instanceName = "", rebuildFlag = True, printLog = False, columnTypes = [], batch = 500):
    importDateTime = datetime.datetime.now()
    dataList = removeStringList2D(split2D(str(requests.get("https://" + instance + ".salesforce.com/" + reportIdentifier + "?export=1&enc=UTF-8&xf=csv",
                                        headers = salesforceConnectionVariable.headers, cookies = {'sid' : salesforceConnectionVariable.session_id}).content)[2:],'"\\n"','","'),'"')
    del dataList[-1] #Parsing causes extra list to be created, and is removed here.
    dataList = listAddColumn(dataList,"PY_DATA_SOURCE",reportIdentifier)
    if(instanceName != ""):
        dataList = listAddColumn(dataList,"PY_INSTANCE",instanceName)
    dataList = listAddColumn(dataList,"PY_EXTRACTED_DATE_VALUE",toSqlDateTime(datetime.datetime.now()))
    if (columnTypes == []):
        columnList = columnListTypes(dataList)[:-1] + [["PY_EXTRACTED_DATE_VALUE","datetime","datetime"]]
    else: 
        columnList = columnTypes
    insertTableBase = webBaseTable(tableName, columnList,rebuildFlag, printLog)
    insertLoop(columnList, dataList, insertTableBase, printLog, batch = batch)
    endDateTime = datetime.datetime.now()
    return "Salesforce Report: " + reportIdentifier + " - Inserted Into " + tableName + " successfully! Time: " + timeDifference(importDateTime, endDateTime)

#Date Created: 2016-03-07
#Last Editted: 2016-03-16
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 2.1
#Change Notes:
#     2016-03-16: allowed for the possiblity of adding a list (V1 - V2)
#     2016-07-28: allow adding column to start or end (V12 - V2.1)
#Purpose: The purpose of this function to add a 'column' to a list with the option of adding a header if the list is setup like a table.
#    baseList <2D list>: a list of multiple dimensions
#    columnName <Sting>: The heading element for the new column. To add the same value to the entire list, put the same value here.
#    columnValue <Value>: a value (string, int etc) to be added to the list
#    columnValueList <boolean>: if true, allow the addition of a list rather than a default value. (default = False)
#    end <boolean>: If true, add the column to the end, else the start (default = True)
#Tests:
#    listAddColumn ([["a","b","c"],[1,2,3],[3,4,5],[5,6,7]], "d",0, end = False) = [['d', 'a', 'b', 'c'], [0, 1, 2, 3], [0, 3, 4, 5], [0, 5, 6, 7]]
def listAddColumn (baseList, columnName, columnValue, columnValueList = False, end = True):
    newBaseList = []
    for i in range(0,len(baseList)):
        if(i == 0):
            if(end):
                newBaseList += [baseList[0] + [columnName]]
            else:
                newBaseList += [[columnName] + baseList[0]]
        else:
            if(columnValueList):
                if(end):
                    newBaseList += [baseList[i] + [columnValue[i - 1]]]
                else:
                    newBaseList += [[columnValue[i - 1]] + baseList[i]]
            else:
                if(end):
                    newBaseList += [baseList[i] + [columnValue]]
                else:
                    newBaseList += [[columnValue] + baseList[i]] 
    return newBaseList

#Date Created: 2015-11-06
#Last Editted: 2016-05-27
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 2
#Change Notes: 
#    2016-05-27: greatly simplified using str (V1 - V2)
#Purpose: The purpose of this function is to convert a python datetime into a recognizable datetime format for Oracle (YYYY-MM-DD HH24:MI:SS)
#    dateTime <datetime>: The date you want to convert
def toSqlDateTime (dateTime):
    return str(dateTime)[:19]    

#Date Created: 2016-05-27
#Last Editted: 2016-05-27
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 2
#Change Notes: 
#    2016-05-27: greatly simplified using str (V1 - V2)
#Purpose: The purpose of this function is to convert a string into a python datetime object
#         While this isn't much of a change of the original functionality, this simplifies it's use with listFunction
#    dateTime <datetime>: The date you want to convert
def stringToDate(dateString, dateMask = "%Y-%m-%d %H:%M:%S"):
    return datetime.datetime.strptime(dateString,dateMask)

#Date Created: 2016-03-09
#Last Editted: 2016-03-09
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to change the value into a list. If already a list, leave as is.
#    value <any>: The value to turn into a list
def listify (value):
    try:
        value.count(1)
        return value
    except:
        return [value]
    
#Date Created: 2016-03-07
#Last Editted: 2016-03-07
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to add an element to a list, and if it already exists, add an identifier
#    baseList <1D List>: an existing list
#    element <String>: An element to add to a list
#    identifier=0 <Number>: Used internally by the function to iterate over multiple identifiers if they already exist
def addUniqueListElement (baseList, element, identifier=0):
    if(identifier == 0):
        padding = ""
    else:
        padding = str(identifier)
    if(len(element) + len(padding) > 30):
        checkElement = element[:-(len(element + padding) - 30)] + padding
    else:
        checkElement = element + padding
    if(checkElement in baseList):
        return addUniqueListElement(baseList, element, identifier + 1)   
    else:
        return checkElement
    
#Date Created: 2016-04-01
#Last Editted: 2016-04-01
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to return a 2 dimensional list from a list of strings as a delimiter
#    baseList <list>: the list to split
#    delimiter <String>: The string to act as a delimiter for splitting (Default: "~~~")
def splitListLoop(baseList, delimiter = "~~~"):
    splitList = []
    for i in baseList:
        splitList += [i.split(delimiter)]
    return splitList

#Date Created: 2016-05-10
#Last Editted: 2016-05-10
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to append the values from one table into another, specifying which columns map together
#    table1 <String>: The table to insert into
#    table2 <String>: The table to insert
#    columnList <String>: The list of columns to map together
def appendTableList(table1, table2, columnList):
    newList = []
    for i in columnList:
        if indexExists(i, 1):
            if(i[1] == ""):
                newList += ["null as " + i[0]]
            else:
                newList += [i[1] + " as " + i[0]]
        else:
            newList += ["null as " + i[0]]
    cursor.execute("insert into " + table1 + "(" + columnListString(listColumn(columnList,0)) + 
            ") (select " + columnListString(newList) + " from " + table2 + ")")
    cursor.execute("commit")
    
#Date Created: 2016-03-09
#Last Editted: 2016-03-09
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to return a comma seperated list of the input values
#    columnList <List>: The list of values to separate
#    prefix <String>: The value to add to the beginning of the strings (Default = "")
#    suffix <String>: The value to add to the end of the strings (Default = "")
#    finalComma <Boolean>: Choose False to not include the final delimiter, True to include. (Default = False)
#    delimiter <String>: Choose what delimiter you want between the values. (Default = ", ")
def columnListString (columnList, prefix = "", suffix = "", finalComma = False, delimiter = ", "):
    finalString = ""
    if(finalComma):
        for i in range(0, len(columnList)):
            padding = delimiter
            finalString += prefix + columnList[i] + suffix + padding
    else:
        for i in range(0, len(columnList)):
            padding = delimiter
            if (i == len(columnList) - 1):
                padding = ""
            finalString += prefix + columnList[i] + suffix + padding
    return finalString

#Date Created: 2016-05-10
#Last Editted: 2016-05-10
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to return a specified column in a list
#    baseList <List>: The list
#    index <number>: The index in the list to return values for
#    default <value>: The default value to return in the value at an index does not exist.
def listColumn(baseList, index, default = ""):
    newList = []
    for i in baseList:
        if indexExists(i, index):
            newList += [i[index]]
        else:
            newList += [default]
    return newList

#Date Created: 2016-05-10
#Last Editted: 2016-05-10
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to find the indexes for the string dates to change into python date objects*
#    dateMastList <2D String List>: A list with column names and python object date masks
#    orderedDict <ordered dictionary>: The ordered dictionary that will be used with sqlAlchemy
def dateMaskListIndex(dateMaskList, orderedDict):
    newList = []
    for i in dateMaskList:
        for j, k in enumerate(list(orderedDict)):
            if(i[0] == k):
                newList += [i + [j]]
    return newList

#Date Created: 2016-05-10
#Last Editted: 2016-05-10
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to filter a 2D list for only the rows that contain or do not contain a string*
#    dataList <2D String List>: A 2 dimensional list
#    index <number>: the index in the list to search the string for
#    baseString <String>: the string to search for
#    include <Boolean>: if the string is found, include or exclude the row based on this value (default = True to include)
def matrixStringFilter (dataList, index, baseString, include = True):
    newList = []
    for i in dataList:
        if (i[index].find(baseString) == -1 and not(include) or 
            i[index].find(baseString) != -1 and include):
            newList += [i]
    return newList

#Date Created: 2016-05-10
#Last Editted: 2016-05-10
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is return true or false based on if an index exists for a list
#    baseList <list>: A 2 dimensional list
#    index <number>: the index to see if it exists
def indexExists(baseList, index):
    try:
        a = baseList[index]
        return True
    except:
        return False

#Date Created: 2015-09-15
#Last Editted: 2015-09-15
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is return true or false based on if an value is an integer (or chosen otherwise)
#    value <any>: any value, typically string
#    function <number>: the number function to use as a criteria
def isNumber(value, function = "int"):
    try:
        exec(function + "(value)")
        return True
    except ValueError:
        return False

#Date Created: 2016-03-20
#Last Editted: 2016-03-20
#Author(s): Steven Henkel
#Edited by: Steven Henkel
#Version: 1
#Purpose: The purpose of this function is to load data from salesforce into a sql database.
#    tableName <String>: The name of the table to create in the sql database
#    reportIdentifier <String>: the report identifier for the salesforce report 
#    instance <String>: The string before "salesforce.com" for the instance
#    columnTypeOrderedDict <ordered dictionary>: The ordered dictionary that is used with sqlAlchemy to specify the column types
#    dateMaskList <2d String List>: the 2d list (column name, python datetime object mask) to use to change string dates to datetime objects
#    instanceName <String>: The name of the instance to add into the sql table
#    ifExists <String>: the sqlAlchemy if_exists parameter for replace, append or fail as possible values.
def importSalesForceToSql (tableName, reportIdentifier, instance, columnTypeOrderedDict, dateMaskList, instanceName = "", ifExists = 'replace'):
    # import data from salesforce into a 2 dimensional list
    dataList = removeStringList2D(split2D(str(requests.get("https://" + instance + ".salesforce.com/" + reportIdentifier + "?export=1&enc=UTF-8&xf=csv",
                                        headers = salesforceConnectionVariable.headers, cookies = {'sid' : salesforceConnectionVariable.session_id}).content)[2:],'"\\n"','","'),'"')

    # Remove invalid enteries where the id contains spaces. Ensure an ID is used as the first column.
    #     First make sure the names of the columns will be unique
    
    columnListRaw = listFunction(dataList[0],convertSQLNames)
    columnList = []

    for i in columnListRaw:
        columnList += [addUniqueListElement(columnList,i)]
        
    #     Add the column list to the valid records   
    dataList = [columnList] + matrixStringFilter(dataList[1:], 0, " ",False)

    # Add the report identifier, salesforce instance and extracted date
    dataList = listAddColumn(dataList,"PY_DATA_SOURCE",reportIdentifier)
    dataList = listAddColumn(dataList,"PY_INSTANCE",instanceName)
    dataList = listAddColumn(dataList,"PY_EXTRACTED_DATE_VALUE",datetime.datetime.now())

    # Convert the dates in the dataList to a datetime object for sqlAlchemy
    for i in dateMaskListIndex(dateMaskList, columnTypeOrderedDict):
        dataList = listFunction(dataList, stringToDate, True, [i[1]], i[2])

    # Due to a problem with pandas if_exists parameter
    ifExistsParam = ifExists
    if(ifExists == 'replace'):
        ifExistsParam = 'fail'
        try:
            cursor.execute("drop table " + tableName)
        except:
            pass
        
    # Create a dataframe and use sqlAlchemy to send the data to sql    
    pd.DataFrame(dataList[1:], columns = dataList[0]).to_sql(tableName,
              engineSqlAlchemy,
              index = False,
              dtype = columnTypeOrderedDict,
              if_exists = ifExistsParam
              )    


# <b><p style="font-size:21px">Credentials</p>
# <p>Purpose: To connect to the database and have the 3 salesforce instance credentials on hand</p></b>

# In[3]:

dbCred = txtToList("login")
cxOracleConnection(name = dbCred[0],
                   password = dbCred[1],
                   connection = dbCred[2])


# In[4]:

bbUser = txtToList("BB_USER")
# Username
# password
# instance number
goodUser = txtToList("GOOD_USER")
# Username
# password
# instance number
athocUser = txtToList("ATHOC_USER")
# Username
# password
# instance number
# token


# <b><p style="font-size:21px">Salesforce Processing: BlackBerry</p>
# <p>Purpose: To load all the data from the BlackBerry Salesforce Instance</p></b>

# In[22]:

salesforceConnection(bbUser[0],bbUser[1],"")


# In[10]:

# https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
dateMaskList = [["CLOSE_DATE","%m/%d/%Y"],
                ["CREATED_DATE","%m/%d/%Y"],
                ["LAST_MODIFIED_DATE","%m/%d/%Y"],
                ["LAST_STAGE_CHANGE_DATE","%m/%d/%Y"]]

columnTypeOrderedDict = collections.OrderedDict([
    ['OPPORTUNITY_ID', sqlalchemy.types.VARCHAR(100)],
    ['STAGE', sqlalchemy.types.VARCHAR(100)],
    ['PRODUCT_FAMILY', sqlalchemy.types.VARCHAR(100)],
    ['BLACKBERRY_REGION', sqlalchemy.types.VARCHAR(100)],
    ['BLACKBERRY_COUNTRY', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_NAME', sqlalchemy.types.VARCHAR(200)],
    ['PRODUCT_NAME', sqlalchemy.types.VARCHAR(200)],
    ['AMOUNT_CONVERTED_CURRENCY', sqlalchemy.types.VARCHAR(100)],
    ['AMOUNT_CONVERTED_', sqlalchemy.types.VARCHAR(100)],
    ['NET_TO_BLACKBERRY_REVENUE_CONV', sqlalchemy.types.VARCHAR(100)],
    ['NET_TO_BLACKBERRY_REVENUE_CON1', sqlalchemy.types.VARCHAR(100)],
    ['OPPORTUNITY_NAME', sqlalchemy.types.VARCHAR(200)],
    ['OPPORTUNITY_OWNER', sqlalchemy.types.VARCHAR(100)],
    ['CLOSE_DATE', sqlalchemy.types.VARCHAR(100)],
    ['LAST_STAGE_CHANGE_DATE', sqlalchemy.types.VARCHAR(100)],
    ['PROBABILITY_', sqlalchemy.types.VARCHAR(100)],
    ['AGE', sqlalchemy.types.VARCHAR(100)],
    ['STAGE_DURATION', sqlalchemy.types.VARCHAR(100)],
    ['CREATED_DATE', sqlalchemy.types.VARCHAR(100)],
    ['LAST_MODIFIED_DATE', sqlalchemy.types.VARCHAR(100)],
    ['FORECAST_CATEGORY', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_OWNER', sqlalchemy.types.VARCHAR(100)],
    ['PRODUCT_CODE', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_ID', sqlalchemy.types.VARCHAR(100)],
    ['PRIMARY_CAMPAIGN_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['LEAD_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['BILLING_COUNTRY', sqlalchemy.types.VARCHAR(100)],
    ['BLACKBERRY_SALES_SUB_GROUP', sqlalchemy.types.VARCHAR(100)],
    ['PY_DATA_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['PY_INSTANCE', sqlalchemy.types.VARCHAR(100)],
    ['PY_EXTRACTED_DATE_VALUE', sqlalchemy.types.DATE]
])

importSalesForceToSql ("BB_OPPORTUNITIES",
                       "00OF0000006hM3t", 
                       bbUser[2], 
                       columnTypeOrderedDict, 
                       dateMaskList, 
                       instanceName = "BlackBerry", 
                       ifExists = 'replace')


# <b><p style="font-size:21px">SalesForce Processing: BlackBerry Marketing</p></b>

# In[54]:

dateMaskList = [["PY_EXTRACTED_DATE_VALUE","%Y-%m-%d %H:%M:%S"]]

columnTypeOrderedDict = collections.OrderedDict([
    ['OPPORTUNITY_ID', sqlalchemy.types.VARCHAR(100)],
    ['STAGE', sqlalchemy.types.VARCHAR(100)],
    ['PRODUCT_FAMILY', sqlalchemy.types.VARCHAR(100)],
    ['BLACKBERRY_REGION', sqlalchemy.types.VARCHAR(100)],
    ['BLACKBERRY_COUNTRY', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_NAME', sqlalchemy.types.VARCHAR(200)],
    ['PRODUCT_NAME', sqlalchemy.types.VARCHAR(200)],
    ['AMOUNT_CONVERTED_CURRENCY', sqlalchemy.types.VARCHAR(100)],
    ['AMOUNT_CONVERTED_', sqlalchemy.types.VARCHAR(100)],
    ['NET_TO_BLACKBERRY_REVENUE_CONV', sqlalchemy.types.VARCHAR(100)],
    ['NET_TO_BLACKBERRY_REVENUE_CON1', sqlalchemy.types.VARCHAR(100)],
    ['OPPORTUNITY_NAME', sqlalchemy.types.VARCHAR(200)],
    ['OPPORTUNITY_OWNER', sqlalchemy.types.VARCHAR(100)],
    ['CLOSE_DATE', sqlalchemy.types.VARCHAR(100)],
    ['LAST_STAGE_CHANGE_DATE', sqlalchemy.types.VARCHAR(100)],
    ['PROBABILITY_', sqlalchemy.types.VARCHAR(100)],
    ['AGE', sqlalchemy.types.VARCHAR(100)],
    ['STAGE_DURATION', sqlalchemy.types.VARCHAR(100)],
    ['CREATED_DATE', sqlalchemy.types.VARCHAR(100)],
    ['LAST_MODIFIED_DATE', sqlalchemy.types.VARCHAR(100)],
    ['FORECAST_CATEGORY', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_OWNER', sqlalchemy.types.VARCHAR(100)],
    ['PRODUCT_CODE', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_ID', sqlalchemy.types.VARCHAR(100)],
    ['PRIMARY_CAMPAIGN_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['LEAD_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['BILLING_COUNTRY', sqlalchemy.types.VARCHAR(100)],
    ['BLACKBERRY_SALES_SUB_GROUP', sqlalchemy.types.VARCHAR(100)],
    ['PY_DATA_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['PY_INSTANCE', sqlalchemy.types.VARCHAR(100)],
    ['PY_EXTRACTED_DATE_VALUE', sqlalchemy.types.DATE]
])

importSalesForceToSql ("IS_MARKETING",
                       "00OF0000006h3bl",
                       bbUser[2],
                       columnTypeOrderedDict,
                       dateMaskList,
                       instanceName = "BlackBerry",
                       ifExists = 'replace')


# In[53]:

dateMaskList = [["PY_EXTRACTED_DATE_VALUE","%Y-%m-%d %H:%M:%S"]]

columnTypeOrderedDict = collections.OrderedDict([
    ['OPPORTUNITY_ID', sqlalchemy.types.VARCHAR(100)],
    ['STAGE', sqlalchemy.types.VARCHAR(100)],
    ['PRODUCT_FAMILY', sqlalchemy.types.VARCHAR(100)],
    ['BLACKBERRY_REGION', sqlalchemy.types.VARCHAR(100)],
    ['BLACKBERRY_COUNTRY', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_NAME', sqlalchemy.types.VARCHAR(200)],
    ['PRODUCT_NAME', sqlalchemy.types.VARCHAR(200)],
    ['AMOUNT_CONVERTED_CURRENCY', sqlalchemy.types.VARCHAR(100)],
    ['AMOUNT_CONVERTED_', sqlalchemy.types.VARCHAR(100)],
    ['NET_TO_BLACKBERRY_REVENUE_CONV', sqlalchemy.types.VARCHAR(100)],
    ['NET_TO_BLACKBERRY_REVENUE_CON1', sqlalchemy.types.VARCHAR(100)],
    ['OPPORTUNITY_NAME', sqlalchemy.types.VARCHAR(200)],
    ['OPPORTUNITY_OWNER', sqlalchemy.types.VARCHAR(100)],
    ['CLOSE_DATE', sqlalchemy.types.VARCHAR(100)],
    ['LAST_STAGE_CHANGE_DATE', sqlalchemy.types.VARCHAR(100)],
    ['PROBABILITY_', sqlalchemy.types.VARCHAR(100)],
    ['AGE', sqlalchemy.types.VARCHAR(100)],
    ['STAGE_DURATION', sqlalchemy.types.VARCHAR(100)],
    ['CREATED_DATE', sqlalchemy.types.VARCHAR(100)],
    ['LAST_MODIFIED_DATE', sqlalchemy.types.VARCHAR(100)],
    ['FORECAST_CATEGORY', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_OWNER', sqlalchemy.types.VARCHAR(100)],
    ['PRODUCT_CODE', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_ID', sqlalchemy.types.VARCHAR(100)],
    ['PRIMARY_CAMPAIGN_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['LEAD_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['BILLING_COUNTRY', sqlalchemy.types.VARCHAR(100)],
    ['BLACKBERRY_SALES_SUB_GROUP', sqlalchemy.types.VARCHAR(100)],
    ['PY_DATA_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['PY_INSTANCE', sqlalchemy.types.VARCHAR(100)],
    ['PY_EXTRACTED_DATE_VALUE', sqlalchemy.types.DATE]
])

importSalesForceToSql ("IS_MARKETING",
                       "00OF0000006h3bq",
                       bbUser[2],
                       columnTypeOrderedDict,
                       dateMaskList,
                       instanceName = "BlackBerry",
                       ifExists = 'append')


# <b><p style="font-size:21px">Salesforce Processing: Good</p>
# <p>Purpose: To load all the data from the Good Salesforce Instance</p></b>

# In[7]:

salesforceConnection(goodUser[0], goodUser[1],"")


# In[14]:

# https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
dateMaskList = [["CLOSE_DATE","%m/%d/%Y"],
                ["CREATED_DATE","%m/%d/%Y"],
                ["LAST_MODIFIED_DATE","%m/%d/%Y"],
                ["LAST_STAGE_CHANGE_DATE","%m/%d/%Y"]]

columnTypeOrderedDict = collections.OrderedDict([
    ['OPPORTUNITY_ID', sqlalchemy.types.VARCHAR(100)],
    ['STAGE', sqlalchemy.types.VARCHAR(100)],
    ['PRODUCT_FAMILY', sqlalchemy.types.VARCHAR(100)],
    ['PRODUCT_NAME', sqlalchemy.types.VARCHAR(100)],
    ['OPPORTUNITY_NAME', sqlalchemy.types.VARCHAR(200)],
    ['OPPORTUNITY_OWNER', sqlalchemy.types.VARCHAR(100)],
    ['CREATED_BY', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_OWNER', sqlalchemy.types.VARCHAR(100)],
    ['PRIMARY_CAMPAIGN_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_NAME', sqlalchemy.types.VARCHAR(300)],
    ['TYPE', sqlalchemy.types.VARCHAR(100)],
    ['LOGO_TYPE', sqlalchemy.types.VARCHAR(100)],
    ['AMOUNT_CONVERTED_CURRENCY', sqlalchemy.types.VARCHAR(100)],
    ['AMOUNT_CONVERTED_', sqlalchemy.types.VARCHAR(100)],
    ['TOTAL_SALES_PRICE_CONVERTED_CU', sqlalchemy.types.VARCHAR(100)],
    ['TOTAL_SALES_PRICE_CONVERTED_', sqlalchemy.types.VARCHAR(100)],
    ['AGE', sqlalchemy.types.VARCHAR(100)],
    ['STAGE_DURATION', sqlalchemy.types.VARCHAR(100)],
    ['LAST_MODIFIED_DATE', sqlalchemy.types.DATE],
    ['CREATED_DATE', sqlalchemy.types.DATE],
    ['CLOSE_DATE', sqlalchemy.types.DATE],
    ['LAST_STAGE_CHANGE_DATE', sqlalchemy.types.DATE],
    ['LEAD_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['FORECAST_CATEGORY', sqlalchemy.types.VARCHAR(100)],
    ['PROBABILITY_', sqlalchemy.types.VARCHAR(100)],
    ['TERRITORY_NAME', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_ID', sqlalchemy.types.VARCHAR(100)],
    ['PRODUCT_CODE', sqlalchemy.types.VARCHAR(100)],
    ['BILLING_COUNTRY', sqlalchemy.types.VARCHAR(100)],
    ['PY_DATA_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['PY_INSTANCE', sqlalchemy.types.VARCHAR(100)],
    ['PY_EXTRACTED_DATE_VALUE', sqlalchemy.types.DATE]

])

importSalesForceToSql ("GOOD_OPPORTUNITIES",
                       "00O16000007gKx8", 
                       goodUser[2], 
                       columnTypeOrderedDict, 
                       dateMaskList, 
                       instanceName = "Good", 
                       ifExists = 'replace')


# <b><p style="font-size:21px">Salesforce Processing: AtHoc</p>
# <p>Purpose: To load all the data from the AtHoc Salesforce Instance</p></b>

# In[48]:

salesforceConnection(athocUser[0], athocUser[1],athocUser[2])


# In[51]:

# https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
dateMaskList = [["CLOSE_DATE","%m/%d/%Y"],
                ["CREATED_DATE","%m/%d/%Y"],
                ["LAST_MODIFIED_DATE","%m/%d/%Y"]]

columnTypeOrderedDict = collections.OrderedDict([
    ['OPPORTUNITY_ID', sqlalchemy.types.VARCHAR(100)],
    ['ATHOC_STAGE', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_NAME', sqlalchemy.types.VARCHAR(100)],
    ['PRODUCT', sqlalchemy.types.VARCHAR(100)],
    ['OPPORTUNITY_NAME', sqlalchemy.types.VARCHAR(200)],
    ['AMOUNT_CONVERTED_CURRENCY', sqlalchemy.types.VARCHAR(100)],
    ['AMOUNT_CONVERTED_', sqlalchemy.types.VARCHAR(100)],
    ['OPPORTUNITY_OWNER', sqlalchemy.types.VARCHAR(100)],
    ['CLOSE_DATE', sqlalchemy.types.DATE],
    ['AGE', sqlalchemy.types.VARCHAR(100)],
    ['STAGE_DURATION', sqlalchemy.types.VARCHAR(100)],
    ['CREATED_DATE', sqlalchemy.types.DATE],
    ['BUDGET_LIKELIHOOD', sqlalchemy.types.VARCHAR(100)],
    ['LAST_MODIFIED_DATE', sqlalchemy.types.DATE],
    ['LEAD_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['SECTOR', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_OWNER', sqlalchemy.types.VARCHAR(100)],
    ['FORECAST_CATEGORY', sqlalchemy.types.VARCHAR(100)],
    ['ACCOUNT_ID', sqlalchemy.types.VARCHAR(100)],
    ['BILLING_COUNTRY', sqlalchemy.types.VARCHAR(100)],
    ['PY_DATA_SOURCE', sqlalchemy.types.VARCHAR(100)],
    ['PY_INSTANCE', sqlalchemy.types.VARCHAR(100)],
    ['PY_EXTRACTED_DATE_VALUE', sqlalchemy.types.DATE]
])

importSalesForceToSql ("ATHOC_OPPORTUNITIES",
                       "00O37000001YKC1", 
                       athocUser[3], 
                       columnTypeOrderedDict, 
                       dateMaskList, 
                       instanceName = "AtHoc", 
                       ifExists = 'replace')


# <b><p style="font-size:21px">Combination Code: Insert into All Opportunities</p></b>
# <p>Purpose: Creates the history table if it does not exists, then appends the 3 instances into 1 table.</p></b>

# In[36]:

from sqlalchemy import schema, types

# This code creates the history table only if it doesn't exist
try:
    cursor.execute("select * from OPPORTUNITY_HISTORY")
except:
    metadata = schema.MetaData()

    page_table = schema.Table('OPPORTUNITY_HISTORY', metadata,
        schema.Column('OPPORTUNITY_ID', types.VARCHAR(1000)),
        schema.Column('STAGE', types.VARCHAR(1000)),
        schema.Column('PRODUCT_FAMILY', types.VARCHAR(1000)),
        schema.Column('BLACKBERRY_REGION', types.VARCHAR(1000)),
        schema.Column('BLACKBERRY_COUNTRY', types.VARCHAR(1000)),
        schema.Column('ACCOUNT_NAME', types.VARCHAR(1000)),
        schema.Column('PRODUCT_NAME', types.VARCHAR(1000)),
        schema.Column('AMOUNT_CONVERTED_CURRENCY', types.VARCHAR(1000)),
        schema.Column('AMOUNT_CONVERTED_', types.VARCHAR(1000)),
        schema.Column('NET_TO_BLACKBERRY_REVENUE_CONV', types.VARCHAR(1000)),
        schema.Column('NET_TO_BLACKBERRY_REVENUE_CON1', types.VARCHAR(1000)),
        schema.Column('OPPORTUNITY_NAME', types.VARCHAR(1000)),
        schema.Column('OPPORTUNITY_OWNER', types.VARCHAR(1000)),
        schema.Column('CLOSE_DATE', types.DATE),
        schema.Column('LAST_STAGE_CHANGE_DATE', types.DATE),
        schema.Column('PROBABILITY_', types.VARCHAR(1000)),
        schema.Column('AGE', types.VARCHAR(1000)),
        schema.Column('STAGE_DURATION', types.VARCHAR(1000)),
        schema.Column('CREATED_DATE', types.DATE),
        schema.Column('LAST_MODIFIED_DATE', types.DATE),
        schema.Column('FORECAST_CATEGORY', types.VARCHAR(1000)),
        schema.Column('ACCOUNT_OWNER', types.VARCHAR(1000)),
        schema.Column('PRODUCT_CODE', types.VARCHAR(1000)),
        schema.Column('ACCOUNT_ID', types.VARCHAR(1000)),
        schema.Column('PRIMARY_CAMPAIGN_SOURCE', types.VARCHAR(1000)),
        schema.Column('LEAD_SOURCE', types.VARCHAR(1000)),
        schema.Column('CREATED_BY', types.VARCHAR(1000)),
        schema.Column('TYPE', types.VARCHAR(1000)),
        schema.Column('LOGO_TYPE', types.VARCHAR(1000)),
        schema.Column('TERRITORY_NAME', types.VARCHAR(1000)),
        schema.Column('BUDGET_LIKELIHOOD', types.VARCHAR(1000)),
        schema.Column('SECTOR', types.VARCHAR(1000)),
        schema.Column('BILLING_COUNTRY', types.VARCHAR(1000)),
        schema.Column('PY_DATA_SOURCE', types.VARCHAR(1000)),
        schema.Column('PY_INSTANCE', types.VARCHAR(1000)),
        schema.Column('PY_EXTRACTED_DATE_VALUE', types.DATE)
    )

    metadata.bind = engineSqlAlchemy

    metadata.create_all(checkfirst=True)


# In[52]:

x = splitListLoop("""OPPORTUNITY_ID,OPPORTUNITY_ID
STAGE,STAGE
PRODUCT_FAMILY,PRODUCT_FAMILY
BLACKBERRY_REGION,BLACKBERRY_REGION
BLACKBERRY_COUNTRY,BLACKBERRY_COUNTRY
ACCOUNT_NAME,ACCOUNT_NAME
PRODUCT_NAME,PRODUCT_NAME
AMOUNT_CONVERTED_CURRENCY,AMOUNT_CONVERTED_CURRENCY
AMOUNT_CONVERTED_,AMOUNT_CONVERTED_
NET_TO_BLACKBERRY_REVENUE_CONV,NET_TO_BLACKBERRY_REVENUE_CONV
NET_TO_BLACKBERRY_REVENUE_CON1,NET_TO_BLACKBERRY_REVENUE_CON1
OPPORTUNITY_NAME,OPPORTUNITY_NAME
OPPORTUNITY_OWNER,OPPORTUNITY_OWNER
CLOSE_DATE,CLOSE_DATE
LAST_STAGE_CHANGE_DATE,LAST_STAGE_CHANGE_DATE
PROBABILITY_,PROBABILITY_
AGE,AGE
STAGE_DURATION,STAGE_DURATION
CREATED_DATE,CREATED_DATE
LAST_MODIFIED_DATE,LAST_MODIFIED_DATE
FORECAST_CATEGORY,FORECAST_CATEGORY
ACCOUNT_OWNER,ACCOUNT_OWNER
PRODUCT_CODE,PRODUCT_CODE
ACCOUNT_ID,ACCOUNT_ID
PRIMARY_CAMPAIGN_SOURCE,PRIMARY_CAMPAIGN_SOURCE
LEAD_SOURCE,LEAD_SOURCE
CREATED_BY,
TYPE,
LOGO_TYPE,
TERRITORY_NAME,BLACKBERRY_SALES_SUB_GROUP
BUDGET_LIKELIHOOD,
SECTOR,
BILLING_COUNTRY,BILLING_COUNTRY
PY_DATA_SOURCE,PY_DATA_SOURCE
PY_INSTANCE,PY_INSTANCE
PY_EXTRACTED_DATE_VALUE,PY_EXTRACTED_DATE_VALUE""".split("\n"),",")

appendTableList("OPPORTUNITY_HISTORY", "BB_OPPORTUNITIES", x)


# In[47]:

x = splitListLoop("""OPPORTUNITY_ID,OPPORTUNITY_ID
STAGE,STAGE
PRODUCT_FAMILY,PRODUCT_FAMILY
BLACKBERRY_REGION,
BLACKBERRY_COUNTRY,
ACCOUNT_NAME,ACCOUNT_NAME
PRODUCT_NAME,PRODUCT_NAME
AMOUNT_CONVERTED_CURRENCY,AMOUNT_CONVERTED_CURRENCY
AMOUNT_CONVERTED_,AMOUNT_CONVERTED_
NET_TO_BLACKBERRY_REVENUE_CONV,TOTAL_SALES_PRICE_CONVERTED_CU
NET_TO_BLACKBERRY_REVENUE_CON1,TOTAL_SALES_PRICE_CONVERTED_
OPPORTUNITY_NAME,OPPORTUNITY_NAME
OPPORTUNITY_OWNER,OPPORTUNITY_OWNER
CLOSE_DATE,CLOSE_DATE
LAST_STAGE_CHANGE_DATE,LAST_STAGE_CHANGE_DATE
PROBABILITY_,PROBABILITY_
AGE,AGE
STAGE_DURATION,STAGE_DURATION
CREATED_DATE,CREATED_DATE
LAST_MODIFIED_DATE,LAST_MODIFIED_DATE
FORECAST_CATEGORY,FORECAST_CATEGORY
ACCOUNT_OWNER,ACCOUNT_OWNER
PRODUCT_CODE,PRODUCT_CODE
ACCOUNT_ID,ACCOUNT_ID
PRIMARY_CAMPAIGN_SOURCE,PRIMARY_CAMPAIGN_SOURCE
LEAD_SOURCE,LEAD_SOURCE
CREATED_BY,CREATED_BY
TYPE,TYPE
LOGO_TYPE,LOGO_TYPE
TERRITORY_NAME,TERRITORY_NAME
BUDGET_LIKELIHOOD,
SECTOR,
BILLING_COUNTRY,BILLING_COUNTRY
PY_DATA_SOURCE,PY_DATA_SOURCE
PY_INSTANCE,PY_INSTANCE
PY_EXTRACTED_DATE_VALUE,PY_EXTRACTED_DATE_VALUE""".split("\n"),",")
#x
appendTableList("OPPORTUNITY_HISTORY", "GOOD_OPPORTUNITIES", x)


# In[46]:

x = splitListLoop("""OPPORTUNITY_ID,OPPORTUNITY_ID
STAGE,ATHOC_STAGE
PRODUCT_FAMILY,
BLACKBERRY_REGION,
BLACKBERRY_COUNTRY,
ACCOUNT_NAME,ACCOUNT_NAME
PRODUCT_NAME,PRODUCT
AMOUNT_CONVERTED_CURRENCY,AMOUNT_CONVERTED_CURRENCY
AMOUNT_CONVERTED_,AMOUNT_CONVERTED_
NET_TO_BLACKBERRY_REVENUE_CONV,AMOUNT_CONVERTED_CURRENCY
NET_TO_BLACKBERRY_REVENUE_CON1,AMOUNT_CONVERTED_
OPPORTUNITY_NAME,OPPORTUNITY_NAME
OPPORTUNITY_OWNER,OPPORTUNITY_OWNER
CLOSE_DATE,CLOSE_DATE
LAST_STAGE_CHANGE_DATE,
PROBABILITY_,
AGE,AGE
STAGE_DURATION,STAGE_DURATION
CREATED_DATE,CREATED_DATE
LAST_MODIFIED_DATE,LAST_MODIFIED_DATE
FORECAST_CATEGORY,FORECAST_CATEGORY
ACCOUNT_OWNER,ACCOUNT_OWNER
PRODUCT_CODE,
ACCOUNT_ID,ACCOUNT_ID
PRIMARY_CAMPAIGN_SOURCE,
LEAD_SOURCE,LEAD_SOURCE
CREATED_BY,
TYPE,
LOGO_TYPE,
TERRITORY_NAME,
BUDGET_LIKELIHOOD,BUDGET_LIKELIHOOD
SECTOR,SECTOR
BILLING_COUNTRY,BILLING_COUNTRY
PY_DATA_SOURCE,PY_DATA_SOURCE
PY_INSTANCE,PY_INSTANCE
PY_EXTRACTED_DATE_VALUE,PY_EXTRACTED_DATE_VALUE""".split("\n"),",")
appendTableList("OPPORTUNITY_HISTORY", "ATHOC_OPPORTUNITIES", x)


# <b><p style="font-size:21px">Opportunity Mapping Information</p></b>
# <p>These are snippets of the code that maintained the star schema that are relevant in terms of how the dimensions we mapped</p></b>

# In[17]:

dimensionList = [[["OPPORTUNITY"],
                  [["OPPORTUNITY_ID","OPPORTUNITY_ID"],
                   ["OPPORTUNITY_NAME", "OPPORTUNITY_NAME"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["STAGE"],
                  [["STAGE","STAGE_NAME"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["PRODUCT"],
                  [["PRODUCT_CODE"],
                   ["PRODUCT_NAME"],
                   ["PRODUCT_FAMILY"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["PRODUCT_FAMILY"],
                  [["PRODUCT_FAMILY"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["CUSTOMER"],
                  [["ACCOUNT_ID", "ACCOUNT_ID"],
                   ["ACCOUNT_NAME", "ACCOUNT_NAME"],
                   ["BLACKBERRY_COUNTRY", "BLACKBERRY_COUNTRY"],
                   ["BLACKBERRY_REGION","BLACKBERRY_REGION"],
                   ["SECTOR","SECTOR"],
                   ["BILLING_COUNTRY"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["COUNTRY"],
                  [["BILLING_COUNTRY","COUNTRY"]
                  ]
                 ],
                 [["FORECAST_CATEGORY"],
                  [["FORECAST_CATEGORY","FORECAST_CATEGORY"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["CURRENCY","PROFIT"],
                  [["AMOUNT_CONVERTED_CURRENCY","CURRENCY_NAME"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["CURRENCY","REVENUE"],
                  [["NET_TO_BLACKBERRY_REVENUE_CONV","CURRENCY_NAME"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["OWNER","OPPORTUNITY"],
                  [["OPPORTUNITY_OWNER","OWNER_NAME"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["OWNER","ACCOUNT"],
                  [["ACCOUNT_OWNER","OWNER_NAME"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["OWNER","CREATED_BY"],
                  [["CREATED_BY","OWNER_NAME"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["SALE_TYPE"],
                  [["TYPE","SALE_TYPE"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["LOGO_TYPE"],
                  [["LOGO_TYPE","LOGO_TYPE"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["TERRITORY"],
                  [["TERRITORY_NAME","TERRITORY_NAME"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["BUDGET_LIKELIHOOD"],
                  [["BUDGET_LIKELIHOOD","BUDGET_LIKELIHOOD"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["CAMPAIGN"],
                  [["PRIMARY_CAMPAIGN_SOURCE","CAMPAIGN_SOURCE"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["LEAD"],
                  [["LEAD_SOURCE","LEAD_SOURCE"],
                   ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["SOURCE"],
                  [["PY_DATA_SOURCE","DATA_SOURCE"],
                  ["PY_INSTANCE","INSTANCE"]
                  ]
                 ],
                 [["DATE"],
                  [["CLOSE_DATE"]]
                 ],
                 [["DATE"],
                  [["LAST_STAGE_CHANGE_DATE"]]
                 ],
                 [["DATE"],
                  [["CREATED_DATE"]]
                 ],
                 [["DATE"],
                  [["LAST_MODIFIED_DATE"]]
                 ]
                ],
factList = ["OPPORTUNITIES",
            [["AMOUNT_CONVERTED_","AMOUNT_CONVERTED"],
             ["NET_TO_BLACKBERRY_REVENUE_CON1","REVENUE"],
             ["PROBABILITY_","PROBABILITY"],
             ["MARKETTING_FLAG","MARKETTING_FLAG"],
             ["LAST_PULL_FLAG","LAST_PULL_FLAG"],
             ["PY_EXTRACTED_DATE_VALUE","RECORD_ADD_DATE"],
             ["FREQUENCY","QUANTITY"]
            ]
           ]
                        


# In[21]:

# Adhoc Dimension Updates

#DA_PRODUCT: Map information from DA_PRODUCT_FAMILY into DA_PRODUCT
log += "\nAdhoc Dimension Modifications:"

log += "\nDA_PRODUCT:\n" + createTable("""create table TEMP_DA_PRODUCT as
select t1.*, 
(case when t2.PORTFOLIO is null then 
    case when t1.Instance = 'Good' then 'BES/GOOD' 
    when t1.INSTANCE = 'AtHoc' then 'AtHoc' 
    when t1.INSTANCE = 'BlackBerry' then 'BES/GOOD' 
    end
else t2.PORTFOLIO 
end) as PORTFOLIO from DA_PRODUCT t1 
left join DA_PRODUCT_FAMILY t2 on t1.PRODUCT_FAMILY = t2.PRODUCT_FAMILY""")
log += "\n" + replaceTable("DA_PRODUCT", "TEMP_DA_PRODUCT")

#DA_OPPORTUNITIES: Create a true unique ID across instances
log += "\nDA_OPPORTUNITY:\n" + createTable("""create table TEMP_DA_OPPORTUNITY as 
select 
t1.*, 
substr(case when t1.INSTANCE is not null then t1.INSTANCE end,1,2) || trim(OPPORTUNITY_ID) as INSTANCE_OPPORTUNITY_ID 
from DA_OPPORTUNITY t1""")
log += "\n" + replaceTable("DA_OPPORTUNITY", "TEMP_DA_OPPORTUNITY")

#DA_CUSTOMER: Map the country information from DAS_COUNTRY as well as COUNTRY_CODE_MAPPING table
log += "\nDA_CUSTOMER:\n" + createTable("""create table TEMP_DA_CUSTOMER as 
select 
t1.CUSTOMER_KEY,
t1.ACCOUNT_ID,
t1.ACCOUNT_NAME,
t1.BLACKBERRY_COUNTRY,
t1.BLACKBERRY_REGION,
t1.SECTOR,
t1.BILLING_COUNTRY,
t1.INSTANCE,
t1.BILLING_COUNTRY_OVERRIDE,
t1.COUNTRY,
(case when t1.SUB_REGION = 'Unassigned' and t2.SUB_REGION is not null then t2.SUB_REGION else t1.SUB_REGION end) as SUB_REGION,
(case when t1.REGION = 'Unassigned' and t2.REGION is not null then t2.REGION else t1.REGION end) as REGION,
(case when t1.SECTOR in ('ADIG', 'U.S. Federal') then 'Federal' when t1.SECTOR is not null then 'Non - Federal' else 'Not Applicable' end) as SECTOR_GROUP
from 
(
    select
    t1.*,
    coalesce(t7.COUNTRY,t2.COUNTRY_OVERRIDE,t3.COUNTRY,t4.COUNTRY,t5.COUNTRY,t6.COUNTRY,t1.BLACKBERRY_COUNTRY,case when t1.CUSTOMER_KEY = 0 then 'Unknown' else 'Unassigned' end) as COUNTRY,
    coalesce(t2.SUB_REGION_OVERRIDE,t3.SUB_REGION,t4.SUB_REGION,t5.SUB_REGION,t6.SUB_REGION,case when t1.CUSTOMER_KEY = 0 then 'Unknown' else 'Unassigned' end) as SUB_REGION,
    coalesce(t2.REGION_OVERRIDE,t3.REGION,t4.REGION,t5.REGION,t6.REGION,case when t1.CUSTOMER_KEY = 0 then 'Unknown' else 'Unassigned' end) as REGION
    from DA_CUSTOMER t1
    left join DA_COUNTRY t2 on t1.BILLING_COUNTRY = t2.COUNTRY
    left join COUNTRY_CODE_MAPPING t3 on t2.COUNTRY = t3.COUNTRY
    left join COUNTRY_CODE_MAPPING t4 on t2.COUNTRY = t4.CODE_2
    left join COUNTRY_CODE_MAPPING t5 on t2.COUNTRY = t5.CODE_3
    left join COUNTRY_CODE_MAPPING t6 on t1.BLACKBERRY_COUNTRY = t6.COUNTRY
    left join COUNTRY_CODE_MAPPING t7 on t1.BILLING_COUNTRY_OVERRIDE = t7.COUNTRY
) t1
left join COUNTRY_CODE_MAPPING t2 on t1.COUNTRY = t2.COUNTRY""")
log += "\n" + replaceTable("DA_CUSTOMER", "TEMP_DA_CUSTOMER")

