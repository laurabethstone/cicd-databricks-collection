# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC #CONNECTION ESTABLISHMENT TO SHAREPOINT
# MAGIC
# MAGIC * Sharepoint - Organizations use Microsoft SharePoint to create websites. You can use it as a secure place to store, organize, share, and access information from any device. All you need is a web browser, such as Microsoft Edge, Internet Explorer, Chrome, or Firefox.
# MAGIC
# MAGIC This notebook is to explain process 
# MAGIC * To establish sharepoint connection from databricks 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Pre-requisities
# MAGIC
# MAGIC * Register Raw Dataset in EDL catalog --> https://confluence.deere.com/display/EDAP/Registering+Datasets
# MAGIC
# MAGIC * These below three details are required to proceed further, these below details should be passed as parameters in above databricks widgets.
# MAGIC   * client ID 
# MAGIC   * client Secret
# MAGIC   * app ID

# COMMAND ----------

dbutils.widgets.text('Client_ID', '', 'Client ID')
dbutils.widgets.text('Client_Secret', '', 'Client Secret')
dbutils.widgets.text('App_ID', '', 'App ID')
dbutils.widgets.text('mySite_Name', '', 'Site Name')
dbutils.widgets.text('myList_Name', '', 'List Name')


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step - 1
# MAGIC
# MAGIC * Import requests, json, pandas libraries
# MAGIC * create function "requestSPToken" which takes client ID, Client secret, app ID as inputs and returns sharepoint access token
# MAGIC * create function getListItems" which takes sharepoint access token or bearer token , site name, list name as inputs and returns data in dictionary format

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> 
# MAGIC * AppId (generated at SharePoint) is Tanent ID
# MAGIC * Client ID & clientSecret is also generated from SharePoint app registration section
# MAGIC * Principal Credential for SharePoint = “00000003-0000-0ff1-ce00-000000000000"

# COMMAND ----------

# python libraries
import requests
import json
import pandas as pd

def requestSPToken(clientID, clientSecret, appID):
  # The following parameters of the payload are defined by Sharepoint using the appID and clientID of the specific Sharepoint add in app
  url = "https://accounts.accesscontrol.windows.net/"+ appID + "/tokens/OAuth/2"
  requestClientID = clientID + '@' + appID
  resource = "00000003-0000-0ff1-ce00-000000000000" + "/deere.sharepoint.com@" + appID
 #Payload and headers of the request
  payload = {'grant_type': 'client_credentials',
             'client_id': requestClientID,
             'client_secret' : clientSecret,
             'resource': resource}
  headers = {'Content-Type': 'application/x-www-form-urlencoded'}
  # Get a token from the sharepoint site
  response = requests.request("GET", url, headers=headers, data=payload)
  if response.status_code == 400 :
    raise Exception("Bad Request: provided credentials are incorrect")
  else :
    responseJson = json.loads(response.text)
    if "access_token" in responseJson:
      return(responseJson["access_token"])
    else:
      return(None)
  
def getListItems(bearerToken, siteName, listName):
  url = "https://deere.sharepoint.com/sites/" + siteName + "/_api/web/lists/getbytitle('" + listName + "')/items?$top=10000"
  payload = {}
  headers = {'Authorization': 'Bearer ' + bearerToken,
            'Accept': 'application/json;odata=nometadata'}
  response = requests.request("GET", url, headers=headers, data=payload)
  if response.status_code == 404 :
    raise Exception("No such List Present with name :" + str(listName) +" in sharepoint Site Name "+ str(siteName))
  else :
    responseJson = json.loads(response.text)
    #print(responseJson)
    if "value" in responseJson:
      return(responseJson["value"])
    else:
      return(None)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step - 2
# MAGIC
# MAGIC * call the function "requestSPToken" to capture access token
# MAGIC * call the function "getListItems" to capture data in dictionary format
# MAGIC * convert the result dictionary format data into pandas dataframe or spark dataframe.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> 
# MAGIC
# MAGIC * Client ID & clientSecret is also generated from SharePoint app registration section
# MAGIC * AppId (generated at SharePoint) is Tanent ID
# MAGIC * site name is the site that we are getting connected to sharepoint
# MAGIC * listname is the list that we need to ingest into edl

# COMMAND ----------

Client_ID = dbutils.widgets.get('Client_ID')
Client_Secret = dbutils.widgets.get('Client_Secret')
App_ID = dbutils.widgets.get('App_ID')
mySite_Name = dbutils.widgets.get('mySite_Name')
myList_Name = dbutils.widgets.get('myList_Name')

try:
  token = requestSPToken(Client_ID, Client_Secret, App_ID)
  listItems = getListItems(token, mySite_Name, myList_Name)
  
except Exception as ex:
  print("Exception Raised "+str(ex))
  
# df = pd.DataFrame.from_dict(listItems)
# print(df)
