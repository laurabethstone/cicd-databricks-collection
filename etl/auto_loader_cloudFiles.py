# Databricks notebook source
# MAGIC %md # Auto Loader
# MAGIC
# MAGIC Auto Loader is an optimized cloud file source for **Apache Spark** that loads data continuously and efficeintly from storage as new data arrives.

# COMMAND ----------

# MAGIC %md  ### Essentially Auot Loader Combines Three Approches Of :-
# MAGIC
# MAGIC * Storing **`Metadata`** about what has been read
# MAGIC * Using **`Structured Streaming`** for immediate processing
# MAGIC * Utilizing **`Cloud-Native Components`** to optimize identifying the arriving files

# COMMAND ----------

# MAGIC %md ### There are two parts to the Auto Loader job
# MAGIC
# MAGIC * **CloudFile DataReader**
# MAGIC * **CloudNotification Services**(Optional)

# COMMAND ----------

# MAGIC %md #### CloudFile DataReader
# MAGIC
# MAGIC * We can specify the schema of input data.
# MAGIC * Reading of CloudFile is same as Reading Streaming data.
# MAGIC * **`format("cloudFiles")`** Tells spark to use Auto Loader.
# MAGIC * **`cloudFiles.format`** Tells Auto Loader to expect specified type of files

# COMMAND ----------

# MAGIC %md #### CloudNotification
# MAGIC
# MAGIC * Uses AWS SNS and SQS services that subscribe to file events from the input directory. Auto Loader automatically sets up the AWS SNS and SQS services. File notification mode is more performant and scalable for large input directories. To use this mode, you must configure permissions for the AWS SNS and SQS services and specify .option("cloudFiles.useNotifications","true").

# COMMAND ----------

dbutils.widgets.text('input_path', 's3://aws-ae-edl-ingest-devl-portfolio-sharepoint/Auto_Loader', 'Input_Path')
dbutils.widgets.text('output_path', '/mnt/edl/raw/dq_demo', 'Mount_Location')
dbutils.widgets.text('checpoint_path', '/mnt/edl/raw/dq_demo/check_point', 'CheckPoint_Path')
dbutils.widgets.text('file_type', 'csv', 'Ingest_File_Type')
dbutils.widgets.text('max_file_per_trigger', '1', 'MaxFilePerTrigger')
dbutils.widgets.text('processing_time', '1', 'ProcessingTime_PerFile')

# COMMAND ----------

bucket_path = dbutils.widgets.get('input_path')
output_path = dbutils.widgets.get('output_path')
checkpint_path = dbutils.widgets.get('checpoint_path')
file_type = dbutils.widgets.get('file_type')
max_file_per_trigger = dbutils.widgets.get('max_file_per_trigger')
processing_time = dbutils.widgets.get('processing_time')

# COMMAND ----------

# Importing required modules 
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# COMMAND ----------

#Defining the Schema Externally for  incoming File

myschema = StructType([StructField('Row_ID',StringType(), True)\
,StructField('Order_ID',StringType(), False)\
,StructField('Order_Date',StringType(), True)\
,StructField('Ship_Date',StringType(), True)\
,StructField('Ship_Mode',StringType(), True)\
,StructField('Customer_ID',StringType(), True)\
,StructField('Customer_Name',StringType(), True)\
,StructField('Segment',StringType(), True)\
,StructField('City',StringType(), True)\
,StructField('State',StringType(), True)\
,StructField('Country',StringType(), True)\
,StructField('Postal_Code',StringType(), True)\
,StructField('Market',StringType(), True)\
,StructField('Region',StringType(), True)\
,StructField('Product_ID',StringType(), True)\
,StructField('Category',StringType(), True)\
,StructField('Sub-Category',StringType(), True)\
,StructField('Product_Name',StringType(), True)\
,StructField('Sales',StringType(), True)\
,StructField('Quantity',StringType(), True)\
,StructField('Discount',StringType(), True)\
,StructField('Profit',StringType(), True)\
,StructField('Shipping_Cost',StringType(), True)\
,StructField('Order_Priority',StringType(), True)])

# COMMAND ----------

# Specifying the S3 bucket path.When ever the new file arrives in this dirctory stream reads the new file data.
#bucket_path = "s3://aws-ae-edl-ingest-devl-portfolio-sharepoint/Auto_Loader/"

# COMMAND ----------

# Specifying the output location and checkpoint location for streaming data
#output_path = "/mnt/edl/raw/dq_demo"
#checkpint_path = "/mnt/edl/raw/dq_demo/check_point"

# COMMAND ----------

# MAGIC %md Reading the newly arrived file using spark stream 
# MAGIC * CloudFile DataReader reads the data from location specified in **`load`** option
# MAGIC * If User wants to read data from S3 location make sure policy has been attached to the bucket to read data using databricks with same AD group

# COMMAND ----------

df = ( spark.readStream.format("cloudFiles")
      .option("cloudFiles.format",file_type)
      .option("maxFilesPerTrigger",max_file_per_trigger) #  Treat a sequence of files as a stream by picking one file at a time
      .option("header","true")
      .schema(myschema) # mandatory for incoming streeaming data 
      .load(bucket_path))

# COMMAND ----------

#Displaying the Stream 
display(df)

# COMMAND ----------

# Writing the Stream data to final location
write_stream = (df.writeStream.format("delta")
  .outputMode("append")
  .queryName("sample_auto_loder")
  .option("checkpointLocation", checkpint_path) 
  .trigger(processingTime = str(processing_time)+" "+ "seconds")
  .start(output_path) )

# COMMAND ----------

#Querying the destination location data 

df_final = spark.read.format('delta').load(output_path)
df_final.count()

# COMMAND ----------


