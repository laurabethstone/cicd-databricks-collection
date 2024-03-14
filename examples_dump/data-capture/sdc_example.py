# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from dateutil.relativedelta import relativedelta
from delta.tables import *
from pyspark.sql import SQLContext

# COMMAND ----------

# DBTITLE 1,table schema will be used for delta table
df_data = [
             (123,'UK,LA','2019-09-09','null'),
		 ]
df=spark.createDataFrame(df_data,['Customer_id', 'Customer_loc', 'Customer_effective_Date', 'Customer_expiration_Date'])

display(df)

# COMMAND ----------

# DBTITLE 1,update dataframe
df_update_data = [
             (123,'US,Las Vegas','2020-09-29','null'),(124,'US,New York City','2020-09-17','null')
		 ]
df_update=spark.createDataFrame(df_update_data,['Customer_id', 'Customer_loc', 'Customer_effective_Date', 'Customer_expiration_Date'])

display(df_update)

# COMMAND ----------

# DBTITLE 1,writing df in cmd1 as delta table
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("dbfs:/mnt/Customer_data_new")
display(spark.sql("DROP TABLE IF EXISTS Customer_data_py"))
#display(spark.sql("TRUNCATE TABLE Customer_data_final"))
display(spark.sql("CREATE TABLE Customer_data_py USING DELTA LOCATION 'dbfs:/mnt/Customer_data_new'"))

# COMMAND ----------

# DBTITLE 1,Merge typ2 operation delta table with update df
customersTable = DeltaTable.forPath(spark,"dbfs:/mnt/Customer_data_new/")  # DeltaTable with schema (customerId, address, current, effectiveDate, endDate)

updatesDF =df_update       # DataFrame with schema (customerId, address, effectiveDate)

# Rows to INSERT new addresses of existing customers
newAddressesToInsert = updatesDF \
  .alias("updates") \
  .join(customersTable.toDF().alias("customers"), "Customer_id") \
  .where("updates.Customer_loc <> customers.Customer_loc") \
  .where("customers.Customer_expiration_date is null")

# Stage the update by unioning two sets of rows
# 1. Rows that will be inserted in the whenNotMatched clause
# 2. Rows that will either update the current addresses of existing customers or insert the new addresses of new customers
stagedUpdates = (
  newAddressesToInsert
  .selectExpr("NULL as mergeKey", "updates.*")   # Rows for 1
  .union(updatesDF.selectExpr("Customer_id as mergeKey", "*"))  # Rows for 2.
)

# Apply SCD Type 2 operation using merge
customersTable.alias("customers").merge(
  stagedUpdates.alias("staged_updates"),
  "customers.Customer_id = mergeKey") \
.whenMatchedUpdate(
  condition = "customers.Customer_loc <> staged_updates.Customer_loc",
  set = {                                      # Set current to false and endDate to source's effective date.
    "Customer_expiration_Date": "staged_updates.Customer_effective_Date"
  }
).whenNotMatchedInsert(
  values = {
    "Customer_id": "staged_updates.Customer_id",
    "Customer_loc": "staged_updates.Customer_loc",
    "Customer_effective_Date": "staged_updates.Customer_effective_Date",  # Set current to true along with the new address and its effective date.
    "Customer_expiration_Date": "null"
  }
).execute()


# COMMAND ----------

# MAGIC %sql select * from  Customer_data_py

# COMMAND ----------

