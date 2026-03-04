# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.dkstorageaccnt.blob.core.windows.net",
  "XXX"
)



# COMMAND ----------

display(dbutils.fs.ls("wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/bronze/crop_data"))

# COMMAND ----------

spark.read.format("delta") \
  .load("wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/bronze/crop_data") \
  .count()

# COMMAND ----------

display(dbutils.fs.ls("wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/silver/crop_data"))

# COMMAND ----------

display(dbutils.fs.ls("wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/gold/crop_trend"))