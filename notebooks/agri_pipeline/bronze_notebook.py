# Databricks notebook source
# Configure storage access
spark.conf.set(
  "fs.azure.account.key.dkstorageaccnt.blob.core.windows.net",
  "XXX"
)

base_path = "wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/"
raw_path = base_path + "raw/crop_data.csv"
bronze_path = base_path + "bronze/crop_data"

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(raw_path)

df_raw.write.format("delta") \
    .mode("overwrite") \
    .save(bronze_path)

print("Bronze layer completed")

# COMMAND ----------

display(
    spark.read.format("delta")
    .load("wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/bronze/crop_data")
)