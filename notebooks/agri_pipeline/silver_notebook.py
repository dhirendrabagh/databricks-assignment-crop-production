# Databricks notebook source
from pyspark.sql.functions import col

spark.conf.set(
  "fs.azure.account.key.dkstorageaccnt.blob.core.windows.net",
  "XXX"
)

base_path = "wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/"
bronze_path = base_path + "bronze/crop_data"
silver_path = base_path + "silver/crop_data"

df_bronze = spark.read.format("delta").load(bronze_path)

df_silver = df_bronze.dropDuplicates() \
    .withColumn("Yield", col("Production") / col("Area"))

df_silver.write.format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print("Silver layer completed")