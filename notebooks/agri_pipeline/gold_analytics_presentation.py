# Databricks notebook source

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS crop_trend
USING DELTA
LOCATION 'wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/gold/crop_trend'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS seasonal_performance
USING DELTA
LOCATION 'wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/gold/seasonal_performance'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS region_performance
USING DELTA
LOCATION 'wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/gold/region_performance'
""")

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

display(spark.sql("SELECT * FROM crop_trend LIMIT 10"))

# COMMAND ----------

display(spark.sql("SELECT * FROM seasonal_performance"))

# COMMAND ----------

display(spark.sql("SELECT * FROM region_performance ORDER BY Avg_Yield DESC"))