# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.dkstorageaccnt.blob.core.windows.net",
  "XXX"
)

base_path = "wasbs://crop-data@dkstorageaccnt.blob.core.windows.net/"
silver_path = base_path + "silver/crop_data"
gold_base = base_path + "gold/"

df_silver = spark.read.format("delta").load(silver_path)

# Crop Trend
crop_trend = df_silver.groupBy("Year", "Crop") \
    .sum("Production") \
    .withColumnRenamed("sum(Production)", "Total_Production")

crop_trend.write.format("delta") \
    .mode("overwrite") \
    .save(gold_base + "crop_trend")

# Seasonal Performance
seasonal_perf = df_silver.groupBy("Season") \
    .avg("Yield") \
    .withColumnRenamed("avg(Yield)", "Avg_Yield")

seasonal_perf.write.format("delta") \
    .mode("overwrite") \
    .save(gold_base + "seasonal_performance")

# Region Ranking
region_perf = df_silver.groupBy("State") \
    .avg("Yield") \
    .withColumnRenamed("avg(Yield)", "Avg_Yield")

region_perf.write.format("delta") \
    .mode("overwrite") \
    .save(gold_base + "region_performance")

print("Gold layer completed")