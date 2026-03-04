-- Databricks notebook source

-- COMMAND ----------

SELECT Year, Crop, Total_Production
FROM crop_trend
ORDER BY Year

-- COMMAND ----------

SELECT Season, Avg_Yield
FROM seasonal_performance

-- COMMAND ----------

SELECT State, Avg_Yield
FROM region_performance
ORDER BY Avg_Yield DESC