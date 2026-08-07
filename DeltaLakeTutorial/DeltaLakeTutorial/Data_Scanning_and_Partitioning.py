# Databricks notebook source
# Demo dataset from Databricks
display(dbutils.fs.ls("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/"))

# COMMAND ----------

# Check the count and verify data scanning
df_delta = spark.read.format("delta").load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/")
display(df_delta)

# COMMAND ----------

# Get row count
row_count = df_delta.count()
print(f"Row count: {row_count}")

# COMMAND ----------

# Check filter data
# select count(1) from nyctaxi where vendor_id = 'VTS' and trip_distance > 1.8

df_delta.where("vendor_id = 'VTS' and trip_distance > 1.8").count()

# COMMAND ----------

# Write the data in partitioned format
df_delta.write.format("delta").mode("overwrite").partitionBy("vendor_id").option("path", "/Workspace/DeltaLakeTutorial/data/input/nyctaxi/partitioned/").saveAsTable("nyctaxi_partitioned")

# COMMAND ----------

df_partitioned = spark.read.parquet("/data/input/nyctaxi/partitioned/")

df_partitioned.where("vendor_id = 'VTS' and trip_distance > 1.8").count()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from nyctaxi_partitioned where vendor_id = 'VTS' and trip_distance > 1.8