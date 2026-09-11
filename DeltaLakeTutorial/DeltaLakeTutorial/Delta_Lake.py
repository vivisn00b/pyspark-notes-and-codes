# Databricks notebook source
# Spark Session
spark

# COMMAND ----------

# Default catalog for Databricks
spark.sql("SHOW CATALOGS").show(truncate=False)
spark.sql("SELECT current_catalog()").show()

# COMMAND ----------

print(spark.catalog.currentDatabase())

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

display(dbutils.fs.ls("file:/Workspace/DeltaLakeTutorial/data/input"))

# COMMAND ----------

df_sales = spark.read.parquet("file:/Workspace/DeltaLakeTutorial/data/input/sales_data.parquet")

# COMMAND ----------

df_sales.show(df_sales.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

# Write data as managed table (delta)
df_sales.write.mode("overwrite").mode("overwrite").saveAsTable("sales_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in default

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended sales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC update default.sales_delta set amount = 0 where trx_id = '1734117021'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.sales_delta

# COMMAND ----------

# Read a particular version - pyspark api
df_sales_delta = spark.read.table("sales_delta@v1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_delta@v0 where trx_id = '1734117021'

# COMMAND ----------

df_new = spark.sql("select *, current_timestamp() as time_now from sales_delta@v0 where trx_id = '1734117021'")

display(df_new)

# COMMAND ----------

# Append data to existing delta table
df_new.write.format("delta").mode("append").option("mergeSchema", True).saveAsTable("sales_delta")

# COMMAND ----------

spark.sql("""
SELECT *
FROM sales_delta
ORDER BY time_now DESC
LIMIT 20
""").show()

# COMMAND ----------

# Reading Delta Table using Delta libraries
from delta import DeltaTable

dt = DeltaTable.forName(spark, "sales_delta")

display(dt.history())

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE sales_delta TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sales_delta where trx_id = '1734117021'

# COMMAND ----------

dt = DeltaTable.forName(spark, "sales_delta")

dt.vacuum(1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sales_delta@v2 where trx_id = '1734117021'

# COMMAND ----------

spark.sql("DESCRIBE HISTORY sales_delta").show(truncate=False)

# COMMAND ----------

