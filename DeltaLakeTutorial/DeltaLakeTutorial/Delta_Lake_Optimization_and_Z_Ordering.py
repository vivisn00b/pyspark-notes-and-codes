# Databricks notebook source
# Dataset
display(dbutils.fs.ls("/databricks-datasets/definitive-guide/data/retail-data/all/"))

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/definitive-guide/data/retail-data/all/online-retail-dataset.csv

# COMMAND ----------

# Write the data in form of delta table
df = spark.read.csv(path = "dbfs:/databricks-datasets/definitive-guide/data/retail-data/all/online-retail-dataset.csv", inferSchema = True, header = True)

# COMMAND ----------

df.repartition(16).write.format("delta").mode("overwrite").partitionBy("country").saveAsTable("sales_delta_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales_delta_partitioned;

# COMMAND ----------

df = spark.table("workspace.default.sales_delta_partitioned")
display(df)

# COMMAND ----------

display(dbutils.fs.ls("workspace/default/sales_delta_partitioned/Country=Australia/"))

# COMMAND ----------

spark.sql("""
DESCRIBE DETAIL workspace.default.sales_delta_partitioned
""").select("location").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS workspace.default.sales_delta_partitioned;

# COMMAND ----------

display(
    spark.table("workspace.default.sales_delta_partitioned")
         .filter("Country = 'Australia'")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sales_delta_partitioned where country = 'Australia' ZORDER BY (InvoiceNo)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select min(invoiceno), max(invoiceno), _metadata.file_name from sales_delta_partitioned
# MAGIC group by _metadata.file_name
# MAGIC order by min(invoiceno)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sales_delta_partitioned where country = 'Australia' ZORDER BY (Country, InvoiceNo)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select country, min(invoiceno), max(invoiceno), _metadata.file_name from sales_delta_partitioned
# MAGIC group by country, _metadata.file_name
# MAGIC order by country, min(invoiceno)

# COMMAND ----------

