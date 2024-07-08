# Databricks notebook source
spark

# COMMAND ----------

product_data = [(1,"Laptop",900), (2,"Smartphone",500), (3,"Tablet",300)]
updated_data = [(2,"Smartphone",80), (4,"Headphones",550)]
col = ["product_id","product_name","price"]
spark.createDataFrame(product_data, col).write.format("delta").mode("overwrite").saveAsTable("demo.product")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.product;

# COMMAND ----------

from delta.tables import DeltaTable
DeltaTable.isDeltaTable(spark,"demo/product")

# COMMAND ----------

spark.table("demo.product").show()

# COMMAND ----------


