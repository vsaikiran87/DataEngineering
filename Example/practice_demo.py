# Databricks notebook source
from pyspark.sql import Row
data1 = [

    Row(id=1, name='John', age=30),

    Row(id=2, name='Alice', age=25),

    Row(id=3, name='Bob', age=35)

]
 
# Sample data for the second DataFrame

data2 = [

   Row(id=7, name='krishna'),

    Row(id=8, name='prithvi'),

]

# How to create Dataframe and merge these 2 
df1 = spark.createDataFrame(data1)
df2 = spark.createDataFrame(data2)

# COMMAND ----------

from pyspark.sql.functions import lit
df2_new = df2.withColumn("age",lit(None))
df2_new.show()

# COMMAND ----------

merged_df = df1.union(df2_new)
merged_df.show()

# COMMAND ----------

# How to select the top second salary of department from below data?
data=[[1,'Charles','A',1000],[2,'Richard','A',2000],[3,'John','A',2000],[4,'Alisha','B',400],[5,'Robin','B',500],
[6,'Kara','C',700],[7,'Natalie','D',900],[8,'Harry','C',600],[9,'Charles','D',500],[10,'Kate','A',1000]]

col=['Id','Name','Dept','Salary']

emp_df = spark.createDataFrame(data,col)
emp_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number,dense_rank
windowSpec = Window.partitionBy("Dept").orderBy(col("Salary").desc())
rank_df = emp_df.withColumn("row",dense_rank().over(windowSpec))
rank_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

final_df = rank_df.filter(col("row") == 2)

# COMMAND ----------

final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --sql logic for top 2nd salaried employees by dept
# MAGIC select * from (
# MAGIC select *, dense_rank() over(partition by Dept order by Salary desc) as rank_by_salary from demo.employee)
# MAGIC where rank_by_salary = 2 ;

# COMMAND ----------

# What if I want my top second salary of all departments combined/the whole data set?
from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number,dense_rank
windowSpec = Window.orderBy(col("Salary").desc())
rank_df = emp_df.withColumn("row",dense_rank().over(windowSpec))
rank_df.show()

# COMMAND ----------

final_df = rank_df.filter(col("row") == 2)
final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --sql logic for top 2nd salaried employees on whole data
# MAGIC select * from (
# MAGIC select *, dense_rank() over(order by Salary desc) as rank_by_salary from demo.employee)
# MAGIC where rank_by_salary = 2 ;

# COMMAND ----------

spark.sql("create database if not exists demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# partitioning
emp_df.write.format("delta").partitionBy("Dept").save("dbfs:/user/hive/warehouse/default/employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.employee;

# COMMAND ----------



# COMMAND ----------


