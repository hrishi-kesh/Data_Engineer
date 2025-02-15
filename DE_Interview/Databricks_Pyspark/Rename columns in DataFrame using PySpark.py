# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

from pyspark.sql.functions import *
# File location and type
file_location = "/FileStore/tables/sample_student.csv"
file_type = "csv"

# CSV options
infer_schema = "true" # if false it will give all schema to string by default
first_row_is_header = "true" #if false then it will auto add header
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#df=df.withColumnRenamed("ID","ID_New").withColumnRenamed("Name","Name_New").withColumnRenamed("Age","Age_New")
#df2=df.selectExpr("ID as ID_New","Name as Name_New","Age as Age_New")
df2=df.select(col("ID").alias("ID_New"),col("Name").alias("Name_New"),col("Age").alias("Age_New"))
df2.show()
display(df) #tabular format
df.show() #dataframe format
#df.collect()
df.take(2) #showing two records
df.printSchema()


