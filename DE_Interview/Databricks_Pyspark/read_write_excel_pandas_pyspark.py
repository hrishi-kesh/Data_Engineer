# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/') #to see folder location

# COMMAND ----------

import os
from shutil import copyfile

# Copy from DBFS to a local path
local_path = '/tmp/read_excel.xlsx'
dbutils.fs.cp('dbfs:/FileStore/tables/read_excel.xlsx', 'file:/tmp/read_excel.xlsx')

# Check if the file exists
if os.path.exists(local_path):
    print("File copied successfully.")
    df = pd.read_excel(local_path, engine='openpyxl')
    print(df.head())
else:
    print("Failed to copy the file.")




# COMMAND ----------

#Directly we can't read excel file in pyspark so 1st read through pandas then covert into pyspark
import pandas as pd
local_path = '/tmp/read_excel.xlsx'
df_pandas1=pd.read_excel(local_path, engine='openpyxl',sheet_name='data_file1')
df_pandas2=pd.read_excel(local_path,engine='openpyxl',sheet_name='data_file2')

df1=spark.createDataFrame(df_pandas1)
df2=spark.createDataFrame(df_pandas2)

display(df1) #this will not work in jupyter
#df1.head()
df1.write.format('delta').save('/dbfs/FileStore/tables/output/data_file1')
df2.write.format('delta').save('/dbfs/FileStore/tables/output/data_file2')

# COMMAND ----------

dbutils.fs.ls('/dbfs/FileStore/tables/output/')
